// Cross-instance write transport. A committed batch is published ONCE, to ONE topic per logical database; a
// subscriber decodes it and feeds the whole batch into its graphs via `router.ingest`, and the router slices
// it per table. Each test INJECTS its own in-process transport (setChangeTransport) so subscribers never
// leak across tests; registryFor is mocked so router.ingest is observable.
//
// What is NOT here any more, and deliberately: cross-topic dedupe, batch-id retention under load, mutable
// subscription ownership, wall-clock jumps, marker sweeping, and delayed second-topic delivery. Every one of
// those proved a failure the per-table fan-out created for itself. Publishing once deletes the failures and
// the proofs together.
//
// Everything here crosses the CODEC boundary, because the in-process default encodes like any other
// transport: what these tests exercise is the same path a Redis adapter runs.

import { beforeEach, describe, expect, it, vi } from 'vitest'
import type { TableChange } from '../router/events.js'

// The mocked registry is PER-DB and tracks registered graphs, so watchedTables() answers truthfully — the
// coarse-all path depends on it.
const engine = vi.hoisted(() => ({ ingest: vi.fn(), watched: new Map<object, string[]>() }))
vi.mock('./dbRuntime.js', () => ({
  registryFor: (db: object) => ({
    router: {
      ingest: engine.ingest,
      register: (graph: { tables: string[] }) =>
        engine.watched.set(db, [...(engine.watched.get(db) ?? []), ...graph.tables]),
      unregister: () => engine.watched.delete(db),
      watchedTables: () => engine.watched.get(db) ?? [],
    },
    acquire: vi.fn(),
  }),
  ingestWrite: vi.fn(),
}))

import { type ChangeTransport, createInMemoryChangeTransport, setChangeTransport } from './changeTransport.js'
import { registryFor } from './dbRuntime.js'
import { CHANGE_TOPIC, ensureSubscribed, publishBatch, publishCoarseAll } from './writeTransport.js'

/** A minimal registered graph — only its `tables` matter to watchedTables(). */
const watching = (table: string) => ({ tables: [table] }) as never

const change = (table: string): TableChange => ({ table, kind: 'insert', new: { id: 1 } })

/** A fresh db + its own injected transport per test → no cross-test subscriber leakage. */
function freshDb() {
  const db = {}
  const transport = createInMemoryChangeTransport()
  setChangeTransport(db, transport)
  return { db, transport }
}

/** Two dbs on ONE transport — the multi-instance path in a single process. */
async function twoInstances() {
  const transport = createInMemoryChangeTransport()
  const dbA = {}
  const dbB = {}
  setChangeTransport(dbA, transport)
  setChangeTransport(dbB, transport)
  await ensureSubscribed(dbA)
  await ensureSubscribed(dbB)
  return { transport, dbA, dbB }
}

/** Everything that reached the wire, as raw payloads — what a broker would actually carry. */
async function wireTap(transport: ChangeTransport) {
  const seen: string[] = []
  await transport.subscribe(CHANGE_TOPIC, (payload) => seen.push(payload))
  return seen
}

beforeEach(() => {
  engine.ingest.mockClear()
  engine.watched.clear()
})

describe('write transport — one topic, one publication per batch', () => {
  it('a MULTI-TABLE batch is published once and ingested once, whole', async () => {
    // The atomicity property, now direct rather than reconstructed: one commit is one message is one tick.
    // Under per-table fan-out this needed k copies plus an apply-once rule to cancel k-1 of them.
    const { transport, dbA } = await twoInstances()
    const changes = [change('users'), change('posts')]
    const wire = await wireTap(transport)

    publishBatch(dbA, { changes })

    expect(wire).toHaveLength(1) // ONE message on the wire for a two-table commit
    expect(engine.ingest).toHaveBeenCalledTimes(1) // ...and one ingest on the OTHER instance
    expect(engine.ingest).toHaveBeenCalledWith({ changes }) // carrying the whole batch — the router slices it
  })

  it('a LOCAL write is NOT double-applied: its own echo is dropped by origin', async () => {
    const { db } = freshDb()
    await ensureSubscribed(db)
    // The batch carries the publishing db's `origin`, so its own subscription drops the round-trip; its
    // graphs were already fed directly in ingestWrite.
    publishBatch(db, { changes: [change('users')] })
    expect(engine.ingest).not.toHaveBeenCalled()
  })

  it('an empty batch publishes nothing', async () => {
    const { db, transport } = freshDb()
    await ensureSubscribed(db)
    const wire = await wireTap(transport)
    publishBatch(db, { changes: [] })
    expect(wire).toEqual([])
  })

  it('TWO dbs sharing one transport: A publishes → B applies, A suppresses its own echo', async () => {
    const { dbA } = await twoInstances()
    const changes = [change('users')]
    publishBatch(dbA, { changes })
    expect(engine.ingest).toHaveBeenCalledTimes(1) // ONLY B applied
    expect(engine.ingest).toHaveBeenCalledWith({ changes })
  })

  it('SQL values survive the wire: BigInt, Date, bytes and null reach the other instance with their types', async () => {
    // The reason the codec exists, proven where it is actually wired rather than in the codec's own unit
    // spec: these values cross a real publish→subscribe boundary between two runtimes.
    const { dbA } = await twoInstances()
    const row = {
      id: BigInt('9007199254740993'),
      at: new Date('2024-03-04T05:06:07.008Z'),
      blob: Uint8Array.from([1, 2, 250]),
      note: null,
    }
    publishBatch(dbA, { changes: [{ table: 'events', kind: 'insert', new: row }] })

    const [batch] = engine.ingest.mock.calls[0] as [{ changes: TableChange[] }]
    const received = (batch.changes[0] as unknown as { new: typeof row }).new
    expect(received.id).toBe(BigInt('9007199254740993'))
    expect(received.at).toBeInstanceOf(Date)
    expect(received.at.toISOString()).toBe('2024-03-04T05:06:07.008Z')
    expect(received.blob).toBeInstanceOf(Uint8Array)
    expect([...received.blob]).toEqual([1, 2, 250])
    expect(received.note).toBeNull()
  })
})

describe('write transport — coarse-all rides the SAME topic', () => {
  it('reaches an instance watching a table the writer does NOT watch', async () => {
    // A ran raw SQL: its touched tables are unknowable, and B watches a table A has never heard of. This is
    // what the reserved wildcard channel used to be for; one topic carries it.
    const { dbA, dbB } = await twoInstances()
    registryFor(dbB).router.register(watching('ledger'))
    publishCoarseAll(dbA)
    expect(engine.ingest).toHaveBeenCalledTimes(1)
    expect(engine.ingest).toHaveBeenCalledWith({ changes: [{ table: 'ledger', kind: 'coarse' }] })
  })

  it('a db does NOT coarsen itself from its own announcement (origin self-suppression)', async () => {
    const { db } = freshDb()
    await ensureSubscribed(db)
    registryFor(db).router.register(watching('users'))
    publishCoarseAll(db) // its own graphs were already fed directly by the local coarse batch
    expect(engine.ingest).not.toHaveBeenCalled()
  })

  it('a coarse-all with nothing watched ingests nothing', async () => {
    const { dbA } = await twoInstances() // B has no registered graphs
    publishCoarseAll(dbA)
    expect(engine.ingest).not.toHaveBeenCalled()
  })
})

describe('write transport — a payload that cannot be trusted is never guessed at', () => {
  it('an UNDECODABLE payload coarsens the watched tables instead of reaching precise ingest', async () => {
    const { db, transport } = freshDb()
    await ensureSubscribed(db)
    registryFor(db).router.register(watching('users'))
    const reported = vi.spyOn(console, 'error').mockImplementation(() => {})

    transport.publish(CHANGE_TOPIC, '{"version":999,"origin":"elsewhere","changes":[]}') // a future codec

    expect(engine.ingest).toHaveBeenCalledWith({ changes: [{ table: 'users', kind: 'coarse' }] })
    expect(reported).toHaveBeenCalled() // and it is reported, not silently absorbed
    reported.mockRestore()
  })

  it('a batch the codec cannot ENCODE is published as coarse-all rather than dropped', async () => {
    const { dbA, dbB } = await twoInstances()
    registryFor(dbB).router.register(watching('users'))
    const reported = vi.spyOn(console, 'error').mockImplementation(() => {})
    const circular: Record<string, unknown> = { id: 1 }
    circular.self = circular // defeats any serializer

    publishBatch(dbA, { changes: [{ table: 'users', kind: 'insert', new: circular }] })

    // The invalidation still crossed — value-free, so B refetches instead of never hearing about the write.
    expect(engine.ingest).toHaveBeenCalledWith({ changes: [{ table: 'users', kind: 'coarse' }] })
    expect(reported).toHaveBeenCalled()
    reported.mockRestore()
  })
})

describe('write transport — an async publish failure never reaches the committed write', () => {
  it('reports a REJECTED publish without rejecting the caller and without an unhandled rejection', async () => {
    const db = {}
    const rejecting: ChangeTransport = {
      publish: () => Promise.reject(new Error('redis: connection lost')),
      subscribe: async () => ({ unsubscribe() {} }),
    }
    setChangeTransport(db, rejecting)
    await ensureSubscribed(db)
    const reported = vi.spyOn(console, 'error').mockImplementation(() => {})
    const unhandled = vi.fn()
    process.on('unhandledRejection', unhandled)

    expect(() => publishBatch(db, { changes: [change('users')] })).not.toThrow() // the write is unaffected
    await new Promise((resolve) => setImmediate(resolve)) // let an unhandled rejection surface if it would
    await new Promise((resolve) => setImmediate(resolve))

    expect(reported).toHaveBeenCalled() // the operator hears about it
    expect(unhandled).not.toHaveBeenCalled()
    process.off('unhandledRejection', unhandled)
    reported.mockRestore()
  })

  it('reports a publish that THROWS synchronously, likewise', async () => {
    const db = {}
    const throwing: ChangeTransport = {
      publish: () => {
        throw new Error('redis: not connected')
      },
      subscribe: async () => ({ unsubscribe() {} }),
    }
    setChangeTransport(db, throwing)
    await ensureSubscribed(db)
    const reported = vi.spyOn(console, 'error').mockImplementation(() => {})

    expect(() => publishBatch(db, { changes: [change('users')] })).not.toThrow()

    expect(reported).toHaveBeenCalled()
    reported.mockRestore()
  })
})
