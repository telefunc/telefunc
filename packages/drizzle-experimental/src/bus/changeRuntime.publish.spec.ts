// PUBLISHING a committed batch. One commit is ONE message on ONE topic per logical database; a subscriber
// decodes it and feeds the whole batch into its graphs via `router.ingest`, and the router slices it per
// table.
//
// What is NOT here any more, and deliberately: cross-topic dedupe, batch-id retention under load, mutable
// subscription ownership, wall-clock jumps, marker sweeping, and delayed second-topic delivery. Every one of
// those proved a failure the per-table fan-out created for itself. Publishing once deletes the failures and
// the proofs together.
//
// Everything here crosses the CODEC boundary, because the in-process default encodes like any other
// transport: what these tests exercise is the same path a Redis adapter runs. That includes the two ways
// encoding can refuse — an unreadable payload arriving, and an unencodable batch leaving — which fail to
// COARSE rather than to silence, and the two ways a transport can fail a publish, which never reach the
// caller's committed write.

import { beforeEach, describe, expect, it, vi } from 'vitest'
vi.mock('./dbRuntime.js', async () => (await import('./changeRuntime.registryMock.js')).dbRuntimeMock())

import type { ChangeTransport } from './changeTransport.js'
import { registryFor } from './dbRuntime.js'
import {
  acquireSubscription,
  changeTopicFor,
  publishBatch,
  publishCoarseAll,
  setChangeTransport,
} from './changeRuntime.js'
import {
  change,
  engine,
  flush,
  freshDb,
  resetEngine,
  twoInstances,
  watching,
  wireTap,
} from './changeRuntime.testKit.js'
import type { TableChange } from './router/events.js'

beforeEach(resetEngine)

describe('write transport — one topic, one publication per batch', () => {
  it('a MULTI-TABLE batch is published once and ingested once, whole', async () => {
    // The atomicity property, now direct rather than reconstructed: one commit is one message is one tick.
    // Under per-table fan-out this needed k copies plus an apply-once rule to cancel k-1 of them.
    const { transport, dbA } = await twoInstances()
    const changes = [change('users'), change('posts')]
    const wire = await wireTap(transport, dbA)

    publishBatch(dbA, { changes })
    await flush()
    expect(wire).toHaveLength(1) // ONE message on the wire for a two-table commit
    expect(engine.ingest).toHaveBeenCalledTimes(1) // ...and one ingest on the OTHER instance
    expect(engine.ingest).toHaveBeenCalledWith({ changes }) // carrying the whole batch — the router slices it
  })

  it('a LOCAL write is NOT double-applied: its own echo is dropped by origin', async () => {
    const { db } = await freshDb()
    // The batch carries the publishing db's `origin`, so its own subscription drops the round-trip; its
    // graphs were already fed directly in ingestWrite.
    publishBatch(db, { changes: [change('users')] })
    await flush()
    expect(engine.ingest).not.toHaveBeenCalled()
  })

  it('an empty batch publishes nothing', async () => {
    const { db, transport } = await freshDb()
    const wire = await wireTap(transport, db)
    publishBatch(db, { changes: [] })
    await flush()
    expect(wire).toEqual([])
  })

  it('TWO dbs sharing one transport: A publishes → B applies, A suppresses its own echo', async () => {
    const { dbA } = await twoInstances()
    const changes = [change('users')]
    publishBatch(dbA, { changes })
    await flush()
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
    await flush()

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
    await flush()
    expect(engine.ingest).toHaveBeenCalledTimes(1)
    expect(engine.ingest).toHaveBeenCalledWith({ changes: [{ table: 'ledger', kind: 'coarse' }] })
  })

  it('a db does NOT coarsen itself from its own announcement (origin self-suppression)', async () => {
    const { db } = await freshDb()
    registryFor(db).router.register(watching('users'))
    publishCoarseAll(db) // its own graphs were already fed directly by the local coarse batch
    await flush()
    expect(engine.ingest).not.toHaveBeenCalled()
  })

  it('a coarse-all with nothing watched ingests nothing', async () => {
    const { dbA } = await twoInstances() // B has no registered graphs
    publishCoarseAll(dbA)
    await flush()
    expect(engine.ingest).not.toHaveBeenCalled()
  })
})

describe('write transport — a payload that cannot be trusted is never guessed at', () => {
  it('an UNDECODABLE payload coarsens the watched tables instead of reaching precise ingest', async () => {
    const { db, transport } = await freshDb()
    registryFor(db).router.register(watching('users'))
    const reported = vi.spyOn(console, 'error').mockImplementation(() => {})

    transport.publish(changeTopicFor(db), '{"version":999,"namespace":"ns","origin":"elsewhere","seq":1,"changes":[]}') // a future codec

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
    await flush()

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
    await acquireSubscription(db)
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
    await acquireSubscription(db)
    const reported = vi.spyOn(console, 'error').mockImplementation(() => {})

    expect(() => publishBatch(db, { changes: [change('users')] })).not.toThrow()
    await flush()

    expect(reported).toHaveBeenCalled()
    reported.mockRestore()
  })
})
