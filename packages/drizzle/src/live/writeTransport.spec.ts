// Cross-instance write transport (T4 slice 3, rehomed onto the injected changeTransport). A committed batch
// is published to each touched table's topic with a batch id; a subscriber feeds the WHOLE batch into its
// graphs via router.ingest, deduped by id so a batch spanning several of its topics applies ONCE and a local
// write's own round-trip is dropped. Each test INJECTS its own in-process transport (setChangeTransport), so
// subscribers never leak across tests; registryFor is mocked so router.ingest is observable.

import { beforeEach, describe, expect, it, vi } from 'vitest'
import type { TableChange } from '../router/events.js'

// The mocked registry is PER-DB and tracks registered graphs, so watchedTables() answers truthfully — the
// wildcard coarse-all path depends on it.
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

import { createInMemoryChangeTransport, setChangeTransport } from './changeTransport.js'
import { registryFor } from './dbRuntime.js'
import { batchTopic, ensureSubscribed, publishBatch, publishCoarseAll } from './writeTransport.js'

/** A minimal registered graph — only its `tables` matter to watchedTables(). */
const watching = (table: string) => ({ tables: [table] }) as never

const change = (table: string): TableChange => ({ table, kind: 'insert', new: { id: 1 } })
let counter = 0
const remoteId = () => `remote-${counter++}`

// A fresh db + its own injected transport per test → no cross-test subscriber leakage.
function freshDb() {
  const db = {}
  const transport = createInMemoryChangeTransport()
  setChangeTransport(db, transport)
  return { db, transport }
}

beforeEach(() => {
  engine.ingest.mockClear()
  engine.watched.clear()
})

describe('write transport — per-table topics + batch-ID dedupe', () => {
  it('a batch spanning TWO of a subscriber’s topics applies exactly ONCE (dedupe by id)', async () => {
    const { db, transport } = freshDb()
    const changes = [change('users'), change('posts')]
    await ensureSubscribed(db, ['users', 'posts'])
    // A REMOTE instance's publish: the same batch on each touched table's topic.
    const message = { origin: remoteId(), tables: ['users', 'posts'], changes }
    transport.publish(batchTopic('users'), message)
    transport.publish(batchTopic('posts'), message)
    expect(engine.ingest).toHaveBeenCalledTimes(1) // the second topic's delivery is deduped
    expect(engine.ingest).toHaveBeenCalledWith({ changes })
  })

  it('a single-topic subscriber receives the WHOLE batch (router slices per table)', async () => {
    const { db, transport } = freshDb()
    const changes = [change('users'), change('posts')]
    await ensureSubscribed(db, ['users']) // only subscribed to one of the two touched topics
    transport.publish(batchTopic('users'), { origin: remoteId(), tables: ['users', 'posts'], changes })
    expect(engine.ingest).toHaveBeenCalledTimes(1)
    expect(engine.ingest).toHaveBeenCalledWith({ changes }) // the whole batch, not just this table's slice
  })

  it('a LOCAL write is NOT double-applied: its own round-trip is deduped (fed directly, not via transport)', async () => {
    const { db } = freshDb()
    await ensureSubscribed(db, ['users'])
    // publishBatch pre-marks its id seen, then publishes — the local subscriber must drop the round-trip.
    publishBatch(db, { changes: [change('users')] })
    expect(engine.ingest).not.toHaveBeenCalled() // the local instance already fed its graphs directly
  })

  it('a genuinely remote batch (unseen id) IS applied', async () => {
    const { db, transport } = freshDb()
    const changes = [change('users')]
    await ensureSubscribed(db, ['users'])
    transport.publish(batchTopic('users'), { origin: remoteId(), tables: ['users'], changes })
    expect(engine.ingest).toHaveBeenCalledTimes(1)
    expect(engine.ingest).toHaveBeenCalledWith({ changes })
  })

  it('RAW-SQL coarse-all reaches an instance watching a table the writer does NOT watch (wildcard channel)', async () => {
    // The per-table topics can't carry this: A has no graph on `ledger`, so it publishes nothing on
    // __live__:ledger. Without the wildcard coarse channel, B's `ledger` query would never hear about A's
    // raw write — a missed invalidation on a remote instance.
    const transport = createInMemoryChangeTransport()
    const dbA = {}
    const dbB = {}
    setChangeTransport(dbA, transport)
    setChangeTransport(dbB, transport)
    await ensureSubscribed(dbA, ['users']) // A watches only `users`
    await ensureSubscribed(dbB, ['ledger']) // B watches only `ledger`
    registryFor(dbB).router.register(watching('ledger'))
    publishCoarseAll(dbA) // A ran raw SQL: touched tables unknowable
    expect(engine.ingest).toHaveBeenCalledTimes(1)
    expect(engine.ingest).toHaveBeenCalledWith({ changes: [{ table: 'ledger', kind: 'coarse' }] })
  })

  it('a db does NOT coarsen itself from its own raw-SQL announcement (origin self-suppression)', async () => {
    const { db, transport } = freshDb()
    void transport
    await ensureSubscribed(db, ['users'])
    registryFor(db).router.register(watching('users'))
    publishCoarseAll(db) // its own graphs were already fed directly by the local coarse batch
    expect(engine.ingest).not.toHaveBeenCalled()
  })

  it('cross-topic dedupe is DETERMINISTIC: an arbitrarily DELAYED second-topic copy still applies only once', async () => {
    // The old count-bounded id memory failed exactly here: once the id aged out, the delayed copy applied a
    // SECOND time (precise application is not idempotent). The origin+owning-table rule has no memory to age.
    const { db, transport } = freshDb()
    const changes = [change('users'), change('posts')]
    await ensureSubscribed(db, ['users', 'posts'])
    const message = { origin: remoteId(), tables: ['users', 'posts'], changes }
    transport.publish(batchTopic('users'), message) // owning table → applied
    for (let i = 0; i < 5000; i++) {
      transport.publish(batchTopic('users'), { origin: remoteId(), tables: ['unwatched'], changes: [] })
    }
    transport.publish(batchTopic('posts'), message) // delayed copy, far beyond any id window → still dropped
    expect(engine.ingest).toHaveBeenCalledTimes(1)
  })

  it('TWO dbs sharing one transport: A publishes → B applies, A suppresses its own round-trip (per-db dedupe)', async () => {
    // The multi-instance path in one process: dbA + dbB, distinct registries, one shared transport. A's
    // publish must reach B (a DIFFERENT db) while A drops its own echo. A GLOBAL seen-set would let A's
    // pre-mark suppress B too — this is the exact bug the per-db scoping fixes.
    const transport = createInMemoryChangeTransport()
    const dbA = {}
    const dbB = {}
    setChangeTransport(dbA, transport)
    setChangeTransport(dbB, transport)
    const changes = [change('users')]
    await ensureSubscribed(dbA, ['users']) // both instances hold a live read on the table
    await ensureSubscribed(dbB, ['users'])
    publishBatch(dbA, { changes }) // A commits a write (its own graphs were already fed directly)
    expect(engine.ingest).toHaveBeenCalledTimes(1) // ONLY B applied; A deduped its own round-trip
    expect(engine.ingest).toHaveBeenCalledWith({ changes })
  })
})
