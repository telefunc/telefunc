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
import { appliedMarkerCount, batchTopic, ensureSubscribed, publishBatch, publishCoarseAll } from './writeTransport.js'

/** Mirrors MAX_FANOUT_SKEW_MS — the marker window the sweep test advances past. */
const MARKER_WINDOW_MS = 30_000

/** A minimal registered graph — only its `tables` matter to watchedTables(). */
const watching = (table: string) => ({ tables: [table] }) as never

const change = (table: string): TableChange => ({ table, kind: 'insert', new: { id: 1 } })
let counter = 0
const remoteId = () => `remote-${counter++}`
const batchId = () => `batch-${counter++}`

/** One published batch: every copy carries the SAME id, which is what makes them duplicates. */
const remoteBatch = (changes: TableChange[]) => ({ id: batchId(), origin: remoteId(), changes })

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
    const message = remoteBatch(changes)
    transport.publish(batchTopic('users'), message)
    transport.publish(batchTopic('posts'), message)
    expect(engine.ingest).toHaveBeenCalledTimes(1) // the second topic's delivery is deduped
    expect(engine.ingest).toHaveBeenCalledWith({ changes })
  })

  it('a single-topic subscriber receives the WHOLE batch (router slices per table)', async () => {
    const { db, transport } = freshDb()
    const changes = [change('users'), change('posts')]
    await ensureSubscribed(db, ['users']) // only subscribed to one of the two touched topics
    transport.publish(batchTopic('users'), remoteBatch(changes))
    expect(engine.ingest).toHaveBeenCalledTimes(1)
    expect(engine.ingest).toHaveBeenCalledWith({ changes }) // the whole batch, not just this table's slice
  })

  it('a LOCAL write is NOT double-applied: its own round-trip is deduped (fed directly, not via transport)', async () => {
    const { db } = freshDb()
    await ensureSubscribed(db, ['users'])
    // The batch carries the publishing db's `origin`, so its own subscription drops the round-trip.
    publishBatch(db, { changes: [change('users')] })
    expect(engine.ingest).not.toHaveBeenCalled() // the local instance already fed its graphs directly
  })

  it('a genuinely remote batch (unseen id) IS applied', async () => {
    const { db, transport } = freshDb()
    const changes = [change('users')]
    await ensureSubscribed(db, ['users'])
    transport.publish(batchTopic('users'), remoteBatch(changes))
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

  it('dedupe is not COUNT-bounded: 5,000 unrelated batches later, the second-topic copy is still dropped', async () => {
    // The original count-bounded `seen` set failed exactly here: after SEEN_CAP unrelated ids the original
    // aged out and the delayed copy applied a SECOND time. The APPLIED marker expires on TIME, not on count,
    // so no volume of unrelated traffic can evict a marker that is still inside its skew window.
    const { db, transport } = freshDb()
    const changes = [change('users'), change('posts')]
    await ensureSubscribed(db, ['users', 'posts'])
    const message = remoteBatch(changes)
    transport.publish(batchTopic('users'), message) // first copy → applied
    // Unrelated traffic: each is a genuinely distinct batch, so each SHOULD apply. They exist to push the
    // original far down the marker set — which is exactly what evicted it under the old count bound.
    for (let i = 0; i < 5000; i++) {
      transport.publish(batchTopic('users'), remoteBatch([change('users')]))
    }
    transport.publish(batchTopic('posts'), message) // second copy of the ORIGINAL, 5,000 ids later
    const appliedOriginal = engine.ingest.mock.calls.filter(([batch]) => batch.changes === changes)
    expect(appliedOriginal).toHaveLength(1) // dropped, despite the volume in between
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

// The two scenarios that killed the previous "owner = first subscribed table of the batch" rule. Both come
// from the same defect: that rule consulted the MUTABLE readiness map at delivery time, so which copy was
// entitled to apply depended on receiver state that could differ between copies. Ownership is now decided by
// arrival order alone, which no later state change can revise.
describe('write transport — ownership cannot depend on mutable subscription state', () => {
  /** A transport whose subscription for a chosen topic is registered but NOT yet delivering — the real state
   *  of an async SUBSCRIBE that has not been acked. `ensureSubscribed` has already recorded the topic in its
   *  readiness map by then, which is precisely what the old rule misread as "listening". */
  function gatedTransport(deadTopic: string) {
    const topics = new Map<string, Set<(message: never) => void>>()
    return {
      publish(topic: string, message: never) {
        if (topic === batchTopic(deadTopic)) return // subscription not live yet: this copy is dropped
        for (const onMessage of topics.get(topic) ?? []) onMessage(message)
      },
      subscribe(topic: string, onMessage: (message: never) => void) {
        const set = topics.get(topic) ?? new Set()
        set.add(onMessage)
        topics.set(topic, set)
        return () => set.delete(onMessage)
      },
    }
  }

  it('a batch is applied even when a PENDING subscription would have been named its owner', async () => {
    // dbX holds a live read on `posts` (proven) and is mid-subscribe on `users` (recorded, not yet
    // delivering). A remote batch touches both. The old rule named `users` the owner because it came first
    // in the batch's table list and was present in the readiness map — but that topic delivered nothing, and
    // the `posts` copy deferred to it. Net: the batch was DROPPED (Evaluator probe: 0 fires).
    const db = {}
    const transport = gatedTransport('users')
    setChangeTransport(db, transport as never)
    await ensureSubscribed(db, ['posts']) // proven and delivering
    void ensureSubscribed(db, ['users']) // recorded in readiness; its topic never delivers
    const changes = [change('users'), change('posts')]
    const message = remoteBatch(changes) as never
    transport.publish(batchTopic('users'), message) // dropped by the gate
    transport.publish(batchTopic('posts'), message) // the only copy that arrives — it must apply
    expect(engine.ingest).toHaveBeenCalledTimes(1)
    expect(engine.ingest).toHaveBeenCalledWith({ changes })
  })

  it('a WALL-CLOCK jump between two copies cannot expire the marker', async () => {
    // The dedupe window is a real-time bound, so it must be measured on a MONOTONIC clock. With Date.now(),
    // an NTP correction / VM resume / operator clock change that steps the wall clock past the window
    // expires the marker while copy 2 is still moments away in real time — and the same precise batch
    // applies twice. Here the wall clock leaps an hour between copies while real time barely moves.
    const { db, transport } = freshDb()
    const changes = [change('users'), change('posts')]
    await ensureSubscribed(db, ['users', 'posts'])
    const message = remoteBatch(changes)

    transport.publish(batchTopic('users'), message) // copy 1 → applied, marker recorded

    vi.useFakeTimers({ toFake: ['Date'] }) // fake ONLY the wall clock; performance.now() stays real
    try {
      vi.setSystemTime(new Date(Date.now() + 60 * 60 * 1000)) // an hour forward, instantly
      transport.publish(batchTopic('posts'), message) // copy 2 → must still be suppressed
    } finally {
      vi.useRealTimers()
    }

    expect(engine.ingest).toHaveBeenCalledTimes(1)
  })

  it('markers are swept on a timer, not only on the next admission', async () => {
    // Pruning lazily means a burst followed by silence pins those ids for the db's lifetime — "short-lived"
    // would not be true of a system that goes quiet, which is exactly when nothing triggers a prune.
    const { db, transport } = freshDb()
    await ensureSubscribed(db, ['users']) // the in-memory transport loops the probe back synchronously
    vi.useFakeTimers()
    try {
      transport.publish(batchTopic('users'), remoteBatch([change('users')]))
      expect(appliedMarkerCount(db)).toBe(1)

      // No further traffic at all — only the passage of time may clear it.
      await vi.advanceTimersByTimeAsync(MARKER_WINDOW_MS + 1_000)
      expect(appliedMarkerCount(db)).toBe(0)
    } finally {
      vi.useRealTimers()
    }
  })

  it('a subscription added BETWEEN two copies of one batch cannot make the second apply again', async () => {
    // The double-apply half. Copy 1 arrives on `posts` and applies. THEN a new live read subscribes `users`,
    // growing the readiness map. Under the old rule the delayed `users` copy would now compute a DIFFERENT
    // owner (`users`, which it is on) and apply the same precise changes a second time — and precise
    // application is not idempotent.
    const { db, transport } = freshDb()
    const changes = [change('users'), change('posts')]
    await ensureSubscribed(db, ['posts'])
    const message = remoteBatch(changes)
    transport.publish(batchTopic('posts'), message) // copy 1 → applied
    await ensureSubscribed(db, ['users']) // the subscribed set CHANGES between copies
    transport.publish(batchTopic('users'), message) // copy 2 → must be dropped
    expect(engine.ingest).toHaveBeenCalledTimes(1)
  })
})
