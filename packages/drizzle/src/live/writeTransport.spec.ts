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
import { CHANGE_CODEC_VERSION } from './changeCodec.js'
import type { TableChange } from '../router/events.js'

// The mocked registry is PER-DB and tracks registered graphs, so watchedTables() answers truthfully — the
// coarse-all path depends on it.
const engine = vi.hoisted(() => ({
  ingest: vi.fn(),
  /** Per-receiver spies, so a test can attribute an ingest to the db it was routed to — the shared spy
   *  above also carries every sibling runtime's calls, which makes it unusable for that. */
  perDb: new Map<object, (batch: unknown) => void>(),
  watched: new Map<object, string[]>(),
}))
vi.mock('./dbRuntime.js', () => ({
  registryFor: (db: object) => ({
    router: {
      ingest: (batch: unknown) => {
        engine.perDb.get(db)?.(batch)
        engine.ingest(batch)
      },
      register: (graph: { tables: string[] }) =>
        engine.watched.set(db, [...(engine.watched.get(db) ?? []), ...graph.tables]),
      unregister: () => engine.watched.delete(db),
      watchedTables: () => engine.watched.get(db) ?? [],
    },
    acquire: vi.fn(),
  }),
  ingestWrite: vi.fn(),
}))

import {
  type ChangeTransport,
  createInMemoryChangeTransport,
  setChangeNamespace,
  setChangeTransport,
} from './changeTransport.js'
import { registryFor } from './dbRuntime.js'
import { acquireSubscription, changeTopicFor, isQuiescent, publishBatch, publishCoarseAll } from './writeTransport.js'

/** A minimal registered graph — only its `tables` matter to watchedTables(). */
const watching = (table: string) => ({ tables: [table] }) as never

const change = (table: string): TableChange => ({ table, kind: 'insert', new: { id: 1 } })

/** A fresh db + its own injected transport per test → no cross-test subscriber leakage. The ref that
 *  acquireSubscription returns is deliberately kept for the test's lifetime: these tests are about what a
 *  SUBSCRIBED db does, and the lifecycle that drops the subscription has its own spec. */
async function freshDb() {
  const db = {}
  const transport = createInMemoryChangeTransport()
  setChangeTransport(db, transport)
  await acquireSubscription(db)
  return { db, transport }
}

/** Two runtimes over ONE logical database, on one transport — the multi-instance path in a single process.
 *  They SHARE a `$client`, which is what makes them the same database: that is how two drizzle objects over
 *  one connection appear, and it is the identity the namespace is derived from. Two dbs with different
 *  clients are different databases and deliberately do NOT exchange changes (see the isolation suite). */
async function twoInstances() {
  const transport = createInMemoryChangeTransport()
  const $client = { connection: 'shared' }
  const dbA = { $client }
  const dbB = { $client }
  setChangeTransport(dbA, transport)
  setChangeTransport(dbB, transport)
  await acquireSubscription(dbA)
  await acquireSubscription(dbB)
  return { transport, dbA, dbB }
}

/** Everything that reached the wire, as raw payloads — what a broker would actually carry. */
async function wireTap(transport: ChangeTransport, db: object) {
  const seen: string[] = []
  await transport.subscribe(changeTopicFor(db), (payload) => seen.push(payload))
  return seen
}

/** Publications are CHAINED per db so an async transport receives them in sequence order, which defers the
 *  first one by a microtask. The local graphs were already fed synchronously by ingestWrite; only the
 *  REMOTE hop is deferred, so a test that watches the wire has to let the chain run. */
const flush = async () => {
  for (let i = 0; i < 5; i++) await Promise.resolve()
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

// ── logical-database isolation ───────────────────────────────────────
//
// The default bus is one process-wide object and the topic used to be one constant, so EVERY reactive db
// shared a stream. Two unrelated databases that both have a `users` table then applied each other's row
// deltas — a wrong row into a precise graph, not a harmless over-fire. Identity now rides the topic and the
// envelope: derived from the connection by default, or named explicitly for a shared/injected transport.
describe('write transport — unrelated databases never see each other', () => {
  it('two DIFFERENT databases on one transport do NOT cross-apply, even with the same table name', async () => {
    const transport = createInMemoryChangeTransport()
    const writer = { $client: { connection: 'db-one' } } // two genuinely different connections…
    const bystander = { $client: { connection: 'db-two' } }
    setChangeTransport(writer, transport)
    setChangeTransport(bystander, transport)
    await acquireSubscription(writer)
    await acquireSubscription(bystander)
    registryFor(bystander).router.register(watching('users'))

    publishBatch(writer, { changes: [change('users')] })
    await flush()

    expect(engine.ingest).not.toHaveBeenCalled() // …so a `users` row from one is not a `users` row in the other
  })

  it('…while two runtimes over the SAME database still do — so the isolation is not just deafness', async () => {
    const { dbA } = await twoInstances()
    publishBatch(dbA, { changes: [change('users')] })
    await flush()
    expect(engine.ingest).toHaveBeenCalledTimes(1)
  })

  it('an explicit namespace joins runtimes that share NO connection object — the cross-process case', async () => {
    // Different processes cannot share a `$client`, so a shared transport is paired with a stable name.
    const transport = createInMemoryChangeTransport()
    const serverA = {}
    const serverB = {}
    setChangeTransport(serverA, transport)
    setChangeTransport(serverB, transport)
    setChangeNamespace(serverA, 'orders-db')
    setChangeNamespace(serverB, 'orders-db')
    await acquireSubscription(serverA)
    await acquireSubscription(serverB)

    publishBatch(serverA, { changes: [change('users')] })
    await flush()

    expect(engine.ingest).toHaveBeenCalledTimes(1) // same name, same database, changes flow
  })
})

describe('write transport — each isolation guard, on its own', () => {
  it('TOPIC: a different database publishes on a topic this one is not subscribed to', async () => {
    const transport = createInMemoryChangeTransport()
    const listener = { $client: { connection: 'db-one' } }
    const stranger = { $client: { connection: 'db-two' } }
    setChangeTransport(listener, transport)
    setChangeTransport(stranger, transport)
    await acquireSubscription(listener)

    const heard: string[] = []
    await transport.subscribe(changeTopicFor(listener), (payload) => heard.push(payload))
    publishBatch(stranger, { changes: [change('users')] })
    await flush()

    expect(heard).toEqual([]) // nothing was even delivered — the topics genuinely differ
  })

  it('ENVELOPE: a foreign-namespace payload delivered ONTO our topic is ignored', async () => {
    // The case the topic cannot cover: an adapter that fans out more widely than the topic it was handed.
    const transport = createInMemoryChangeTransport()
    const listener = { $client: { connection: 'db-one' } }
    setChangeTransport(listener, transport)
    await acquireSubscription(listener)
    registryFor(listener).router.register(watching('users'))

    const foreign = JSON.stringify({
      version: CHANGE_CODEC_VERSION,
      namespace: 'some-other-database',
      origin: 'elsewhere',
      seq: 1,
      changes: [{ table: 'users', kind: 'insert', new: { id: 1 } }],
    })
    transport.publish(changeTopicFor(listener), foreign)

    // Not applied — and not coarsened either: another database's rows say nothing about ours, so a refetch
    // would be spurious rather than merely cautious.
    expect(engine.ingest).not.toHaveBeenCalled()
  })
})

// ── ordering, and the deferred baseline ──────────────────────────────
//
// The adapter contract asked for at-most-once and said nothing about ORDER, which is the wrong way round: a
// transport can obey it to the letter and still deliver update B before update A, corrupting a precise graph
// exactly as a duplicate would. So the envelope carries origin+seq and the RUNTIME decides. The adapter now
// owes at-least-once only.
//
// The baseline is DEFERRED rather than pre-emptive. A receiver joining mid-stream cannot tell "this is
// simply my next one" from "this overtook the one before it" — but it does not have to guess, because the
// two cases resolve themselves: sequences published before our admission are already in our snapshot, and
// sequences published after it are still owed to us by at-least-once, so a reorder always eventually shows
// up as a straggler. Bet precise, and coarsen only when a straggler proves the bet wrong.
describe('write transport — the runtime, not the adapter, owns duplicate and order', () => {
  /** A receiver of this database that has never heard of the publisher, with its OWN ingest spy so its
   *  behaviour is attributable — the shared `engine.ingest` also carries the sibling runtime's calls. */
  async function isolatedReceiver(transport: ChangeTransport, $client: object) {
    const receiver = { $client }
    setChangeTransport(receiver, transport)
    await acquireSubscription(receiver)
    registryFor(receiver).router.register(watching('users'))
    const ingest = vi.fn()
    engine.perDb.set(receiver, ingest)
    return { receiver, ingest }
  }

  /** Capture what a publisher puts on the wire, so it can be replayed in any order. */
  async function wireOf(transport: ChangeTransport, db: object) {
    const captured: string[] = []
    await transport.subscribe(changeTopicFor(db), (payload) => captured.push(payload))
    return captured
  }

  const preciseCalls = (ingest: ReturnType<typeof vi.fn>) =>
    ingest.mock.calls.filter(([batch]) =>
      (batch as { changes: TableChange[] }).changes.some((c) => c.kind !== 'coarse'),
    )
  const coarseCalls = (ingest: ReturnType<typeof vi.fn>) =>
    ingest.mock.calls.filter(([batch]) =>
      (batch as { changes: TableChange[] }).changes.every((c) => c.kind === 'coarse'),
    )

  it('DROPS a redelivered payload — at-most-once is no longer the adapter’s problem', async () => {
    const { transport, dbA, dbB } = await twoInstances()
    const captured = await wireOf(transport, dbA)
    const { receiver, ingest } = await isolatedReceiver(transport, (dbB as { $client: object }).$client)

    publishBatch(dbA, { changes: [change('users')] })
    await flush()
    expect(ingest).toHaveBeenCalledTimes(1)

    transport.publish(changeTopicFor(receiver), captured[0]!) // the very same payload again
    expect(ingest).toHaveBeenCalledTimes(1) // not applied twice
  })

  it('ROUTINE RESUBSCRIBE against a busy peer stays PRECISE — zero coarsening', async () => {
    // The owner's case, and the whole point of deferring: a peer is mid-stream at seq 3 when a fresh
    // receiver joins. Nothing is coarsened; the graphs it just compiled stay precise.
    const { transport, dbA, dbB } = await twoInstances()
    publishBatch(dbA, { changes: [change('users')] }) // 1
    publishBatch(dbA, { changes: [change('users')] }) // 2
    await flush()

    const { ingest } = await isolatedReceiver(transport, (dbB as { $client: object }).$client)
    publishBatch(dbA, { changes: [change('users')] }) // 3 — first this receiver ever sees
    publishBatch(dbA, { changes: [change('users')] }) // 4 — in order after it
    await flush()

    expect(coarseCalls(ingest)).toEqual([]) // NOT ONE coarsening
    expect(preciseCalls(ingest)).toHaveLength(2) // both applied precisely
  })

  it('BET WRONG: a straggler below the baseline triggers exactly one corrective coarsen', async () => {
    // The reordered first pair. Seq 2 is taken precisely; seq 1 arriving afterwards is proof that the skipped
    // region was post-admission after all, so it is corrected — once — rather than silently lost.
    const { transport, dbA, dbB } = await twoInstances()
    const captured = await wireOf(transport, dbA)
    publishBatch(dbA, { changes: [change('users')] }) // 1
    publishBatch(dbA, { changes: [change('posts')] }) // 2
    await flush()

    const { receiver, ingest } = await isolatedReceiver(transport, (dbB as { $client: object }).$client)
    transport.publish(changeTopicFor(receiver), captured[1]!) // seq 2 first → the bet
    expect(preciseCalls(ingest)).toHaveLength(1)
    expect(coarseCalls(ingest)).toEqual([])

    transport.publish(changeTopicFor(receiver), captured[0]!) // seq 1 straggles in → the bet was wrong
    expect(coarseCalls(ingest)).toHaveLength(1)

    transport.publish(changeTopicFor(receiver), captured[0]!) // and again — already covered
    expect(coarseCalls(ingest)).toHaveLength(1) // still ONE: no double-coarsen
  })

  it('COARSENS on a real GAP, and the gap then COVERS the outstanding bet', async () => {
    // The baseline is deliberately seq 3, not seq 1: with a baseline of 1 there is no unaccounted region
    // below it, so the gap's "this coarsen also covers the bet" step would be unreachable and a test built
    // on it could not disagree with an implementation that omitted the step.
    const { transport, dbA, dbB } = await twoInstances()
    const captured = await wireOf(transport, dbA)
    for (let i = 0; i < 5; i++) publishBatch(dbA, { changes: [change('users')] }) // seq 1..5
    await flush()

    const { receiver, ingest } = await isolatedReceiver(transport, (dbB as { $client: object }).$client)
    transport.publish(changeTopicFor(receiver), captured[2]!) // seq 3 → baseline; 1..2 unaccounted
    expect(preciseCalls(ingest)).toHaveLength(1)

    transport.publish(changeTopicFor(receiver), captured[4]!) // seq 5 — skips 4 → gap
    expect(coarseCalls(ingest)).toHaveLength(1)

    // That coarsen covered EVERYTHING below seq 5, including the 1..2 the bet had left open. Both a
    // straggler from the gap and one from under the baseline are now accounted for → dropped, not
    // coarsened again.
    transport.publish(changeTopicFor(receiver), captured[3]!) // seq 4, from the gap
    transport.publish(changeTopicFor(receiver), captured[0]!) // seq 1, from under the baseline
    expect(coarseCalls(ingest)).toHaveLength(1) // still ONE
    expect(preciseCalls(ingest)).toHaveLength(1)
  })
})

// A first-unknown envelope that is COARSE-ALL closes the baseline bet as surely as a gap does: coarsening
// rebuilds every watched graph from the database, so every lower sequence is accounted for. Leaving the bet
// open buys a redundant second reseed — or, if the straggler lands while the first is still running, the
// storm guard turns a recoverable event into a terminal demotion.
describe('write transport — a first-unknown coarse-all closes the bet', () => {
  it('a later straggler does NOT trigger a second coarsening', async () => {
    const { transport, dbA, dbB } = await twoInstances()
    const captured: string[] = []
    await transport.subscribe(changeTopicFor(dbA), (payload) => captured.push(payload))
    publishBatch(dbA, { changes: [change('users')] }) // seq 1
    publishCoarseAll(dbA) // seq 2
    await flush()

    const receiver = { $client: (dbB as { $client: object }).$client }
    setChangeTransport(receiver, transport)
    await acquireSubscription(receiver)
    registryFor(receiver).router.register(watching('users'))
    const ingest = vi.fn()
    engine.perDb.set(receiver, ingest)

    transport.publish(changeTopicFor(receiver), captured[1]!) // seq 2, coarse-all, first thing seen
    expect(ingest).toHaveBeenCalledTimes(1) // the coarse-all itself

    transport.publish(changeTopicFor(receiver), captured[0]!) // seq 1 straggles in — already covered
    expect(ingest).toHaveBeenCalledTimes(1) // …no redundant second coarsening
  })
})

// Quiescence is what admits a change-transport rotation (changeTransport.ts). It has to mean the strong
// thing — nobody holding a ref, no listener attached, no unconfirmed detach, and no transition still in
// flight — because a db that merely LOOKS idle would let a swap strand a live listener on the old transport.
describe('write transport — the quiescent boundary', () => {
  it('a db that never had a live read is quiescent', () => {
    expect(isQuiescent({})).toBe(true)
  })

  it('is NOT quiescent while a ref is held, and is again once it is released and settled', async () => {
    const db = {}
    setChangeTransport(db, createInMemoryChangeTransport())
    const ref = await acquireSubscription(db)
    expect(isQuiescent(db)).toBe(false)
    ref.release()
    await flush()
    expect(isQuiescent(db)).toBe(true)
  })

  it('is NOT quiescent mid-detach, before the unsubscribe has confirmed', async () => {
    const db = {}
    let confirmDetach: (() => void) | undefined
    setChangeTransport(db, {
      publish: () => {},
      subscribe: async () => ({ unsubscribe: () => new Promise<void>((resolve) => (confirmDetach = resolve)) }),
    })
    const ref = await acquireSubscription(db)
    ref.release()
    await flush()
    // `active` is cleared before its detach is awaited, so refs===0 and active===undefined ALREADY hold
    // here. Only the in-flight transition distinguishes this from a finished detach — rotating now would
    // swap the transport out from under a listener that is still attached to it.
    expect(isQuiescent(db)).toBe(false)
    confirmDetach!()
    await flush()
    expect(isQuiescent(db)).toBe(true)
  })

  it('is NOT quiescent while a detach is UNCONFIRMED, and the sequence state is not silently inherited', async () => {
    const warn = vi.spyOn(console, 'error').mockImplementation(() => {})
    const db = {}
    setChangeTransport(db, {
      publish: () => {},
      subscribe: async () => ({ unsubscribe: () => Promise.reject(new Error('broker unreachable')) }),
    })
    const ref = await acquireSubscription(db)
    ref.release()
    await flush()
    // The listener's status is unknown, so there is nothing safe to rotate to: a new transport would leave
    // the old listener attached and the next precise batch applied twice.
    expect(isQuiescent(db)).toBe(false)
    warn.mockRestore()
  })
})

// Rotation (changeTransport.ts) is admitted at a quiescent boundary, and a db with no live read is
// quiescent — including one that has just WRITTEN. Publications are queued, so the era a publication is
// resolved in is the difference between reaching the peers who were listening when the write committed and
// reaching a set of subscribers who were not there for it while the first set never hear about it.
describe('write transport — a publication belongs to the transport era of its write', () => {
  const recording = () => {
    const published: string[] = []
    const transport: ChangeTransport = {
      publish: (_topic, payload) => {
        published.push(payload)
      },
      subscribe: async () => ({ unsubscribe: () => {} }),
    }
    return { transport, published }
  }

  it('a QUEUED publication goes to the transport its write committed on, not one rotated in afterwards', async () => {
    const db = {}
    const a = recording()
    const b = recording()
    setChangeTransport(db, a.transport, true)

    publishBatch(db, { changes: [change('users')] }) // committed and enqueued in A's era…
    setChangeTransport(db, b.transport, true) // …rotated before the chain's microtask runs
    await flush()

    expect(a.published).toHaveLength(1) // the write's own era hears it
    expect(b.published).toHaveLength(0) // subscribers who were not there for it do not
  })

  it('and writes AFTER the rotation go to the new transport — the era moves, it is not pinned', async () => {
    const db = {}
    const a = recording()
    const b = recording()
    setChangeTransport(db, a.transport, true)
    publishBatch(db, { changes: [change('users')] })
    setChangeTransport(db, b.transport, true)
    await flush()

    publishBatch(db, { changes: [change('users')] })
    await flush()
    expect(a.published).toHaveLength(1) // still just the pre-rotation write
    expect(b.published).toHaveLength(1) // the post-rotation one landed here
  })
})

// A rotation cuts the change stream in a way the SEQUENCE CANNOT SHOW. The receiver's deferred baseline
// assumes a sequence below the first one it sees is either pre-admission (already in its snapshot) or still
// owed by an at-least-once adapter. A message published on the writer's PREVIOUS transport is neither:
// published after admission, onto a transport the receiver was never on. Nothing can ever deliver it, so a
// precise bet there is a permanent hole rather than a brief one.
describe('write transport — a transport rotation is an OBSERVABLE cut, not an inferred one', () => {
  /** The Evaluator's schedule: R is admitted on B; W publishes seq 1 on A (unreachable — after R's snapshot,
   *  and not deliverable on B); W rotates to B and publishes seq 2. */
  async function crossEra() {
    const $client = { connection: 'shared' }
    const writer = { $client }
    const receiver = { $client }
    const transportA = createInMemoryChangeTransport()
    const transportB = createInMemoryChangeTransport()

    setChangeTransport(receiver, transportB)
    await acquireSubscription(receiver) // R admitted on B, snapshot taken
    registryFor(receiver).router.register(watching('users'))
    const ingest = vi.fn()
    engine.perDb.set(receiver, ingest)

    setChangeTransport(writer, transportA, true)
    publishBatch(writer, { changes: [change('users')] }) // seq 1 → A. R is on B: it never arrives, ever.
    await flush()
    expect(ingest).not.toHaveBeenCalled() // …confirmed unreachable, not merely late

    return { writer, receiver, transportB, ingest }
  }

  it('the first post-rotation publication cuts: the receiver coarsens instead of betting precise', async () => {
    const { writer, transportB, ingest } = await crossEra()

    setChangeTransport(writer, transportB, true) // W rotates to B — quiescent, it has only written
    publishBatch(writer, { changes: [change('users')] }) // seq 2 → B, carrying the cut
    await flush()

    expect(ingest).toHaveBeenCalledTimes(1)
    const [batch] = ingest.mock.calls[0] as [{ changes: TableChange[] }]
    // COARSE, not the precise row. Precise here would leave seq 1 missing forever with nothing able to
    // correct it — incorrect by omission, permanently, which is the failure this marker exists to prevent.
    expect(batch.changes.every((c) => c.kind === 'coarse')).toBe(true)
    expect(batch.changes.map((c) => c.table)).toEqual(['users'])
  })

  it('a redelivered cut does not coarsen twice', async () => {
    const { writer, transportB, ingest } = await crossEra()
    setChangeTransport(writer, transportB, true)
    const wire: string[] = []
    await transportB.subscribe(changeTopicFor(writer), (payload) => wire.push(payload))
    publishBatch(writer, { changes: [change('users')] })
    await flush()
    expect(ingest).toHaveBeenCalledTimes(1)

    transportB.publish(changeTopicFor(writer), wire[0]!) // at-least-once: the same cut again
    expect(ingest).toHaveBeenCalledTimes(1) // …already accounted for
  })

  it('NEGATIVE: with no rotation there is no cut, and the deferred baseline is unchanged', async () => {
    // Same shape, one difference: the writer never changes transport. The first message the receiver sees is
    // taken PRECISELY, exactly as before — so the cut above is caused by the rotation, not by the schedule.
    const { transport, dbA, dbB } = await twoInstances()
    registryFor(dbB).router.register(watching('users'))
    const ingest = vi.fn()
    engine.perDb.set(dbB, ingest)
    void transport

    publishBatch(dbA, { changes: [change('users')] })
    publishBatch(dbA, { changes: [change('users')] })
    await flush()

    const [first] = ingest.mock.calls[0] as [{ changes: TableChange[] }]
    expect(first.changes[0]!.kind).toBe('insert') // precise baseline, no coarsening
  })
})
