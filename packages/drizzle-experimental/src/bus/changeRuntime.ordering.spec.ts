// ORDERING, THE DEFERRED BASELINE, AND THE ERA A PUBLICATION BELONGS TO.
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
//
// A transport ROTATION is the one cut that reasoning cannot cover, which is why it lives here rather than
// with the transport primitive: a message published on the writer's previous transport is neither
// pre-admission nor still owed, so it must be marked rather than inferred.

import { beforeEach, describe, expect, it, vi } from 'vitest'
vi.mock('./dbRuntime.js', async () => (await import('./changeRuntime.registryMock.js')).dbRuntimeMock())

import { decodeChangePayload } from './changeCodec.js'
import { type ChangeTransport, createInMemoryChangeTransport } from './changeTransport.js'
import { registryFor } from './dbRuntime.js'
import {
  acquireSubscription,
  changeTopicFor,
  publishBatch,
  publishCoarseAll,
  setChangeTransport,
} from './changeRuntime.js'
import { change, engine, flush, resetEngine, twoInstances, watching } from './changeRuntime.testKit.js'
import type { TableChange } from './router/events.js'

beforeEach(resetEngine)

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
    setChangeTransport(db, a.transport)

    publishBatch(db, { changes: [change('users')] }) // committed and enqueued in A's era…
    setChangeTransport(db, b.transport) // …rotated before the chain's microtask runs
    await flush()

    expect(a.published).toHaveLength(1) // the write's own era hears it
    expect(b.published).toHaveLength(0) // subscribers who were not there for it do not
  })

  it('and writes AFTER the rotation go to the new transport — the era moves, it is not pinned', async () => {
    const db = {}
    const a = recording()
    const b = recording()
    setChangeTransport(db, a.transport)
    publishBatch(db, { changes: [change('users')] })
    setChangeTransport(db, b.transport)
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

    setChangeTransport(writer, transportA)
    publishBatch(writer, { changes: [change('users')] }) // seq 1 → A. R is on B: it never arrives, ever.
    await flush()
    expect(ingest).not.toHaveBeenCalled() // …confirmed unreachable, not merely late

    return { writer, receiver, transportB, ingest }
  }

  it('the first post-rotation publication cuts: the receiver coarsens instead of betting precise', async () => {
    const { writer, transportB, ingest } = await crossEra()

    setChangeTransport(writer, transportB) // W rotates to B — quiescent, it has only written
    publishBatch(writer, { changes: [change('users')] }) // seq 2 → B, carrying the cut
    await flush()

    expect(ingest).toHaveBeenCalledTimes(1)
    const [batch] = ingest.mock.calls[0] as [{ changes: TableChange[] }]
    // COARSE, not the precise row. Precise here would leave seq 1 missing forever with nothing able to
    // correct it — incorrect by omission, permanently, which is the failure this marker exists to prevent.
    expect(batch.changes.every((c) => c.kind === 'coarse')).toBe(true)
    expect(batch.changes.map((c) => c.table)).toEqual(['users'])
  })

  it('a cut OVERTAKEN by the publication after it still coarsens — no FIFO is owed', async () => {
    // The adapter owes AT-LEAST-ONCE and nothing whatsoever about order, so the post-rotation seq 3 can
    // reach a receiver before the seq 2 that carries the cut. Read as a plain redelivery, that cut is
    // swallowed: the receiver keeps a precise baseline over an era whose messages went to a transport it was
    // never on, and no straggler can ever correct it. The hole `eraCut` exists to close, reopened by the
    // order it arrived in.
    const $client = { connection: 'shared' }
    const writer = { $client }
    const transportA = createInMemoryChangeTransport()
    const transportB = createInMemoryChangeTransport()

    setChangeTransport(writer, transportA)
    publishBatch(writer, { changes: [change('users')] }) // seq 1 → A: unreachable from B, forever
    await flush()

    const wire: string[] = []
    await transportB.subscribe(changeTopicFor(writer), (payload) => wire.push(payload))
    setChangeTransport(writer, transportB) // rotate — quiescent, the writer has only written
    publishBatch(writer, { changes: [change('users')] }) // seq 2 → B, carrying the cut
    publishBatch(writer, { changes: [change('users')] }) // seq 3 → B, an ordinary one after it
    await flush()

    // The fixture is what this case claims it is, asserted rather than assumed: the CUT is on seq 2, and
    // seq 3 carries none. A test built on the wrong payload would pass for the wrong reason.
    expect(decodeChangePayload(wire[0]!)).toMatchObject({ seq: 2, eraCut: true })
    expect('eraCut' in (decodeChangePayload(wire[1]!) as object)).toBe(false)

    // A receiver admitted only NOW, so it has no history and no backlog: both payloads are new to it.
    const receiver = { $client }
    setChangeTransport(receiver, transportB)
    await acquireSubscription(receiver)
    registryFor(receiver).router.register(watching('users'))
    const ingest = vi.fn()
    engine.perDb.set(receiver, ingest)

    transportB.publish(changeTopicFor(receiver), wire[1]!) // seq 3 FIRST → precise baseline, bet left open
    expect(ingest).toHaveBeenCalledTimes(1)
    const [baseline] = ingest.mock.calls[0] as [{ changes: TableChange[] }]
    expect(baseline.changes[0]!.kind).toBe('insert') // it really did bet precise

    transportB.publish(changeTopicFor(receiver), wire[0]!) // …then the overtaken cut at seq 2
    expect(ingest).toHaveBeenCalledTimes(2) // NOT dropped as a duplicate
    const [corrected] = ingest.mock.calls[1] as [{ changes: TableChange[] }]
    expect(corrected.changes.every((c) => c.kind === 'coarse')).toBe(true) // the cut still lands
  })

  it('a redelivered cut does not coarsen twice', async () => {
    const { writer, transportB, ingest } = await crossEra()
    setChangeTransport(writer, transportB)
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

// The header and the send must agree about which era a publication belongs to. Encoding sits between them
// and runs USER-CONTROLLED code — a row's enumerable getter, reachable from a custom decoder's returned
// value — which can synchronously rotate the transport. Resolving the transport once per publication is what
// makes that window uninteresting; resolving it twice let the header say "no cut" about the old transport
// while the send went to the new one, which is an unmarked publication on a transport whose receivers have
// no history: the permanent hole again, one TOCTOU wide.
describe('write transport — one publication resolves its transport exactly once', () => {
  const recorder = () => {
    const published: string[] = []
    const transport: ChangeTransport = {
      publish: (_topic, payload) => {
        published.push(payload)
      },
      subscribe: async () => ({ unsubscribe: () => {} }),
    }
    return { transport, published }
  }

  it('a rotation DURING encoding cannot produce an unmarked publication on the new transport', async () => {
    const writer = {}
    const a = recorder()
    const b = recorder()
    setChangeTransport(writer, a.transport)
    publishBatch(writer, { changes: [change('users')] }) // seq 1 → A, establishing the era
    await flush()

    // The attack: an enumerable getter on the row, evaluated by the encoder, rotates the transport.
    const row = {
      get id() {
        setChangeTransport(writer, b.transport)
        return 1
      },
    }
    publishBatch(writer, { changes: [{ table: 'users', kind: 'insert', new: row }] }) // seq 2
    await flush()

    // THE INVARIANT: unmarked-on-B is impossible. Single resolution yields unmarked-on-A — the publication
    // belongs wholly to the era it began in, where it is in order and needs no marker. (The split resolved
    // A for the header and B for the send, landing seq 2 on B with no marker and no history: a receiver
    // there would first-see the origin at seq 2 and bet precise on a seq 1 it can never receive.)
    expect(b.published).toHaveLength(0)
    expect(a.published).toHaveLength(2)
    const seq2 = decodeChangePayload(a.published[1]!)
    expect(seq2).toMatchObject({ seq: 2 })
    expect('eraCut' in (seq2 as object)).toBe(false) // correctly unmarked: on A, this is simply the next one

    // …and the rotation is not lost, it is attributed to the NEXT publication, atomically.
    publishBatch(writer, { changes: [change('users')] })
    await flush()
    expect(b.published).toHaveLength(1)
    expect(decodeChangePayload(b.published[0]!)).toMatchObject({ seq: 3, eraCut: true })
  })

  it('CONTROL: with no re-entrant rotation the same shape publishes normally and unmarked', async () => {
    const writer = {}
    const a = recorder()
    setChangeTransport(writer, a.transport)
    publishBatch(writer, { changes: [change('users')] })
    const row = {
      get id() {
        return 1
      },
    } // same getter shape, no rotation
    publishBatch(writer, { changes: [{ table: 'users', kind: 'insert', new: row }] })
    await flush()
    expect(a.published).toHaveLength(2)
    expect('eraCut' in (decodeChangePayload(a.published[1]!) as object)).toBe(false)
  })
})
