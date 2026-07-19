export { publishBatch, publishCoarseAll, acquireSubscription, changeTopicFor, isQuiescent }
export type { SubscriptionRef }

import { randomUUID } from 'node:crypto'
import { registryFor } from './dbRuntime.js'
import { type ChangeSubscription, namespaceFor, transportFor } from './changeTransport.js'
import { CHANGE_CODEC_VERSION, decodeChangePayload, encodeChangePayload } from './changeCodec.js'
import type { ChangeBatch } from '../router/events.js'

// Cross-instance change transport. A committed batch is published ONCE, to ONE topic per logical database,
// over the db's DEDICATED changeTransport (never the user's app Broadcast — see changeTransport.ts). A
// receiver feeds the whole batch into its own graphs via `router.ingest`; the router already slices it per
// table and notifies each affected graph at most once, so one message is one atomic graph tick.
//
// SINGLE TOPIC, deliberately. Receiver filtering is the router's job — it ignores tables no graph watches —
// and publishing a copy per touched table instead would buy that filtering at the price of a correctness
// protocol to undo its own duplicates: apply-once memory, a time bound on it, a sweeper, and a cross-topic
// skew guarantee every adapter would owe. If measured broker traffic ever justifies per-table filtering, it
// returns as a versioned additive capability, never as a baseline the runtime compensates for.
//
// Two obligations survive, and neither is about fan-out:
//   - ORIGIN self-suppression — a db feeds its own graphs DIRECTLY in `ingestWrite`, before publishing, so
//     it must drop its own echo or apply the same delta twice.
//   - ORDERING, which the RUNTIME now owns rather than the adapter: every envelope carries the publisher's
//     origin and a monotonic seq, so a duplicate is dropped and a gap or reorder coarsens. The adapter owes
//     at-least-once delivery of the exact payload and nothing more.
//
// NAMESPACED per logical database (topic and envelope both). One process-wide default bus with a constant
// topic meant two unrelated databases sharing a table name applied each other's ROW DELTAS — a wrong row in
// a precise graph, not an over-fire. See namespaceFor() in changeTransport.ts.
//
// READINESS BARRIER (acquireSubscription): a live read is not admitted until the transport has ADMITTED
// this db's subscription — which is what awaiting `subscribe()` means (changeTransport.ts). Readiness is a
// control-plane fact and is asked of the control plane; proving it on the data plane instead (publish a
// token, await it back) would demand self-delivery from every adapter and still say nothing about the
// cluster, since an isolated Redis namespace loops its own probe back happily. A rejected subscribe fails
// the read CLOSED.
//
// LIFETIME: the subscription is REFCOUNTED against the ownership the graph registry already models — a read
// token holds a ref from before `registry.acquire` until it redeems into a channel lease, and the last lease
// to close drops it. At zero the listener is detached, so a db nobody reads live is not pinned by a callback
// closing over it.

/** The change topic for ONE logical database. Namespaced, so a transport shared by several databases keeps
 *  their streams apart rather than leaving that to every adapter to remember. */
function changeTopicFor(db: object): string {
  return `__live__:${namespaceFor(db)}:changes`
}

/** The publishing identity of a db — stable for its lifetime, used to drop our own echo. Its `seq` is that
 *  identity's monotonic publication counter: the receiver's only way to tell a duplicate from a reorder from
 *  a gap without demanding both properties from every adapter. */
const publishers = new WeakMap<object, { origin: string; seq: number; chain: Promise<void> }>()
function publisherOf(db: object): { origin: string; seq: number; chain: Promise<void> } {
  let publisher = publishers.get(db)
  if (!publisher) publishers.set(db, (publisher = { origin: randomUUID(), seq: 0, chain: Promise.resolve() }))
  return publisher
}

/** Publish a committed batch cross-instance: ONE message, carrying the whole batch. No-op for an empty
 *  batch. Receivers slice it per table themselves. */
function publishBatch(db: object, batch: ChangeBatch): void {
  if (batch.changes.length === 0) return
  const header = nextHeader(db)
  let payload: string
  try {
    payload = encodeChangePayload({ ...header, changes: batch.changes })
  } catch (error) {
    // A value the codec cannot carry must not cost the invalidation itself: fall back to the value-free
    // coarse-all envelope, so remote graphs refetch instead of never hearing about the write. It keeps the
    // seq it already took — the stream must stay contiguous or every receiver reads a gap.
    console.error(
      '[telefunc] live: a change batch could not be encoded for the transport, so a COARSE invalidation was published instead. Remote live queries over-fetch rather than miss the write.',
      error,
    )
    payload = encodeChangePayload({ ...header, coarseAll: true })
  }
  publishPayload(db, payload)
}

/** Announce a mutation whose touched tables are UNKNOWABLE (raw SQL, batch) to every OTHER instance: each
 *  coarsens its own watched tables, since the publisher cannot know them. Rides the SAME topic — the
 *  separate wildcard channel it used to need was an artefact of per-table fan-out. The publisher's own
 *  graphs are fed directly, and the `origin` check makes its own subscription drop this. */
function publishCoarseAll(db: object): void {
  publishPayload(db, encodeChangePayload({ ...nextHeader(db), coarseAll: true }))
}

/** The envelope header for the next publication, taking this db's next sequence number. */
function nextHeader(db: object): { version: number; namespace: string; origin: string; seq: number } {
  const publisher = publisherOf(db)
  publisher.seq++
  return {
    version: CHANGE_CODEC_VERSION,
    namespace: namespaceFor(db),
    origin: publisher.origin,
    seq: publisher.seq,
  }
}

/** Hand a payload to the transport. The write that produced it has ALREADY COMMITTED, so a transport
 *  failure — thrown outright, or a rejection from an async client that publishes over a socket — is
 *  reported and dropped. It must never reject the caller's write (which succeeded) and must never surface
 *  as an unhandled rejection.
 *
 *  Publications are CHAINED per db so an async transport is handed them in sequence order. A receiver
 *  tolerates reordering by coarsening, so this is not what makes the system sound — it is what stops us
 *  manufacturing the reordering ourselves and paying the precision for it. */
function publishPayload(db: object, payload: string): void {
  const publisher = publisherOf(db)
  const topic = changeTopicFor(db)
  publisher.chain = publisher.chain.then(() => {
    try {
      const published = transportFor(db).publish(topic, payload)
      if (isThenable(published)) return published.then(undefined, reportPublishFailure)
    } catch (error) {
      reportPublishFailure(error)
    }
    return undefined
  })
}

function isThenable(value: void | Promise<void>): value is Promise<void> {
  return typeof (value as Promise<void> | undefined)?.then === 'function'
}

function reportPublishFailure(error: unknown): void {
  console.error(
    '[telefunc] live: publishing a change batch to the changeTransport failed. The write COMMITTED and its result is unaffected; live queries on other instances may be stale until the next write touching those tables.',
    error,
  )
}

/** One owner's hold on this db's change subscription. Released when that owner is done — the read token it
 *  was minted for, or the channel lease that token redeemed into. */
type SubscriptionRef = { release(): void }

/** What the runtime knows about a db's presence on the change topic: how many owners want it, the live
 *  handle if there is one, the chain that serializes every transition between those two facts — and any
 *  handle whose detachment FAILED, whose listener is therefore of unknown status. */
type SubscriptionState = {
  refs: number
  active: ChangeSubscription | undefined
  /** Per publishing origin: the highest sequence seen (`last`), and the sequence below which nothing has
   *  been accounted for yet (`unknownBelow` — the outstanding baseline bet, 0 once a coarsen has covered
   *  it). Cleared on detach, which both discards state that is stale and bounds what a long-lived process
   *  accumulates. */
  seen: Map<string, { last: number; unknownBelow: number }>
  /** A subscription we asked to detach and could not confirm. Until it is confirmed gone, subscribing again
   *  could put a second listener alongside one that never left — so this blocks every new subscribe. */
  undetached: ChangeSubscription | undefined
  transition: Promise<void>
  /** Transitions started and not yet settled. `active` is cleared BEFORE its detach is awaited, so without
   *  this a db mid-detach would look identical to one that had finished detaching. */
  settling: number
}

const subscriptions = new WeakMap<object, SubscriptionState>()

function stateFor(db: object): SubscriptionState {
  let state = subscriptions.get(db)
  if (!state) {
    subscriptions.set(
      db,
      (state = {
        refs: 0,
        active: undefined,
        seen: new Map(),
        undetached: undefined,
        transition: Promise.resolve(),
        settling: 0,
      }),
    )
  }
  return state
}

/** Take a ref on this db's change subscription, resolving once the transport has ADMITTED it. A live read
 *  awaits this before both `registry.acquire` and its snapshot read, so a write on another instance can
 *  never land in the window between the read and a not-yet-established subscription; a transport that
 *  refuses fails the read CLOSED (and leaves nothing cached, so the next read retries).
 *
 *  Concurrent reads share ONE underlying subscription and hold a ref each. `release()` is idempotent, and
 *  EVERY path a read can leave by must call it — never serialized, compile/hydrate failure, snapshot
 *  failure, lease closed — or the db stays subscribed with nobody reading it. */
async function acquireSubscription(db: object): Promise<SubscriptionRef> {
  const state = stateFor(db)
  state.refs++
  let released = false
  const ref: SubscriptionRef = {
    release() {
      if (released) return // idempotent: releasing twice must not drop somebody else's ref
      released = true
      state.refs--
      // Teardown is nobody's read to fail: a transport that cannot detach is reported, not thrown at a
      // caller who has already finished with its live query.
      reconcile(db, state).catch(reportUnsubscribeFailure)
    },
  }
  try {
    await reconcile(db, state)
  } catch (error) {
    ref.release()
    throw error
  }
  return ref
}

/** Drive the transport toward the current refcount. Every transition is chained onto the previous one, and
 *  that chaining is the whole point: a re-subscribe can never start before the unsubscribe it follows has
 *  detached, so an old callback and a new one never both receive the same batch — which for a precise row
 *  delta would be a double-apply, not a harmless duplicate. */
function reconcile(db: object, state: SubscriptionState): Promise<void> {
  state.settling++
  const next = state.transition.then(
    () => step(db, state),
    () => step(db, state), // one failed transition must not wedge the chain behind it
  )
  state.transition = next
  // The counter is decremented by the handler, not by whoever awaits: an unobserved transition still has
  // to settle, or a db would look permanently mid-transition because nobody happened to await its release.
  return next.finally(() => {
    state.settling--
  })
}

/** Nobody wants this db's change subscription, no listener is attached, nothing is left whose detachment we
 *  could not confirm, and no transition is still in flight. That is the only boundary at which the db's
 *  change transport can be swapped without stranding a listener or a readiness proof — see
 *  `configureChanges`. It is also the point at which `seen` is provably empty (a confirmed detach clears it,
 *  and a failed one leaves `undetached` set), so a rotated-in transport cannot inherit a stale sequence
 *  baseline and drop a live message as a duplicate. */
function isQuiescent(db: object): boolean {
  const state = subscriptions.get(db)
  if (!state) return true // never had a live read at all
  return state.refs === 0 && !state.active && !state.undetached && state.settling === 0
}

/** One transition toward the desired state. Anything that changed the refcount during an await has already
 *  queued its own step behind this one, so a single reconciliation per call is enough. */
async function step(db: object, state: SubscriptionState): Promise<void> {
  // FAIL CLOSED on an unconfirmed detach. A previous unsubscribe rejected, so its callback may well still be
  // attached; subscribing now would leave two listeners on one topic and apply the next precise batch TWICE.
  // Retry the detach first (the contract requires unsubscribe to be idempotent) and let a still-failing one
  // throw — which rejects the live read waiting on it, rather than admitting one onto a doubled listener.
  if (state.undetached) {
    await state.undetached.unsubscribe()
    state.undetached = undefined
  }
  if (state.refs > 0 && !state.active) {
    state.active = await transportFor(db).subscribe(changeTopicFor(db), (payload) => receive(db, state, payload))
    return
  }
  if (state.refs === 0 && state.active) {
    const active = state.active
    state.active = undefined // `active` means a subscription we intend to keep; from here we are letting go of it
    try {
      await active.unsubscribe()
      // Detached: we are no longer following anyone's stream, so the per-origin sequence state is stale.
      // Dropping it also bounds it — a long-lived process does not accumulate an entry per peer that ever
      // restarted. Re-subscribing sees every origin as first-seen and coarsens, which is the sound reading.
      state.seen.clear()
    } catch (error) {
      state.undetached = active // its listener's status is now UNKNOWN — block subscribing until it is not
      throw error
    }
  }
}

function reportUnsubscribeFailure(error: unknown): void {
  console.error(
    '[telefunc] live: detaching the change subscription failed, so its listener may still be attached. Live reads on this db FAIL CLOSED until the transport confirms detachment (each one retries it); admitting them would risk applying a change twice.',
    error,
  )
}

/** One delivered payload. Decodes, drops what is not ours, orders what is, and feeds the rest into this
 *  db's graphs. */
function receive(db: object, state: SubscriptionState, payload: string): void {
  const envelope = decodeChangePayload(payload)
  if (!envelope) {
    // Unreadable — an unknown codec version, or a payload the transport mangled. `origin` is unreadable
    // too, so this may even be our own echo: coarsening costs a redundant refetch, while applying a guessed
    // row would be wrong at any price.
    console.error(
      `[telefunc] live: an undecodable payload arrived on "${changeTopicFor(db)}" (unknown codec version, or a transport that does not deliver the exact string it was given). Coarsening every watched table rather than interpreting it.`,
    )
    coarsenWatched(db)
    return
  }
  // ANOTHER DATABASE's changes. Only reachable through an adapter that delivers beyond the topic it was
  // given; dropping is right, because a foreign database's rows say nothing about ours — coarsening on them
  // would be a spurious refetch, and applying them would be a wrong row.
  if (envelope.namespace !== namespaceFor(db)) return
  if (envelope.origin === publisherOf(db).origin) return // our own batch — fed directly in ingestWrite

  // ORDERING. The adapter owes us at-least-once and nothing more, so a payload here may be a duplicate, may
  // be out of order, and may follow a gap. `seq` is contiguous per origin, which makes all three decidable:
  //   seq === expected  → in order, apply precisely
  //   seq  >  expected  → something was lost or is late; we cannot know what, so COARSEN and move the
  //                       watermark up. Anything below it is then already covered.
  //   seq <=  last      → a duplicate, or a straggler the coarsen above already accounted for → DROP
  //
  // DEFERRED BASELINE. A receiver that subscribes — or RE-subscribes, which clears the watermarks — has no
  // position for a publisher already running, and cannot tell "this is simply my next one" from "this
  // overtook the one before it". Coarsening pre-emptively for that would pay on EVERY resubscribe against a
  // busy peer, and `graph.coarsen()` is terminal — the graphs alive at that moment would stay coarse for the
  // rest of their lives. So instead the first message from an unknown origin is taken PRECISELY, and the
  // sequences below it are recorded as unaccounted-for (`unknownBelow`).
  //
  // That bet rests on the adapter contract's NO-BACKLOG clause (changeTransport.ts): a subscription only
  // receives what was published after it was admitted. Given that, the sequences below the first one we see
  // fall into exactly two cases:
  //   - published BEFORE our admission → the readiness barrier means our snapshot was read after admission,
  //     so they are already IN the data, and they will never be delivered to us. Nothing to apply.
  //   - published AFTER admission → the adapter owes us at-least-once, so each one MUST still be delivered.
  //     It arrives late and below the watermark, and THAT is the signal we coarsen on.
  // A reorder is therefore always eventually observable, so we can wait for proof instead of guessing.
  //
  // A REPLAY/BACKLOG transport breaks this, which is why the clause is written down rather than assumed: a
  // pre-admission payload delivered after admission is already in the snapshot, and taking it as the
  // baseline would apply it a second time. It is indistinguishable from a legitimate first message — an
  // unknown origin's opening sequence looks the same either way — so there is no cheap runtime detection to
  // fall back on, and the obligation is carried by the contract and pinned against the shipped default
  // transport instead. (Once an origin HAS a watermark, a late pre-admission payload is a straggler and
  // coarsens like any other; only the very first message from an origin is exposed.)
  //
  // The cost of a wrong bet, stated honestly: between the precise baseline and the straggler's arrival, a
  // precise graph is serving a result that is missing one delta — briefly INCORRECT BY OMISSION, not merely
  // stale — and the straggler's coarsen corrects it. Same class as the documented drop-after-readiness
  // limit, and unlike that one it closes deterministically rather than waiting for the next write.
  //
  // THE INVARIANT: a change is applied at most once; anything not applied precisely is over-fired; and any
  // sequence skipped by a baseline bet is either already in the snapshot or still owed to us by the adapter.
  const tracked = state.seen.get(envelope.origin)
  if (tracked === undefined) {
    // The bet. Everything below this sequence is unaccounted for until it either proves irrelevant (never
    // arrives, because it predates admission) or arrives as a straggler.
    state.seen.set(envelope.origin, { last: envelope.seq, unknownBelow: envelope.seq })
  } else if (envelope.seq <= tracked.last) {
    // At or below the watermark: either a sequence we skipped, or a redelivery of one we already handled.
    // `unknownBelow` is what tells them apart — a duplicate is not evidence of anything and must not coarsen.
    if (envelope.seq >= tracked.unknownBelow) return // already seen → duplicate → drop
    tracked.unknownBelow = 0 // the bet was wrong; one coarsen covers the whole unaccounted region
    coarsenWatched(db)
    return
  } else if (envelope.seq !== tracked.last + 1) {
    // A real gap. Applying this before the ones it overtook would apply a delta out of order, so coarsen —
    // which also accounts for everything below it, closing any outstanding bet.
    tracked.last = envelope.seq
    tracked.unknownBelow = 0
    coarsenWatched(db)
    return
  } else {
    tracked.last = envelope.seq
  }
  if ('coarseAll' in envelope) {
    // A mutation whose touched tables are unknowable happened on ANOTHER instance: coarsen every table WE
    // watch. Sound over-fire; never a fabricated row.
    //
    // This also CLOSES any outstanding baseline bet. Coarsening rebuilds every watched graph from the
    // database, which accounts for every lower sequence as surely as a gap does — so a later straggler must
    // not trigger a second one. Leaving the bet open would buy a redundant reseed at best, and at worst a
    // terminal demotion, if that straggler landed while this reseed was still in flight.
    state.seen.get(envelope.origin)!.unknownBelow = 0
    coarsenWatched(db)
    return
  }
  // One publication, one ingest — there are no sibling copies to reconcile against.
  registryFor(db).router.ingest({ changes: envelope.changes })
}

/** Coarsen every table this db currently watches — the sound response to a change whose reach we cannot
 *  read. Nothing watched, nothing to do. */
function coarsenWatched(db: object): void {
  const watched = registryFor(db).router.watchedTables()
  if (watched.length === 0) return
  registryFor(db).router.ingest({ changes: watched.map((table) => ({ table, kind: 'coarse' as const })) })
}
