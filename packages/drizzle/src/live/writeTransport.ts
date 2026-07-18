export { publishBatch, publishCoarseAll, acquireSubscription, CHANGE_TOPIC }
export type { SubscriptionRef }

import { randomUUID } from 'node:crypto'
import { registryFor } from './dbRuntime.js'
import { type ChangeSubscription, transportFor } from './changeTransport.js'
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
//   - AT-MOST-ONCE per topic, which the adapter still owes: precise row deltas are not idempotent.
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

/** The one change topic for a logical database. Every runtime sharing a changeTransport exchanges changes
 *  on it; scoping several logical databases onto one transport is the adapter's namespacing concern. */
const CHANGE_TOPIC = '__live__:changes'

/** The publishing identity of a db — stable for its lifetime, used to drop our own echo. */
const dbIds = new WeakMap<object, string>()
function dbIdOf(db: object): string {
  let id = dbIds.get(db)
  if (!id) dbIds.set(db, (id = randomUUID()))
  return id
}

/** Publish a committed batch cross-instance: ONE message, carrying the whole batch. No-op for an empty
 *  batch. Receivers slice it per table themselves. */
function publishBatch(db: object, batch: ChangeBatch): void {
  if (batch.changes.length === 0) return
  const origin = dbIdOf(db)
  let payload: string
  try {
    payload = encodeChangePayload({ version: CHANGE_CODEC_VERSION, origin, changes: batch.changes })
  } catch (error) {
    // A value the codec cannot carry must not cost the invalidation itself: fall back to the value-free
    // coarse-all envelope, so remote graphs refetch instead of never hearing about the write.
    console.error(
      '[telefunc] live: a change batch could not be encoded for the transport, so a COARSE invalidation was published instead. Remote live queries over-fetch rather than miss the write.',
      error,
    )
    payload = encodeChangePayload({ version: CHANGE_CODEC_VERSION, origin, coarseAll: true })
  }
  publishPayload(db, payload)
}

/** Announce a mutation whose touched tables are UNKNOWABLE (raw SQL, batch) to every OTHER instance: each
 *  coarsens its own watched tables, since the publisher cannot know them. Rides the SAME topic — the
 *  separate wildcard channel it used to need was an artefact of per-table fan-out. The publisher's own
 *  graphs are fed directly, and the `origin` check makes its own subscription drop this. */
function publishCoarseAll(db: object): void {
  publishPayload(db, encodeChangePayload({ version: CHANGE_CODEC_VERSION, origin: dbIdOf(db), coarseAll: true }))
}

/** Hand a payload to the transport. The write that produced it has ALREADY COMMITTED, so a transport
 *  failure — thrown outright, or a rejection from an async client that publishes over a socket — is
 *  reported and dropped. It must never reject the caller's write (which succeeded) and must never surface
 *  as an unhandled rejection. */
function publishPayload(db: object, payload: string): void {
  try {
    const published = transportFor(db).publish(CHANGE_TOPIC, payload)
    if (isThenable(published)) published.then(undefined, reportPublishFailure)
  } catch (error) {
    reportPublishFailure(error)
  }
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
 *  handle if there is one, and the chain that serializes every transition between those two facts. */
type SubscriptionState = {
  refs: number
  active: ChangeSubscription | undefined
  transition: Promise<void>
}

const subscriptions = new WeakMap<object, SubscriptionState>()

function stateFor(db: object): SubscriptionState {
  let state = subscriptions.get(db)
  if (!state) subscriptions.set(db, (state = { refs: 0, active: undefined, transition: Promise.resolve() }))
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
  const next = state.transition.then(
    () => step(db, state),
    () => step(db, state), // one failed transition must not wedge the chain behind it
  )
  state.transition = next
  return next
}

/** One transition toward the desired state. Anything that changed the refcount during an await has already
 *  queued its own step behind this one, so a single reconciliation per call is enough. */
async function step(db: object, state: SubscriptionState): Promise<void> {
  if (state.refs > 0 && !state.active) {
    state.active = await transportFor(db).subscribe(CHANGE_TOPIC, (payload) => receive(db, payload))
    return
  }
  if (state.refs === 0 && state.active) {
    const active = state.active
    state.active = undefined // `active` means a subscription we intend to keep; from here we are letting go of it
    await active.unsubscribe()
  }
}

function reportUnsubscribeFailure(error: unknown): void {
  console.error(
    '[telefunc] live: detaching the change subscription failed. Nothing is stale as a result; the runtime will subscribe again for the next live read.',
    error,
  )
}

/** One delivered payload. Decodes, drops our own echo, and feeds the rest into this db's graphs. */
function receive(db: object, payload: string): void {
  const envelope = decodeChangePayload(payload)
  if (!envelope) {
    // Unreadable — an unknown codec version, or a payload the transport mangled. `origin` is unreadable
    // too, so this may even be our own echo: coarsening costs a redundant refetch, while applying a guessed
    // row would be wrong at any price.
    console.error(
      `[telefunc] live: an undecodable payload arrived on "${CHANGE_TOPIC}" (unknown codec version, or a transport that does not deliver the exact string it was given). Coarsening every watched table rather than interpreting it.`,
    )
    coarsenWatched(db)
    return
  }
  if (envelope.origin === dbIdOf(db)) return // our own batch — these graphs were fed directly in ingestWrite
  if ('coarseAll' in envelope) {
    // A mutation whose touched tables are unknowable happened on ANOTHER instance: coarsen every table WE
    // watch. Sound over-fire; never a fabricated row.
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
