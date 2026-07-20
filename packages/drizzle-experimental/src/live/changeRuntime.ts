export { configureChangeRuntime, setChangeTransport, setChangeNamespace }
export { transportFor, namespaceFor, changeTopicFor }
export { publishBatch, publishCoarseAll, acquireSubscription, isQuiescent }
export type { SubscriptionRef }

import { randomUUID } from 'node:crypto'
import { report } from './captureReport.js'
import { registryFor } from './dbRuntime.js'
import { CHANGE_CODEC_VERSION, decodeChangePayload, encodeChangePayload } from './changeCodec.js'
import { type ChangeSubscription, type ChangeTransport, defaultChangeTransport } from './changeTransport.js'
import { type OriginSequence, coarsenClosingBet, observeEnvelopeSequence } from './changeSequence.js'
import type { ChangeBatch } from '../router/events.js'

// THE CHANGE RUNTIME for one logical database — everything that is true of a db's presence on the change
// bus, in one place: which transport and namespace it resolved, what it has published, who is subscribed on
// its behalf, whether it is quiescent, and what to do with each delivery.
//
// ONE quiescence rule, read from this module's own state, because every entry point that can rotate a
// transport has to agree about when that is admissible. Splitting the transport config from the runtime puts
// the two on opposite sides of a boundary: the caller then has to read quiescence out of one and hand it to
// the other, which makes a private state machine's internals part of the calling convention and lets the two
// entry points drift into contradictory contracts.
//
// The deletion test settles the merge: remove this abstraction and its state and reconciliation logic
// reappear across configuration, publish and subscription callers alike.
//
// A committed batch is published ONCE, to ONE topic per logical database, over the db's DEDICATED
// changeTransport (never the user's app Broadcast — see changeTransport.ts). A receiver feeds the whole
// batch into its own graphs via `router.ingest`; the router already slices it per table and notifies each
// affected graph at most once, so one message is one atomic graph tick.
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
//   - ORDERING, which the RUNTIME owns rather than the adapter: every envelope carries the publisher's
//     origin and a monotonic seq, so a duplicate is dropped and a gap or reorder coarsens. The rules
//     themselves are a pure transition — see changeSequence.ts. The adapter owes at-least-once delivery of
//     the exact payload and nothing more.
//
// NAMESPACED per logical database (topic and envelope both). One process-wide default bus with a constant
// topic meant two unrelated databases sharing a table name applied each other's ROW DELTAS — a wrong row in
// a precise graph, not an over-fire.
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

// ── which transport this db resolved ───────────────────────────────
//
// SET-ONCE WHILE IN USE. Subscriptions and their proven-listening readiness are established against whichever
// transport a db RESOLVED, so swapping one out from under them would leave readiness proven on a transport
// nobody is listening on (remote writes silently missed). The resolution is therefore frozen while there is
// something to protect — and a later, different transport throws.
//
// QUIESCENCE IS THE WHOLE RULE, and it is read HERE rather than asserted by a caller. What the freeze
// protects is live subscriptions and the readiness they proved; once a db has no refs, no attached listener,
// nothing whose detachment went unconfirmed, and no transition still in flight (`isQuiescent`), there is no
// such thing left to strand. A db that has only ever PUBLISHED — resolving the default on a write, with no
// live read anywhere — is quiescent by that definition too, and is NOT frozen to the default forever on the
// strength of one write. Rotation there lets a client be replaced, or a test reset, without minting a new db
// object. Mid-subscription swaps stay forbidden, which is the case the freeze was written for.
const configured = new WeakMap<object, ChangeTransport>() // explicit override, registered before first use
const resolved = new WeakMap<object, ChangeTransport>() // frozen at first use (an override OR the default)

const SWAP_MESSAGE =
  'telefunc live: the change transport for this db is in use and cannot be replaced. `changeTransport` can only be swapped while the db is quiescent — no live query holding a subscription, and none still detaching (the default counts as a resolution). Pass the same transport instance on every reactiveDrizzle(db, …) call for this db, close its live queries before rotating, or use a distinct db instance.'

/** Reject a transport that conflicts with this db's existing one, WITHOUT recording anything. */
function checkChangeTransport(db: object, transport: ChangeTransport | undefined): void {
  if (!transport) return
  if (isQuiescent(db)) return // nothing subscribed, nothing detaching — no listener or readiness proof to strand
  const frozen = resolved.get(db)
  if (frozen !== undefined && frozen !== transport) throw new Error(SWAP_MESSAGE) // resolved (incl. default)
  // NOT redundant with the check above. A subscription makes the db non-quiescent SYNCHRONOUSLY (`refs++`,
  // `settling++`) while the `transportFor` that resolves it runs a microtask later — so there is a real
  // window in which a db is in use and has an override registered but no resolution yet. A rotation admitted
  // in that window would be resolved by the very subscription now being established. Pinned by test.
  const existing = configured.get(db)
  if (existing !== undefined && existing !== transport) throw new Error(SWAP_MESSAGE)
}

function setChangeTransport(db: object, transport: ChangeTransport | undefined): void {
  if (!transport) return
  checkChangeTransport(db, transport)
  if (resolved.get(db) === transport) return // already resolved to this same transport — nothing to record
  // A quiescent rotation to a DIFFERENT transport: unfreeze, so the next `transportFor` re-resolves to the
  // new one instead of handing back the old resolution.
  resolved.delete(db)
  configured.set(db, transport)
}

/** Install a db's whole change configuration ATOMICALLY: validate every part first, then commit. Setting
 *  them one at a time can install the transport and then throw on the namespace, leaving a db configured
 *  with a transport its identity does not match.
 *
 *  Whether a rotation is admissible is decided HERE, from this module's own subscription state — the caller
 *  neither knows nor shuttles it. The namespace is NOT rotatable on the same terms: it identifies the
 *  logical database itself, and changing it moves future publications to a topic that remote listeners are
 *  not on. */
function configureChangeRuntime(db: object, options: { transport?: ChangeTransport; namespace?: string } = {}): void {
  checkChangeTransport(db, options.transport)
  checkChangeNamespace(db, options.namespace)
  setChangeTransport(db, options.transport)
  setChangeNamespace(db, options.namespace)
}

/** The transport for a db — its registered override, else the shared in-process default — FROZEN on first
 *  call so every later subscription, publish and readiness proof refers to the same transport identity. */
function transportFor(db: object): ChangeTransport {
  let transport = resolved.get(db)
  if (!transport) {
    transport = configured.get(db) ?? defaultChangeTransport
    resolved.set(db, transport)
  }
  return transport
}

// ── logical-database identity ─────────────────────────────────────
//
// Which DATABASE a change belongs to. Every topic and envelope carries it, so a bus shared by several
// databases cannot cross-feed row deltas between them.
//
// Two drizzle objects over the SAME connection are the same logical database and MUST exchange changes —
// that is the ordinary two-runtime topology. Two drizzle objects over DIFFERENT databases must not, even
// when their tables are named alike. So the default identity is derived from the underlying client
// (`db.$client`), which is shared by the first pair and distinct for the second.
//
// That identity is an object, so it cannot span processes. A caller who injects a transport is by
// definition reaching other processes, and must supply a stable `changeNamespace` instead — see the
// assertion in reactiveDrizzle. Deriving one automatically would mean guessing at database authority, which
// is exactly the kind of guess this package refuses elsewhere.
//
// SET-ONCE, like the transport but WITHOUT the quiescent exception: subscriptions are admitted on the topic
// a db RESOLVED, so changing the identity afterwards would move future publications to a new topic while the
// live listener stays on the old one — a systematic miss rather than an over-fire. Quiescence cannot rescue
// that, because the identity is what REMOTE instances agree on, and their subscriptions are not observable
// from here.

const configuredNamespaces = new WeakMap<object, string>() // explicit, registered before first use
const resolvedNamespaces = new WeakMap<object, string>() // frozen at first use (explicit OR derived)
const derivedNamespaces = new WeakMap<object, string>() // per identity owner, shared by sibling dbs

const NAMESPACE_SWAP_MESSAGE =
  'telefunc live: the change namespace for this db is already in use and cannot be replaced. `changeNamespace` identifies the logical database on a shared transport (a derived one counts as a resolution) — pass the same value on every reactiveDrizzle(db, …) call for this db, or use a distinct db instance.'

/** Reject a namespace that conflicts with this db's existing one, WITHOUT recording anything. Splitting
 *  the check from the commit is what lets the whole configuration be validated before any of it is
 *  installed — otherwise a transport could be installed and then a namespace throw, leaving the db
 *  half-configured. */
function checkChangeNamespace(db: object, namespace: string | undefined): void {
  if (namespace === undefined) return
  const frozen = resolvedNamespaces.get(db)
  if (frozen !== undefined && frozen !== namespace) throw new Error(NAMESPACE_SWAP_MESSAGE)
  const existing = configuredNamespaces.get(db)
  if (existing !== undefined && existing !== namespace) throw new Error(NAMESPACE_SWAP_MESSAGE)
}

function setChangeNamespace(db: object, namespace: string | undefined): void {
  if (namespace === undefined) return
  checkChangeNamespace(db, namespace)
  configuredNamespaces.set(db, namespace)
}

/** The logical-database identity for a db: the caller's `changeNamespace` if there is one, else an identity
 *  derived from the connection this db runs on — FROZEN on first call, so every later topic, envelope and
 *  admitted subscription refers to the same database. */
function namespaceFor(db: object): string {
  let namespace = resolvedNamespaces.get(db)
  if (namespace === undefined) {
    namespace = configuredNamespaces.get(db) ?? derivedNamespaceFor(db)
    resolvedNamespaces.set(db, namespace)
  }
  return namespace
}

/** An identity shared by every db over the same connection. Drizzle clients are not all plain objects —
 *  `postgres-js` hands back its `sql` client as a FUNCTION — and a function is both a valid WeakMap key and
 *  a real client, so excluding it would silently split one database into two namespaces and drop every
 *  sibling invalidation. Anything that cannot key a WeakMap falls back to the db itself, which is the
 *  narrower choice: it can miss a sibling, but it can never cross-feed a stranger. */
function derivedNamespaceFor(db: object): string {
  const connection = (db as { $client?: unknown }).$client
  const identityOwner =
    (typeof connection === 'object' && connection !== null) || typeof connection === 'function'
      ? (connection as object)
      : db
  let derived = derivedNamespaces.get(identityOwner)
  if (!derived) derivedNamespaces.set(identityOwner, (derived = `local:${randomUUID()}`))
  return derived
}

/** The change topic for ONE logical database. Namespaced, so a transport shared by several databases keeps
 *  their streams apart rather than leaving that to every adapter to remember. */
function changeTopicFor(db: object): string {
  return `__live__:${namespaceFor(db)}:changes`
}

// ── publishing ──────────────────────────────────────────────────────

/** The publishing identity of a db — stable for its lifetime, used to drop our own echo. Its `seq` is that
 *  identity's monotonic publication counter: the receiver's only way to tell a duplicate from a reorder from
 *  a gap without demanding both properties from every adapter. */
type PublisherState = {
  origin: string
  seq: number
  chain: Promise<void>
  /** The transport the previous publication went out on, so a rotation is detectable at the next one. */
  transport: ChangeTransport | undefined
}
const publishers = new WeakMap<object, PublisherState>()
function publisherOf(db: object): PublisherState {
  let publisher = publishers.get(db)
  if (!publisher) {
    publishers.set(db, (publisher = { origin: randomUUID(), seq: 0, chain: Promise.resolve(), transport: undefined }))
  }
  return publisher
}

/** Publish a committed batch cross-instance: ONE message, carrying the whole batch. No-op for an empty
 *  batch. Receivers slice it per table themselves. */
function publishBatch(db: object, batch: ChangeBatch): void {
  if (batch.changes.length === 0) return
  // ONE resolution for the whole publication — see `nextHeader`. Encoding below runs user-controlled code
  // (a row's enumerable getter), which can rotate the transport re-entrantly; resolving again afterwards
  // would let the header and the send disagree about which era this publication belongs to.
  const transport = transportFor(db)
  const header = nextHeader(db, transport)
  let payload: string
  try {
    payload = encodeChangePayload({ ...header, changes: batch.changes })
  } catch (error) {
    // Fall back to the value-free coarse-all envelope, keeping the seq it already took — the stream must
    // stay contiguous or every receiver reads a gap.
    report('encode-failed', { cause: error })
    payload = encodeChangePayload({ ...header, coarseAll: true })
  }
  publishPayload(db, payload, transport)
}

/** Announce a mutation whose touched tables are UNKNOWABLE (raw SQL, batch) to every OTHER instance: each
 *  coarsens its own watched tables, since the publisher cannot know them. Rides the SAME topic — a separate
 *  wildcard channel is only needed where publication fans out per table. The publisher's own graphs are fed
 *  directly, and the `origin` check makes its own subscription drop this. */
function publishCoarseAll(db: object): void {
  const transport = transportFor(db)
  publishPayload(db, encodeChangePayload({ ...nextHeader(db, transport), coarseAll: true }), transport)
}

/** The envelope header for the next publication, taking this db's next sequence number — and marking an ERA
 *  CUT when this publication is the first to go out on a different transport than the last one did.
 *
 *  Only the publisher can know this. A receiver first-seeing an origin at seq 2 cannot tell whether seq 1
 *  went to a transport it was never on or is simply about to arrive, and its deferred baseline assumes the
 *  latter — correctly, right up until a rotation makes the earlier messages undeliverable rather than late.
 *  The FIRST publication ever is not a cut: nothing precedes seq 1, so there is nothing a receiver could be
 *  betting wrong about.
 *
 *  SINGLE RESOLUTION. `transport` is passed in rather than resolved here, because the caller must resolve it
 *  exactly ONCE and use that same instance for both the era comparison and the send. Encoding sits between
 *  those two points and runs user-controlled code — a row's enumerable getter, reachable from a custom
 *  decoder's returned value — which can synchronously rotate the transport. Resolving twice let the header
 *  decide "no cut" against the old transport while the send went to the new one: an unmarked publication on a
 *  transport whose receivers have no history, which is precisely the permanent hole `eraCut` exists to close.
 *  With one resolution the publication belongs wholly to the era it started in, and a rotation that lands
 *  mid-encode is attributed to the NEXT publication, which compares against this one and marks the cut. */
function nextHeader(
  db: object,
  transport: ChangeTransport,
): {
  version: number
  namespace: string
  origin: string
  seq: number
  eraCut?: true
} {
  const publisher = publisherOf(db)
  publisher.seq++
  const eraCut = publisher.transport !== undefined && publisher.transport !== transport
  publisher.transport = transport
  return {
    version: CHANGE_CODEC_VERSION,
    namespace: namespaceFor(db),
    origin: publisher.origin,
    seq: publisher.seq,
    ...(eraCut ? { eraCut: true as const } : {}),
  }
}

/** Hand a payload to the transport. The write that produced it has ALREADY COMMITTED, so a transport
 *  failure — thrown outright, or a rejection from an async client that publishes over a socket — is
 *  reported and dropped. It must never reject the caller's write (which succeeded) and must never surface
 *  as an unhandled rejection.
 *
 *  Publications are CHAINED per db so an async transport is handed them in sequence order. A receiver
 *  tolerates reordering by coarsening, so this is not what makes the system sound — it is what stops us
 *  manufacturing the reordering ourselves and paying the precision for it.
 *
 *  The transport is resolved at ENQUEUE, and not inside the chain callback: a publication belongs to the
 *  transport ERA of the write that produced it. The peers owed this message are the ones that were listening
 *  when the write committed, and they are reachable on the transport that was current then — so a rotation
 *  at a quiescent boundary must not drag an already-committed write's message onto a new transport whose
 *  subscribers were not there for it, while the old transport's listeners, who were, never hear it.
 *  Resolving late would do exactly that, silently, for any publish still queued.
 *
 *  The instance is handed in by the caller rather than resolved here, so that ONE resolution decides both the
 *  era this publication is stamped with and the transport it goes to — see `nextHeader`. (`topic` is safe to
 *  derive here: the namespace is frozen on first use, so unlike the transport it cannot change underneath.) */
function publishPayload(db: object, payload: string, transport: ChangeTransport): void {
  const publisher = publisherOf(db)
  const topic = changeTopicFor(db)
  publisher.chain = publisher.chain.then(() => {
    try {
      const published = transport.publish(topic, payload)
      if (isThenable(published))
        return published.then(undefined, (error: unknown) => report('publish-failed', { cause: error }))
    } catch (error) {
      report('publish-failed', { cause: error })
    }
    return undefined
  })
}

function isThenable(value: void | Promise<void>): value is Promise<void> {
  return typeof (value as Promise<void> | undefined)?.then === 'function'
}

// ── subscription lifetime ───────────────────────────────────────────

/** One owner's hold on this db's change subscription. Released when that owner is done — the read token it
 *  was minted for, or the channel lease that token redeemed into. */
type SubscriptionRef = { release(): void }

/** What the runtime knows about a db's presence on the change topic: how many owners want it, the live
 *  handle if there is one, the chain that serializes every transition between those two facts — and any
 *  handle whose detachment FAILED, whose listener is therefore of unknown status. */
type SubscriptionState = {
  refs: number
  active: ChangeSubscription | undefined
  /** Per publishing origin, the ordering position this db has reached — see changeSequence.ts. Cleared on
   *  detach, which both discards state that is stale and bounds what a long-lived process accumulates. */
  seen: Map<string, OriginSequence>
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
      reconcile(db, state).catch((error: unknown) => report('detach-failed', { cause: error }))
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
 *  `configureChangeRuntime`. It is also the point at which `seen` is provably empty (a confirmed detach
 *  clears it, and a failed one leaves `undetached` set), so a rotated-in transport cannot inherit a stale
 *  sequence baseline and drop a live message as a duplicate. */
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
      // restarted. Re-subscribing then sees every origin as FIRST-SEEN, which the automaton takes precisely
      // while recording an open baseline bet (changeSequence.ts) — not a coarsen. Keeping the watermarks
      // instead would judge live traffic against a position we no longer hold, and anything at or below one
      // is dropped as a duplicate: a silent missed invalidation rather than an over-fire.
      state.seen.clear()
    } catch (error) {
      state.undetached = active // its listener's status is now UNKNOWN — block subscribing until it is not
      throw error
    }
  }
}

// ── one delivered payload ───────────────────────────────────────────

/** DECODE → ADMIT SCOPE → CLASSIFY ORDERING → ROUTE. Each step answers one question, and the ordering rules
 *  it defers to are a pure transition (changeSequence.ts) rather than branches inlined into this callback. */
function receive(db: object, state: SubscriptionState, payload: string): void {
  const envelope = decodeChangePayload(payload)
  if (!envelope) {
    // Coarsening costs a redundant refetch, while applying a guessed row would be wrong at any price.
    report('undecodable-payload', { topic: changeTopicFor(db) })
    coarsenWatched(db)
    return
  }
  // ANOTHER DATABASE's changes. Only reachable through an adapter that delivers beyond the topic it was
  // given; dropping is right, because a foreign database's rows say nothing about ours — coarsening on them
  // would be a spurious refetch, and applying them would be a wrong row.
  if (envelope.namespace !== namespaceFor(db)) return
  if (envelope.origin === publisherOf(db).origin) return // our own batch — fed directly in ingestWrite

  const { decision, next } = observeEnvelopeSequence(state.seen.get(envelope.origin), envelope)
  if (next) state.seen.set(envelope.origin, next)
  if (decision === 'drop') return
  if (decision === 'coarsen') return coarsenWatched(db)

  if ('coarseAll' in envelope) {
    // A mutation whose touched tables are unknowable happened on ANOTHER instance: coarsen every table WE
    // watch. Sound over-fire; never a fabricated row. This envelope was ADMITTED in order, so the watermark
    // stays exactly where the transition just put it — but coarsening also closes any outstanding bet.
    state.seen.set(envelope.origin, coarsenClosingBet(state.seen.get(envelope.origin)!.last))
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
