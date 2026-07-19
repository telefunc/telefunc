export type { ChangeTransport, ChangeSubscription }
export {
  defaultChangeTransport,
  createInMemoryChangeTransport,
  transportFor,
  setChangeTransport,
  setChangeNamespace,
  configureChanges,
  namespaceFor,
}

import { randomUUID } from 'node:crypto'

/**
 * The transport the Live feature fans captured writes out over — DEDICATED to Live and configured on the
 * Live side (`reactiveDrizzle(db, { changeTransport })`), INDEPENDENT of the user's app-level
 * `config.broadcast.transport`. (Owner correction: never route Live change traffic through the user's app
 * Broadcast by default — a user must be able to run, e.g., Redis for their app Broadcast and something else
 * for Live.) It mirrors the Broadcast publish/subscribe SHAPE.
 *
 * DEPLOYMENT TRUTH — assume NO cross-server magic from the default: it fans out **in-process only** (one
 * Node process, see below). A multi-process deployment MUST inject a shared, transport-backed one, or a
 * write on one server will not reach a live query on another. Telefunc's own default Broadcast adapter is
 * likewise process-local — nothing cross-process is silently provided here.
 *
 * The payload is an OPAQUE STRING the package encodes and decodes itself (see `changeCodec.ts`), so a
 * transport is never asked to preserve the JS value domain a SQL row is made of — BigInt, Date, byte
 * arrays. Deliver the exact string it was given; parsing, rewriting or re-encoding it is the one way an
 * adapter can corrupt a precise row delta.
 *
 * SCOPE — one transport can carry several LOGICAL DATABASES, so every topic and every envelope is
 * namespaced by database identity (`changeNamespace`). An adapter is not asked to namespace anything; the
 * package does it, because leaving it to the adapter is how two unrelated databases with a `users` table
 * end up applying each other's row deltas.
 *
 * CONTRACT — a custom transport MUST honor all three, or the Live guarantees silently weaken:
 *  - **`subscribe()` resolves only once the subscription is ADMITTED** — for Redis, after the broker's
 *    `SUBSCRIBE` acknowledgement, not when the command is written. Resolution IS the readiness proof a live
 *    read waits on: it asserts that publications made after it can be observed. A transport that resolves
 *    early re-opens the window where a remote write lands before anyone is listening; one that rejects (or
 *    never resolves) fails the live read CLOSED, which is the honest outcome.
 *  - **`unsubscribe()` detaches the listener**, and its promise (if it returns one) resolves only once it
 *    HAS. The runtime serializes teardown against re-subscription on that promise, so an early resolve can
 *    leave an old and a new listener overlapping — and a precise batch delivered to both is applied twice.
 *    A REJECTION is read as "detachment unconfirmed", not as "detached anyway": the runtime then refuses to
 *    subscribe again and fails live reads on that db closed until a retry succeeds. So `unsubscribe()` must
 *    be **IDEMPOTENT** — the runtime calls it again after a failure, and calling it on an already-detached
 *    subscription must resolve rather than throw, or that db never recovers. (Redis `UNSUBSCRIBE` is
 *    naturally idempotent; an adapter that tracks its own listeners should treat "not subscribed" as
 *    success.)
 *  - **Deliver each published payload to each subscriber AT LEAST ONCE, and deliver it verbatim.** Neither
 *    de-duplication nor FIFO ordering is asked of you: the envelope carries the publisher's identity and a
 *    monotonic sequence, and the runtime drops duplicates and coarsens on a detected gap or reorder. This
 *    contract used to demand at-most-once and say nothing about order — which was the wrong way round, since
 *    an adapter obeying it to the letter could still deliver update B before update A and corrupt a precise
 *    graph exactly as a duplicate would.
 *  - **NO BACKLOG: a subscription receives only what was published AFTER its `subscribe()` resolved.**
 *    Store-and-forward and replay-from-offset transports are unsupported. This is ordinary pub/sub semantics
 *    — in-process and Redis both behave this way — and the runtime depends on it: a live read's snapshot is
 *    taken after admission, so anything published before that is ALREADY in the data it read. Replaying such
 *    a payload afterwards would apply it a second time, and the first message from a publisher is
 *    indistinguishable from a legitimate one, so the runtime cannot detect it and fail closed. (An earlier
 *    version of this contract relied on the same property without writing it down.)
 *
 * `publish()` may return a promise (real clients publish asynchronously). It is NOT awaited by the write
 * that produced it — the database has already committed by then — but a rejection is reported rather than
 * swallowed. Self-delivery is not required: a publisher's own graphs are fed directly, and an echo that
 * does come back is dropped by origin.
 */
interface ChangeTransport {
  publish(topic: string, payload: string): void | Promise<void>
  subscribe(topic: string, onPayload: (payload: string) => void): Promise<ChangeSubscription>
}

/** A live subscription's teardown handle — the only thing `subscribe()` hands back. */
interface ChangeSubscription {
  unsubscribe(): void | Promise<void>
}

/**
 * A dedicated in-process pub/sub — the zero-setup default. It is a DEDICATED channel-space for Live change
 * traffic, separate from the global app Broadcast. Multiple `reactiveDrizzle(...)` calls in one process
 * share one instance (see `defaultChangeTransport`), so two in-process db instances see each other's writes
 * — the multi-instance path, exercised without any external infra. Real multi-process fan-out injects a
 * transport-backed one.
 *
 * It carries the encoded payload like any other transport rather than passing live objects by reference:
 * the default is then the same code path a Redis adapter runs, so a value the codec cannot carry surfaces
 * in development instead of on the first cross-server deployment.
 */
function createInMemoryChangeTransport(): ChangeTransport {
  const topics = new Map<string, Set<(payload: string) => void>>()
  return {
    publish(topic, payload) {
      const subscribers = topics.get(topic)
      if (!subscribers) return
      // Snapshot the set: a subscriber that (un)subscribes DURING dispatch must not perturb this delivery.
      for (const onPayload of [...subscribers]) onPayload(payload)
    },
    async subscribe(topic, onPayload) {
      let subscribers = topics.get(topic)
      if (!subscribers) topics.set(topic, (subscribers = new Set()))
      subscribers.add(onPayload)
      return {
        unsubscribe() {
          const set = topics.get(topic)
          if (!set) return
          set.delete(onPayload)
          if (set.size === 0) topics.delete(topic)
        },
      }
    },
  }
}

/** The shared process-wide default — one dedicated in-process Live bus for every db that doesn't inject its
 *  own transport. Shared (not per-db) so two in-process instances fan out to each other. */
const defaultChangeTransport: ChangeTransport = createInMemoryChangeTransport()

// SET-ONCE, ENFORCED. Subscriptions and their proven-listening readiness are established against whichever
// transport a db RESOLVED, so a later swap would leave readiness proven on a transport nobody is listening on
// (remote writes silently missed). The resolution is therefore FROZEN at first use — including when the
// resolution is the DEFAULT — and a later, different transport throws instead of being silently ignored.
const configured = new WeakMap<object, ChangeTransport>() // explicit override, registered before first use
const resolved = new WeakMap<object, ChangeTransport>() // frozen at first use (an override OR the default)

const SWAP_MESSAGE =
  'telefunc live: the change transport for this db is already in use and cannot be replaced. `changeTransport` is set-once per db (the default counts as a resolution) — pass the same transport instance on every reactiveDrizzle(db, …) call for this db, or use a distinct db instance.'

/** Reject a transport that conflicts with this db's existing one, WITHOUT recording anything. */
function checkChangeTransport(db: object, transport: ChangeTransport | undefined): void {
  if (!transport) return
  const frozen = resolved.get(db)
  if (frozen !== undefined && frozen !== transport) throw new Error(SWAP_MESSAGE) // resolved (incl. default)
  const existing = configured.get(db)
  if (existing !== undefined && existing !== transport) throw new Error(SWAP_MESSAGE)
}

function setChangeTransport(db: object, transport: ChangeTransport | undefined): void {
  if (!transport) return
  checkChangeTransport(db, transport)
  if (resolved.get(db) !== undefined) return // already resolved to this same transport — nothing to record
  configured.set(db, transport)
}

/** Install a db's whole change configuration ATOMICALLY: validate every part first, then commit. Setting
 *  them one at a time can install the transport and then throw on the namespace, leaving a db configured
 *  with a transport its identity does not match. */
function configureChanges(db: object, options: { transport?: ChangeTransport; namespace?: string } = {}): void {
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
// SET-ONCE, like the transport and for the same reason: subscriptions are admitted on the topic a db
// RESOLVED, so changing the identity afterwards would move future publications to a new topic while the
// live listener stays on the old one — a systematic miss rather than an over-fire.

const configuredNamespaces = new WeakMap<object, string>() // explicit, registered before first use
const resolvedNamespaces = new WeakMap<object, string>() // frozen at first use (explicit OR derived)
const derivedNamespaces = new WeakMap<object, string>() // per identity owner, shared by sibling dbs

const NAMESPACE_SWAP_MESSAGE =
  'telefunc live: the change namespace for this db is already in use and cannot be replaced. `changeNamespace` identifies the logical database on a shared transport (a derived one counts as a resolution) — pass the same value on every reactiveDrizzle(db, …) call for this db, or use a distinct db instance.'

/** Reject a namespace that conflicts with this db's existing one, WITHOUT recording anything. Splitting
 *  the check from the commit is what lets `reactiveDrizzle` validate the whole configuration before it
 *  installs any of it — otherwise a transport could be installed and then a namespace throw, leaving the db
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
