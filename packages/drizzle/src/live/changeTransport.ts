export type { ChangeTransport, ChangeSubscription }
export { defaultChangeTransport, createInMemoryChangeTransport, transportFor, setChangeTransport }

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
 *  - **At-most-once delivery per topic.** A transport MUST NOT redeliver the same published message to the
 *    same subscriber on the same topic. Precise row application is NOT idempotent (a stateful aggregate
 *    would count the same delta twice). A transport that can redeliver must deduplicate internally before
 *    calling the subscriber.
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

function setChangeTransport(db: object, transport: ChangeTransport | undefined): void {
  if (!transport) return
  const frozen = resolved.get(db)
  if (frozen !== undefined) {
    if (frozen !== transport) throw new Error(SWAP_MESSAGE) // already resolved (incl. default) → refuse the swap
    return
  }
  const existing = configured.get(db)
  if (existing !== undefined && existing !== transport) throw new Error(SWAP_MESSAGE)
  configured.set(db, transport)
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
