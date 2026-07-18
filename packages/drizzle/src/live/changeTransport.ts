export type { ChangeTransport, ChangeMessage }
export { defaultChangeTransport, createInMemoryChangeTransport, transportFor, setChangeTransport }

import type { TableChange } from '../router/events.js'

/**
 * A message on the db's change topic (`__live__:changes`): either a committed write BATCH or a readiness
 * PROBE (a unique token the publisher awaits back to prove its OWN subscription is listening — see the
 * readiness barrier in writeTransport.ts). The two are discriminated structurally (`'probe' in message`).
 *
 * A batch carries `origin` (the publishing db's identity) and the changes themselves. There is exactly ONE
 * publication per committed batch, so a receiver simply drops its own echo and ingests everything else — no
 * batch ids, no apply-once reconciliation. See `writeTransport.ts`.
 */
type ChangeMessage =
  | { origin: string; changes: TableChange[] }
  | { origin: string; coarseAll: true }
  | { probe: string }

/**
 * The transport the Live feature fans captured writes out over — DEDICATED to Live and configured on the
 * Live side (`reactiveDrizzle(db, { changeTransport })`), INDEPENDENT of the user's app-level
 * `config.broadcast.transport`. (Owner correction: never route Live change traffic through the user's app
 * Broadcast by default — a user must be able to run, e.g., Redis for their app Broadcast and something else
 * for Live.) It mirrors the Broadcast publish/subscribe SHAPE; the default is an in-process dedicated
 * channel-space (below), and a multi-process deployment injects a transport-backed implementation.
 *
 * DEPLOYMENT TRUTH — assume NO cross-server magic from the default: the built-in default fans out
 * **in-process only** (one Node process). A multi-process / multi-server deployment MUST inject a shared,
 * transport-backed ChangeTransport (Redis/Postgres-backed, or telefunc's Broadcast wired to a real
 * transport) via `reactiveDrizzle(db, { changeTransport })`, or a write on one server will not reach a live
 * query on another. (Telefunc's own default Broadcast adapter is likewise process-local — nothing
 * cross-process is silently provided here.)
 *
 * CONTRACT — a custom transport MUST honor both, or the Live guarantees silently weaken:
 *  - **Self-delivery (loopback).** A message published to a topic is delivered to EVERY current subscriber
 *    of that topic, INCLUDING the publisher's own subscription. The readiness barrier proves a subscription
 *    is listening by publishing a probe and awaiting that SAME probe back; a transport that does NOT loop a
 *    publisher's own messages back makes the probe time out → the live read FAILS CLOSED (rejects) rather
 *    than admitting a read that could silently miss remote writes. (Redis pub/sub and the in-memory default
 *    both self-deliver.)
 *  - **Raw reads over-invalidate.** A raw statement run on the REACTIVE db (`db.run(sql`…`)`) coarsens every
 *    watched table, because its touched tables are unknowable without parsing SQL — including a raw SELECT.
 *    Run raw READS on the base db you still hold if you don't want that (the reactive db is only needed for
 *    `.live()` and captured writes).
 *  - **At-most-once delivery per topic.** A transport MUST NOT redeliver the same published message to the
 *    same subscriber on the same topic. Precise row application is NOT idempotent (a stateful aggregate would
 *    count the same delta twice). A transport that can redeliver must deduplicate internally before calling
 *    the subscriber.
 *    (There is no cross-topic timing requirement. An earlier design published one copy of a batch per
 *    touched table and asked adapters to bound the skew between those copies to 30 seconds; a single
 *    publication per batch removes both the duplicates and the guarantee they needed.)
 *  - **Structure preservation.** Change rows carry ordinary SQL values — BigInt, Date, byte arrays, NULL,
 *    composite keys — so a serializing transport MUST round-trip these faithfully (plain `JSON` does not:
 *    it throws on BigInt and drops Date/byte types). The in-memory default delivers by reference (same
 *    process, no serialization); subscribers treat a delivered message as read-only.
 */
interface ChangeTransport {
  publish(topic: string, message: ChangeMessage): void
  subscribe(topic: string, onMessage: (message: ChangeMessage) => void): () => void
}

/**
 * A dedicated in-process pub/sub — the zero-setup default. It is a DEDICATED channel-space for Live change
 * traffic, separate from the global app Broadcast, and mirrors Broadcast's in-memory fan-out (delivers by
 * reference, no serialization). Multiple `reactiveDrizzle(...)` calls in one process share one instance (see
 * `defaultChangeTransport`), so two in-process db instances see each other's writes — the multi-instance
 * path, exercised without any external infra. Real multi-process fan-out injects a transport-backed one.
 */
function createInMemoryChangeTransport(): ChangeTransport {
  const topics = new Map<string, Set<(message: ChangeMessage) => void>>()
  return {
    publish(topic, message) {
      const subscribers = topics.get(topic)
      if (!subscribers) return
      // Snapshot the set: a subscriber that (un)subscribes DURING dispatch must not perturb this delivery.
      for (const onMessage of [...subscribers]) onMessage(message)
    },
    subscribe(topic, onMessage) {
      let subscribers = topics.get(topic)
      if (!subscribers) topics.set(topic, (subscribers = new Set()))
      subscribers.add(onMessage)
      return () => {
        const set = topics.get(topic)
        if (!set) return
        set.delete(onMessage)
        if (set.size === 0) topics.delete(topic)
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
