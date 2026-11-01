export type {
  Backend,
  BackendPayload,
  BroadcastDriver,
  BroadcastPayload,
  BroadcastRoute,
  PublishResult,
  Subscriber,
  Unsubscribe,
}

import type { BroadcastKind } from '../wire/payloads.js'
import type { OrderingInfo } from '../wire/orderingFrame.js'

/** A Broadcast route. The text and binary routes of one key share one order: one `seq` counts both. */
type BroadcastRoute<Kind extends BroadcastKind = BroadcastKind> = { key: string; kind: Kind }

/** What a route carries: a text route the serialized message, a binary route its bytes. */
type BroadcastPayload<Kind extends BroadcastKind = BroadcastKind> = { text: string; binary: Uint8Array }[Kind]

/** What `installBackend()` installs: Broadcast's driver, and the driver of each other plane it serves. */
type Backend = BroadcastDriver & {
  /** The planes this backend serves, by extension key; each is built on its first `backendPlane()` call, given the
   *  `kit` the extension passes it. */
  readonly planes?: { readonly [key: symbol]: (kit: never) => unknown }
}

/** What a subscription delivers: a string on a text route, bytes on a binary route or a Room lane. */
type BackendPayload = string | Uint8Array

/** A publish's ordering, and how many subscribers the backend handed it to, counted its own way (absent where it
 *  can't tell). */
type PublishResult = OrderingInfo & { receivers?: number }

/**
 * What a backend implements for Broadcast.
 *
 * An ordering's `seq` is a positive safe integer that grows with every publish on a key, of either kind; its
 * `timestamp` is a non-negative safe integer (ms) that never goes back on a key.
 */
type BroadcastDriver = {
  /** Publishes `payload` on `route` and returns its ordering. A publish this process makes after `subscribe()`
   *  returned reaches that subscription: if your publishes and subscribes travel by different connections, hold a
   *  publish until your own new subscriptions on its route are ready, as the Redis driver does. The caller may reuse
   *  `payload`'s bytes once this returns: copy what you keep. */
  publish<Kind extends BroadcastKind>(
    route: BroadcastRoute<Kind>,
    payload: BroadcastPayload<Kind>,
  ): PublishResult | Promise<PublishResult>
  /** Subscribes this host to `route` and tells `subscriber` how it goes. Core calls it once per route and host (the
   *  process; a session Durable Object on Cloudflare), however many consumers share it. Throw only for the caller's
   *  mistake, which reaches the caller; report a refusal with `subscriber.failed(error)`. */
  subscribe(route: BroadcastRoute, subscriber: Subscriber): Unsubscribe
  /** Only for a driver that fixes a publish's recipients as it publishes (memory): the `seq` of `key`'s last
   *  publish, so a consumer joining a live subscription skips what was published before it. */
  lastSeq?(key: string): number
}

/**
 * Core's end of one driver subscription. Call `ready()` once every publish made from now on reaches it, `lost()` when
 * it may miss messages (its connection dropped), `ready()` again once it can't, and `ended()` or `failed()` when it is
 * over for good. Deliver each key's messages in `seq` order with `message()`. Core ignores a call that repeats the
 * state, and every call after `ended()`, `failed()` or its unsubscribe.
 */
interface Subscriber {
  message(payload: BackendPayload, ordering: OrderingInfo): void
  ready(): void
  lost(): void
  /** The backend ended it on purpose (a Room incarnation was dropped), which its consumers expect. */
  ended(): void
  /** It failed for good; core reports `error` once, however many consumers share it. */
  failed(error: unknown): void
  /** How many consumers share this subscription now. */
  readonly consumers: number
}

/** Ends a subscription and settles once the driver's own cleanup did. Core calls it at most once, and never after
 *  `ended()` or `failed()`; the driver doesn't call either from it. */
type Unsubscribe = () => Promise<void>
