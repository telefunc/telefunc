export { DriverAttempt, planeKey, servePlane }
export type {
  AttemptState,
  BackendListener,
  BackendPayload,
  BackendReceiver,
  BroadcastDriver,
  BroadcastPayload,
  BroadcastRoute,
  PlaneKey,
  PlaneOf,
  PlaneServer,
  PublishResult,
  SubscriptionDriver,
}

import type { BroadcastKind } from '../wire/payloads.js'
import type { OrderingInfo } from './driverOrdering.js'

/** A Broadcast route. The text and binary routes of one key share one order: one `seq` counts both. */
type BroadcastRoute<Kind extends BroadcastKind = BroadcastKind> = { key: string; kind: Kind }

/** What a route carries: a text route the serialized message, a binary route its bytes. */
type BroadcastPayload<Kind extends BroadcastKind = BroadcastKind> = { text: string; binary: Uint8Array }[Kind]

/** What an attempt delivers: a string on a text route, bytes on a binary route or a Room lane. */
type BackendPayload = string | Uint8Array

/** A consumer's callback, called with each message its subscription delivers. */
type BackendListener<Payload extends BackendPayload = BackendPayload> = (
  payload: Payload,
  ordering: OrderingInfo,
) => void

/** A publish's ordering. */
type PublishResult = OrderingInfo

/**
 * What a backend implements to subscribe to a plane's sources (a Broadcast route, a Room lane). Core keeps at most one
 * live attempt per source and host, however many consumers share it, and subscribes again once that one closed or
 * was unsubscribed.
 */
type SubscriptionDriver<Source> = {
  /** Returns the attempt at once, establishing or ready; throws a refusal it knows now. */
  subscribe(source: Source, receiver: BackendReceiver): DriverAttempt
  /** This host's key, read at each subscribe: absent, the process; on Cloudflare this session DO's incarnation, and a
   *  usage error outside one. */
  hostKey?(): string
}

/**
 * What `installBackend()` installs: Broadcast's driver, and the driver of each other plane it serves. An ordering's
 * `seq` is a positive safe integer that grows with every publish on a key, of either kind; its `timestamp` is a
 * non-negative safe integer (ms) that never goes back on a key.
 */
type BroadcastDriver = SubscriptionDriver<BroadcastRoute> & {
  /** Keeps `payload` past the call only as a copy: the caller reuses its bytes. A publish this host makes after
   *  `subscribe()` returned reaches that subscription, so a driver whose publish can overtake its own subscribe (Redis,
   *  on its two connections) holds the publish until the subscription is ready. */
  publish<Kind extends BroadcastKind>(
    route: BroadcastRoute<Kind>,
    payload: BroadcastPayload<Kind>,
  ): PublishResult | Promise<PublishResult>
  /** Memory only, as it fixes a publish's recipients as it publishes: the `seq` of `key`'s last publish, so a consumer
   *  joining a live attempt skips what was published before it. */
  published?(key: string): number
  /** The planes this backend serves; each is built on its first `backendPlane()` call, given the `kit` the extension
   *  passes it. */
  readonly planes?: readonly PlaneServer[]
}

declare const planeTypes: unique symbol
declare const servedPlane: unique symbol

/** A plane's key: `Symbol.for(Name)`, typed by the kit a backend builds the plane's driver from and the driver it
 *  builds. The extension that defines the plane exports the type, so a backend checks against it without importing
 *  the extension at runtime. */
type PlaneKey<Name extends string = string, Kit = never, Driver = unknown> = symbol & {
  readonly [planeTypes]?: { name: Name; build(kit: Kit): Driver }
}

/** What the plane `Key` names: the kit a backend builds its driver from, and that driver. */
type PlaneOf<Key extends PlaneKey> = Key extends PlaneKey<string, infer Kit, infer Driver>
  ? { kit: Kit; driver: Driver }
  : never

/** How a backend builds one plane's driver. Only `servePlane()` makes one, so an entry in `planes` was checked against
 *  its key. */
type PlaneServer = { readonly key: PlaneKey; build(kit: never): unknown; readonly [servedPlane]: true }

/** The key `Key` types, by its name, which the compiler checks against `Key`. */
function planeKey<Key extends PlaneKey>(name: Key extends PlaneKey<infer Name> ? Name : never): Key {
  return Symbol.for(name) as Key
}

/** A backend's entry in `planes`: `build` makes, from the plane's kit, the driver the plane `key` names. The key alone
 *  types the entry, so a driver that lacks one of its methods doesn't compile. */
function servePlane<Key extends PlaneKey>(
  key: Key,
  build: (kit: PlaneOf<Key>['kit']) => PlaneOf<Key>['driver'],
): PlaneServer {
  const entry: Omit<PlaneServer, typeof servedPlane> = { key, build }
  // The brand is the type's only: it marks what this function checked
  return entry as PlaneServer
}

/**
 * Core's end of one attempt. Deliver each source's messages in `seq` order, as they arrive: one that comes before the
 * attempt's first `ready` is kept by core and reaches the consumers right after it, and one after a loss goes out as
 * it arrives. What a receiver throws is a bug the driver reports: it goes on with its other receivers and messages.
 */
type BackendReceiver = {
  deliver(payload: BackendPayload, ordering: OrderingInfo): void
}

type AttemptState = 'establishing' | 'ready' | 'lost' | 'closed'

type StateListener = (state: Exclude<AttemptState, 'establishing'>, reason?: Error) => void

/**
 * One driver subscription to a source. It starts `establishing`; the driver makes it `ready` once every publish made
 * from now on reaches it, `lost` when it may miss messages (its connection dropped), `ready` again once it can't, and
 * `closed` when it is over for good: without a reason when the backend ended it on purpose (a Room incarnation was
 * dropped), which its consumers expect; with the reason when it failed, which core reports once. `closed` is final.
 */
abstract class DriverAttempt {
  private current: AttemptState = 'establishing'
  private readonly listeners = new Set<StateListener>()

  state(): AttemptState {
    return this.current
  }

  onStateChange(listener: StateListener): () => void {
    this.listeners.add(listener)
    return () => this.listeners.delete(listener)
  }

  /** Settles after the driver's own cleanup. Core calls it at most once, never on a closed attempt, and stops
   *  listening first. */
  abstract unsubscribe(): Promise<void>

  /** `lost` on a drop, `ready` again after it, `closed` with the reason. */
  protected transition(state: Exclude<AttemptState, 'establishing'>, reason?: unknown): void {
    if (this.current === state || this.current === 'closed') return
    this.current = state
    const error = reason === undefined || reason instanceof Error ? reason : new Error(String(reason))
    for (const listener of this.listeners) listener(state, error)
  }
}
