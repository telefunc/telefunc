export { DriverAttempt }
export type {
  AttemptState,
  BackendPayload,
  BackendReceiver,
  BroadcastDriver,
  BroadcastPayload,
  BroadcastRoute,
  PublishResult,
  SubscriptionDriver,
}

import type { BroadcastKind } from '../wire/payloads.js'
import type { OrderingInfo } from '../wire/orderingFrame.js'

/** A Broadcast route. The text and binary routes of one key share one order: one `seq` counts both. */
type BroadcastRoute<Kind extends BroadcastKind = BroadcastKind> = { key: string; kind: Kind }

/** What a route carries: a text route the serialized message, a binary route its bytes. */
type BroadcastPayload<Kind extends BroadcastKind = BroadcastKind> = { text: string; binary: Uint8Array }[Kind]

/** What an attempt delivers: a string on a text route, bytes on a binary route or a Room lane. */
type BackendPayload = string | Uint8Array

/** A publish's ordering, and how many subscribers the backend handed it to, counted its own way (absent where it
 *  can't tell). */
type PublishResult = OrderingInfo & { receivers?: number }

/**
 * What a backend implements to subscribe to a plane's sources (a Broadcast route, a Room lane). Core keeps at most one
 * live attempt per source and host, however many consumers share it, and subscribes again once that one closed or
 * was unsubscribed.
 */
type SubscriptionDriver<Source> = {
  /** Returns the attempt at once, establishing or ready; throws a refusal it knows now. */
  subscribe(source: Source, receiver: BackendReceiver): DriverAttempt
  /** This host's key: absent, the process; on Cloudflare this session DO's incarnation, null outside one (subscribe
   *  throws, publish doesn't wait). */
  hostKey?(): string | null
  /** True where a publish can reach the store before the subscribe this host made just before it (Redis: PUBLISH and
   *  SUBSCRIBE go by two connections). Core then holds a publish while one of its host's attempts on the publish's
   *  source is establishing, at most 60 s. Absent, a publish never waits. */
  readonly publishOvertakesSubscribe?: boolean
}

/**
 * What `installBackend()` installs: Broadcast's driver, and the driver of each other plane it serves. An ordering's
 * `seq` is a positive safe integer that grows with every publish on a key, of either kind; its `timestamp` is a
 * non-negative safe integer (ms) that never goes back on a key.
 */
type BroadcastDriver = SubscriptionDriver<BroadcastRoute> & {
  /** Keeps `payload` past the call only as a copy: the caller reuses its bytes. */
  publish<Kind extends BroadcastKind>(
    route: BroadcastRoute<Kind>,
    payload: BroadcastPayload<Kind>,
  ): PublishResult | Promise<PublishResult>
  /** Memory only, as it fixes a publish's recipients as it publishes: the `seq` of `key`'s last publish, so a consumer
   *  joining a live attempt skips what was published before it. */
  published?(key: string): number
  /** The planes this backend serves, by extension key; each is built on its first `backendPlane()` call, given the
   *  `kit` the extension passes it. */
  readonly planes?: { readonly [key: symbol]: (kit: never) => unknown }
}

/**
 * Core's end of one attempt. Deliver each source's messages in `seq` order, as they arrive: one that comes before the
 * attempt's first `ready` is kept by core and reaches the consumers right after it, and one after a loss goes out as
 * it arrives.
 */
type BackendReceiver = {
  deliver(payload: BackendPayload, ordering: OrderingInfo): void
  /** How many subscriptions share the attempt now. */
  readonly consumers: number
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
