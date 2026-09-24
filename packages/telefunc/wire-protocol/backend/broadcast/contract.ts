export type { BroadcastBackend, BroadcastDriver, BroadcastLane, PublishResult }

import type { BackendReceiver, BackendSubscription, SubscriptionDriver } from '../subscription.js'

/** A Broadcast route; text and binary of one key share one ordering domain. */
type BroadcastLane = { key: string; kind: 'text' | 'binary' }

/** An accepted publish's position: a positive safe-integer seq and the authority's timestamp. */
type PublishResult = {
  seq: number
  timestamp: number
  receivers?: number
  meta?: Record<string, unknown>
}

/** What a backend implements for Broadcast. */
type BroadcastDriver = {
  publish(lane: BroadcastLane, payload: Uint8Array): PublishResult | Promise<PublishResult>
  readonly subscriptions: SubscriptionDriver<BroadcastLane>
}

/** What core consumes: the driver, supervised. */
type BroadcastBackend = {
  /** A publish held while this instance's subscription on its route is establishing counts against `bufferLimit`. */
  publish(lane: BroadcastLane, payload: Uint8Array, bufferLimit: number): PublishResult | Promise<PublishResult>
  subscribe(lane: BroadcastLane, receiver: BackendReceiver): BackendSubscription
  hasSubscriptions(): boolean
  dispose(): Promise<void>
}
