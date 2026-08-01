export type { BroadcastBackend, BroadcastDriver, BroadcastLane, PublishResult }

import type { BackendReceiver, BackendSubscription, SubscriptionDriver } from '../subscription.js'

/** A cheap text/binary route; both kinds share one per-key ordering domain. */
type BroadcastLane = { key: string; kind: 'text' | 'binary' }

/** Marks identify this accepted publish: positive-safe shared text/binary seq and non-decreasing safe authority time. */
type PublishResult = {
  seq: number
  timestamp: number
  receivers?: number
  meta?: Record<string, unknown>
}

/** Raw author contract for the cheap Broadcast plane. */
type BroadcastDriver = {
  publish(lane: BroadcastLane, payload: Uint8Array): PublishResult | Promise<PublishResult>
  readonly subscriptions: SubscriptionDriver<BroadcastLane>
}

/** Internal supervised Broadcast consumer contract. */
type BroadcastBackend = {
  publish(lane: BroadcastLane, payload: Uint8Array): PublishResult | Promise<PublishResult>
  subscribe(lane: BroadcastLane, receiver: BackendReceiver): BackendSubscription
  dispose(): Promise<void>
}
