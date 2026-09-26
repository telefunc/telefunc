export type { BroadcastBackend, BroadcastDriver, BroadcastRoute, PublishResult }

import type { BroadcastKind } from '../../shared-ws.js'
import type { BackendReceiver, BackendSubscription, SubscriptionDriver } from '../subscription.js'

/** A Broadcast route; text and binary of one key share one ordering domain. */
type BroadcastRoute = { key: string; kind: BroadcastKind }

/** An accepted publish's position: a positive safe-integer seq and the authority's timestamp. */
type PublishResult = {
  seq: number
  timestamp: number
  receivers?: number
  meta?: Record<string, unknown>
}

/** What a backend implements for Broadcast. */
type BroadcastDriver = {
  publish(route: BroadcastRoute, payload: Uint8Array): PublishResult | Promise<PublishResult>
  readonly subscriptions: SubscriptionDriver<BroadcastRoute>
}

/** What core consumes: the driver, supervised. */
type BroadcastBackend = {
  /** Publishes `payload` as it is at the call. A publish held while this instance's subscriptions on its key establish
   *  counts against `config.channel.bufferLimit` (`bufferLimitBinary`). */
  publish(route: BroadcastRoute, payload: Uint8Array): PublishResult | Promise<PublishResult>
  subscribe(route: BroadcastRoute, receiver: BackendReceiver): BackendSubscription
  dispose(): Promise<void>
}
