export type { BroadcastBackend, BroadcastDriver, BroadcastPayload, BroadcastRoute, PublishResult }

import type { BroadcastKind } from '../../wire/connectionFrame.js'
import type { ChannelPublishAck } from '../../channel/channel.js'
import type { BackendReceiver, BackendSubscription, SubscriptionDriver } from '../subscription.js'

/** A Broadcast route; text and binary of one key share one ordering domain. */
type BroadcastRoute<Kind extends BroadcastKind = BroadcastKind> = { key: string; kind: Kind }

/** What a route carries: a text route the serialized message, a binary route its bytes. */
type BroadcastPayload<Kind extends BroadcastKind = BroadcastKind> = { text: string; binary: Uint8Array }[Kind]

/** An accepted publish's position: a positive safe-integer seq and the authority's timestamp. */
type PublishResult = {
  seq: number
  timestamp: number
  receivers?: number
}

/** What a backend implements for Broadcast. */
type BroadcastDriver = {
  /** Copies `payload` where it holds it past the call: the caller may reuse its buffer once the call returns. */
  publish<Kind extends BroadcastKind>(
    route: BroadcastRoute<Kind>,
    payload: BroadcastPayload<Kind>,
  ): PublishResult | Promise<PublishResult>
  readonly subscriptions: SubscriptionDriver<BroadcastRoute>
  /** For a driver that fixes a publish's recipients as it publishes: a seq at or above every publish on `key` a
   *  subscription made now could still be delivered, so a subscriber skips what was published before it. */
  published?(key: string): number
}

/** What core consumes: the driver, supervised. */
type BroadcastBackend = {
  /** Publishes `payload` as it is at the call. A publish held while this instance's subscriptions on its key establish
   *  counts against `config.channel.bufferLimit` (`bufferLimitBinary`). */
  publish<Kind extends BroadcastKind>(
    route: BroadcastRoute<Kind>,
    payload: BroadcastPayload<Kind>,
  ): ChannelPublishAck | Promise<ChannelPublishAck>
  subscribe<Kind extends BroadcastKind>(
    route: BroadcastRoute<Kind>,
    receiver: BackendReceiver<BroadcastPayload<Kind>>,
  ): BackendSubscription
  dispose(): Promise<void>
}
