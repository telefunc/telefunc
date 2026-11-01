export type { BroadcastBackend }

import type { BroadcastKind } from '../../wire/payloads.js'
import type { ChannelPublishAck } from '../../channel/channel.js'
import type { BroadcastPayload, BroadcastRoute } from '../driver.js'
import type { BackendReceiver, BackendSubscription } from '../Subscriptions.js'

/** What core consumes: the driver, supervised. */
type BroadcastBackend = {
  /** Publishes `payload` as it is at the call. */
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
