export type { TELEFUNC_SHIELDS } from '../shared/transformer/generateShield/shieldKey.js'
export type {
  BroadcastDriver,
  BroadcastPayload,
  BroadcastRoute,
  PublishResult,
} from '../../wire-protocol/backend/broadcast/contract.js'
export { installBackend, backendPlane } from '../../wire-protocol/backend/install.js'
export type { BackendDriver } from '../../wire-protocol/backend/install.js'
export { DriverAttempt } from '../../wire-protocol/backend/attempt.js'
export { MemoryBackend } from '../../wire-protocol/backend/memory/backend.js'
export type { PlaneSubscriptions, SendHold } from '../../wire-protocol/backend/SubscriptionManager.js'
export { decodeOrderingFrame } from '../../wire-protocol/wire/orderingFrame.js'
export type { OrderingInfo } from '../../wire-protocol/wire/orderingFrame.js'
export type {
  BackendPayload,
  BackendReceiver,
  BackendSubscription,
  SubscriptionAttempt,
  SubscriptionBinding,
  SubscriptionDriver,
} from '../../wire-protocol/backend/subscription.js'
export type { BroadcastEndOwner } from '../../wire-protocol/channel/server/ServerBroadcastEnd.js'
export type { PageEnd, PageClient }
export { encodePublish } from '../../wire-protocol/wire/connectionFrame.js'
export type { BroadcastKind } from '../../wire-protocol/wire/connectionFrame.js'
export { hostLocal } from '../../wire-protocol/channel/server/mux.js'
export { reportBug } from '../../wire-protocol/channel/server/channel.js'
export { getChannelLimits } from './serverConfig.js'
export { untether } from '../../utils/wrapProxy.js'
export { MAX_CHANNELS_PER_CONNECTION } from '../../wire-protocol/constants.js'
export { registerCloudflareExtension } from '../../wire-protocol/adapter/cloudflare/extensions.js'
export type {
  TelefuncExtensionNamespace,
  TelefuncExtensionStub,
} from '../../wire-protocol/adapter/cloudflare/extensions.js'
export type { ChannelPublishAck, ChannelPublishInfo } from '../../wire-protocol/channel/channel.js'
export { MemoryPubSub, type Subscriptions } from '../../wire-protocol/backend/memory/deliveries.js'
export { isChannelOverflowError } from '../../wire-protocol/channel/channelErrors.js'

import type { ServerBroadcastEnd } from '../../wire-protocol/channel/server/ServerBroadcastEnd.js'
import type { ClientBroadcast } from '../../wire-protocol/channel/client/channel.js'

/** The members of a page's broadcast end that an add-on drives. */
type PageEnd = Pick<
  ServerBroadcastEnd,
  | 'id'
  | '_setOwner'
  | '_terminateWire'
  | '_trackAck'
  | 'isAttached'
  | 'isClosed'
  | 'isConnected'
  | 'deliver'
  | 'deliverBinary'
  | 'listen'
  | 'onClose'
  | 'onDisconnect'
  | 'onReconnect'
  | 'close'
>

/** The members of a page's `ClientBroadcast` that an add-on drives. */
type PageClient = Pick<
  ClientBroadcast,
  | '_publishUnreported'
  | '_publishBinaryUnreported'
  | '_setSubscribed'
  | '_sendReply'
  | '_subscribeLocal'
  | 'isClosed'
  | 'isConnected'
  | 'send'
  | 'onOpen'
  | 'onClose'
  | 'onDisconnect'
  | 'onReconnect'
  | 'close'
>
