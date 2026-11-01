// Every add-on: Room (`@telefunc/room`), `@telefunc/rxjs` and `@telefunc/redis`.
export type { TELEFUNC_SHIELDS } from '../shared/transformer/generateShield/shieldKey.js'
export type { OrderingInfo } from '../../wire-protocol/wire/orderingFrame.js'
export { untether } from '../../utils/wrapProxy.js'
export { reportBug } from '../../wire-protocol/channel/server/channel.js'
export { hostLocal } from '../../wire-protocol/channel/server/mux.js'
export { getChannelLimits } from './serverConfig.js'
export { MAX_CHANNELS_PER_CONNECTION } from '../../wire-protocol/constants.js'

// An add-on that drives a page's channel ends: Room's stubs and its client.
export type { ServerBroadcastEnd, ClientBroadcast }
export type { BroadcastEndOwner } from '../../wire-protocol/channel/server/broadcastEnd.js'
export type { BroadcastKind } from '../../wire-protocol/wire/payloads.js'
export type { ChannelPublishAck, ChannelPublishInfo } from '../../wire-protocol/channel/channel.js'
export { encodePublish } from '../../wire-protocol/wire/publishInfo.js'
export { isChannelOverflowError } from '../../wire-protocol/channel/channelErrors.js'
export {
  ProtocolViolationError,
  assertProtocol,
  isProtocolViolationError,
  parsePeerText,
} from '../../wire-protocol/wire/protocolViolation.js'

// A backend: one `installBackend` installs (`@telefunc/redis`, a test's), and the drivers of a plane (Room's).
export { installBackend, backendPlane } from '../../wire-protocol/backend/install.js'
export type { BackendDriver } from '../../wire-protocol/backend/install.js'
export type {
  BroadcastDriver,
  BroadcastPayload,
  BroadcastRoute,
  PublishResult,
} from '../../wire-protocol/backend/broadcast/contract.js'
export type {
  BackendPayload,
  BackendReceiver,
  BackendSubscription,
  SubscriptionAttempt,
  SubscriptionBinding,
  SubscriptionDriver,
} from '../../wire-protocol/backend/subscription.js'
export type { PlaneSubscriptions, SendHold } from '../../wire-protocol/backend/SubscriptionManager.js'
export { DriverAttempt, SourceDroppedError } from '../../wire-protocol/backend/attempt.js'
export { assertDriverOrdering } from '../../wire-protocol/backend/driverOrdering.js'
export { decodeOrderingFrame } from '../../wire-protocol/wire/orderingFrame.js'
export { MemoryBackend } from '../../wire-protocol/backend/memory/backend.js'
export { MemoryPubSub, type Subscriptions } from '../../wire-protocol/backend/memory/deliveries.js'

// A Cloudflare extension: Room's session and authority Durable Objects.
export { registerCloudflareExtension } from '../../wire-protocol/adapter/cloudflare/extensions.js'
export type {
  TelefuncExtensionNamespace,
  TelefuncExtensionStub,
} from '../../wire-protocol/adapter/cloudflare/extensions.js'
export { OrderedStubs, reportLostDeliveries } from '../../wire-protocol/adapter/cloudflare/OrderedStubs.js'

import type { ServerBroadcastEnd as ServerBroadcastEndClass } from '../../wire-protocol/channel/server/broadcastEnd.js'
import type { ClientBroadcast as ClientBroadcastClass } from '../../wire-protocol/channel/client/broadcast.js'

/** The members of a page's broadcast end that an add-on drives. */
type ServerBroadcastEnd = Pick<
  ServerBroadcastEndClass,
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
type ClientBroadcast = Pick<
  ClientBroadcastClass,
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
