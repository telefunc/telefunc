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
export type {
  Backend,
  BackendPayload,
  BroadcastDriver,
  BroadcastPayload,
  BroadcastRoute,
  PublishResult,
  Subscriber,
  Unsubscribe,
} from '../../wire-protocol/backend/driver.js'
export { installBackend, backendPlane, type PlaneDefinition } from '../../wire-protocol/backend/install.js'
export type { BackendReceiver, BackendSubscription } from '../../wire-protocol/backend/Subscriptions.js'
export { Subscriptions } from '../../wire-protocol/backend/Subscriptions.js'
export { assertDriverOrdering } from '../../wire-protocol/backend/driverOrdering.js'
export { decodeOrderingFrame } from '../../wire-protocol/wire/orderingFrame.js'
export { MemoryBackend } from '../../wire-protocol/backend/memory/MemoryBackend.js'
export { MemoryPubSub, type Subscribers } from '../../wire-protocol/backend/memory/deliveries.js'
export { ChannelOverflowError } from '../../wire-protocol/channel/channelErrors.js'

// A Cloudflare extension: Room's session and authority Durable Objects.
export {
  registerCloudflareExtension,
  currentCloudflareMember,
  CLOUDFLARE_BUCKETS,
} from '../../wire-protocol/adapter/cloudflare/extensions.js'
export type {
  CloudflareExtensionKit,
  CloudflareMember,
  CloudflareRegions,
  TelefuncExtensionNamespace,
  TelefuncExtensionStub,
} from '../../wire-protocol/adapter/cloudflare/extensions.js'
export { OrderedStubs } from '../../wire-protocol/adapter/cloudflare/OrderedStubs.js'

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
