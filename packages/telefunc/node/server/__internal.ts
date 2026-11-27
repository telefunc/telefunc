// Every add-on: Room (`@telefunc/room`), `@telefunc/rxjs` and `@telefunc/redis`.
export type { TELEFUNC_SHIELDS } from '../shared/transformer/generateShield/shieldKey.js'
export type { OrderingInfo } from '../../wire-protocol/backend/driverOrdering.js'
export { tetherTo, untether } from '../../utils/wrapProxy.js'
export { invokeChannelListener, soleAnswer } from './context/invokeWithoutContext.js'
export { handleCallbackBug } from './runTelefunc/validateTelefunctionError.js'
export { hostLocal } from '../../wire-protocol/channel/server/ChannelMux.js'
export { reconnectWindow } from './serverConfig.js'
export { MAX_CHANNELS_PER_CONNECTION, PUBLISH_HOLD_BYTES } from '../../wire-protocol/constants.js'
export { HELD_SEND_OVERHEAD_BYTES, queueMemory } from '../../wire-protocol/channel/server/channelMemory.js'
export type { QueueMemory } from '../../wire-protocol/channel/channelMessages.js'

// Utilities the add-ons share with core.
export { createDeferred, type Deferred } from '../../utils/createDeferred.js'
export { Fifo } from '../../utils/Fifo.js'
export { TWO_ANSWERS } from '../../wire-protocol/channel/listenerAnswer.js'
export { getOrCreate } from '../../utils/getOrCreate.js'
export { isObject } from '../../utils/isObject.js'
export { Listeners } from '../../utils/Listeners.js'
export { markHandled } from '../../utils/markHandled.js'
export { nextTurn } from '../../wire-protocol/flowControl/macrotaskYield.js'
export { unrefTimer } from '../../utils/unrefTimer.js'
export { utf8ByteLength } from '../../utils/utf8ByteLength.js'

// An add-on that drives a page's channel ends: Room's stubs and its client.
export type { ServerBroadcastEnd, ClientBroadcast }
export type { BroadcastEndOwner } from '../../wire-protocol/channel/server/ServerBroadcastEnd.js'
export type { BroadcastKind } from '../../wire-protocol/wire/payloads.js'
export type { ChannelPublishAck, ChannelPublishInfo } from '../../wire-protocol/channel/channel.js'
export { publishBody, type PublishBody } from '../../wire-protocol/channel/server/PublishBody.js'
export { publishedData } from '../../wire-protocol/channel/broadcastPayload.js'
export { isChannelOverflowError } from '../../wire-protocol/channel/channelErrors.js'
export {
  ProtocolViolationError,
  assertProtocol,
  parsePeerText,
} from '../../wire-protocol/wire/protocolViolation.js'

// A backend: one `installBackend` installs (`@telefunc/redis`, a test's), and the drivers of a plane (Room's).
export type {
  BackendListener,
  BackendPayload,
  BackendReceiver,
  BroadcastDriver,
  BroadcastPayload,
  BroadcastRoute,
  PlaneKey,
  PlaneServer,
  PublishResult,
  SubscriptionDriver,
} from '../../wire-protocol/backend/driver.js'
export { DriverAttempt, planeKey, servePlane } from '../../wire-protocol/backend/driver.js'
export { installBackend, backendPlane, type PlaneDefinition } from '../../wire-protocol/backend/install.js'
export { BackendError } from '../../wire-protocol/backend/BackendError.js'
export { BackendCall } from '../../wire-protocol/backend/BackendCall.js'
export type { BackendSubscription, SourceRules } from '../../wire-protocol/backend/SharedSubscriptions.js'
export { SharedSubscriptions } from '../../wire-protocol/backend/SharedSubscriptions.js'
export { assertDriverOrdering, isOrdering } from '../../wire-protocol/backend/driverOrdering.js'
export { reportBroadcastDeliveryLost } from '../../wire-protocol/backend/broadcast/reportBroadcastDeliveryLost.js'
export { MemoryBackend } from '../../wire-protocol/backend/memory/MemoryBackend.js'
export {
  addSubscriber,
  removeSubscriber,
  deliverTo,
  type Subscribers,
} from '../../wire-protocol/backend/memory/memorySubscribers.js'
export { isCloudflareWorkers } from '../../utils/isCloudflareWorkers.js'
export { ChannelOverflowError } from '../../wire-protocol/channel/channelErrors.js'

// A Cloudflare extension: Room's session and authority Durable Objects.
export {
  registerCloudflareExtension,
  serveCloudflarePlane,
  currentCloudflareMember,
  currentCloudflareHostKey,
  extensionStub,
  CLOUDFLARE_BUCKETS,
} from '../../wire-protocol/adapter/cloudflare/extensions.js'
export type {
  CloudflareExtensionKit,
  CloudflareExtensionMethods,
  CloudflareMember,
  CloudflarePlaneKit,
  CloudflareRegions,
  TelefuncExtensionNamespace,
  TelefuncExtensionStub,
} from '../../wire-protocol/adapter/cloudflare/extensions.js'
export { OrderedStubs, CallNotSent, CallTimedOut } from '../../wire-protocol/adapter/cloudflare/OrderedStubs.js'
export type { CallOptions, Invoke } from '../../wire-protocol/adapter/cloudflare/OrderedStubs.js'
export { TransientRetry } from '../../wire-protocol/adapter/cloudflare/transientRetry.js'
export {
  HolderGrants,
  LEASE_CALL_DEADLINE_MS,
  LEASE_RENEW_EVERY_MS,
  dropLeaseScopes,
  renewsAfterPause,
  grantHolder,
  initLeaseSchema,
  leaseRegistered,
  liveLeases,
  nextLeaseSweep,
  registerLease,
  removeLease,
  renewLease,
  sweepLeases,
} from '../../wire-protocol/adapter/cloudflare/leases.js'
export type {
  LeaseOrder,
  LeaseRegistered,
  LeaseRemoved,
  LeaseWritten,
} from '../../wire-protocol/adapter/cloudflare/leases.js'
export {
  blobColumn,
  integerColumn,
  nullableIntegerColumn,
  nullableTextColumn,
  textColumn,
} from '../../wire-protocol/adapter/cloudflare/sqlStorage.js'
export type {
  DurableStorage,
  SqlBinding,
  SqlCursor,
  SqlRow,
  SqlStore,
  SqlValue,
} from '../../wire-protocol/adapter/cloudflare/sqlStorage.js'

import type { ServerBroadcastEnd as ServerBroadcastEndClass } from '../../wire-protocol/channel/server/ServerBroadcastEnd.js'
import type { ClientBroadcast as ClientBroadcastClass } from '../../wire-protocol/channel/client/ClientBroadcast.js'

/** The members of a page's broadcast end that an add-on drives. */
type ServerBroadcastEnd = Pick<
  ServerBroadcastEndClass,
  | 'id'
  | 'setOwner'
  | 'terminateWire'
  | 'trackAck'
  | 'isAttached'
  | 'isClosed'
  | 'isConnected'
  | 'deliver'
  | 'listen'
  | 'onClose'
  | 'onDisconnect'
  | 'onReconnect'
  | 'close'
>

/** The members of a page's `ClientBroadcast` that an add-on drives. A page holds the class as its `Broadcast`, so the
 *  internal ones are `_`-prefixed, as `ClientChannel`'s are. It receives `T` and publishes `Publish`. */
type ClientBroadcast<T = unknown, Publish = T> = Pick<
  ClientBroadcastClass<T, Publish>,
  | '_publishUnreported'
  | '_publishBinaryUnreported'
  | '_sendRequest'
  | '_setSubscribed'
  | '_setWantParts'
  | '_sendReply'
  | '_subscribeLocal'
  | '_confirmSoon'
  | 'isClosed'
  | 'isConnected'
  | 'onOpen'
  | 'onClose'
  | 'onDisconnect'
  | 'onReconnect'
  | 'close'
>
export { isPeerGone } from './runTelefunc/isPeerGone.js'
