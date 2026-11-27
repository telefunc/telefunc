// The browser half of telefunc/__internal: what extension code shared with the browser imports.
export type { TELEFUNC_SHIELDS } from '../node/shared/transformer/generateShield/shieldKey.js'
export type { ChannelPublishAck, ChannelPublishInfo } from '../wire-protocol/channel/channel.js'
export { invokeChannelListener } from '../wire-protocol/channel/channel.js'
export { tetherTo, untether } from '../utils/wrapProxy.js'
export { MAX_CHANNELS_PER_CONNECTION, PUBLISH_HOLD_BYTES } from '../wire-protocol/constants.js'
export { isChannelOverflowError } from '../wire-protocol/channel/channelErrors.js'
export type { ClientBroadcast }

// Utilities the add-ons share with core.
export { createDeferred, type Deferred } from '../utils/createDeferred.js'
export { Fifo } from '../utils/Fifo.js'
export { soleAnswer, TWO_ANSWERS } from '../wire-protocol/channel/listenerAnswer.js'
export { getOrCreate } from '../utils/getOrCreate.js'
export { isObject } from '../utils/isObject.js'
export { Listeners } from '../utils/Listeners.js'
export { markHandled } from '../utils/markHandled.js'
export { nextTurn } from '../wire-protocol/flowControl/macrotaskYield.js'
export { utf8ByteLength } from '../utils/utf8ByteLength.js'

import type { ClientBroadcast as ClientBroadcastClass } from '../wire-protocol/channel/client/ClientBroadcast.js'

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
