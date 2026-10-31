// The browser half of telefunc/__internal: what extension code shared with the browser imports.
export type { TELEFUNC_SHIELDS } from '../node/shared/transformer/generateShield/shieldKey.js'
export type { ChannelPublishAck, ChannelPublishInfo } from '../wire-protocol/channel/channel.js'
export { untether } from '../utils/wrapProxy.js'
export { MAX_CHANNELS_PER_CONNECTION } from '../wire-protocol/constants.js'
export { isChannelOverflowError } from '../wire-protocol/channel/channelErrors.js'
