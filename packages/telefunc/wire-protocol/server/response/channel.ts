export { channelServerPlaceholderType }

import { SERIALIZER_PREFIX_CHANNEL } from '../../constants.js'
import type { PlaceholderReplacerType, ChannelContract } from '../../placeholder-types.js'
import { ServerChannel } from '../channel.js'
import { assertIsNotBrowser } from '../../../utils/assertIsNotBrowser.js'
assertIsNotBrowser()

const channelServerPlaceholderType: PlaceholderReplacerType<ChannelContract> = {
  prefix: SERIALIZER_PREFIX_CHANNEL,
  detect(value): value is ChannelContract['value'] {
    return ServerChannel.isServerChannel(value)
  },
  getMetadata(channel) {
    // Reset the connect TTL to CHANNEL_CONNECT_TTL_MS from now — the client is about
    // to receive this channel reference in the HTTP response and has that window to
    // connect and reconcile.
    channel._registerChannel()
    return { channelId: channel.id, ...(channel.ackMode && { ack: channel.ackMode }) }
  },
}
