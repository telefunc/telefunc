export { channelReplacer }

import type { ChannelContract, ReplacerType, ServerReplacerContext } from '../../types.js'
import { SERIALIZER_PREFIX_CHANNEL } from '../../constants.js'

import { ServerChannel } from '../channel.js'
import { assertIsNotBrowser } from '../../../utils/assertIsNotBrowser.js'
assertIsNotBrowser()

const channelReplacer: ReplacerType<ChannelContract, ServerReplacerContext> = {
  prefix: SERIALIZER_PREFIX_CHANNEL,
  detect(value): value is ChannelContract['value'] {
    return ServerChannel.isServerChannel(value)
  },
  getMetadata(channel, { registerChannel }) {
    // Reset the connect TTL to CHANNEL_CONNECT_TTL_MS from now — the client is about
    // to receive this channel reference in the HTTP response and has that window to
    // connect and reconcile.
    channel._registerChannel()
    registerChannel(channel)
    return {
      channelId: channel.id,
      ...(channel.ackMode && { ack: channel.ackMode }),
    }
  },
}
