export { channelServerPlaceholderType }

import { SERIALIZER_PREFIX_CHANNEL } from '../../constants.js'
import type { ServerPlaceholderType, ChannelContract } from '../../placeholder-types.js'
import { ServerChannel } from '../channel.js'
import { assertIsNotBrowser } from '../../../utils/assertIsNotBrowser.js'
assertIsNotBrowser()

const channelServerPlaceholderType: ServerPlaceholderType<ChannelContract> = {
  prefix: SERIALIZER_PREFIX_CHANNEL,
  detect(value) {
    return ServerChannel.isServerChannel(value)
  },
  getMetadata(channel) {
    return { channelId: channel.id, ...(channel.ackMode && { ack: channel.ackMode }) }
  },
}
