export { channelServerPlaceholderType }

import { SERIALIZER_PREFIX_CHANNEL } from '../../constants.js'
import type { ServerPlaceholderType, ChannelContract } from '../../placeholder-types.js'
import { ServerChannel } from '../../../node/server/channel.js'
import { assertIsNotBrowser } from '../../../utils/assertIsNotBrowser.js'
assertIsNotBrowser()

const channelServerPlaceholderType: ServerPlaceholderType<ChannelContract> = {
  prefix: SERIALIZER_PREFIX_CHANNEL,
  detect(value): value is ServerChannel {
    return ServerChannel.isServerChannel(value)
  },
  getMetadata(channel) {
    return { channelId: channel.id }
  },
}
