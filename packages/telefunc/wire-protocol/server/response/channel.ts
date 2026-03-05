export { channelServerPlaceholderType }

import { SERIALIZER_PREFIX_CHANNEL } from '../../constants.js'
import type { ServerPlaceholderType, ChannelContract } from '../../placeholder-types.js'
import { ServerChannel } from '../channel.js'
import { assertIsNotBrowser } from '../../../utils/assertIsNotBrowser.js'
assertIsNotBrowser()

const channelServerPlaceholderType: ServerPlaceholderType<ChannelContract> = {
  prefix: SERIALIZER_PREFIX_CHANNEL,
  detect(value) {
    return value instanceof ServerChannel
  },
  getMetadata(channel) {
    return { channelId: channel.id }
  },
}
