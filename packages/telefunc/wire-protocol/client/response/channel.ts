export { channelClientPlaceholderType }

import { SERIALIZER_PREFIX_CHANNEL } from '../../constants.js'
import type { PlaceholderReviverType, ChannelContract } from '../../placeholder-types.js'
import { ClientChannel } from '../channel.js'

const channelClientPlaceholderType: PlaceholderReviverType<ChannelContract> = {
  prefix: SERIALIZER_PREFIX_CHANNEL,
  createValue(metadata, context) {
    const channel = new ClientChannel(metadata.channelId, metadata.ack, context.shard)
    context.registerChannel(channel)
    return {
      value: channel,
      close: () => channel.close(),
    }
  },
}
