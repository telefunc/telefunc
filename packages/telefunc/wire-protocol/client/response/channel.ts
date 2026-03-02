export { channelClientPlaceholderType }

import { SERIALIZER_PREFIX_CHANNEL } from '../../constants.js'
import type { ClientPlaceholderType, ChannelContract } from '../../placeholder-types.js'
import { ClientChannel } from '../../../client/channel.js'

const channelClientPlaceholderType: ClientPlaceholderType<ChannelContract> = {
  prefix: SERIALIZER_PREFIX_CHANNEL,
  createValue(metadata) {
    return new ClientChannel(metadata.channelId)
  },
}
