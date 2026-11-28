export { responseTypes }
export { ClientChannel } from '../../channel/client/ClientChannel.js'
export { ChannelStreamSource } from '../ChannelStreamSource.js'
export { pumpProducerToChannel } from './request/pumpToChannel.js'

import type { FeatureResponseTypes } from './response/registry.js'
import { channelReviver } from './response/channel.js'
import { functionReviver } from './response/function.js'
import { SERIALIZER_PREFIX_CHANNEL, SERIALIZER_PREFIX_FUNCTION } from '../../constants.js'

const responseTypes: FeatureResponseTypes<'channels'> = {
  [SERIALIZER_PREFIX_CHANNEL]: channelReviver,
  [SERIALIZER_PREFIX_FUNCTION]: functionReviver,
}
