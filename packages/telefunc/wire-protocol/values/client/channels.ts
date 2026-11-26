export { responseTypes }
export { ClientChannel } from '../../channel/client/ClientChannel.js'
export { ChannelStreamSource } from '../ChannelStreamSource.js'
export { pumpClientProducerToChannel } from './request/pumpToChannel.js'

import type { ClientReviverContext, ReviverType, TypeContract } from '../types.js'
import { channelReviver } from './response/channel.js'
import { functionReviver } from './response/function.js'

const responseTypes: ReviverType<TypeContract, ClientReviverContext>[] = [channelReviver, functionReviver]
