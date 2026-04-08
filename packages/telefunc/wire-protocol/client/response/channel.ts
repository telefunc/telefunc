export { channelReviver }

import type { ChannelContract, ClientReviverContext, ReviverType } from '../../types.js'
import { SERIALIZER_PREFIX_CHANNEL } from '../../constants.js'

const channelReviver: ReviverType<ChannelContract, ClientReviverContext> = {
  prefix: SERIALIZER_PREFIX_CHANNEL,
  createValue(metadata, context) {
    const channel = context.createChannel({
      channelId: metadata.channelId,
      ack: metadata.ack,
    })
    return {
      value: channel,
      async close() {
        await channel.close()
      },
      abort(abortError) {
        channel.abort(abortError.abortValue, abortError.message)
      },
    }
  },
}
