export { pubsubReviver }

import type { PubSubContract, ClientReviverContext, ReviverType } from '../../types.js'
import { SERIALIZER_PREFIX_PUBSUB } from '../../constants.js'

const pubsubReviver: ReviverType<PubSubContract, ClientReviverContext> = {
  prefix: SERIALIZER_PREFIX_PUBSUB,
  createValue(metadata, context) {
    const channel = context.createPubSub({
      channelId: metadata.channelId,
      key: metadata.key,
    })
    return {
      value: channel,
      async close() {
        await channel.close()
      },
      abort(abortError) {
        channel._abortWithValue(abortError.abortValue, abortError.message)
      },
    }
  },
}
