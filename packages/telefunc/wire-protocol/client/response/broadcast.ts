export { broadcastReviver }

import type { BroadcastContract, ClientReviverContext, ReviverType } from '../../types.js'
import { SERIALIZER_PREFIX_BROADCAST } from '../../constants.js'

const broadcastReviver: ReviverType<BroadcastContract, ClientReviverContext> = {
  prefix: SERIALIZER_PREFIX_BROADCAST,
  revive(metadata, context) {
    const channel = context.createBroadcast({
      channelId: metadata.channelId,
      key: metadata.key,
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
