export { broadcastReviver }

import type { BroadcastContract, ClientReviverContext, ReviverType } from '../../types.js'
import { SERIALIZER_PREFIX_BROADCAST } from '../../../constants.js'

const broadcastReviver: ReviverType<BroadcastContract, ClientReviverContext> = {
  prefix: SERIALIZER_PREFIX_BROADCAST,
  revive(metadata, context) {
    const channel = context.createBroadcast({
      channelId: metadata.channelId,
      key: metadata.key,
      maxFrameBytes: metadata.maxFrameBytes,
    })
    return {
      value: channel,
      // Not methods: on Bun (JavaScriptCore) a method keeps this object alive, and the proxy is put on it (parse.ts).
      close: async () => {
        await channel.close()
      },
      abort: (abortError) => {
        channel.abort(abortError.abortValue, abortError.message)
      },
      onEnd: (callback) => {
        channel.onClose(callback)
      },
    }
  },
}
