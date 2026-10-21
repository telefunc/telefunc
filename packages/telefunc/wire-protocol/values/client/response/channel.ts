export { channelReviver }

import type { ChannelContract, ClientReviverContext, ReviverType } from '../../types.js'
import { SERIALIZER_PREFIX_CHANNEL } from '../../../constants.js'

const channelReviver: ReviverType<ChannelContract, ClientReviverContext> = {
  prefix: SERIALIZER_PREFIX_CHANNEL,
  revive(metadata, context) {
    const channel = context.createChannel({
      channelId: metadata.channelId,
      ack: metadata.ack,
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
