export { channelReplacer }

import type { ChannelContract, ReplacerType, ServerReplacerContext } from '../../types.js'
import { SERIALIZER_PREFIX_CHANNEL } from '../../constants.js'

import { ServerChannel } from '../channel.js'
import { assertIsNotBrowser } from '../../../utils/assertIsNotBrowser.js'
assertIsNotBrowser()

const channelReplacer: ReplacerType<ChannelContract, ServerReplacerContext> = {
  prefix: SERIALIZER_PREFIX_CHANNEL,
  detect(value): value is ChannelContract['value'] {
    return ServerChannel.isServerChannel(value)
  },
  replace(channel, context) {
    context.registerChannel(channel)
    return {
      metadata: {
        channelId: channel.id,
        ...(channel.ack && { ack: channel.ack }),
      },
      async close() {
        await channel.close()
      },
      abort(abortError) {
        channel.abort(abortError.abortValue)
      },
    }
  },
}
