export { channelReplacer }

import type { ChannelContract, ReplacerType, ServerReplacerContext } from '../../types.js'
import { SERIALIZER_PREFIX_CHANNEL } from '../../constants.js'

import { ServerChannel } from '../../channel/server/channel.js'
import { getAdapterMaxFrameBytes } from '../../../node/server/serverConfig.js'
import { assertIsNotBrowser } from '../../../utils/assertIsNotBrowser.js'
assertIsNotBrowser()

const channelReplacer: ReplacerType<ChannelContract, ServerReplacerContext> = {
  prefix: SERIALIZER_PREFIX_CHANNEL,
  detect(value): value is ChannelContract['value'] {
    return ServerChannel.isServerChannel(value)
  },
  replace(channel, context) {
    context.registerChannel(channel)
    const metadata: ChannelContract['metadata'] = { channelId: channel.id }
    if (channel.ack) metadata.ack = channel.ack
    const maxFrameBytes = getAdapterMaxFrameBytes()
    if (maxFrameBytes) metadata.maxFrameBytes = maxFrameBytes
    return {
      metadata,
      async close() {
        await channel.close()
      },
      abort(abortError) {
        channel.abort(abortError.abortValue)
      },
    }
  },
}
