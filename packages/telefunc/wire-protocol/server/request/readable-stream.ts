export { readableStreamReviver }

import type { ReviverType, ReadableStreamRequestContract, ServerReviverContext } from '../../types.js'
import { SERIALIZER_PREFIX_STREAM } from '../../constants.js'
import { ChannelChunkReader } from '../../ChannelChunkReader.js'
import { ServerChannel } from '../channel.js'
import { assertIsNotBrowser } from '../../../utils/assertIsNotBrowser.js'
assertIsNotBrowser()

const readableStreamReviver: ReviverType<ReadableStreamRequestContract, ServerReviverContext> = {
  prefix: SERIALIZER_PREFIX_STREAM,
  createValue: ({ channelId }, context) => {
    const channel = new ServerChannel({ id: channelId })
    channel._registerChannel()
    context.registerChannel(channel)
    return {
      value: ChannelChunkReader.toReadableStream(channel),
    }
  },
}
