export { readableStreamReviver }

import type { ReviverType, ReadableStreamRequestContract, ServerReviverContext } from '../../types.js'
import { SERIALIZER_PREFIX_STREAM } from '../../constants.js'
import { ChannelChunkReader } from '../../ChannelChunkReader.js'
import { assertIsNotBrowser } from '../../../utils/assertIsNotBrowser.js'
assertIsNotBrowser()

const readableStreamReviver: ReviverType<ReadableStreamRequestContract, ServerReviverContext> = {
  prefix: SERIALIZER_PREFIX_STREAM,
  createValue: ({ channelId }, context) => {
    const channel = context.createChannel({ id: channelId })
    return {
      value: ChannelChunkReader.toReadableStream(channel),
      async close() {
        await channel.close()
      },
      abort(abortError) {
        channel.abort(abortError.abortValue)
      },
    }
  },
}
