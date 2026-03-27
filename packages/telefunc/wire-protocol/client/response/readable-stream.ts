export { readableStreamReviver }

import type { ReviverType, ReadableStreamContract, ClientReviverContext } from '../../types.js'
import { SERIALIZER_PREFIX_STREAM } from '../../constants.js'
import { getGlobalObject } from '../../../utils/getGlobalObject.js'

const globalObject = getGlobalObject('wire-protocol/client/response/readable-stream.ts', {
  gcRegistry: new FinalizationRegistry<() => void>((cancel) => cancel()),
})

const readableStreamReviver: ReviverType<ReadableStreamContract, ClientReviverContext> = {
  prefix: SERIALIZER_PREFIX_STREAM,
  createValue: (metadata, context) => {
    const { readNextChunk, cancel } =
      'channelId' in metadata
        ? context.createChannelChunkReader(metadata.channelId)
        : context.createInlineChunkReader(metadata.__index)
    const stream = new ReadableStream<Uint8Array<ArrayBuffer>>({
      async pull(controller) {
        try {
          const chunk = await readNextChunk()
          if (chunk === null) controller.close()
          else controller.enqueue(chunk)
        } catch (err) {
          cancel()
          controller.error(err)
        }
      },
      cancel,
    })
    globalObject.gcRegistry.register(stream, cancel)
    return {
      value: stream,
      close: cancel,
    }
  },
}
