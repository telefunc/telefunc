export { readableStreamReviver }

import type { ReviverType, ReadableStreamContract, ClientReviverContext } from '../../types.js'
import { SERIALIZER_PREFIX_STREAM } from '../../constants.js'

const readableStreamReviver: ReviverType<ReadableStreamContract, ClientReviverContext> = {
  prefix: SERIALIZER_PREFIX_STREAM,
  createValue: (metadata, context) => {
    const { readNextChunk, cancel, abort } = context.receiveStream(metadata)
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
    return {
      value: stream,
      close: cancel,
      abort,
    }
  },
}
