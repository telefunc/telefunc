export { readableStreamClientType }

import { SERIALIZER_PREFIX_STREAM } from '../../constants.js'
import type { ClientStreamingType, ReadableStreamContract } from '../../streaming-types.js'

const readableStreamClientType: ClientStreamingType<ReadableStreamContract> = {
  prefix: SERIALIZER_PREFIX_STREAM,
  createValue: (_metadata, readNextChunk, cancel) => {
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
    }
  },
}
