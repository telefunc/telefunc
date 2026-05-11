export { createReadableChunkStream }

import { getStreamCompletionError } from './getStreamCompletionError.js'
import type { StreamReadOptions } from './types.js'

function createReadableChunkStream(opts: {
  readNextChunk: () => Promise<Uint8Array<ArrayBuffer> | null>
  cancel: () => void
  isCancelled: () => boolean
  readOptions?: StreamReadOptions
  highWaterMark?: number
}): ReadableStream<Uint8Array<ArrayBuffer>> {
  let received = 0
  return new ReadableStream<Uint8Array<ArrayBuffer>>(
    {
      pull: async (controller) => {
        try {
          const chunk = await opts.readNextChunk()
          if (chunk === null) {
            const completionError = getStreamCompletionError({
              wasCancelled: opts.isCancelled(),
              received,
              readOptions: opts.readOptions,
            })
            if (completionError) {
              throw completionError
            }
            controller.close()
            return
          }
          received += chunk.byteLength
          opts.readOptions?.onChunk?.(chunk.byteLength)
          controller.enqueue(chunk)
        } catch (err) {
          opts.cancel()
          controller.error(err)
        }
      },
      cancel: opts.cancel,
    },
    opts.highWaterMark === undefined ? undefined : { highWaterMark: opts.highWaterMark },
  )
}
