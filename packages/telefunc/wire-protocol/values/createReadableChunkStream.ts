export { createReadableChunkStream }

import { getStreamCompletionError } from './getStreamCompletionError.js'
import type { StreamReadOptions } from './types.js'

/**
 * Public-boundary `ReadableStream` builder — wraps a pull-based chunk reader
 * into a WHATWG `ReadableStream<Uint8Array>` that's handed to user code
 * (`arg.stream()` on a streaming telefunction argument, telefunction return
 * values, etc.). User code uses standard `ReadableStream` affordances
 * (`for await`, `getReader`, `pipeTo`, `tee`, ...), so returning an
 * `AsyncIterable` here would break that contract.
 *
 * Distinct from telefunc's *internal* response/request body pipes — those
 * stay as bare `AsyncIterable` so the runtime adapter (`Readable.from` on
 * Node, `new ReadableStream({ pull })` on Web) can wrap them once at the
 * very edge, skipping the spec overhead webstreams charge per chunk.
 */
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
