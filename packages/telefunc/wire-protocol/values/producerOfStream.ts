export { producerOfStream }

import type { StreamingProducer } from './types.js'

/** The producer of `stream`'s chunks, each passed to `checkChunk`, if given, before it goes. The reader is taken at
 *  once, so that `cancel()` can interrupt a pending `reader.read()`: `chunks.return()` alone can't resolve a suspended
 *  await. */
function producerOfStream(
  stream: ReadableStream<Uint8Array<ArrayBuffer>>,
  checkChunk?: (chunk: unknown) => void,
): StreamingProducer {
  const reader = stream.getReader()
  const chunks = (async function* () {
    try {
      while (true) {
        const { done, value: chunk } = await reader.read()
        if (done) break
        checkChunk?.(chunk)
        yield chunk
      }
    } finally {
      await reader.cancel()
    }
  })()
  return {
    chunks,
    cancel: (reason) => {
      // Nothing awaits these: they reject with the source's error, or its cancel() failure.
      chunks.return(undefined).catch(() => {})
      reader.cancel(reason).catch(() => {})
    },
  }
}
