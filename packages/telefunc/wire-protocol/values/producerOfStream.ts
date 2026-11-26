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
  let lettingGo: Promise<void> | null = null
  // The stream is the app's: at its end or on `cancel()`, whichever comes first, it is let go of for the app's own
  // `cancel()` or another read. A `cancel()` before the first read ends `chunks` without running its `finally`.
  const letGo = (reason?: unknown) => (lettingGo ??= cancelAndRelease(reader, reason))
  const chunks = (async function* () {
    try {
      while (true) {
        const { done, value: chunk } = await reader.read()
        if (done) break
        checkChunk?.(chunk)
        yield chunk
      }
    } finally {
      await letGo()
    }
  })()
  return {
    chunks,
    cancel: (reason) => {
      // Nothing awaits it: it rejects with the source's error.
      chunks.return(undefined).catch(() => {})
      void letGo(reason)
    },
  }
}

/** Cancels `reader`'s stream, then releases it. */
async function cancelAndRelease(reader: ReadableStreamDefaultReader<Uint8Array<ArrayBuffer>>, reason: unknown) {
  try {
    await reader.cancel(reason)
  } catch {
    // The app's source failed to cancel: a read that failed already threw its error, and nothing else awaits a cancel.
  } finally {
    reader.releaseLock()
  }
}
