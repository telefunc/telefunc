export { producerOfStream }

import { assertUsage } from '../../utils/assert.js'
import type { StreamingProducer } from './types.js'

/** The producer of `stream`'s chunks, each validated by `checkChunk` before it goes. The reader is taken at
 *  once, so that `cancel()` can interrupt a pending `reader.read()`: `chunks.return()` alone can't resolve a suspended
 *  await. */
function producerOfStream(
  stream: ReadableStream<Uint8Array<ArrayBuffer>>,
  checkChunk = checkResponseChunk,
): StreamingProducer {
  const reader = stream.getReader()
  /** `ended`: its last read found the stream's end, so no `cancel()` is due. */
  let phase: 'reading' | 'ended' | 'released' = 'reading'
  // The stream is the app's: at its end or on `cancel()`, whichever comes first, it is let go of at once for the app's
  // own `cancel()` or another read, while its source may still clean up. A `cancel()` before the first read ends
  // `chunks` without running its `finally`.
  const letGo = (reason?: unknown): void => {
    if (phase === 'released') return
    const reading = phase === 'reading'
    phase = 'released'
    // Nothing awaits it: a read that failed already threw its error.
    if (reading) reader.cancel(reason).catch(() => {})
    reader.releaseLock()
  }
  const chunks = (async function* () {
    try {
      while (true) {
        const { done, value: chunk } = await reader.read()
        if (done) {
          if (phase === 'reading') phase = 'ended'
          break
        }
        checkChunk(chunk)
        yield chunk
      }
    } finally {
      letGo()
    }
  })()
  return {
    chunks,
    cancel: (reason) => {
      letGo(reason)
      // Nothing awaits it: it rejects with the source's error.
      chunks.return(undefined).catch(() => {})
    },
  }
}

function checkResponseChunk(chunk: unknown): void {
  assertUsage(chunk instanceof Uint8Array, 'Stream chunks returned by a telefunction must be Uint8Array.')
}
