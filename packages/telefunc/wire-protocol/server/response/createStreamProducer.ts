export { createStreamProducer }

import type { StreamingProducer } from '../../types.js'
import { assertUsage } from '../../../utils/assert.js'

// Reader acquired eagerly so `cancel()` can interrupt a pending `reader.read()` —
// `gen.return()` alone can't resolve a suspended await.
function createStreamProducer(stream: ReadableStream<Uint8Array<ArrayBuffer>>): StreamingProducer {
  const reader = stream.getReader()
  const chunks = (async function* () {
    try {
      while (true) {
        const { done, value: chunk } = await reader.read()
        if (done) break
        assertUsage(chunk instanceof Uint8Array, 'Stream chunks returned by a telefunction must be Uint8Array.')
        yield chunk
      }
    } finally {
      await reader.cancel()
    }
  })()
  return {
    chunks,
    cancel: (reason) => {
      chunks.return(undefined)
      reader.cancel(reason)
    },
  }
}
