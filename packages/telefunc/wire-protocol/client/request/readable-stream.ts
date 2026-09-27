export { readableStreamReplacer }

import type { ClientReplacerContext, StreamingReplacerType, ReadableStreamRequestContract } from '../../types.js'
import { SERIALIZER_PREFIX_STREAM } from '../../constants.js'

const readableStreamReplacer: StreamingReplacerType<ReadableStreamRequestContract, ClientReplacerContext> = {
  prefix: SERIALIZER_PREFIX_STREAM,
  detect: (value) => value instanceof ReadableStream,
  replace: (value, context) => {
    const { metadata, close, abort } = context.sendStream(() => readableStreamReplacer.createProducer(value))
    return { metadata, close, abort }
  },
  createProducer: (value) => {
    const reader = value.getReader()
    const chunks = (async function* () {
      try {
        while (true) {
          const { done, value: chunk } = await reader.read()
          if (done) break
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
        // Nothing awaits this cancel: it rejects with the error an errored stream's read already threw, or with a
        // source's own cancel() failure.
        reader.cancel(reason).catch(() => {})
      },
    }
  },
}
