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
        // Nothing awaits these: they reject with an error the read already threw, or with the source's cancel() failure.
        chunks.return(undefined).catch(() => {})
        reader.cancel(reason).catch(() => {})
      },
    }
  },
}
