export { readableStreamReplacer }

import type { ClientReplacerContext, StreamingReplacerType, ReadableStreamRequestContract } from '../../types.js'
import { SERIALIZER_PREFIX_STREAM } from '../../constants.js'
import { getGlobalObject } from '../../../utils/getGlobalObject.js'

const globalObject = getGlobalObject('wire-protocol/client/request/readable-stream.ts', {
  /** Close the pump channel when the ReadableStream is GC'd without being consumed. */
  gcRegistry: new FinalizationRegistry<() => void>((close) => close()),
})

const readableStreamReplacer: StreamingReplacerType<ReadableStreamRequestContract, ClientReplacerContext> = {
  prefix: SERIALIZER_PREFIX_STREAM,
  detect: (value) => value instanceof ReadableStream,
  getMetadata: (value, context) => {
    const { channelId, close } = context.pumpToChannel(() => readableStreamReplacer.createProducer(value))
    globalObject.gcRegistry.register(value, close)
    return { channelId }
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
        reader.cancel(reason)
      },
    }
  },
}
