export { readableStreamReplacer }

import type { ClientReplacerContext, StreamingReplacerType, ReadableStreamRequestContract } from '../../types.js'
import { SERIALIZER_PREFIX_STREAM } from '../../../constants.js'
import { producerOfStream } from '../../producerOfStream.js'
import { assertUsage } from '../../../../utils/assert.js'

const readableStreamReplacer: StreamingReplacerType<ReadableStreamRequestContract, ClientReplacerContext> = {
  prefix: SERIALIZER_PREFIX_STREAM,
  detect: (value) => value instanceof ReadableStream,
  replace: (value, context) => {
    const { metadata, close, abort } = context.sendStream(() => readableStreamReplacer.createProducer(value))
    return { metadata, close, abort }
  },
  createProducer: (value) =>
    producerOfStream(value, (chunk) => {
      try {
        assertUsage(chunk instanceof Uint8Array, 'Stream chunks given to a telefunction must be Uint8Array.')
      } catch (error) {
        // The upload fails as a failing stream does, whose error the app has: only the console tells it this one.
        console.error(error)
        throw error
      }
    }),
}
