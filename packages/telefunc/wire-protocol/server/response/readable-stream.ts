export { readableStreamReplacer }

import type { StreamingReplacerType, ReadableStreamContract, ServerReplacerContext } from '../../types.js'
import { SERIALIZER_PREFIX_STREAM } from '../../constants.js'
import { createStreamProducer } from './createStreamProducer.js'

const readableStreamReplacer: StreamingReplacerType<ReadableStreamContract, ServerReplacerContext> = {
  prefix: SERIALIZER_PREFIX_STREAM,
  detect: (value): value is ReadableStream<Uint8Array<ArrayBuffer>> => value instanceof ReadableStream,
  replace: (value, context) => {
    const { metadata, close, abort } = context.sendStream(() => readableStreamReplacer.createProducer(value))
    return { metadata, close, abort }
  },
  createProducer: (value) => createStreamProducer(value),
}
