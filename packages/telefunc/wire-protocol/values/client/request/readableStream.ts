export { readableStreamReplacer }

import type { ClientReplacerContext, StreamingReplacerType, ReadableStreamRequestContract } from '../../types.js'
import { SERIALIZER_PREFIX_STREAM } from '../../../constants.js'
import { producerOfStream } from '../../producerOfStream.js'

const readableStreamReplacer: StreamingReplacerType<ReadableStreamRequestContract, ClientReplacerContext> = {
  prefix: SERIALIZER_PREFIX_STREAM,
  detect: (value) => value instanceof ReadableStream,
  replace: (value, context) => {
    const { metadata, close, abort } = context.sendStream(() => readableStreamReplacer.createProducer(value))
    return { metadata, close, abort }
  },
  createProducer: (value) => producerOfStream(value),
}
