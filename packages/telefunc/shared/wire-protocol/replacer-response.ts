export { createStreamReplacer }

import {
  SERIALIZER_PREFIX_STREAM,
  SERIALIZER_PREFIX_GENERATOR,
  type StreamMetadata,
  type GeneratorMetadata,
} from './constants.js'
import { isAsyncGenerator } from '../../utils/isAsyncGenerator.js'

/**
 * Replacer for json-serializer that replaces a single ReadableStream or AsyncGenerator
 * with a metadata placeholder. Only one streaming value per response is supported.
 */
function createStreamReplacer(callbacks: {
  onStream: (stream: ReadableStream<Uint8Array>) => void
  onGenerator: (gen: AsyncGenerator<unknown>) => void
}) {
  return (_key: string, value: unknown, serializer: (v: unknown) => string) => {
    if (value instanceof ReadableStream) {
      callbacks.onStream(value)
      const meta: StreamMetadata = {}
      return {
        replacement: SERIALIZER_PREFIX_STREAM + serializer(meta),
        resolved: true,
      }
    }
    if (isAsyncGenerator(value)) {
      callbacks.onGenerator(value)
      const meta: GeneratorMetadata = {}
      return {
        replacement: SERIALIZER_PREFIX_GENERATOR + serializer(meta),
        resolved: true,
      }
    }
    return undefined
  }
}
