export { createStreamReviver }

import type { Reviver } from '@brillout/json-serializer/parse'
import {
  SERIALIZER_PREFIX_STREAM,
  SERIALIZER_PREFIX_GENERATOR,
  type StreamMetadata,
  type GeneratorMetadata,
} from './constants.js'

/**
 * Reviver for json-serializer that reconstructs a single ReadableStream or AsyncGenerator
 * from a metadata placeholder.
 */
function createStreamReviver(callbacks: {
  createStream: (meta: StreamMetadata) => ReadableStream<Uint8Array>
  createGenerator: (meta: GeneratorMetadata) => AsyncGenerator<unknown>
}): Reviver {
  return (_key: undefined | string, value: string, parser: (str: string) => unknown) => {
    if (value.startsWith(SERIALIZER_PREFIX_STREAM)) {
      const meta = parser(value.slice(SERIALIZER_PREFIX_STREAM.length)) as StreamMetadata
      return { replacement: callbacks.createStream(meta) }
    }
    if (value.startsWith(SERIALIZER_PREFIX_GENERATOR)) {
      const meta = parser(value.slice(SERIALIZER_PREFIX_GENERATOR.length)) as GeneratorMetadata
      return { replacement: callbacks.createGenerator(meta) }
    }
    return undefined
  }
}
