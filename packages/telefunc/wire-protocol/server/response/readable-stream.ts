export { readableStreamServerType }

import { assertUsage } from '../../../utils/assert.js'
import { SERIALIZER_PREFIX_STREAM } from '../../constants.js'
import type { ServerStreamingType, ReadableStreamContract } from '../../streaming-types.js'
import type { ServerResponseContext } from './registry.js'

const readableStreamServerType: ServerStreamingType<ReadableStreamContract, ServerResponseContext> = {
  prefix: SERIALIZER_PREFIX_STREAM,
  detect: (value): value is ReadableStream<Uint8Array<ArrayBuffer>> => value instanceof ReadableStream,
  getMetadata: (_value, _context) => ({}),
  createProducer: (value) => {
    // Acquire the reader here so cancel() can call reader.cancel() directly.
    // gen.return() alone cannot interrupt a suspended reader.read();
    // reader.cancel() resolves it immediately and fires the upstream cancel callback.
    const reader = value.getReader()
    const chunks = (async function* () {
      try {
        while (true) {
          const { done, value: chunk } = await reader.read()
          if (done) break
          assertUsage(
            chunk instanceof Uint8Array,
            'ReadableStream returned by a telefunction must yield Uint8Array chunks.',
          )
          yield chunk
        }
      } finally {
        await reader.cancel()
      }
    })()
    return {
      chunks,
      cancel: () => {
        chunks.return(undefined)
        reader.cancel()
      },
    }
  },
}
