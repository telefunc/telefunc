export { readableStreamServerType }

import { assertUsage } from '../../../utils/assert.js'
import type { ServerStreamingType, StreamingProducer } from './interface.js'

const readableStreamServerType: ServerStreamingType = {
  prefix: '!TelefuncStream:',
  detect: (value: unknown): boolean => value instanceof ReadableStream,
  getMetadata: (_value: unknown, index: number) => ({ index }),
  createProducer: (value: unknown): StreamingProducer => {
    const stream = value as ReadableStream<Uint8Array>
    // Acquire the reader here so cancel() can call reader.cancel() directly.
    // gen.return() alone cannot interrupt a suspended reader.read();
    // reader.cancel() resolves it immediately and fires the upstream cancel callback.
    const reader = stream.getReader()
    const chunks = (async function* () {
      try {
        while (true) {
          console.log('[server:readable-stream] calling reader.read()')
          const { done, value: chunk } = await reader.read()
          if (done) {
            console.log('[server:readable-stream] reader.read() returned done=true')
            break
          }
          assertUsage(
            chunk instanceof Uint8Array,
            'ReadableStream returned by a telefunction must yield Uint8Array chunks.',
          )
          console.log(`[server:readable-stream] yielding ${chunk.byteLength} bytes`)
          yield chunk
        }
      } finally {
        console.log('[server:readable-stream] finally block — calling reader.cancel()')
        await reader.cancel()
      }
    })()
    return {
      chunks,
      cancel: () => {
        console.log('[server:readable-stream] cancel() called')
        chunks.return(undefined)
        reader.cancel()
      },
    }
  },
}
