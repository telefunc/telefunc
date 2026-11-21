export { createStreamProducer }

import type { StreamingProducer } from '../../types.js'
import { producerOfStream } from '../../producerOfStream.js'
import { assertUsage } from '../../../../utils/assert.js'

/** The producer of a stream a telefunction returns, whose chunks must be bytes. */
function createStreamProducer(stream: ReadableStream<Uint8Array<ArrayBuffer>>): StreamingProducer {
  return producerOfStream(stream, (chunk) =>
    assertUsage(chunk instanceof Uint8Array, 'Stream chunks returned by a telefunction must be Uint8Array.'),
  )
}
