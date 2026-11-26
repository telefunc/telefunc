export { readableStreamType }

import type { ReadableStreamRequestContract, StreamingProducer } from '../../types.js'
import type { ChannelCarriedType } from './registry.js'
import { SERIALIZER_PREFIX_STREAM } from '../../../constants.js'
import { producerOfStream } from '../../producerOfStream.js'
import { assertUsage } from '../../../../utils/assert.js'
import { randomUuid } from '../../../../utils/randomUuid.js'

const readableStreamType: ChannelCarriedType<ReadableStreamRequestContract> & {
  createProducer(stream: ReadableStreamRequestContract['value']): StreamingProducer
} = {
  prefix: SERIALIZER_PREFIX_STREAM,
  detect: (value) => value instanceof ReadableStream,
  claim(stream) {
    // The call takes the stream's reader now: a locked stream throws here, and the app can't read what it passed.
    const producer = readableStreamType.createProducer(stream)
    const channelId = randomUuid()
    return {
      metadata: { channelId },
      open: ({ ClientChannel, pumpProducerToChannel }, connection) =>
        pumpProducerToChannel(producer, new ClientChannel({ channelId, connection })),
      release: (reason) => producer.cancel(reason),
    }
  },
  createProducer: (stream) =>
    producerOfStream(stream, (chunk) => {
      try {
        assertUsage(chunk instanceof Uint8Array, 'Stream chunks given to a telefunction must be Uint8Array.')
      } catch (error) {
        // The upload fails as a failing stream does, whose error the app has: only the console tells it this one.
        console.error(error)
        throw error
      }
    }),
}
