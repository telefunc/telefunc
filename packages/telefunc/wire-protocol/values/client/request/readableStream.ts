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
    const channelId = randomUuid()
    return {
      metadata: { channelId },
      open({ ClientChannel, pumpProducerToChannel }, connection) {
        const channel = new ClientChannel({ channelId, connection })
        return pumpProducerToChannel(readableStreamType.createProducer(stream), channel)
      },
      // A stream the app holds locked rejects its cancel, which is the app's.
      release: (reason) => stream.cancel(reason).catch(() => {}),
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
