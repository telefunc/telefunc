export { sendChunks }

import { CHANNEL_PUMP_TAG_DATA, CHANNEL_PUMP_TAG_END, STREAM_CHUNK_PART_BYTES } from '../constants.js'
import { concat } from '../wire/envelope.js'
import { createStopRace } from '../../utils/createStopRace.js'
import type { StreamingProducer } from './types.js'

const TAG_DATA = new Uint8Array([CHANNEL_PUMP_TAG_DATA])
const TAG_END = new Uint8Array([CHANNEL_PUMP_TAG_END])

type ChunkChannel = {
  sendBinary(data: Uint8Array): Promise<void>
  _sendBinary(data: Uint8Array): void | Promise<unknown>
}

/** Sends the producer's chunks, each in parts, then its end. `stop` resolving null ends it, and rejecting throws. */
async function sendChunks(channel: ChunkChannel, producer: StreamingProducer, stop: Promise<null>): Promise<void> {
  const race = createStopRace(stop)
  while (true) {
    const next = await race(producer.chunks.next())
    if (next === null) return
    if (next.done) {
      channel.sendBinary(TAG_END)
      return
    }
    for (let at = 0; at < next.value.byteLength; at += STREAM_CHUNK_PART_BYTES) {
      const pending = channel._sendBinary(concat(TAG_DATA, next.value.subarray(at, at + STREAM_CHUNK_PART_BYTES)))
      if (pending) await pending
    }
  }
}
