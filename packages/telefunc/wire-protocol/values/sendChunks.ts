export { sendChunks }

import {
  CHANNEL_PUMP_TAG_DATA,
  CHANNEL_PUMP_TAG_END,
  CHANNEL_PUMP_TAG_PART,
  STREAM_CHUNK_PART_BYTES,
} from '../constants.js'
import { concat } from '../wire/envelope.js'
import { createStopRace } from '../../utils/createStopRace.js'
import type { StreamingProducer } from './types.js'

const TAG_DATA = new Uint8Array([CHANNEL_PUMP_TAG_DATA])
const TAG_END = new Uint8Array([CHANNEL_PUMP_TAG_END])
const TAG_PART = new Uint8Array([CHANNEL_PUMP_TAG_PART])

type ChunkChannel = {
  sendBinary(data: Uint8Array): Promise<void>
  _sendBinary(data: Uint8Array): void | Promise<unknown>
}

/** Sends the producer's chunks, each in parts with its last tagged `TAG_DATA`, then its end. `stop` resolving null ends
 *  it, and rejecting throws. */
async function sendChunks(channel: ChunkChannel, producer: StreamingProducer, stop: Promise<null>): Promise<void> {
  const race = createStopRace(stop)
  while (true) {
    const next = await race(producer.chunks.next())
    if (next === null) return
    if (next.done) {
      channel.sendBinary(TAG_END)
      return
    }
    for (let at = 0; ; at += STREAM_CHUNK_PART_BYTES) {
      const end = at + STREAM_CHUNK_PART_BYTES
      const last = end >= next.value.byteLength
      const pending = channel._sendBinary(concat(last ? TAG_DATA : TAG_PART, next.value.subarray(at, end)))
      if (pending) await pending
      if (last) break
    }
  }
}
