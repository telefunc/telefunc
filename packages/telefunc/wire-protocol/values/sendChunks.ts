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
 *  it, and rejecting throws, also while a part waits for the page's credit. */
async function sendChunks(
  channel: ChunkChannel,
  producer: StreamingProducer,
  stop: Promise<null>,
  options?: { preserveEmptyChunks: true },
): Promise<void> {
  const race = createStopRace(stop)
  while (true) {
    const next = await race(producer.chunks.next())
    if (next === null) return
    if (next.done) {
      channel.sendBinary(TAG_END)
      return
    }
    const chunk = next.value
    if (chunk.byteLength === 0 && !options?.preserveEmptyChunks) continue
    let at = 0
    do {
      const end = at + STREAM_CHUNK_PART_BYTES
      const tag = end >= chunk.byteLength ? TAG_DATA : TAG_PART
      const pending = channel._sendBinary(concat(tag, chunk.subarray(at, end)))
      if (pending && (await race(pending)) === null) return
      at = end
    } while (at < chunk.byteLength)
  }
}
