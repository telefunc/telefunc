export { decodeOrderingFrame }

import { isOrdering, type OrderingInfo } from 'telefunc/__internal'

// `[seq_hi][seq_lo][ts_hi][ts_lo][payload]`, u32 big-endian words, as commands.ts's Lua writes them.
const ORDERING_FRAME_HEADER_BYTES = 16
const WORD_RANGE = 0x1_0000_0000

/** The frame's ordering and payload, or null where it is too short or its ordering isn't one Telefunc's scripts write:
 *  any Redis client can publish to a channel. */
function decodeOrderingFrame(frame: Uint8Array): { payload: Uint8Array; info: OrderingInfo } | null {
  if (frame.byteLength < ORDERING_FRAME_HEADER_BYTES) return null
  const view = new DataView(frame.buffer, frame.byteOffset, frame.byteLength)
  const info = {
    seq: view.getUint32(0) * WORD_RANGE + view.getUint32(4),
    timestamp: view.getUint32(8) * WORD_RANGE + view.getUint32(12),
  }
  if (!isOrdering(info)) return null
  return { payload: frame.subarray(ORDERING_FRAME_HEADER_BYTES), info }
}
