export { decodeOrderingFrame }
export type { DecodedOrderingFrame }

import { isOrdering, type OrderingInfo } from 'telefunc/__internal'
import { NO_ID } from './orderRecord.js'

// `[seq_hi][seq_lo][ts_hi][ts_lo]`, u32 big-endian words, then the commit's id and its predecessor's on its lane, 8 bytes
// each, then the payload, as commands.ts's Lua writes them.
const ORDERING_FRAME_HEADER_BYTES = 32
const WORD_RANGE = 0x1_0000_0000

/** A frame's ordering, its commit's id and the id of the commit before it on its lane (`NO_ID` for none), and its
 *  payload. */
type DecodedOrderingFrame = { payload: Uint8Array; info: OrderingInfo; id: bigint; prev: bigint }

/** The frame's parts, or null where it is too short or its ordering isn't one Telefunc's scripts write: any Redis client
 *  can publish to a channel. */
function decodeOrderingFrame(frame: Uint8Array): DecodedOrderingFrame | null {
  if (frame.byteLength < ORDERING_FRAME_HEADER_BYTES) return null
  const view = new DataView(frame.buffer, frame.byteOffset, frame.byteLength)
  const info = {
    seq: view.getUint32(0) * WORD_RANGE + view.getUint32(4),
    timestamp: view.getUint32(8) * WORD_RANGE + view.getUint32(12),
  }
  const id = view.getBigUint64(16)
  if (!isOrdering(info) || id === NO_ID) return null
  return { payload: frame.subarray(ORDERING_FRAME_HEADER_BYTES), info, id, prev: view.getBigUint64(24) }
}
