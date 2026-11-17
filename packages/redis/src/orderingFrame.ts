export { decodeOrderingFrame }

import { assertDriverOrdering, type OrderingInfo } from 'telefunc/__internal'
import { assert } from './assert.js'

// `[seq_hi][seq_lo][ts_hi][ts_lo][payload]`, u32 big-endian words, as commands.ts's Lua writes them.
const ORDERING_FRAME_HEADER_BYTES = 16
const WORD_RANGE = 0x1_0000_0000

function decodeOrderingFrame(frame: Uint8Array): { payload: Uint8Array; info: OrderingInfo } {
  assert(frame.byteLength >= ORDERING_FRAME_HEADER_BYTES, 'Ordering frame is shorter than its 16-byte header')
  const view = new DataView(frame.buffer, frame.byteOffset, frame.byteLength)
  const info = {
    seq: view.getUint32(0) * WORD_RANGE + view.getUint32(4),
    timestamp: view.getUint32(8) * WORD_RANGE + view.getUint32(12),
  }
  assertDriverOrdering(info)
  return { payload: frame.subarray(ORDERING_FRAME_HEADER_BYTES), info }
}
