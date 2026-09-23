export { ORDERING_FRAME_HEADER_BYTES, encodeOrderingFrame, decodeOrderingFrame, isOrderingPosition }
export type { OrderingInfo }

import { assert } from '../utils/assert.js'

type OrderingInfo = { seq: number; timestamp: number }

// `[seq_hi][seq_lo][ts_hi][ts_lo][payload]`, u32 big-endian words; Redis's Lua writes the same bytes.
const ORDERING_FRAME_HEADER_BYTES = 16
const WORD_RANGE = 0x1_0000_0000

/** A positive safe-integer `seq` and a non-negative safe-integer `timestamp`. */
function isOrderingPosition({ seq, timestamp }: OrderingInfo): boolean {
  return Number.isSafeInteger(seq) && seq > 0 && Number.isSafeInteger(timestamp) && timestamp >= 0
}

/** `prefix`, when given, leads the frame in the same allocation. */
function encodeOrderingFrame(payload: Uint8Array, info: OrderingInfo, prefix?: Uint8Array): Uint8Array {
  assert(isOrderingPosition(info))
  const start = prefix?.byteLength ?? 0
  const frame = new Uint8Array(start + ORDERING_FRAME_HEADER_BYTES + payload.byteLength)
  if (prefix) frame.set(prefix)
  const view = new DataView(frame.buffer, start)
  view.setUint32(0, Math.floor(info.seq / WORD_RANGE))
  view.setUint32(4, info.seq % WORD_RANGE)
  view.setUint32(8, Math.floor(info.timestamp / WORD_RANGE))
  view.setUint32(12, info.timestamp % WORD_RANGE)
  frame.set(payload, start + ORDERING_FRAME_HEADER_BYTES)
  return frame
}

function decodeOrderingFrame(frame: Uint8Array): { payload: Uint8Array; info: OrderingInfo } {
  assert(frame.byteLength >= ORDERING_FRAME_HEADER_BYTES, 'Ordering frame is shorter than its 16-byte header')
  const view = new DataView(frame.buffer, frame.byteOffset, frame.byteLength)
  const info = {
    seq: view.getUint32(0) * WORD_RANGE + view.getUint32(4),
    timestamp: view.getUint32(8) * WORD_RANGE + view.getUint32(12),
  }
  assert(isOrderingPosition(info), 'Ordering frame carries an invalid position')
  return { payload: frame.subarray(ORDERING_FRAME_HEADER_BYTES), info }
}
