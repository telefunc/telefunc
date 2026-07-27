export { ORDERING_FRAME_HEADER_BYTES, encodeOrderingFrame, decodeOrderingFrame }
export type { OrderingInfo }

import { assert } from '../utils/assert.js'

type OrderingInfo = { seq: number; timestamp: number }

const ORDERING_FRAME_HEADER_BYTES = 16
const U32_RANGE = 0x1_0000_0000

/** The one L4 ordering frame: `[seq_hi][seq_lo][ts_hi][ts_lo][payload]`, all u32 big-endian. */
function encodeOrderingFrame(payload: Uint8Array, info: OrderingInfo): Uint8Array {
  assertOrderingInfo(info)
  const frame = new Uint8Array(ORDERING_FRAME_HEADER_BYTES + payload.byteLength)
  const view = new DataView(frame.buffer)
  view.setUint32(0, Math.floor(info.seq / U32_RANGE), false)
  view.setUint32(4, info.seq % U32_RANGE, false)
  view.setUint32(8, Math.floor(info.timestamp / U32_RANGE), false)
  view.setUint32(12, info.timestamp % U32_RANGE, false)
  frame.set(payload, ORDERING_FRAME_HEADER_BYTES)
  return frame
}

function decodeOrderingFrame(frame: Uint8Array): { payload: Uint8Array; info: OrderingInfo } {
  assert(frame.byteLength >= ORDERING_FRAME_HEADER_BYTES, 'Ordering frame is shorter than its 16-byte header')
  const view = new DataView(frame.buffer, frame.byteOffset, frame.byteLength)
  const info = {
    seq: view.getUint32(0, false) * U32_RANGE + view.getUint32(4, false),
    timestamp: view.getUint32(8, false) * U32_RANGE + view.getUint32(12, false),
  }
  assertOrderingInfo(info)
  return { payload: frame.subarray(ORDERING_FRAME_HEADER_BYTES), info }
}

function assertOrderingInfo(info: OrderingInfo): void {
  assert(Number.isSafeInteger(info.seq) && info.seq > 0, 'Ordering seq must be a positive safe integer')
  assert(
    Number.isSafeInteger(info.timestamp) && info.timestamp >= 0,
    'Ordering timestamp must be a non-negative safe integer',
  )
}
