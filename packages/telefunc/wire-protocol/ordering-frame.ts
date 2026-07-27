export { ORDERING_FRAME_HEADER_BYTES, ORDERING_FRAME_LAYOUT, encodeOrderingFrame, decodeOrderingFrame }
export type { OrderingInfo }

import { assert } from '../utils/assert.js'

type OrderingInfo = { seq: number; timestamp: number }

const ORDERING_FRAME_LAYOUT = Object.freeze({
  headerBytes: 16,
  wordBytes: 4,
  wordRange: 0x1_0000_0000,
  endianness: 'big',
  offsets: Object.freeze({
    seqHigh: 0,
    seqLow: 4,
    timestampHigh: 8,
    timestampLow: 12,
  }),
} as const)
const ORDERING_FRAME_HEADER_BYTES = ORDERING_FRAME_LAYOUT.headerBytes

/** The one L4 ordering frame: `[seq_hi][seq_lo][ts_hi][ts_lo][payload]`, all u32 big-endian. */
function encodeOrderingFrame(payload: Uint8Array, info: OrderingInfo): Uint8Array {
  assertOrderingInfo(info)
  const frame = new Uint8Array(ORDERING_FRAME_HEADER_BYTES + payload.byteLength)
  const view = new DataView(frame.buffer)
  const { offsets, wordRange } = ORDERING_FRAME_LAYOUT
  view.setUint32(offsets.seqHigh, Math.floor(info.seq / wordRange), false)
  view.setUint32(offsets.seqLow, info.seq % wordRange, false)
  view.setUint32(offsets.timestampHigh, Math.floor(info.timestamp / wordRange), false)
  view.setUint32(offsets.timestampLow, info.timestamp % wordRange, false)
  frame.set(payload, ORDERING_FRAME_HEADER_BYTES)
  return frame
}

function decodeOrderingFrame(frame: Uint8Array): { payload: Uint8Array; info: OrderingInfo } {
  assert(frame.byteLength >= ORDERING_FRAME_HEADER_BYTES, 'Ordering frame is shorter than its 16-byte header')
  const view = new DataView(frame.buffer, frame.byteOffset, frame.byteLength)
  const { offsets, wordRange } = ORDERING_FRAME_LAYOUT
  const info = {
    seq: view.getUint32(offsets.seqHigh, false) * wordRange + view.getUint32(offsets.seqLow, false),
    timestamp: view.getUint32(offsets.timestampHigh, false) * wordRange + view.getUint32(offsets.timestampLow, false),
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
