import { describe, expect, it } from 'vitest'
import { decodePublishBinary, encodePublishBinary } from './shared-ws.js'
import {
  ORDERING_FRAME_HEADER_BYTES,
  ORDERING_FRAME_LAYOUT,
  decodeOrderingFrame,
  encodeOrderingFrame,
} from './ordering-frame.js'

describe('shared ordering frame', () => {
  it('preserves sequence and timestamp values wider than 32 bits', () => {
    const payload = new Uint8Array([0, 127, 128, 255])
    const info = { seq: 0x1_0000_0007, timestamp: 0x2_0000_0009 }
    const frame = encodeOrderingFrame(payload, info)

    expect(frame.byteLength).toBe(ORDERING_FRAME_HEADER_BYTES + payload.byteLength)
    expect(decodeOrderingFrame(frame)).toEqual({ payload, info })
    expect(decodePublishBinary(encodePublishBinary(payload, info))).toEqual({ data: payload, info })
  })

  it('encodes the shared 16-byte header as four big-endian u32 words', () => {
    expect(ORDERING_FRAME_LAYOUT).toEqual({
      headerBytes: 16,
      wordBytes: 4,
      wordRange: 0x1_0000_0000,
      endianness: 'big',
      offsets: { seqHigh: 0, seqLow: 4, timestampHigh: 8, timestampLow: 12 },
    })
    const frame = encodeOrderingFrame(new Uint8Array([9]), {
      seq: 0x2_0000_0003,
      timestamp: 0x4_0000_0005,
    })
    const view = new DataView(frame.buffer, frame.byteOffset, frame.byteLength)

    expect([
      view.getUint32(0, false),
      view.getUint32(4, false),
      view.getUint32(8, false),
      view.getUint32(12, false),
    ]).toEqual([2, 3, 4, 5])
    expect(frame[ORDERING_FRAME_HEADER_BYTES]).toBe(9)
  })

  it('rejects truncated frames and unsafe ordering metadata', () => {
    expect(() => decodeOrderingFrame(new Uint8Array(ORDERING_FRAME_HEADER_BYTES - 1))).toThrow(
      'shorter than its 16-byte header',
    )
    expect(() => encodeOrderingFrame(new Uint8Array(), { seq: Number.MAX_SAFE_INTEGER + 1, timestamp: 0 })).toThrow(
      'positive safe integer',
    )
  })
})
