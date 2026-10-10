import { describe, expect, it } from 'vitest'
import { decodeOrderingFrame, encodeOrderingFrame } from './ordering-frame.js'
describe('ordering frame', () => {
  it('encodes the wide ordering frame as four big-endian u32 words ahead of the payload', () => {
    const payload = new Uint8Array([1, 255])
    const info = { seq: 0x1_0000_0007, timestamp: 0x2_0000_0009 }
    const frame = encodeOrderingFrame(payload, info)
    expect([...frame]).toEqual([0, 0, 0, 1, 0, 0, 0, 7, 0, 0, 0, 2, 0, 0, 0, 9, 1, 255])
    expect(decodeOrderingFrame(frame)).toEqual({ payload, info })
  })
})
