import { describe, expect, it } from 'vitest'
import { decodePublishBinary, encodePublishBinary } from './shared-ws.js'

const payload = new Uint8Array([0, 1, 2, 3, 4, 5, 6, 7])
const info = { seq: 17, timestamp: 1_700_000_000_000 }
const legacyWire = new Uint8Array([17, 0, 0, 0, 0, 0, 128, 86, 254, 188, 120, 66, ...payload])

describe('PUBLISH_BINARY rolling-version compatibility', () => {
  it('decodes the previous writer layout', () => {
    expect(decodePublishBinary(legacyWire)).toEqual({ data: payload, info })
  })

  it('writes the previous reader layout while ordering values fit it', () => {
    expect(encodePublishBinary(payload, info)).toEqual(legacyWire)
  })

  it('makes a wide ordering value fail loudly in the previous reader', () => {
    const wide = { ...info, seq: 0x1_0000_0000 }
    const wire = encodePublishBinary(payload, wide)

    expect(() => legacyDecode(wire)).toThrow('finite numbers')
    expect(decodePublishBinary(wire)).toEqual({ data: payload, info: wide })
  })
})

function legacyDecode(wire: Uint8Array): { data: Uint8Array; info: typeof info } {
  if (wire.byteLength < 12) throw new Error('PUBLISH_BINARY frame too short for info header')
  const view = new DataView(wire.buffer, wire.byteOffset, wire.byteLength)
  const seq = view.getUint32(0, true)
  const timestamp = view.getFloat64(4, true)
  if (!Number.isFinite(seq) || !Number.isFinite(timestamp)) {
    throw new Error('PUBLISH_BINARY frame info must be finite numbers')
  }
  return { data: wire.subarray(12), info: { seq, timestamp } }
}
