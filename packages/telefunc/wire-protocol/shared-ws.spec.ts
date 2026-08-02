import { describe, expect, it } from 'vitest'
import { decodePublishBinary, encodePublishBinary } from './shared-ws.js'

const payload = new Uint8Array([0, 1, 2, 3, 4, 5, 6, 7])
const info = { seq: 17, timestamp: 1_700_000_000_000 }
const legacyWire = new Uint8Array([17, 0, 0, 0, 0, 0, 128, 86, 254, 188, 120, 66, ...payload])

describe('PUBLISH_BINARY wire-version boundary', () => {
  it.each([info, { ...info, seq: 0x1_0000_0000 }])(
    'round-trips current ordering info and fails loudly in the previous reader',
    (ordering) => {
      const wire = encodePublishBinary(payload, ordering)
      expect(() => legacyDecode(wire)).toThrow('finite numbers')
      expect(decodePublishBinary(wire)).toEqual({ data: payload, info: ordering })
    },
  )

  it('rejects the previous unversioned writer layout', () => {
    expect(() => decodePublishBinary(legacyWire)).toThrow('unsupported legacy wire format')
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
