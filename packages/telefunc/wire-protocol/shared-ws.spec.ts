import { describe, expect, it } from 'vitest'
import { TAG, decode, encode, encodePublishBinary } from './shared-ws.js'
import { encodeOrderingFrame } from './ordering-frame.js'

const payload = new Uint8Array([0, 1, 2, 3, 4, 5, 6, 7])

describe('PUBLISH_BINARY', () => {
  it.each([
    { seq: 17, timestamp: 1_700_000_000_000 },
    { seq: 0x1_0000_0000, timestamp: 1_700_000_000_000 },
  ])('carries the 16-byte ordering header, then the data, and nothing else (%o)', (info) => {
    const wire = encodePublishBinary(payload, info)
    expect(wire).toEqual(encodeOrderingFrame(payload, info))
    expect(decode(encode.publishBinary(0, wire, 1))).toMatchObject({ tag: TAG.PUBLISH_BINARY, data: payload, info })
  })
})
