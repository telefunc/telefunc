import { describe, expect, it } from 'vitest'
import { decodeLaneFrame } from './orderingFrame.js'
import type { LaneRoute } from './LaneHistory.js'

/** A frame as commands.ts's publish script writes it: `[seq_hi][seq_lo][ts_hi][ts_lo]`, u32 big-endian, its id and
 *  its predecessor's, then the payload. */
function frameOf(seq: number, timestamp: number, payload: Uint8Array): Buffer {
  const frame = Buffer.alloc(32 + payload.byteLength)
  const view = new DataView(frame.buffer, frame.byteOffset, frame.byteLength)
  const word = 0x1_0000_0000
  view.setUint32(0, Math.floor(seq / word))
  view.setUint32(4, seq % word)
  view.setUint32(8, Math.floor(timestamp / word))
  view.setUint32(12, timestamp % word)
  view.setBigUint64(16, BigInt(seq))
  view.setBigUint64(24, BigInt(seq - 1))
  frame.set(payload, 32)
  return frame
}

const route: LaneRoute = { deliver() {}, dropped() {}, reportMissed() {}, waitEnded() {} }

describe('Redis live delivery', () => {
  it('decodes the binary header and UTF-8 payload for text subscribers with the same seq/timestamp the publisher saw', () => {
    const published = [
      { text: 'msg-1', seq: 1, timestamp: 1_700_000_002_000 },
      // A seq past one 32-bit word.
      { text: 'héllo ✓', seq: 0x1_0000_0002, timestamp: 1_700_000_002_001 },
    ]
    const received = published.map(({ text, seq, timestamp }) => {
      const frame = frameOf(seq, timestamp, new TextEncoder().encode(text))
      const decoded = decodeLaneFrame(frame, { fanIn: false, placed: false }, route)
      if (decoded === null) throw new Error(`seq ${seq}: not an ordering frame`)
      const { payload, info } = decoded
      return { text: new TextDecoder().decode(payload), ...info }
    })
    expect(received).toEqual(published)
  })
})
