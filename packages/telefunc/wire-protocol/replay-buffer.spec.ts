import { afterEach, expect, test, vi } from 'vitest'

import { ReplayBuffer } from './replay-buffer.js'
import { ERROR_REASON, encode } from './shared-ws.js'

/** A text frame of `bytes` payload bytes. */
const text = (seq: number, bytes = 100) => encode.text(0, 'x'.repeat(bytes), seq)

afterEach(() => {
  vi.useRealTimers()
})

test('gives a peer all it lacks, text, binary and closing frames merged by seq', () => {
  const replay = new ReplayBuffer(1_024, 60_000, 1_024)
  replay.push(1, text(1))
  replay.push(2, encode.binary(0, new Uint8Array(8), 2))
  replay.push(3, encode.close(0, 1_000, 3))
  expect(replay.getAfter(0)).toEqual([text(1), encode.binary(0, new Uint8Array(8), 2), encode.close(0, 1_000, 3)])
  expect(replay.getAfter(2)).toEqual([encode.close(0, 1_000, 3)])
  expect(replay.getAfter(3)).toEqual([])
})

test('a frame it dropped to stay within its size, and one larger than that, fail a peer that lacks them, and no other', () => {
  const replay = new ReplayBuffer(256, 60_000, 1_024)
  replay.push(1, text(1, 200))
  replay.push(2, text(2, 200)) // drops 1
  replay.push(3, text(3, 300)) // larger than the text budget
  replay.push(4, encode.binary(0, new Uint8Array(8), 4))
  replay.push(5, text(5, 100)) // drops 2
  expect(replay.getAfter(0)).toBe(ERROR_REASON.LOST)
  expect(replay.getAfter(2)).toBe(ERROR_REASON.LOST)
  expect(replay.getAfter(3)).toEqual([encode.binary(0, new Uint8Array(8), 4), text(5, 100)])
})

test('a frame it dropped for its age fails a peer that lacks it', () => {
  vi.useFakeTimers()
  const replay = new ReplayBuffer(1_024, 1_000, 1_024)
  replay.push(1, text(1))
  vi.advanceTimersByTime(1_500)
  replay.push(2, text(2))
  expect(replay.getAfter(0)).toBe(ERROR_REASON.EXPIRED)
  expect(replay.getAfter(1)).toEqual([text(2)])
})

test('what is past `throughSeq` is not asked for, so its loss fails no one', () => {
  const replay = new ReplayBuffer(256, 60_000, 1_024)
  replay.push(1, text(1))
  replay.push(2, text(2, 300)) // larger than the text budget
  expect(replay.getAfter(0, 1)).toEqual([text(1)])
  expect(replay.getAfter(0)).toBe(ERROR_REASON.LOST)
})

test('a closing frame is kept past the data budgets', () => {
  const replay = new ReplayBuffer(8, 60_000, 8)
  replay.push(1, encode.close(0, 1_000, 1))
  expect(replay.getAfter(0)).toEqual([encode.close(0, 1_000, 1)])
})

test('lets go of what the peer acknowledged, in every lane, and gives the rest', () => {
  const replay = new ReplayBuffer(1_024, 60_000, 1_024)
  replay.push(1, text(1))
  replay.push(2, encode.binary(0, new Uint8Array(8), 2))
  replay.push(3, text(3))
  replay.push(4, encode.close(0, 1_000, 4))
  replay.acknowledge(2)
  expect(replay.length).toBe(2)
  expect(replay.getAfter(2)).toEqual([text(3), encode.close(0, 1_000, 4)])
  replay.acknowledge(4)
  expect(replay.length).toBe(0)
  expect(replay.byteLength).toBe(0)
  expect(replay.getAfter(4)).toEqual([])
})

test('counts what flow control counts, the bytes of the payloads', () => {
  const replay = new ReplayBuffer(300, 60_000, 1_024)
  replay.push(1, text(1, 150))
  replay.push(2, text(2, 150)) // with their headers, more than 300 bytes
  expect(replay.byteLength).toBe(300)
  expect(replay.getAfter(0)).toEqual([text(1, 150), text(2, 150)])
})

test('a lower budget keeps what was sent under the higher one until the peer has it, then holds what came after to it', () => {
  const replay = new ReplayBuffer(1_024, 60_000, 1_024)
  const send = (bytes: number) => {
    const seq = replay.nextSeq()
    replay.push(seq, text(seq, bytes))
  }
  send(300)
  send(300)
  const queued = replay.nextSeq() // sent before the budget was known, stored after it
  replay.setLimits(256, 60_000, 1_024)
  replay.push(queued, text(queued, 200))
  send(200)
  expect(replay.getAfter(0)).toEqual([text(1, 300), text(2, 300), text(3, 200), text(4, 200)])
  replay.acknowledge(2)
  expect(replay.getAfter(2)).toEqual([text(3, 200), text(4, 200)])
  replay.acknowledge(3)
  expect(replay.getAfter(3)).toEqual([text(4, 200)])
  send(200) // with 4, past the lower budget
  expect(replay.getAfter(3)).toBe(ERROR_REASON.LOST)
})

test('a higher budget applies at once, and a lower one at once when the peer has all that was sent', () => {
  const replay = new ReplayBuffer(256, 60_000, 1_024)
  replay.setLimits(1_024, 60_000, 1_024)
  replay.push(1, text(1, 400))
  replay.push(2, text(2, 400))
  expect(replay.getAfter(0)).toEqual([text(1, 400), text(2, 400)])
  replay.acknowledge(2)
  replay.setLimits(256, 60_000, 1_024)
  replay.push(3, text(3, 200))
  replay.push(4, text(4, 200))
  expect(replay.getAfter(2)).toBe(ERROR_REASON.LOST)
})
