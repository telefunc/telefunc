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
