import { afterEach, expect, test, vi } from 'vitest'
import { PieceSender } from './pieces.js'

afterEach(() => void vi.restoreAllMocks())

test('an acknowledgement within one tick of a coarsened clock leaves the link unmeasured', () => {
  vi.spyOn(performance, 'now').mockReturnValue(1_000)
  const transmitted: Uint8Array[] = []
  const sender = new PieceSender(
    (message) => void transmitted.push(message),
    () => 0,
  )
  sender.send(new Uint8Array(20_000), 5_000)
  expect(sender.acknowledged(1, 0)).toBe(true)
  transmitted.length = 0
  sender.send(new Uint8Array(600_000), 5_000)
  expect(transmitted.length).toBeGreaterThan(1)
})

test('an acknowledgement counts the bytes of every frame sent, and refuses more than was sent or nothing new', () => {
  const sender = new PieceSender(
    () => {},
    () => 0,
  )
  sender.send(new Uint8Array(20_000), 5_000)
  sender.send(new Uint8Array(100), 5_000)
  expect(sender.acknowledged(20_100, 0)).toBe(true)
  expect(sender.acknowledged(20_100, 0)).toBe(false)
  sender.send(new Uint8Array(100), 5_000)
  expect(sender.acknowledged(20_300, 0)).toBe(false)
  expect(sender.acknowledged(20_200, 0)).toBe(true)
})

test('an acknowledgement is read modulo 2^32', () => {
  const sender = new PieceSender(
    () => {},
    () => 0,
  )
  const frame = new Uint8Array(40_000_000)
  for (let sent = 0; sent < 2 ** 32 + 40_000_000; sent += frame.byteLength) {
    sender.send(frame, 5_000)
    expect(sender.acknowledged((sent + frame.byteLength) % 2 ** 32, 0)).toBe(true)
  }
})
