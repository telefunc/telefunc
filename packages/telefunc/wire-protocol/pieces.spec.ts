import { afterEach, expect, test, vi } from 'vitest'
import { PieceReceiver, PieceSender } from './pieces.js'
import { decode, TAG } from './shared-ws.js'

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

test('a receiver whose peer holds data back acknowledges frames of any size once more than 16 KiB of them are unacknowledged, with the span between arrivals', () => {
  vi.useFakeTimers()
  vi.spyOn(performance, 'now').mockImplementation(() => Date.now())
  const sent: Uint8Array[] = []
  const receiver = new PieceReceiver((frame) => void sent.push(frame), { peerHoldsBack: true })
  receiver.arrived(8_000, true, Date.now())
  receiver.arrived(8_000, true, Date.now())
  expect(sent).toHaveLength(0)
  vi.advanceTimersByTime(20)
  receiver.arrived(1_000, true, Date.now())
  expect(sent.map((frame) => decode(frame))).toEqual([{ tag: TAG.PIECES_ACK, bytes: 17_000, heldMs: 0, spanUs: 0 }])
  // 100 ms on, 10 KB arrive, and 30 ms later 10 KB more: the second ack covers both.
  vi.advanceTimersByTime(100)
  receiver.arrived(10_000, true, Date.now())
  vi.advanceTimersByTime(30)
  receiver.arrived(10_000, true, Date.now())
  expect(sent.map((frame) => decode(frame))[1]).toEqual({
    tag: TAG.PIECES_ACK,
    bytes: 37_000,
    heldMs: 0,
    spanUs: 130_000,
  })
  vi.useRealTimers()
})

test('a receiver whose peer holds nothing back acknowledges a frame over 16 KiB, at most every 50 ms, and no small frames', () => {
  vi.useFakeTimers()
  vi.spyOn(performance, 'now').mockImplementation(() => Date.now())
  const sent: Uint8Array[] = []
  const receiver = new PieceReceiver((frame) => void sent.push(frame), { peerHoldsBack: false })
  for (let i = 0; i < 1_000; i++) receiver.arrived(64, true, Date.now())
  vi.advanceTimersByTime(1_000)
  expect(sent).toHaveLength(0)
  receiver.arrived(20_000, true, Date.now())
  expect(sent.map((frame) => decode(frame))).toEqual([{ tag: TAG.PIECES_ACK, bytes: 84_000, heldMs: 0, spanUs: 0 }])
  vi.advanceTimersByTime(10)
  receiver.arrived(20_000, true, Date.now())
  expect(sent).toHaveLength(1)
  vi.advanceTimersByTime(40)
  expect(sent.map((frame) => decode(frame))[1]).toMatchObject({ bytes: 104_000, heldMs: 40 })
  vi.useRealTimers()
})
