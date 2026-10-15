import { afterEach, expect, test, vi } from 'vitest'
import { Link } from './link.js'
import { PieceSender } from './pieces.js'
import { PagePieceReceiver } from './client/page-piece-receiver.js'
import { ServerPieceReceiver } from './server/server-piece-receiver.js'
import { Uplink } from './client/uplink.js'
import { decode, encode, messageFrames, TAG } from './shared-ws.js'

afterEach(() => void vi.restoreAllMocks())

const data = (bytes: number) => Object.assign(new Uint8Array(bytes), { 0: TAG.BINARY })

/** The bytes a PIECE and an ANNOUNCE add, which both ends count. */
const PIECE = encode.piece(0, new Uint8Array(0)).byteLength
const ANNOUNCE = encode.announce(0, 0).byteLength
/** Where the two pieces of a probe pair end, behind its announcement and its frame's empty first piece. */
const PAIR_FIRST = ANNOUNCE + 2 * PIECE + 64
const PAIR_SECOND = PAIR_FIRST + PIECE + 2_048
/** The piece a rate of `bytes` per `ms` carries in 5 s / 8. */
const pieceAt = (bytes: number, ms: number) => Math.floor((bytes / ms) * 625)

/** What each message `transmitted` holds: a whole frame's length, a piece's, or the bytes an announcement announces. */
const lengths = (transmitted: Uint8Array[]) =>
  transmitted.map((message) => {
    const frame = decode(message)
    if (frame.tag === TAG.ANNOUNCE) return `announce ${frame.bytes}`
    return frame.tag === TAG.PIECE ? `piece ${frame.piece.byteLength}` : message.byteLength
  })

/** What the microtasks queued so far send: the end of the task. */
const flush = () => Promise.resolve()

test("with no rate measured, a task's frames go as they are sent behind an announcement it closes as it ends, the first large one in pieces that at most double from a probe pair", async () => {
  const transmitted: Uint8Array[] = []
  const sender = new PieceSender(
    (message) => void transmitted.push(message),
    () => 0,
  )
  sender.send(data(60_000), 5_000)
  // 8 KiB may follow any piece; the last two of a frame are cut so the last is the larger.
  expect(lengths(transmitted)).toEqual([
    'announce 0',
    'piece 0',
    'piece 64',
    'piece 2048',
    'piece 8192',
    'piece 16384',
    'piece 11104',
    'piece 22208',
  ])
  sender.send(data(10), 5_000)
  sender.send(data(200_000), 5_000)
  expect(lengths(transmitted.slice(8))).toEqual([
    10,
    'piece 8192',
    'piece 16384',
    'piece 32768',
    'piece 65536',
    'piece 77120',
  ])
  await flush()
  expect(lengths(transmitted.slice(14))).toEqual(['announce 260010'])
})

test("a probe pair's receiver spacing measures the rate later frames are cut by, a ping interval's eighth", async () => {
  const clock = { now: 0 }
  vi.spyOn(performance, 'now').mockImplementation(() => clock.now)
  const transmitted: Uint8Array[] = []
  const sender = new PieceSender(
    (message) => void transmitted.push(message),
    () => 0,
  )
  sender.send(data(100_000), 5_000)
  await flush()
  clock.now = 200
  expect(sender.acknowledged({ bytes: PAIR_FIRST, heldMs: 0, spanUs: 0 })).toBe(true)
  // Until the pair's second is acknowledged, frames still go behind an announcement.
  sender.send(data(5_000), 5_000)
  await flush()
  expect(lengths(transmitted.slice(-3))).toEqual(['announce 0', 5_000, 'announce 5000'])
  // The second, 2048 B and its header, arrived 100 ms after the first.
  expect(sender.acknowledged({ bytes: PAIR_SECOND, heldMs: 0, spanUs: 100_000 })).toBe(true)
  transmitted.length = 0
  sender.send(data(30_000), 5_000)
  const piece = pieceAt(PIECE + 2_048, 100)
  expect(lengths(transmitted)).toEqual(['piece 0', `piece ${piece}`, `piece ${piece}`, `piece ${30_000 - 2 * piece}`])
})

test('a rate that lapsed still cuts a frame at once, until the next measure replaces it, though lower', async () => {
  const clock = { now: 0 }
  vi.spyOn(performance, 'now').mockImplementation(() => clock.now)
  const transmitted: Uint8Array[] = []
  const sender = new PieceSender(
    (message) => void transmitted.push(message),
    () => 0,
  )
  sender.send(data(20_000), 5_000)
  await flush()
  clock.now = 200
  expect(sender.acknowledged({ bytes: PAIR_FIRST, heldMs: 0, spanUs: 0 })).toBe(true)
  expect(sender.acknowledged({ bytes: PAIR_SECOND, heldMs: 0, spanUs: 100_000 })).toBe(true)
  clock.now = 2_200
  expect(sender.acknowledged({ bytes: sender.transmittedBytes, heldMs: 0, spanUs: 0 })).toBe(true)
  clock.now = 70_000
  transmitted.length = 0
  sender.send(data(30_000), 5_000)
  const piece = pieceAt(PIECE + 2_048, 100)
  expect(lengths(transmitted)).toEqual(['piece 0', `piece ${piece}`, `piece ${piece}`, `piece ${30_000 - 2 * piece}`])
  // 30 000 B and three headers in 10 s: about 3 B per ms.
  clock.now = 80_000
  expect(sender.acknowledged({ bytes: sender.transmittedBytes, heldMs: 0, spanUs: 0 })).toBe(true)
  transmitted.length = 0
  sender.send(data(5_000), 5_000)
  const slower = pieceAt(30_000 + 3 * PIECE, 10_000)
  expect(lengths(transmitted)).toEqual(['piece 0', `piece ${slower}`, `piece ${slower}`, `piece ${5_000 - 2 * slower}`])
})

test('an acknowledgement within one tick of a coarsened clock measures a probe at the tick, and nothing of a larger frame', async () => {
  vi.spyOn(performance, 'now').mockReturnValue(1_000)
  const transmitted: Uint8Array[] = []
  const sender = new PieceSender(
    (message) => void transmitted.push(message),
    () => 0,
  )
  sender.send(data(20_000), 5_000)
  await flush()
  expect(sender.acknowledged({ bytes: PAIR_FIRST, heldMs: 0, spanUs: 0 })).toBe(true)
  expect(sender.acknowledged({ bytes: PAIR_SECOND, heldMs: 0, spanUs: 0 })).toBe(true)
  // 2048 B in a tick: a frame that rate carries in 5 s / 8 goes whole.
  const before = transmitted.length
  sender.send(data(1_000_000), 5_000)
  await flush()
  expect(lengths(transmitted.slice(before))).toEqual([1_000_000])
  expect(sender.acknowledged({ bytes: sender.transmittedBytes, heldMs: 0, spanUs: 0 })).toBe(true)
  transmitted.length = 0
  sender.send(data(2_000_000), 5_000)
  expect(transmitted.length).toBeGreaterThan(1)
})

test('a rate counts what the socket holds ahead of a frame but the largest message, which a frame sent in pieces is not', async () => {
  const clock = { now: 0 }
  vi.spyOn(performance, 'now').mockImplementation(() => clock.now)
  let queued = 0
  const link = new Link()
  // 32 B per ms: a frame of 20 000 B goes whole in 5 s / 8.
  link.rise(32, 0)
  const sender = new PieceSender(
    () => {},
    () => queued,
    link,
  )
  sender.send(data(80_000), 5_000)
  clock.now = 10_000
  expect(sender.acknowledged({ bytes: sender.transmittedBytes, heldMs: 0, spanUs: 0 })).toBe(true)
  // The rate lapsed; the socket holds 40 000 B as a whole frame goes.
  clock.now = 70_000
  queued = 40_000
  sender.send(data(20_000), 5_000)
  await flush()
  clock.now = 71_000
  expect(sender.acknowledged({ bytes: sender.transmittedBytes, heldMs: 0, spanUs: 0 })).toBe(true)
  // In 1 s, the 40 000 B less the largest message, a piece of 20 000 B, and the frame.
  expect(link.fastest).toBe((40_000 - (PIECE + 20_000) + 20_000) / 1_000)
})

test('a rate leaves out how long its receiver held what arrived before it acknowledged', async () => {
  const clock = { now: 0 }
  vi.spyOn(performance, 'now').mockImplementation(() => clock.now)
  const link = new Link()
  link.rise(32, 0)
  const sender = new PieceSender(
    () => {},
    () => 0,
    link,
  )
  sender.send(data(20_000), 5_000)
  await flush()
  clock.now = 1_000
  expect(sender.acknowledged({ bytes: sender.transmittedBytes, heldMs: 500, spanUs: 0 })).toBe(true)
  expect(link.fastest).toBe(sender.transmittedBytes / 500)
})

test('an acknowledgement counts the bytes handed to the socket, announcements and piece headers included, and refuses more, or nothing new', () => {
  const sender = new PieceSender(
    () => {},
    () => 0,
  )
  sender.send(new Uint8Array(4_000), 5_000)
  sender.send(new Uint8Array(100), 5_000)
  expect(sender.acknowledged({ bytes: 4_100, heldMs: 0, spanUs: 0 })).toBe(true)
  expect(sender.acknowledged({ bytes: 4_100, heldMs: 0, spanUs: 0 })).toBe(false)
  sender.send(new Uint8Array(100), 5_000)
  expect(sender.acknowledged({ bytes: 4_300, heldMs: 0, spanUs: 0 })).toBe(false)
  expect(sender.acknowledged({ bytes: 4_200, heldMs: 0, spanUs: 0 })).toBe(true)
  // A frame to announce goes behind an announcement, an empty piece, the probe pair, and two more pieces, whose headers
  // count.
  sender.send(new Uint8Array(20_000), 5_000)
  const all = 24_200 + ANNOUNCE + 5 * PIECE
  expect(sender.acknowledged({ bytes: all + 1, heldMs: 0, spanUs: 0 })).toBe(false)
  expect(sender.acknowledged({ bytes: all, heldMs: 0, spanUs: 0 })).toBe(true)
})

test('a frame held for room goes, once room comes, in one message behind what was carried while it waited, whose bytes an acknowledgement counts', () => {
  const transmitted: Uint8Array<ArrayBuffer>[] = []
  let dataRoom = 0
  const link = new Link()
  link.rise(100, performance.now())
  const sender = new PieceSender(
    (message) => void transmitted.push(message),
    () => 0,
    link,
    { dataRoom: () => dataRoom, handed: () => {}, sentFrame: () => {} },
  )
  sender.send(data(1_000), 5_000)
  expect(transmitted).toEqual([])
  const window = encode.window(1, 0, 0)
  sender.carry(() => window)
  dataRoom = Number.POSITIVE_INFINITY
  sender.sendHeld()
  expect(transmitted.map((message) => Array.from(messageFrames(message), (frame) => frame[0]))).toEqual([
    [TAG.WINDOW, TAG.PIECE],
  ])
  const all = window.byteLength + PIECE + 1_000
  expect(sender.acknowledged({ bytes: all + 1, heldMs: 0, spanUs: 0 })).toBe(false)
  expect(sender.acknowledged({ bytes: all, heldMs: 0, spanUs: 0 })).toBe(true)
})

test('an acknowledgement is read modulo 2^32', () => {
  const sender = new PieceSender(
    () => {},
    () => 0,
  )
  sender.send(new Uint8Array(5_000), 5_000)
  expect(sender.acknowledged({ bytes: 4_096, heldMs: 0, spanUs: 0 })).toBe(true)
  expect(sender.acknowledged({ bytes: 5_000, heldMs: 0, spanUs: 0 })).toBe(true)
  const frame = new Uint8Array(40_000_000)
  for (let sent = 5_000; sent < 2 ** 32 + 40_000_000; sent += frame.byteLength) {
    sender.send(frame, 5_000)
    expect(sender.acknowledged({ bytes: (sent + frame.byteLength) % 2 ** 32, heldMs: 0, spanUs: 0 })).toBe(true)
  }
})

test('a receiver whose peer holds data back acknowledges data within 25 ms once 2 KiB of it are unacknowledged, and frames of any size at once once more than 16 KiB are, with the span between arrivals', () => {
  vi.useFakeTimers()
  vi.spyOn(performance, 'now').mockImplementation(() => Date.now())
  const sent: Uint8Array[] = []
  const acks = () => sent.map((frame) => decode(frame))
  const receiver = new ServerPieceReceiver(
    (frame) => void sent.push(frame),
    () => {},
  )
  receiver.arrived(1_000, true, Date.now(), false)
  vi.advanceTimersByTime(100)
  expect(sent).toHaveLength(0)
  receiver.arrived(1_500, true, Date.now(), false)
  vi.advanceTimersByTime(24)
  expect(sent).toHaveLength(0)
  vi.advanceTimersByTime(1)
  expect(acks()).toEqual([{ tag: TAG.PIECES_ACK, bytes: 2_500, heldMs: 25, spanUs: 0 }])
  // 100 ms on, 8 KB twice and 1 KB: past 16 KiB, at once, 125 ms after the data the first ack covered.
  vi.advanceTimersByTime(100)
  receiver.arrived(8_000, true, Date.now(), false)
  receiver.arrived(8_000, true, Date.now(), false)
  receiver.arrived(1_000, true, Date.now(), false)
  expect(acks()[1]).toEqual({ tag: TAG.PIECES_ACK, bytes: 19_500, heldMs: 0, spanUs: 125_000 })
  vi.useRealTimers()
})

test('a receiver whose peer holds nothing back acknowledges a frame over 16 KiB in the next message its side sends, at most every 50 ms, and no small frames', () => {
  vi.useFakeTimers()
  vi.spyOn(performance, 'now').mockImplementation(() => Date.now())
  const sent: Uint8Array[] = []
  let carried: (() => Uint8Array | null) | null = null
  const receiver = new PagePieceReceiver(
    (frame) => void sent.push(frame),
    (ack) => void (carried = ack),
    new Uplink(),
  )
  const next = () => {
    const ack = carried?.() ?? null
    carried = null
    return ack && decode(ack)
  }
  for (let i = 0; i < 1_000; i++) receiver.arrived(64, true, Date.now(), false)
  vi.advanceTimersByTime(1_000)
  expect(carried).toBeNull()
  receiver.arrived(20_000, true, Date.now(), false)
  vi.advanceTimersByTime(5)
  expect(next()).toEqual({ tag: TAG.PIECES_ACK, bytes: 84_000, heldMs: 5, spanUs: 0 })
  // 50 ms after that one went, which was 5 ms after the first arrived.
  vi.advanceTimersByTime(5)
  receiver.arrived(20_000, true, Date.now(), false)
  vi.advanceTimersByTime(44)
  expect(carried).toBeNull()
  vi.advanceTimersByTime(1)
  expect(next()).toMatchObject({ bytes: 104_000, heldMs: 45 })
  expect(sent).toHaveLength(0)
  vi.useRealTimers()
})

test('a receiver puts back a frame from pieces of any size, acknowledging the two after an empty one at once, and holds nothing for empty ones', () => {
  vi.useFakeTimers()
  vi.spyOn(performance, 'now').mockImplementation(() => Date.now())
  const sent: Uint8Array[] = []
  const receiver = new PagePieceReceiver(
    (frame) => void sent.push(frame),
    () => {},
    new Uplink(),
  )
  const frame = data(10_000).map((byte, i) => (i === 0 ? byte : i % 251))
  expect(receiver.add(10_000, frame.subarray(0, 0), Date.now())).toBeNull()
  expect(receiver.add(10_000, frame.subarray(0, 0), Date.now())).toBeNull()
  expect(receiver.held).toBe(0)
  expect(receiver.add(10_000, frame.subarray(0, 1), Date.now())).toBeNull()
  vi.advanceTimersByTime(7)
  expect(receiver.add(10_000, frame.subarray(1, 300), Date.now())).toBeNull()
  // Each piece's header counts, the empty ones' too.
  expect(sent.map((ack) => decode(ack))).toEqual([
    { tag: TAG.PIECES_ACK, bytes: 3 * PIECE + 1, heldMs: 0, spanUs: 0 },
    { tag: TAG.PIECES_ACK, bytes: 4 * PIECE + 300, heldMs: 0, spanUs: 7_000 },
  ])
  expect(receiver.add(10_000, frame.subarray(300, 9_999), Date.now())).toBeNull()
  expect(receiver.held).toBe(9_999)
  expect(receiver.add(10_000, frame.subarray(9_999), Date.now())).toEqual(frame)
  expect(receiver.held).toBe(0)
  // A frame of 10 000 B is too small for its last pieces to be acknowledged.
  expect(sent).toHaveLength(2)
  // Nor are a frame's first two, with no empty piece before them.
  expect(receiver.add(10_000, frame.subarray(0, 1), Date.now())).toBeNull()
  expect(receiver.add(10_000, frame.subarray(1), Date.now())).toEqual(frame)
  expect(sent).toHaveLength(2)
  vi.useRealTimers()
})

test('a receiver waits for the next message of an open announcement twice the time it takes at the rate the newest came at, which may carry twice as much, or 8 KiB, until an announcement of their bytes closes it', () => {
  const receiver = new PagePieceReceiver(
    () => {},
    () => {},
    new Uplink(),
  )
  receiver.announce(0, 0)
  // Before a message of it arrived, it knows no rate.
  expect(receiver.expectedBy(10_000)).toBe(0)
  receiver.add(100_000, new Uint8Array(0), 0)
  receiver.add(100_000, data(64), 10)
  // 64 B in 10 ms; the next may carry 8 KiB: 2 × 8192 / 6.4.
  expect(receiver.expectedBy(10_000)).toBe(10 + 2_560)
  receiver.add(100_000, new Uint8Array(16_384), 1_000)
  // 16 KiB in 990 ms; the next may carry twice as much: 2 × 32 KiB at that rate.
  expect(receiver.expectedBy(10_000)).toBe(1_000 + 3_960)
  expect(receiver.add(100_000, new Uint8Array(100_000 - 64 - 16_384), 2_000)).not.toBeNull()
  // 83,552 B in 1 s; the next may carry twice as much.
  expect(receiver.expectedBy(10_000)).toBe(2_000 + 4_000)
  receiver.announce(100_000, 2_001)
  expect(receiver.expectedBy(10_000)).toBe(0)
})

test.each([
  ['empty after the first', [100, 0]],
  ['longer than what is left of its frame', [100, 9_901]],
])('a receiver refuses a piece %s', (_, sizes) => {
  const receiver = new PagePieceReceiver(
    () => {},
    () => {},
    new Uplink(),
  )
  const frame = data(20_000)
  expect(() => {
    let at = 0
    for (const size of sizes) receiver.add(10_000, frame.subarray(at, (at += size)), 0)
  }).toThrow('PIECE not cut as its frame is')
})
