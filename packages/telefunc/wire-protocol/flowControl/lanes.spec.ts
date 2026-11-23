import { afterEach, beforeEach, describe, expect, test, vi } from 'vitest'

import { addWireCopies, dropWireCopies } from '../channel/server/channelMemory.js'
import { CHANNEL_TRANSPORT, WIRE_QUEUE_DELAY_MS, WIRE_SEND_AHEAD_MIN_BYTES } from '../constants.js'
import { decode, encode, isOneFrame, messageFrames, type PiecesAck } from '../wire/frame.js'
import { HEADER, readU16 } from '../wire/header.js'
import { TAG, isPublishBody, isSequencedTag } from '../wire/tags.js'
import { ClientPieceReceiver } from './client/ClientPieceReceiver.js'
import { ServerHeadroom } from './server/ServerHeadroom.js'
import { PieceRate } from './PieceRate.js'
import { PieceSender } from './pieces.js'

// The link runs on the fake clock, `performance.now()` included, so a busy machine can't slow it.
beforeEach(() => {
  vi.useFakeTimers({ toFake: ['setTimeout', 'clearTimeout', 'setInterval', 'clearInterval', 'performance', 'Date'] })
})
afterEach(() => vi.useRealTimers())
const sleep = (ms: number) => vi.advanceTimersByTimeAsync(ms)

/** More copies than any wire's allowance: the ledger every wire shares has no copy room left. */
const ALL_COPIES = 2 ** 40

/** 100 MB/s, 2 ms each way: a frame's own crossing is under a millisecond, so what it waits is the sender's. */
const FAST = { bytesPerMs: 100_000, delayMs: 2 }

/** Slow 3G: 400 kbit/s, 200 ms each way. */
const SLOW = { bytesPerMs: 50, delayMs: 200 }

/** A server wire to a page through a socket that takes whatever it is handed and reports none of it buffered, as one
 *  whose kernel buffers hold it: the link carries what the socket took, in order, at its rate, `delayMs` each way, and
 *  both can change. The page takes every message as it arrives and answers through its receiver. */
function wireTo(
  { bytesPerMs, delayMs }: { bytesPerMs: number; delayMs: number },
  { pingInterval = 60_000, copiesOnSend = false } = {},
) {
  const shape = { bytesPerMs, delayMs }
  const link: Uint8Array<ArrayBuffer>[] = []
  let linkBytes = 0
  /** Each whole frame the page got: its tag, its channel where it has one, and when. */
  const arrivals: { tag: number; ix: number; at: number }[] = []
  let heldReplies: Uint8Array<ArrayBuffer>[] | null = null
  const sendAck = (frame: Uint8Array<ArrayBuffer>) =>
    void setTimeout(() => headroom.acknowledged(decode(frame) as PiecesAck), shape.delayMs)
  const receiver = new ClientPieceReceiver({
    owner: {
      sendAck: (frame) => {
        if (heldReplies !== null) heldReplies.push(frame)
        else sendAck(frame)
      },
      // The page sends nothing else: an acknowledgement waiting to ride along goes alone.
      carryAck: () => {},
    },
  })
  const got = (frame: Uint8Array, at: number) => {
    const tag = frame[0]!
    arrivals.push({ tag, ix: isSequencedTag(tag) && !isPublishBody(tag) ? readU16(frame, 1) : -1, at })
  }
  const take = (message: Uint8Array<ArrayBuffer>) => {
    const at = performance.now()
    const frames = isOneFrame(message) ? [message] : Array.from(messageFrames(message))
    const own = frames.pop()!
    for (const each of frames) {
      receiver.arrived(each[0]!, each.byteLength, at, 'riding')
      got(each, at)
    }
    const frame = decode(own)
    if (frame.tag === TAG.ANNOUNCE) receiver.announce(frame.bytes, at, frame.floor)
    else if (frame.tag === TAG.PIECE) {
      const whole = receiver.add(frame.total, frame.piece, at)
      if (whole) got(whole, at)
    } else {
      receiver.arrived(frame.tag, own.byteLength, at, 'alone')
      got(own, at)
    }
  }
  let budget = 0
  let last = performance.now()
  const timer = setInterval(() => {
    const now = performance.now()
    budget += (now - last) * shape.bytesPerMs
    last = now
    while (link.length > 0 && link[0]!.byteLength <= budget) {
      const message = link.shift()!
      budget -= message.byteLength
      linkBytes -= message.byteLength
      setTimeout(() => take(message), shape.delayMs)
    }
    if (link.length === 0) budget = 0
  }, 1)
  const headroom: ServerHeadroom = new ServerHeadroom({
    socket: {
      send: (message) => {
        link.push(message)
        linkBytes += message.byteLength
      },
      bufferedAmount: 0,
      copiesOnSend,
    },
    pingInterval,
  })
  let seq = 0
  const send = (frame: (seq: number) => Uint8Array<ArrayBuffer>) => headroom.send(frame(++seq), performance.now())
  return {
    headroom,
    send,
    binary: (ix: number, bytes: number) => send((seq) => encode.binary(ix, new Uint8Array(bytes), seq)),
    holdReplies: () => {
      heldReplies = []
    },
    releaseReplies: () => {
      const replies = heldReplies ?? []
      heldReplies = null
      for (const reply of replies) sendAck(reply)
    },
    reshape: (next: { bytesPerMs: number; delayMs: number }) => Object.assign(shape, next),
    linkBytes: () => linkBytes,
    handed: () => headroom.sender.transmittedBytes,
    arrivals,
    /** When the first frame of `tag` arrived, of channel `ix` where it has one, past `since`. */
    arrived: (tag: number, ix = -1, since = 0) =>
      arrivals.find((a) => a.tag === tag && a.ix === ix && a.at >= since)?.at,
    stop: () => {
      clearInterval(timer)
      headroom.close()
    },
  }
}

type Wire = ReturnType<typeof wireTo>

/** A wire that carried a 64 KiB download on a fast link, then nineteen on Slow 3G 2.5 s apart, each over and the link
 *  idle by the next. */
async function wireThatSlowed(): Promise<Wire> {
  const wire = wireTo({ bytesPerMs: 1_000, delayMs: 10 })
  wire.binary(1, 64 * 1024)
  await sleep(500)
  wire.reshape(SLOW)
  for (let n = 0; n < 19; n++) {
    wire.binary(1, 64 * 1024)
    await sleep(2_500)
  }
  expect(wire.linkBytes()).toBe(0)
  return wire
}

/** A stream on the fast link that keeps 512 KiB waiting for 200 ms, which grows the flight past two messages. */
async function growFlight(wire: Wire): Promise<void> {
  wire.binary(1, 128 * 1024)
  await sleep(1_000)
  for (let t = 0; t < 200; t += 5) {
    while (wire.headroom.sender.waitingBytes < 512 * 1024) wire.binary(1, 64 * 1024 - HEADER)
    await sleep(5)
  }
  await sleep(20)
  expect(wire.headroom.window()).toBeGreaterThan(2 * 64 * 1024)
}

/** The RECONCILED of a page that had `channels` channels open, as large as a page's many joins make it. */
function reconciledOf(channels: number): Uint8Array<ArrayBuffer> {
  return encode.reconciled({
    sessionId: crypto.randomUUID(),
    open: Array.from({ length: channels }, (_, ix) => ({ ix, lastSeq: 1_000_000 + ix })),
    reconnectTimeout: 60_000,
    idleTimeout: 60_000,
    pingInterval: 60_000,
    maxFrameBytes: 16 * 1024 * 1024,
    creditWindow: 1024 * 1024,
    sseFlushThrottle: 10,
    ssePostIdleFlushDelay: 100,
    transports: [CHANNEL_TRANSPORT.WS, CHANNEL_TRANSPORT.SSE],
  })
}

/** A piece sender with lanes whose flight is `flight` at most, at a rate that sends any frame whole that fits it, and the
 *  channel of each binary frame its page got whole, in order. */
function senderUnderFlight(flight: number) {
  const received: number[] = []
  const take = (frame: Uint8Array<ArrayBuffer>) => {
    if (frame[0] === TAG.BINARY) received.push(readU16(frame, 1))
  }
  const receiver = new ClientPieceReceiver({ owner: { sendAck() {}, carryAck() {} } })
  const pieceRate = new PieceRate()
  pieceRate.rise(1_000, 0, false)
  const sender = new PieceSender({
    socket: {
      bufferedAmount: 0,
      send: (message) => {
        for (const frame of isOneFrame(message) ? [message] : messageFrames(message)) {
          const decoded = decode(frame)
          if (decoded.tag !== TAG.PIECE) take(frame)
          else {
            const whole = receiver.add(decoded.total, decoded.piece, performance.now())
            if (whole) take(whole)
          }
        }
      },
    },
    pingInterval: 5_000,
    pieceRate,
    cuts: 'whole',
    lanes: true,
    owner: { window: () => flight, handed() {} },
  })
  return { sender, received }
}

describe('the lanes', () => {
  test("while one channel's data waits, 64 other channels' sends hand the socket nothing past the same window", async () => {
    const wire = await wireThatSlowed()
    try {
      for (let n = 0; n < 32; n++) wire.binary(1, 64 * 1024)
      expect(wire.headroom.sender.dataWaits).toBe(true)
      const before = wire.handed()
      for (let ix = 2; ix < 66; ix++) wire.binary(ix, 32 * 1024)
      expect(wire.handed() - before).toBeLessThanOrEqual(WIRE_SEND_AHEAD_MIN_BYTES)
    } finally {
      wire.stop()
    }
  })

  test("once a fast link slowed, a PONG waits behind no more than the fast link's flight and what the link held", async () => {
    const wire = wireTo(FAST)
    try {
      await growFlight(wire)
      const fastWindow = wire.headroom.window()
      const at = performance.now()
      const queuedAtSwitch = wire.linkBytes()
      wire.reshape(SLOW)
      for (let n = 0; n < 64; n++) wire.binary(1, 64 * 1024 - HEADER)
      wire.send((seq) => encode.text(2, 'small', seq))
      wire.send(() => encode.pong())
      const ahead = wire.linkBytes()
      await sleep(100_000)
      // One round trip of the fast link is handed before any report shows the slowdown, a frame each side of it.
      const bound = queuedAtSwitch + fastWindow + 2 * 64 * 1024
      expect(ahead).toBeLessThan(bound)
      expect(wire.arrived(TAG.PONG, -1, at)! - at).toBeLessThan(bound / SLOW.bytesPerMs + SLOW.delayMs + 50)
    } finally {
      wire.stop()
    }
  })

  test("a publish larger than the wire's copy allowance goes once the copy pressure ends", async () => {
    const wire = wireTo(FAST, { copiesOnSend: true })
    let pressured = false
    try {
      await growFlight(wire)
      await sleep(1_000)
      addWireCopies(ALL_COPIES)
      pressured = true
      wire.send((seq) => encode.publishTo(2, seq))
      wire.send(() => encode.publishBinary(new Uint8Array(100 * 1024), { seq: 1, timestamp: 0 }))
      await sleep(1_000)
      dropWireCopies(ALL_COPIES)
      pressured = false
      await sleep(1_000)
      expect(wire.headroom.sender.waitingBytes).toBe(0)
    } finally {
      if (pressured) dropWireCopies(ALL_COPIES)
      wire.stop()
    }
  })

  test('acknowledgements the page held back grow the flight by what the link carries, not by what arrived meanwhile', async () => {
    const rate = 200
    const wire = wireTo({ bytesPerMs: rate, delayMs: 2 })
    try {
      wire.binary(1, 128 * 1024)
      while (wire.headroom.sender.dataWaits || wire.headroom.sender.confirmedBytes < wire.handed()) await sleep(1)
      wire.holdReplies()
      const window = wire.headroom.window()
      const before = wire.headroom.sender.confirmedBytes
      for (let n = 0; n < Math.floor(window / 1_024); n++) {
        wire.binary(1, 1_024 - HEADER)
        await sleep(8)
      }
      expect(wire.headroom.sender.confirmedBytes).toBe(before)
      wire.binary(1, 1_024 - HEADER)
      await sleep(4)
      wire.releaseReplies()
      await sleep(16)
      expect(wire.headroom.window()).toBeLessThanOrEqual(2 * window + rate * WIRE_QUEUE_DELAY_MS)
    } finally {
      wire.stop()
    }
  })

  test('a heartbeat and an acknowledgement of the same bytes grow the flight once', async () => {
    const headroom = new ServerHeadroom({ socket: { send: () => {}, bufferedAmount: 0 }, pingInterval: 60_000 })
    try {
      let seq = 0
      // 2 KiB frames past the flight, which wait whole: no probe.
      for (let n = 0; n < 40; n++) headroom.send(encode.binary(1, new Uint8Array(2048), ++seq), performance.now())
      await Promise.resolve()
      const took = headroom.sender.transmittedBytes
      const start = headroom.window()
      headroom.heartbeat(took, performance.now())
      const afterHeartbeat = headroom.window()
      headroom.acknowledged({ bytes: took, heldMs: 0, spanUs: 1_000 })
      expect(afterHeartbeat).toBeGreaterThan(start)
      expect(headroom.window()).toBe(afterHeartbeat)
    } finally {
      headroom.close()
    }
  })

  test('under a fixed flight, a channel with a frame past it gets its turn while another channel has many', () => {
    const flight = WIRE_SEND_AHEAD_MIN_BYTES
    const bytes = 2 * flight - 7 * encode.piece(0, new Uint8Array(0)).byteLength - HEADER
    const { sender, received } = senderUnderFlight(flight)
    const send = (ix: number, seq: number, headroom = 0) =>
      sender.send(encode.binary(ix, new Uint8Array(bytes), seq), headroom)
    send(1, 1, flight / 2)
    send(2, 1)
    send(1, 2)
    for (let seq = 3; seq <= 32; seq++) {
      sender.sendWaiting(flight)
      sender.sendWaiting(flight)
      send(1, seq)
    }
    // Channel 1 still has frames waiting: channel 2's went on its turn, not once channel 1 had none left.
    expect(received.filter((ix) => ix === 1).length).toBeGreaterThan(20)
    expect(received.filter((ix) => ix === 2)).toEqual([2])
  })

  test('under a fixed flight, a frame in pieces among channels that always have small messages waiting goes within one turn of each channel per flight of it', () => {
    const flight = WIRE_SEND_AHEAD_MIN_BYTES
    const pieces = 4
    const small = [2, 3, 4, 5]
    const { sender, received } = senderUnderFlight(flight)
    const seqs = new Map<number, number>()
    const send = (ix: number, bytes: number, headroom = 0) => {
      const seq = (seqs.get(ix) ?? 0) + 1
      seqs.set(ix, seq)
      sender.send(encode.binary(ix, new Uint8Array(bytes), seq), headroom)
    }
    send(2, 1_024, flight)
    send(1, pieces * flight)
    // Each release the flight frees goes to one channel's turn, which takes all of it; each turn of channel 1 takes it a
    // flight further.
    const bound = (1 + small.length) * pieces
    let releases = 0
    for (; releases < 2 * bound && !received.includes(1); releases++) {
      for (const ix of small) while ((seqs.get(ix) ?? 0) - received.filter((r) => r === ix).length < 80) send(ix, 1_024)
      sender.sendWaiting(flight)
    }
    expect(releases).toBeLessThanOrEqual(bound)
  })

  test.each([96 * 1024, 130_988, 192 * 1024])(
    'on Slow 3G under sustained copy pressure, two channels with frames of %d B past the copy allowance each get theirs through',
    async (bytes) => {
      const wire = wireTo(SLOW, { copiesOnSend: true, pingInterval: 5_000 })
      addWireCopies(ALL_COPIES)
      try {
        wire.binary(1, bytes)
        wire.binary(2, bytes)
        for (let n = 0; n < 12; n++) wire.binary(1, bytes)
        await sleep(22_000)
        const arrived = wire.arrivals.filter(({ tag }) => tag === TAG.BINARY)
        expect(arrived.filter(({ ix }) => ix === 1).length).toBeGreaterThan(3)
        expect(arrived.filter(({ ix }) => ix === 2).length).toBe(1)
      } finally {
        dropWireCopies(ALL_COPIES)
        wire.stop()
      }
    },
  )
})

describe("the wire's control frames", () => {
  test("a channel's BDP_PING_ACK goes behind what that channel has waiting, so the page's probe counts what its window let out", async () => {
    const wire = wireTo(SLOW)
    try {
      for (let n = 0; n < 4; n++) wire.binary(1, 64 * 1024 - HEADER)
      expect(wire.headroom.sender.dataWaits).toBe(true)
      wire.send(() => encode.bdpPingAck(1, 7, 0, 0))
      await sleep(60_000)
      const order = wire.arrivals
        .filter(({ tag }) => tag === TAG.BINARY || tag === TAG.BDP_PING_ACK)
        .map(({ tag }) => tag)
      expect(order).toEqual([TAG.BINARY, TAG.BINARY, TAG.BINARY, TAG.BINARY, TAG.BDP_PING_ACK])
    } finally {
      wire.stop()
    }
  })

  test('a RECONCILED that waited for the rate goes ahead of the channels once it may pass the frame in pieces', async () => {
    const wire = wireTo(SLOW)
    try {
      // 1 MB in pieces, its rest waiting for the probe's answer.
      wire.binary(1, 1024 * 1024)
      const reconciled = reconciledOf(2_000)
      const at = performance.now()
      wire.send(() => reconciled)
      expect([...wire.headroom.sender.waitingFrames]).toContain(reconciled)
      await sleep(30_000)
      // Behind the 1 MB it would have waited out 20 s of the link.
      expect(wire.arrived(TAG.RECONCILED)! - at).toBeLessThan(5_000)
      expect(wire.arrived(TAG.RECONCILED)!).toBeLessThan(wire.arrived(TAG.BINARY, 1)!)
    } finally {
      wire.stop()
    }
  })

  test('a RECONCILED past what goes whole, smaller than what the frame in pieces has left, goes at once', async () => {
    const wire = wireTo(SLOW)
    try {
      wire.binary(1, 1024 * 1024)
      const reconciled = reconciledOf(800)
      wire.send(() => reconciled)
      expect([...wire.headroom.sender.waitingFrames]).not.toContain(reconciled)
      await sleep(30_000)
      expect(wire.arrived(TAG.RECONCILED)!).toBeLessThan(wire.arrived(TAG.BINARY, 1)!)
    } finally {
      wire.stop()
    }
  })
})
