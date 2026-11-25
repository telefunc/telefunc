import { afterEach, beforeEach, describe, expect, test, vi } from 'vitest'

import { addWireCopies, dropWireCopies } from '../channel/server/channelMemory.js'
import { CHANNEL_PING_INTERVAL_MS, CHANNEL_TRANSPORT, WIRE_SEND_AHEAD_MIN_BYTES } from '../constants.js'
import { decode, encode, isOneFrame, messageFrames, type PiecesAck } from '../wire/frame.js'
import { HEADER, readU16 } from '../wire/header.js'
import { pingDeadline } from '../wire/pingDeadline.js'
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

/** The most copies put past the page's word: the least send-ahead, which a frame's header may cross. */
const COPY_BOUND = WIRE_SEND_AHEAD_MIN_BYTES + HEADER

/** 100 MB/s, 2 ms each way: a frame's own crossing is under a millisecond, so what it waits is the sender's. */
const FAST = { bytesPerMs: 100_000, delayMs: 2 }

/** Slow 3G: 400 kbit/s, 200 ms each way. */
const SLOW = { bytesPerMs: 50, delayMs: 200 }

/** A server wire to a page through a socket that reports what its link holds as buffered, as Node's does once its
 *  kernel buffers are full: the link carries what the socket took, in order, at its rate, `delayMs` each way, and both
 *  can change. The page takes every message as it arrives and answers through its receiver. */
function wireTo(
  { bytesPerMs, delayMs }: { bytesPerMs: number; delayMs: number },
  { pingInterval = 60_000, copiesOnSend = false } = {},
) {
  const shape = { bytesPerMs, delayMs }
  const link: Uint8Array<ArrayBuffer>[] = []
  let linkBytes = 0
  /** Each whole frame the page got: its tag, its channel where it has one, and when. */
  const arrivals: { tag: number; ix: number; at: number }[] = []
  /** When each message reached the page, a piece's included: what its heartbeat counts as the wire delivering. */
  const delivered: number[] = []
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
    delivered.push(at)
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
      get bufferedAmount() {
        return linkBytes
      },
      copiesOnSend,
    },
    pingInterval,
  })
  let seq = 0
  const send = (frame: (seq: number) => Uint8Array<ArrayBuffer>) => headroom.send(frame(++seq))
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
    delivered,
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

/** A step of the clock under a round trip of a millisecond: such a link is stepped by hand, as timers fire whole ms. */
const STEP_MS = 0.05

/** As `wireTo`, on a link whose round trip may be under a millisecond, with the clock stepped by `step()`. */
function stepWireTo({ bytesPerMs, delayMs }: { bytesPerMs: number; delayMs: number }) {
  const toPage: { message: Uint8Array<ArrayBuffer>; due: number }[] = []
  const toServer: { ack: PiecesAck; due: number }[] = []
  let linkFreeAt = 0
  let handed = 0
  let delivered = 0
  const receiver = new ClientPieceReceiver({
    owner: {
      sendAck: (frame) => void toServer.push({ ack: decode(frame) as PiecesAck, due: performance.now() + delayMs }),
      carryAck: () => {},
    },
  })
  const take = (message: Uint8Array<ArrayBuffer>, at: number) => {
    delivered += message.byteLength
    const frames = isOneFrame(message) ? [message] : Array.from(messageFrames(message))
    const own = frames.pop()!
    for (const each of frames) receiver.arrived(each[0]!, each.byteLength, at, 'riding')
    const frame = decode(own)
    if (frame.tag === TAG.ANNOUNCE) receiver.announce(frame.bytes, at, frame.floor)
    else if (frame.tag === TAG.PIECE) receiver.add(frame.total, frame.piece, at)
    else receiver.arrived(frame.tag, own.byteLength, at, 'alone')
  }
  const headroom = new ServerHeadroom({
    socket: {
      send: (message) => {
        handed += message.byteLength
        linkFreeAt = Math.max(linkFreeAt, performance.now()) + message.byteLength / bytesPerMs
        toPage.push({ message, due: linkFreeAt + delayMs })
      },
      bufferedAmount: 0,
    },
    pingInterval: 60_000,
  })
  return {
    headroom,
    /** What the socket was handed, crossing the link alone: the least time it takes. */
    linkMs: () => handed / bytesPerMs + delayMs,
    crossing: () => delivered < handed,
    async step() {
      await vi.advanceTimersByTimeAsync(STEP_MS)
      const now = performance.now()
      while (toPage.length > 0 && toPage[0]!.due <= now) take(toPage.shift()!.message, now)
      while (toServer.length > 0 && toServer[0]!.due <= now) headroom.acknowledged(toServer.shift()!.ack)
    },
    stop: () => headroom.close(),
  }
}

/** The most bytes the page hasn't said it took, over a second of full copy pressure in which `send` sends. */
async function mostUnconfirmedUnderCopyPressure(wire: Wire, send: () => void): Promise<number> {
  const { sender } = wire.headroom
  addWireCopies(ALL_COPIES)
  try {
    send()
    let most = 0
    for (let ms = 0; ms < 1_000; ms++) {
      most = Math.max(most, sender.transmittedBytes - sender.confirmedBytes)
      await sleep(1)
    }
    return most
  } finally {
    dropWireCopies(ALL_COPIES)
  }
}

/** A 64 KiB and a 16 KiB frame of one channel, then 40 KiB of each of three others. */
function framesOfFourChannels(wire: Wire): void {
  wire.send((seq) => encode.binary(1, new Uint8Array(64 * 1024 - HEADER), seq))
  wire.binary(1, 16 * 1024)
  for (const ix of [2, 3, 4]) wire.binary(ix, 40 * 1024)
}

/** A wire that carried a 64 KiB download on a fast link, then nineteen on Slow 3G 2.5 s apart, each over and the link
 *  idle by the next. Each goes as two frames: the second queues behind the first, so its acknowledgement measures the
 *  link. */
async function wireThatSlowed(): Promise<Wire> {
  const wire = wireTo({ bytesPerMs: 1_000, delayMs: 10 })
  wire.binary(1, 64 * 1024)
  await sleep(500)
  wire.reshape(SLOW)
  for (let n = 0; n < 19; n++) {
    wire.binary(1, 32 * 1024)
    wire.binary(1, 32 * 1024)
    await sleep(2_500)
  }
  expect(wire.linkBytes()).toBe(0)
  return wire
}

/** A 16 MiB burst on the fast link: it queues past a rate's least span, so its acknowledgements measure the link. */
async function measureFastLink(wire: Wire): Promise<void> {
  for (let n = 0; n < 256; n++) wire.binary(1, 64 * 1024 - HEADER)
  await sleep(1_000)
  expect(wire.headroom.window()).toBeLessThan(Number.POSITIVE_INFINITY)
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

/** A piece sender with lanes whose window is `window` at most, at a rate that sends any frame whole that fits it, and
 *  the channel of each binary frame its page got whole, in order. */
function senderUnderWindow(window: number) {
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
    owner: { window: () => window, handed() {} },
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

  test("a publish larger than the wire's copy allowance goes once the copy pressure ends", async () => {
    const wire = wireTo(FAST, { copiesOnSend: true })
    let pressured = false
    try {
      await measureFastLink(wire)
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

  test("a report the page sent before it read a request for one isn't its answer, though it covers what went before the request", async () => {
    const asks: number[] = []
    const headroom: ServerHeadroom = new ServerHeadroom({
      socket: {
        send: (message) => {
          const frames = isOneFrame(message) ? [message] : Array.from(messageFrames(message))
          if (frames.some((frame) => frame[0] === TAG.PIECES_ACK_REQ)) asks.push(headroom.sender.transmittedBytes)
        },
        bufferedAmount: 0,
        copiesOnSend: true,
      },
      pingInterval: 60_000,
    })
    addWireCopies(ALL_COPIES)
    try {
      let seq = 0
      // 2 KiB frames past what copies may hold, which wait for the page's word: it hears none, so it asks.
      for (let n = 0; n < 200; n++) headroom.send(encode.binary(1, new Uint8Array(2048), ++seq))
      await sleep(10)
      const [asked = 0] = asks
      expect(asks).toEqual([asked])
      // Half of what went confirmed frees copy room for more data while the request is open.
      await sleep(10)
      headroom.acknowledged({ bytes: Math.floor(asked / 2), heldMs: 0, spanUs: 0 })
      // The answer to the request, 20 ms after it: the page hears nothing again, and it asks of the data handed since.
      await sleep(10)
      headroom.acknowledged({ bytes: asked, heldMs: 0, spanUs: 0 })
      await sleep(10)
      const [, again = 0] = asks
      expect(asks).toEqual([asked, again])
      // 1 ms after that request, the page's report of all it got before it: the request ends its message.
      await sleep(1)
      headroom.acknowledged({ bytes: again - encode.piecesAckReq().byteLength, heldMs: 0, spanUs: 0 })
      expect(headroom['leastRtt']).toBe(20)
    } finally {
      dropWireCopies(ALL_COPIES)
      headroom.close()
    }
  })

  for (const { mbPerS, delayMs, within } of [
    { mbPerS: 100, delayMs: 0.2, within: 1.1 },
    { mbPerS: 1_000, delayMs: 0.05, within: 1.03 },
  ])
    test(`on a ${mbPerS} MB/s link with a ${2 * delayMs} ms round trip, a 64 MiB download reaches the page within ${within} times the link's time`, async () => {
      const wire = stepWireTo({ bytesPerMs: mbPerS * 1_000, delayMs })
      try {
        const TOTAL = 64 * 1024 * 1024
        const FRAME = 64 * 1024
        let seq = 0
        let sent = 0
        let steps = 0
        while (sent < TOTAL || wire.headroom.sender.waitingBytes > 0 || wire.crossing()) {
          // The app keeps 512 KiB waiting, as a stream that writes ahead of the socket does.
          while (sent < TOTAL && wire.headroom.sender.waitingBytes < 512 * 1024) {
            wire.headroom.send(encode.binary(1, new Uint8Array(FRAME - HEADER), ++seq))
            sent += FRAME
          }
          await wire.step()
          steps++
        }
        expect((steps * STEP_MS) / wire.linkMs()).toBeLessThan(within)
      } finally {
        wire.stop()
      }
    })

  test('on a wire whose page took its first frame, frames sent under copy pressure put no more past the page than the least send-ahead', async () => {
    const wire = wireTo(FAST, { copiesOnSend: true })
    try {
      wire.send((seq) => encode.text(1, 'x'.repeat(20 * 1024), seq))
      while (wire.headroom.sender.confirmedBytes < wire.handed()) await sleep(1)
      expect(await mostUnconfirmedUnderCopyPressure(wire, () => framesOfFourChannels(wire))).toBeLessThanOrEqual(
        COPY_BOUND,
      )
    } finally {
      wire.stop()
    }
  })

  test('a second after a 128 KiB frame, frames sent under copy pressure put no more past the page than the least send-ahead', async () => {
    const wire = wireTo(FAST, { copiesOnSend: true })
    try {
      wire.binary(1, 128 * 1024)
      await sleep(1_000)
      expect(await mostUnconfirmedUnderCopyPressure(wire, () => framesOfFourChannels(wire))).toBeLessThanOrEqual(
        COPY_BOUND,
      )
    } finally {
      wire.stop()
    }
  })

  test('under a fixed window, a channel with a frame past it gets its turn while another channel has many', () => {
    const window = WIRE_SEND_AHEAD_MIN_BYTES
    const bytes = 2 * window - 7 * encode.piece(0, new Uint8Array(0)).byteLength - HEADER
    const { sender, received } = senderUnderWindow(window)
    const send = (ix: number, seq: number, headroom = 0) =>
      sender.send(encode.binary(ix, new Uint8Array(bytes), seq), headroom)
    send(1, 1, window / 2)
    send(2, 1)
    send(1, 2)
    for (let seq = 3; seq <= 32; seq++) {
      sender.sendWaiting(window)
      sender.sendWaiting(window)
      send(1, seq)
    }
    // Channel 1 still has frames waiting: channel 2's went on its turn, not once channel 1 had none left.
    expect(received.filter((ix) => ix === 1).length).toBeGreaterThan(20)
    expect(received.filter((ix) => ix === 2)).toEqual([2])
  })

  test('under a fixed window, a frame in pieces among channels that always have small messages waiting goes within one turn of each channel per window of it', () => {
    const window = WIRE_SEND_AHEAD_MIN_BYTES
    const pieces = 4
    const small = [2, 3, 4, 5]
    const { sender, received } = senderUnderWindow(window)
    const seqs = new Map<number, number>()
    const send = (ix: number, bytes: number, headroom = 0) => {
      const seq = (seqs.get(ix) ?? 0) + 1
      seqs.set(ix, seq)
      sender.send(encode.binary(ix, new Uint8Array(bytes), seq), headroom)
    }
    send(2, 1_024, window)
    send(1, pieces * window)
    // Each release the window frees goes to one channel's turn, which takes all of it; each turn of channel 1 takes it a
    // window further.
    const bound = (1 + small.length) * pieces
    let releases = 0
    for (; releases < 2 * bound && !received.includes(1); releases++) {
      for (const ix of small) while ((seqs.get(ix) ?? 0) - received.filter((r) => r === ix).length < 80) send(ix, 1_024)
      sender.sendWaiting(window)
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

/** The longest a page's WebSocket goes with nothing arriving before it is taken for dead, at the default interval. */
const DEADLINE = pingDeadline(CHANNEL_PING_INTERVAL_MS, CHANNEL_TRANSPORT.WS)

/** Channel 1 sends 64 KiB every 50 ms for `ms`, which the link then carries: the longest quiet, and the frames. */
async function downloadFor(wire: Wire, ms: number, during: (t: number) => void = () => {}) {
  const at = performance.now()
  let frames = 0
  for (let t = 0; t < ms; t += 50) {
    during(t)
    wire.binary(1, 64 * 1024 - HEADER)
    frames++
    await sleep(50)
  }
  await sleep(Math.ceil(wire.linkBytes() / SLOW.bytesPerMs) + 60_000)
  const times = [at, ...wire.delivered.filter((t) => t >= at)]
  let quiet = 0
  for (let i = 1; i < times.length; i++) quiet = Math.max(quiet, times[i]! - times[i - 1]!)
  const got = wire.arrivals.filter((a) => a.tag === TAG.BINARY && a.ix === 1 && a.at >= at).length
  return { quiet, frames, got }
}

// What follows a first burst on a slow link waits behind it, as on `main`; a wire that keeps delivering is alive.
describe('a link that is or becomes slow', () => {
  test('on Slow 3G from the start, a download keeps the page within its ping deadline and arrives whole', async () => {
    const wire = wireTo(SLOW)
    try {
      const seen = await downloadFor(wire, 1_000)
      expect(seen.quiet).toBeLessThan(DEADLINE)
      expect(seen.got).toBe(seen.frames)
    } finally {
      wire.stop()
    }
  })

  for (const slow of [
    { bytesPerMs: 1_000, delayMs: 20 },
    { bytesPerMs: 50, delayMs: 200 },
  ])
    test(`a download at 100 MB/s whose link slows to ${slow.bytesPerMs} B/ms keeps the page within its ping deadline and arrives whole`, async () => {
      const wire = wireTo(FAST)
      try {
        const seen = await downloadFor(wire, 2_000, (t) => t === 1_000 && wire.reshape(slow))
        expect(seen.quiet).toBeLessThan(DEADLINE)
        expect(seen.got).toBe(seen.frames)
      } finally {
        wire.stop()
      }
    })
})

describe("the wire's control frames", () => {
  test("a channel's BDP_PING_ACK goes behind what that channel has waiting, so the page's probe counts what its window let out", async () => {
    const wire = await wireThatSlowed()
    try {
      for (let n = 0; n < 4; n++) wire.binary(1, 64 * 1024 - HEADER)
      expect(wire.headroom.sender.dataWaits).toBe(true)
      wire.send(() => encode.bdpPingAck(1, 7, 0, 0))
      const at = performance.now()
      await sleep(60_000)
      const order = wire.arrivals
        .filter((arrival) => arrival.at >= at)
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
