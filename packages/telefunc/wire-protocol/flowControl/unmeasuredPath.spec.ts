import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'

import { decode, encode, isOneFrame, messageFrames, type PiecesAck } from '../wire/frame.js'
import { HEADER } from '../wire/header.js'
import { TAG } from '../wire/tags.js'
import { ClientPieceReceiver } from './client/ClientPieceReceiver.js'
import { ServerHeadroom } from './server/ServerHeadroom.js'

// The link runs on the fake clock, `performance.now()` included, so a busy machine can't slow it.
beforeEach(() =>
  vi.useFakeTimers({ toFake: ['setTimeout', 'clearTimeout', 'setInterval', 'clearInterval', 'performance'] }),
)
afterEach(() => vi.useRealTimers())
const sleep = (ms: number) => vi.advanceTimersByTimeAsync(ms)

/** 125 kB/s, 1 Mbit/s. */
const LINK_BYTES_PER_MS = 125

/** A server wire to a page behind a slow link, through a socket that reports what its link holds as buffered, as
 *  Node's does once its kernel buffers are full, or, `buffersNothing`, none of it, as Bun's does while its kernel takes
 *  all it is handed: the link carries what the socket took, in order, at its rate, `delayMs` each way. A page busy as it
 *  starts reads at `startBytesPerMs` for its first `startMs`. The page sends nothing but its lone acknowledgements. */
function wireBehindSlowLink({
  bytesPerMs = LINK_BYTES_PER_MS,
  delayMs = 0,
  buffersNothing = false,
  startMs = 0,
  startBytesPerMs = 0,
} = {}) {
  const link: Uint8Array<ArrayBuffer>[] = []
  let linkBytes = 0
  /** When each data frame arrived whole at the page. */
  const arrivals: number[] = []
  const receiver: ClientPieceReceiver = new ClientPieceReceiver({
    owner: {
      sendAck: (frame) => {
        setTimeout(() => headroom.acknowledged(decode(frame) as PiecesAck), Math.max(20, delayMs))
      },
      carryAck: () => {},
    },
  })
  const take = (message: Uint8Array<ArrayBuffer>) => {
    const at = performance.now()
    for (const frame of isOneFrame(message) ? [message] : Array.from(messageFrames(message))) {
      const decoded = decode(frame)
      if (decoded.tag === TAG.ANNOUNCE) receiver.announce(decoded.bytes, at, decoded.floor)
      else if (decoded.tag === TAG.PIECE) {
        if (receiver.add(decoded.total, decoded.piece, at)?.[0] === TAG.BINARY) arrivals.push(at)
      } else {
        receiver.arrived(decoded.tag, frame.byteLength, at, 'alone')
        if (decoded.tag === TAG.BINARY) arrivals.push(at)
      }
    }
  }
  let budget = 0
  const opened = performance.now()
  let last = opened
  const timer = setInterval(() => {
    const now = performance.now()
    budget += (now - last) * (now - opened < startMs ? startBytesPerMs : bytesPerMs)
    last = now
    while (link.length > 0 && link[0]!.byteLength <= budget) {
      const message = link.shift()!
      budget -= message.byteLength
      linkBytes -= message.byteLength
      if (delayMs === 0) take(message)
      else setTimeout(() => take(message), delayMs)
    }
    if (link.length === 0) budget = 0
  }, 5)
  const headroom: ServerHeadroom = new ServerHeadroom({
    socket: {
      send: (message) => {
        link.push(message)
        linkBytes += message.byteLength
      },
      get bufferedAmount() {
        return buffersNothing ? 0 : linkBytes
      },
      copiesOnSend: false,
    },
    pingInterval: 5_000,
  })
  let seq = 0
  return {
    send: (frame: (seq: number) => Uint8Array<ArrayBuffer>) => headroom.send(frame(++seq)),
    linkBytes: () => linkBytes,
    sentBytes: () => headroom.sender.transmittedBytes,
    window: () => headroom.window(),
    arrivals,
    stop: () => clearInterval(timer),
  }
}

describe('ServerHeadroom.headroom', () => {
  it.each([
    ['64 KiB', 64 * 1024],
    ['16 KiB', 16 * 1024],
    ['1 KiB', 1024],
  ])('a download in %s frames on a link not yet measured goes to the socket as the app hands it', (_, frameBytes) => {
    const wire = wireBehindSlowLink()
    try {
      const download = new Uint8Array(frameBytes)
      for (let sent = 0; sent < 2 * 1024 * 1024; sent += frameBytes) wire.send((seq) => encode.binary(1, download, seq))
      // As on a server that measures nothing: the kernel's TCP paces what the socket took.
      expect(wire.sentBytes()).toBeGreaterThan(2 * 1024 * 1024)
    } finally {
      wire.stop()
    }
  })

  it("once the page's acknowledgements measured the link, the link holds no more than the window and a frame", async () => {
    const wire = wireBehindSlowLink()
    try {
      // 256 KiB queue on the link for 2 s, so their acknowledgements give its rate.
      const frame = new Uint8Array(16 * 1024)
      for (let n = 0; n < 16; n++) wire.send((seq) => encode.binary(1, frame, seq))
      await sleep(3_000)
      // 125 B/ms over four least round trips, each a frame's 131 ms on the link, its 5 ms tick and the page's 20 ms answer.
      expect(wire.window()).toBeGreaterThanOrEqual(4 * 125 * 151)
      expect(wire.window()).toBeLessThanOrEqual(4 * 125 * 161)
      for (let n = 0; n < 64; n++) wire.send((seq) => encode.binary(1, frame, seq))
      let most = 0
      for (let ms = 0; ms < 4_000; ms += 5) {
        most = Math.max(most, wire.linkBytes())
        await sleep(5)
      }
      expect(most).toBeLessThanOrEqual(wire.window() + frame.byteLength + HEADER)
    } finally {
      wire.stop()
    }
  })

  it("a large message's rest, released by its probe's answer, measures the link through a socket that reports nothing buffered", async () => {
    const wire = wireBehindSlowLink({ buffersNothing: true })
    try {
      wire.send((seq) => encode.binary(1, new Uint8Array(1024 * 1024), seq))
      // The probe's answer releases the rest a round trip in; its pieces after the first arrive as the link takes them.
      await sleep(2_000)
      expect(wire.window()).toBeLessThan(Number.POSITIVE_INFINITY)
    } finally {
      wire.stop()
    }
  })

  it('a measured link that frames wait for wakes its wire as the window frees what goes next, not every millisecond', async () => {
    const wire = wireBehindSlowLink()
    try {
      const frame = new Uint8Array(16 * 1024)
      for (let n = 0; n < 16; n++) wire.send((seq) => encode.binary(1, frame, seq))
      await sleep(3_000)
      for (let n = 0; n < 64; n++) wire.send((seq) => encode.binary(1, frame, seq))
      const timers = vi.spyOn(globalThis, 'setTimeout')
      await sleep(2_000)
      // The link frees a 16 KiB frame's room in about 131 ms: the wire, the page's acknowledgements and their answers
      // look a few times for each, far fewer than once a millisecond.
      expect(timers.mock.calls.length).toBeLessThan(2_000 / 10)
      timers.mockRestore()
    } finally {
      wire.stop()
    }
  })

  it("a wire a page measured low as it started rises to the link's rate once the page reads at it", async () => {
    // 32 MiB/s; the page reads 200 kB/s for its first 1.5 s, and the app publishes 8 MiB a second.
    const wire = wireBehindSlowLink({ bytesPerMs: 32 * 1024, startMs: 1_500, startBytesPerMs: 200 })
    try {
      const publish = new Uint8Array(8 * 1024 * 1024)
      const start = performance.now()
      for (let n = 0; n < 10; n++) {
        wire.send((seq) => encode.binary(1, publish, seq))
        await sleep(1_000)
      }
      await sleep(500)
      const tookMs = wire.arrivals.map((at, n) => at - start - n * 1_000)
      expect(tookMs).toHaveLength(10)
      // From the third, each publish takes the 256 ms the link takes for 8 MiB, not what the first rate allows.
      expect(Math.max(...tookMs.slice(2))).toBeLessThan(2 * 256)
    } finally {
      wire.stop()
    }
  })

  it('a stream on a link with a long round trip, not yet measured, keeps the link busy', async () => {
    // 50 kB/s, 450 ms each way, and 3300 B texts at 30 Hz: twice what the link carries.
    const wire = wireBehindSlowLink({ bytesPerMs: 50, delayMs: 450 })
    let seq = 0
    const body = 'x'.repeat(3300)
    const stream = setInterval(() => wire.send(() => encode.text(1, body, ++seq)), 1000 / 30)
    try {
      await sleep(3_000)
      // What the link carries in that time.
      expect(wire.sentBytes()).toBeGreaterThanOrEqual(50 * 3_000)
    } finally {
      clearInterval(stream)
      wire.stop()
    }
  })
})
