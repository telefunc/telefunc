// Channel flow control end to end: a real ClientChannel and ClientConnection talk to a real ChannelMux and
// ServerChannel over in-process WebSockets with a fixed latency, on fake timers, so each run is deterministic.

import { afterEach, beforeEach, expect, test, vi } from 'vitest'

import { ClientBroadcast, ClientChannel } from './client/channel.js'
import { ServerChannel } from './server/channel.js'
import { Broadcast, ServerBroadcast } from './server/server-broadcast.js'
import { ChannelMux, type ServerTransport } from './server/mux.js'
import {
  CHANNEL_TRANSPORT,
  CREDIT_MSG_WINDOW_INITIAL,
  CREDIT_WINDOW_INITIAL_BYTES,
  CREDIT_WINDOW_MAX_BYTES,
  CHANNEL_PING_INTERVAL_MS,
  CHANNEL_RECONNECT_INITIAL_DELAY_MS,
  MAX_CHANNELS_PER_CONNECTION,
  RECONCILE_TIMEOUT_MS,
} from './constants.js'
import { ChannelOverflowError } from './channel-errors.js'
import { Fifo } from '../utils/Fifo.js'
import { TAG, encode, isOneFrame, messageFrames } from './shared-ws.js'
import { SendBudget } from './client/send-budget.js'
import { NetworkError } from '../shared/NetworkError.js'
import { config, setAdapterMaxFrameBytes } from '../node/server/serverConfig.js'
import { Pipe } from './test/loopback.js'
import { flowOf, produce, run, runUntil } from './test/channel-specs.js'

const LATENCY_MS = 5

/** What a socket read hands over at once; the receiver's microtasks run between two reads. */
const READ_BYTES = 64 * 1024

let loop: Loopback

/** The page's `WebSocket`, wired to `loop.mux`. */
class LoopbackSocket {
  static readonly OPEN = 1
  readyState = 0
  binaryType = 'blob'
  onopen: (() => void) | null = null
  onmessage: ((event: { data: ArrayBuffer }) => void) | null = null
  onclose: (() => void) | null = null
  onerror: (() => void) | null = null
  readonly toServer = new Pipe(LATENCY_MS, READ_BYTES)
  readonly toPage = new Pipe(LATENCY_MS, READ_BYTES)
  constructor(_url: string) {
    this.toServer.bytesPerMs = this.toPage.bytesPerMs = loop.linkBytesPerMs
    loop.sockets.push(this)
    setTimeout(() => {
      if (this.readyState !== 0) return
      loop.mux.onConnectionOpen(this, loop.transport)
      this.readyState = 1
      this.onopen?.()
    }, LATENCY_MS)
  }
  get bufferedAmount(): number {
    return this.toServer.bytes
  }
  send(frame: Uint8Array<ArrayBuffer>): void {
    loop.sends('page', frame)
    const sent = frame.slice()
    this.toServer.push(sent.byteLength, () => loop.receive(this, sent))
  }
  /** The server sends `frame` to the page. */
  sendToPage(frame: Uint8Array<ArrayBuffer>): void {
    const sent = frame.slice()
    this.toPage.push(sent.byteLength, () => this.onmessage?.({ data: sent.buffer }))
  }
  close(): void {
    this.cut()
  }
  /** The network drops this wire. */
  cut(): void {
    if (this.readyState === 3) return
    this.readyState = 3
    this.toServer.clear()
    this.toPage.clear()
    loop.mux.onConnectionClosed(this, { permanent: false })
    this.onclose?.()
  }
}

class Loopback {
  /** The speed each socket's link starts at, both ways. */
  linkBytesPerMs = Infinity
  readonly mux = new ChannelMux()
  readonly sockets: LoopbackSocket[] = []
  readonly errors: unknown[] = []
  private readonly sessions = new Map<LoopbackSocket, string>()
  readonly transport: ServerTransport<LoopbackSocket> = {
    getSessionId: (socket) => this.sessions.get(socket),
    setSessionId: (socket, id) => void this.sessions.set(socket, id),
    getConnId: () => null,
    wholeMessages: true,
    sendNow: (socket, frame) => {
      this.sends('server', frame)
      socket.sendToPage(frame)
    },
    bufferedAmount: (socket) => socket.toPage.bytes,
    terminateConnection: (socket) => socket.cut(),
  }
  private readonly connectionKey = crypto.randomUUID()
  private readonly pages: ClientChannel[] = []
  private watch: { from: 'page' | 'server'; tag: number; then: () => void } | null = null
  /** Every frame each side sent, as `[tag, channel ix, bytes, the number of the WebSocket message it went in]`, and
   *  when. */
  readonly sent = {
    page: [] as [number, number, number, number][],
    server: [] as [number, number, number, number][],
  }
  readonly sentAt = { page: [] as number[], server: [] as number[] }
  /** The WebSocket messages each side sent. */
  readonly messages = { page: 0, server: 0 }
  /** The tag of each frame that reached the server, and when. */
  readonly arrived: [number, number][] = []
  /** Runs `then` once, as `from` sends its next frame of `tag`. */
  onSend(from: 'page' | 'server', tag: number, then: () => void): void {
    this.watch = { from, tag, then }
  }
  sends(from: 'page' | 'server', message: Uint8Array<ArrayBuffer>): void {
    const n = this.messages[from]++
    for (const frame of isOneFrame(message) ? [message] : messageFrames(message)) {
      this.sent[from].push([frame[0]!, frame[1]! | (frame[2]! << 8), frame.byteLength, n])
      this.sentAt[from].push(Date.now())
      const watch = this.watch
      if (!watch || watch.from !== from || watch.tag !== frame[0]) continue
      this.watch = null
      watch.then()
    }
  }
  receive(socket: LoopbackSocket, message: Uint8Array<ArrayBuffer>): void {
    for (const frame of isOneFrame(message) ? [message] : messageFrames(message)) {
      this.arrived.push([frame[0]!, Date.now()])
    }
    this.mux.onConnectionRawMessage(socket, message).catch((err) => this.errors.push(err))
  }
  /** The wire the page uses now. */
  get socket(): LoopbackSocket {
    return this.sockets.at(-1)!
  }
  /** A channel the server has registered, and its page end, which all share one connection. */
  open<ClientToServer, ServerToClient>(opts: { registered?: boolean } = {}) {
    const server = new ServerChannel<ClientToServer, ServerToClient>()
    if (opts.registered !== false) this.mux.registerChannel(server)
    const page = new ClientChannel<ClientToServer, ServerToClient>({
      channelId: server.id,
      transports: [CHANNEL_TRANSPORT.WS],
      telefuncUrl: 'http://loopback.test/_telefunc',
      connectionKey: this.connectionKey,
    })
    this.pages.push(page as ClientChannel)
    return { server, page, register: () => this.mux.registerChannel(server) }
  }
  /** A broadcast the server has registered, and its page end, on the same connection. */
  openBroadcast<T>(key: string) {
    const server = new ServerBroadcast<T>({ key })
    this.mux.registerChannel(server)
    const page = new ClientBroadcast<T>({
      channelId: server.id,
      key,
      transports: [CHANNEL_TRANSPORT.WS],
      telefuncUrl: 'http://loopback.test/_telefunc',
      connectionKey: this.connectionKey,
    })
    this.pages.push(page as ClientChannel)
    return { server, page }
  }
  dispose(): void {
    for (const page of this.pages) page.abort()
  }
}

/** The server's room (with its piece sender) and receiver of a socket, which the specs read into. */
const piecesOf = (socket: LoopbackSocket) =>
  (
    loop.mux as unknown as {
      connectionEntries: Map<unknown, { state: { pieces: { room: { sender: unknown }; receiver: unknown } } }>
    }
  ).connectionEntries.get(socket)?.state.pieces

/** The bytes a PIECE adds to its piece. */
const PIECE_HEADER_BYTES = encode.piece(0, new Uint8Array(0)).byteLength

/** What the server holds for the page on `socket`: what the wire carries, and what waits for a probe. */
const serverHolds = (socket: LoopbackSocket) =>
  socket.toPage.bytes + (piecesOf(socket)?.room.sender as { heldBytes: number }).heldBytes

/** Takes what arrives, in order: at once, or, when slow, one message every 10 ms. */
function consume<T = number>(
  channel: { listen(cb: (data: T) => void | Promise<void>): unknown },
  { slow = false } = {},
) {
  const received: T[] = []
  const waiting: (() => void)[] = []
  let consumed = 0
  channel.listen((data) => {
    received.push(data)
    if (!slow) {
      consumed++
      return
    }
    return new Promise<void>((resolve) =>
      waiting.push(() => {
        consumed++
        resolve()
      }),
    )
  })
  if (slow) setInterval(() => waiting.shift()?.(), 10)
  return {
    received,
    get consumed() {
      return consumed
    },
  }
}

/** Where each end's `onClose` leaves what it got: `'open'` until it fires. */
function closedWith(ends: { page: { onClose(cb: (err?: Error) => void): void }; server: typeof ends.page }) {
  const closed: { page: unknown; server: unknown } = { page: 'open', server: 'open' }
  ends.page.onClose((err) => void (closed.page = err))
  ends.server.onClose((err) => void (closed.server = err))
  return closed
}

/** What a channel ends with when a reconnect needs messages `side`'s replay buffer dropped to stay within its size. */
const lostOn = (side: 'server' | 'client') =>
  new NetworkError(
    `Channel closed: a reconnect needed messages the ${side}'s replay buffer had dropped to stay within its size. Raise config.channel.${side}ReplayBuffer, or ${side}ReplayBufferBinary for binary messages and streams.`,
    true,
  )

const KIB = 1024

/** The server's credit left with its page, in bytes. */
const creditOf = (channel: unknown) => {
  const flow = (channel as { _flow: { _limitBytes: number; _sentBytes: number } })._flow
  return flow._limitBytes - flow._sentBytes
}

/** A 16 KiB message that names its place in the stream. */
const message = (n: number) => String(n).padEnd(16 * KIB)

/** `send(n)` for n = 0, 1, 2…, none awaited, until one rejects. */
async function sendUntilRejected(send: (n: number) => Promise<unknown>) {
  let error: unknown
  let sends = 0
  while (error === undefined && sends < 6_000) {
    send(sends++).catch((err: unknown) => (error = err))
    await run(0)
  }
  return { error, sends }
}

beforeEach(() => {
  vi.useFakeTimers()
  loop = new Loopback()
  vi.stubGlobal('WebSocket', LoopbackSocket)
})

afterEach(() => {
  loop.dispose()
  expect(loop.errors).toEqual([])
  vi.unstubAllGlobals()
  vi.useRealTimers()
  config.channel = {}
})

test('a stream the page consumes while the server awaits another channel its first reconcile named keeps flowing past 100 messages', async () => {
  const clock = loop.open<never, number>()
  const page = consume(clock.page)
  produce(clock.server)
  // The page opens a second channel whose server side isn't registered yet, which the server awaits.
  const late = loop.open<never, number>({ registered: false })
  await run(50)
  expect(page.received.length).toBeGreaterThan(CREDIT_MSG_WINDOW_INITIAL)
  late.register()
  const before = page.received.length
  await run(50)
  expect(page.received.length).toBeGreaterThan(before + CREDIT_MSG_WINDOW_INITIAL)
  expect(page.received).toEqual([...page.received.keys()])
})

test("a page listener's data has a buffer of its own frame, which it may transfer, in a message of several", async () => {
  const { server, page } = loop.open()
  const got: number[] = []
  const buffers: number[] = []
  page.listenBinary((data) => {
    got.push(data.byteLength)
    buffers.push(data.buffer.byteLength)
    structuredClone(data.buffer, { transfer: [data.buffer] })
  })
  await run(50)
  // Sent in one turn, they reach the page in one message.
  for (let i = 0; i < 3; i++) void server.sendBinary(new Uint8Array(100))
  await run(50)
  expect(got).toEqual([100, 100, 100])
  const frameBytes = encode.binary(0, new Uint8Array(100)).byteLength
  expect(buffers).toEqual([frameBytes, frameBytes, frameBytes])
  expect(loop.sockets.length).toBe(1)
})

test('a stream keeps flowing past its grown window after the page opens another channel, and after a reconnect', async () => {
  const clock = loop.open<never, number>()
  const page = consume(clock.page)
  produce(clock.server)
  await runUntil(() => flowOf(clock.page).msgWindow > 4 * CREDIT_MSG_WINDOW_INITIAL, 1_000, LATENCY_MS)
  let window = flowOf(clock.page).msgWindow
  expect(window).toBeGreaterThan(4 * CREDIT_MSG_WINDOW_INITIAL)

  loop.open<never, number>() // the server attaches the stream's channel again, with the new one
  let before = page.received.length
  await run(100)
  expect(page.received.length - before).toBeGreaterThan(2 * window)

  window = flowOf(clock.page).msgWindow
  loop.socket.cut()
  before = page.received.length
  await runUntil(() => page.received.length - before > 2 * window, 1_000, LATENCY_MS)
  expect(page.received.length - before).toBeGreaterThan(2 * window)
  expect(page.received).toEqual([...page.received.keys()])
})

test('an upload keeps flowing past its grown window after a reconnect, the server getting it all in order', async () => {
  const upload = loop.open<number, never>()
  const server = consume(upload.server)
  produce(upload.page)
  await runUntil(() => flowOf(upload.server).msgWindow > 4 * CREDIT_MSG_WINDOW_INITIAL, 1_000, LATENCY_MS)
  const window = flowOf(upload.server).msgWindow
  expect(window).toBeGreaterThan(4 * CREDIT_MSG_WINDOW_INITIAL)
  loop.socket.cut() // what it carries is lost, and replays
  const before = server.received.length
  await runUntil(() => server.received.length - before > 2 * window, 1_000, LATENCY_MS)
  expect(server.received.length - before).toBeGreaterThan(2 * window)
  expect(server.received).toEqual([...server.received.keys()])
})

test('a reattach on the live wire sends no flow-control frames, and one on a new wire repairs with them', async () => {
  const clock = loop.open<never, number>()
  await run(100)
  const flowControl = (from: 'page' | 'server') =>
    loop.sent[from].filter(([tag, ix]) => ix === 0 && (tag === TAG.WINDOW || tag === TAG.MSG_WINDOW)).length
  const before = { page: flowControl('page'), server: flowControl('server') }

  loop.open<never, number>() // its RECONCILE attaches the clock again, on the same wire
  await run(100)
  expect(clock.page.isClosed).toBe(false)
  expect({ page: flowControl('page'), server: flowControl('server') }).toEqual(before)

  loop.socket.cut()
  await run(1_000)
  expect(loop.sockets).toHaveLength(2)
  expect({ page: flowControl('page'), server: flowControl('server') }).toEqual({
    page: before.page + 2,
    server: before.server + 2,
  })
})

test("what the server has in flight to a slow page never exceeds the page's window, also right after a refresh", async () => {
  const clock = loop.open<never, number>()
  const page = consume(clock.page, { slow: true })
  let excess = -Infinity
  produce(clock.server, {
    onSent: (sent) => (excess = Math.max(excess, sent - page.consumed - flowOf(clock.page).msgWindow)),
  })
  await run(3_000)
  expect(page.consumed).toBeGreaterThan(2 * CREDIT_MSG_WINDOW_INITIAL)
  expect(excess).toBeLessThanOrEqual(0)
})

test("what a page has in flight to a slow server listener never exceeds the server's window, so the server buffers no more", async () => {
  const upload = loop.open<string, never>()
  // 64 KiB a message on the wire: the byte window, not the message window, is the one that binds.
  const payload = 'x'.repeat(64 * 1024 - 2)
  const bytes = payload.length + 2
  const server = consume(upload.server, { slow: true })
  let excess = -Infinity
  produce(upload.page, {
    message: () => payload,
    onSent: (sent) => (excess = Math.max(excess, (sent - server.consumed) * bytes - flowOf(upload.server).byteWindow)),
  })
  await run(3_000)
  expect(server.consumed).toBeGreaterThan(64)
  expect(excess).toBeLessThanOrEqual(0)
})

test('a stream whose window grew to 16 MiB, cut off with its window on the wire, resumes after the reconnect without loss', async () => {
  const clock = loop.open<never, string>()
  const got: number[] = []
  clock.page.listen((text) => void got.push(Number.parseInt(text)))
  const closed = closedWith(clock)
  // As the estimator grows it on a path whose BDP is that large (see flow-control.spec.ts).
  const estimator = (clock.page as unknown as { _flow: { _bdp: { growBytes(): void } } })._flow._bdp
  for (let n = 0; n < 3; n++) estimator.growBytes()
  const window = flowOf(clock.page).byteWindow
  expect(window).toBe(16 * KIB * KIB)
  const text = (n: number) => String(n).padEnd(256 * KIB)
  produce(clock.server, { message: text })
  await run(100)

  // From its next refresh on, the page stops hearing from the server, which sends the window that refresh grants. Then
  // the wire drops, with all of it on the way.
  loop.onSend('page', TAG.WINDOW, () => loop.socket.toPage.hold())
  await run(50)
  expect(loop.socket.toPage.bytes).toBeGreaterThan(window / 2)
  loop.socket.cut()
  const before = got.length
  await runUntil(() => got.length > before + (2 * window) / (256 * KIB), 2_000, LATENCY_MS)
  expect(got.length).toBeGreaterThan(before + (2 * window) / (256 * KIB))
  expect(got).toEqual([...got.keys()])
  expect(closed).toEqual({ page: 'open', server: 'open' })
})

test.each(['server', 'page'] as const)(
  'an awaited stream of empty binary messages from the %s, cut off with what credit lets be in flight on the wire, resumes after the reconnect without loss',
  async (from) => {
    config.channel = { serverReplayBufferBinary: KIB, clientReplayBufferBinary: KIB }
    const ends = loop.open<never, never>()
    const [sender, receiver] = from === 'server' ? [ends.server, ends.page] : [ends.page, ends.server]
    const closed = closedWith(ends)
    let got = 0
    receiver.listenBinary(() => void got++)
    void (async () => {
      for (let n = 0; n < 1_000; n++) await sender.sendBinary(new Uint8Array(0))
    })().catch(() => {})
    await run(50)
    ;(from === 'server' ? loop.socket.toPage : loop.socket.toServer).hold()
    await run(50)
    loop.socket.cut()
    await runUntil(() => got === 1_000, 2_000, LATENCY_MS)
    expect(got).toBe(1_000)
    expect(closed).toEqual({ page: 'open', server: 'open' })
  },
)

test.each((['server', 'page'] as const).flatMap((from) => (['text', 'binary'] as const).map((kind) => [from, kind])))(
  'awaited %s %s messages, the last one half the replay buffer and sent as the credit runs out, all arrive after a reconnect',
  async (from, kind) => {
    config.channel = {
      serverReplayBuffer: 2 * KIB,
      serverReplayBufferBinary: 2 * KIB,
      clientReplayBuffer: 2 * KIB,
      clientReplayBufferBinary: 2 * KIB,
    }
    const ends = loop.open<string, string>()
    const [sender, receiver] = from === 'server' ? [ends.server, ends.page] : [ends.page, ends.server]
    const closed = closedWith(ends)
    const got: number[] = []
    receiver.listen((text) => void got.push(text.length + 2))
    receiver.listenBinary((data) => void got.push(data.byteLength))
    // A text message's bytes are its JSON string, quotes included.
    const send = (bytes: number) =>
      kind === 'text' ? sender.send('x'.repeat(bytes - 2)) : sender.sendBinary(new Uint8Array(bytes))
    await run(100)
    ;(from === 'server' ? loop.socket.toPage : loop.socket.toServer).hold()
    // The first message's frame, its 7-byte header included, leaves a byte of credit.
    const first = creditOf(sender) - 1 - 7
    void (async () => {
      await send(first)
      await send(KIB)
    })().catch(() => {})
    await run(50)
    loop.socket.cut()
    await runUntil(() => got.length === 2, 2_000, LATENCY_MS)
    expect(got).toEqual([first, KIB])
    expect(closed).toEqual({ page: 'open', server: 'open' })
  },
)

test.each(['server', 'page'] as const)(
  "the %s's ack answer of half the replay buffer, sent behind its awaited message as the credit runs out, arrives after a reconnect",
  async (from) => {
    config.channel = {
      serverReplayBuffer: 2 * KIB,
      serverReplayBufferBinary: 2 * KIB,
      clientReplayBuffer: 2 * KIB,
      clientReplayBufferBinary: 2 * KIB,
    }
    const ends = loop.open<string, string>()
    const [answerer, asker] = from === 'server' ? [ends.server, ends.page] : [ends.page, ends.server]
    const closed = closedWith(ends)
    const got: number[] = []
    asker.listen((text) => void got.push(text.length + 2))
    let answer!: (text: string) => void
    answerer.listen(() => new Promise<string>((resolve) => (answer = resolve)))
    await run(100)
    const acked = asker.send('?', { ack: true })
    await run(50)
    ;(from === 'server' ? loop.socket.toPage : loop.socket.toServer).hold()
    // The message's frame, its 7-byte header included, leaves a byte of credit.
    const first = creditOf(answerer) - 1 - 7
    await answerer.send('x'.repeat(first - 2))
    // An answer's bytes are its JSON string, quotes included.
    answer('y'.repeat(KIB - 2))
    await run(50)
    loop.socket.cut()
    await runUntil(() => got.length === 1, 2_000, LATENCY_MS)
    expect(got).toEqual([first])
    expect(((await acked) as string).length + 2).toBe(KIB)
    expect(closed).toEqual({ page: 'open', server: 'open' })
  },
)

test("sends nobody awaits, past the page's window and more than the server's replay holds, end the channel with NetworkError on both ends at a reconnect, the page having got them in order up to there", async () => {
  config.channel.serverReplayBuffer = 4 * KIB
  const feed = loop.open<never, string>()
  const page = consume(feed.page)
  const closed = closedWith(feed)
  await run(100)
  loop.socket.toPage.hold()
  const text = (n: number) => String(n).padEnd(256)
  for (let n = 0; n < 64; n++) feed.server.send(text(n)).catch(() => {})
  await run(50)
  loop.socket.cut()
  await runUntil(() => closed.page !== 'open', 1_000, LATENCY_MS)
  expect(closed).toEqual({ page: lostOn('server'), server: lostOn('server') })
  expect(page.received).toEqual(page.received.map((_, n) => text(n)))
})

test("sends nobody awaits, past the server's window and more than the page's replay holds, end the channel with NetworkError on both ends at a reconnect, the server having got them in order up to there", async () => {
  config.channel.clientReplayBuffer = 4 * KIB
  const upload = loop.open<string, never>()
  const server = consume(upload.server)
  const closed = closedWith(upload)
  await run(100)
  loop.socket.toServer.hold()
  const text = (n: number) => String(n).padEnd(256)
  for (let n = 0; n < 64; n++) upload.page.send(text(n)).catch(() => {})
  await run(50)
  loop.socket.cut()
  await runUntil(() => closed.server !== 'open', 1_000, LATENCY_MS)
  expect(closed).toEqual({ page: lostOn('client'), server: lostOn('client') })
  expect(server.received).toEqual(server.received.map((_, n) => text(n)))
})

test("a page that opens another channel while more of what the server sent is on the wire than the server's replay buffer holds gets it all", async () => {
  config.channel.serverReplayBuffer = 512
  const feed = loop.open<never, string>()
  const page = consume(feed.page)
  const closed = closedWith(feed)
  await run(50)
  loop.socket.toPage.hold()
  const text = (n: number) => String(n).padEnd(64)
  for (let n = 0; n < 32; n++) feed.server.send(text(n)).catch(() => {})
  await run(20)
  expect(loop.socket.toPage.bytes).toBeGreaterThan(2 * 1_024)
  loop.open<never, number>() // its RECONCILE attaches the channel again, on the wire that carries what it sent
  await run(50)
  loop.socket.toPage.release()
  await run(100)
  expect(page.received).toEqual(Array.from({ length: 32 }, (_, n) => text(n)))
  expect(closed).toEqual({ page: 'open', server: 'open' })
})

test("a broadcast whose page is cut off further behind than the server's replay buffer holds leaves it with ChannelOverflowError on both ends, having got what was published before it in order", async () => {
  config.channel.serverReplayBuffer = 1_024
  const key = `room:${crypto.randomUUID()}`
  const room = loop.openBroadcast<string>(key)
  const closed = closedWith(room)
  const seen: string[] = []
  room.page.subscribe((text) => void seen.push(text))
  await run(100)
  Broadcast.publish(key, 'before')
  await run(50)
  loop.socket.toPage.hold()
  const publication = (n: number) => String(n).padEnd(256)
  for (let n = 0; n < 8; n++) Broadcast.publish(key, publication(n))
  await run(50)
  loop.socket.cut()
  await runUntil(() => closed.page !== 'open', 1_000, LATENCY_MS)
  expect(closed.page).toBeInstanceOf(ChannelOverflowError)
  expect(closed.server).toBeInstanceOf(ChannelOverflowError)
  expect(seen).toEqual(['before', ...seen.slice(1).map((_, n) => publication(n))])
})

test.each([
  [false, 'Uint8Array'],
  [true, 'Uint8Array'],
  [false, 'Buffer'],
  [true, 'Buffer'],
] as const)(
  'a server sendBinary() before its page attaches delivers the bytes it was given, though its caller reuses the buffer (ack: %s, a %s)',
  async (ack, kind) => {
    const { server, page } = loop.open<never, never>()
    const got: number[][] = []
    page.listenBinary((data) => void got.push(Array.from(data)))
    const send = (data: Uint8Array) => (ack ? server.sendBinary(data, { ack: true }) : server.sendBinary(data))
    const buffer = kind === 'Buffer' ? Buffer.from([1, 1, 1]) : Uint8Array.from([1, 1, 1])
    void send(buffer).catch(() => {})
    buffer.set([2, 2, 2])
    void send(buffer).catch(() => {})
    buffer.set([3, 3, 3])
    await run(300)
    expect(got).toEqual([
      [1, 1, 1],
      [2, 2, 2],
    ])
  },
)

test('a broadcast whose page was offline while more was published than config.channel.bufferLimit holds closes on both ends with ChannelOverflowError at its reconnect', async () => {
  const key = `room:${crypto.randomUUID()}`
  const room = loop.openBroadcast<string>(key)
  const closed = closedWith(room)
  const seen: string[] = []
  room.page.subscribe((text) => void seen.push(text))
  await run(100)
  Broadcast.publish(key, 'before')
  await run(50)
  loop.socket.cut()
  // Offline, the server holds 512 KiB of text for the page: these are 1 MiB.
  for (let n = 0; n < 8; n++) Broadcast.publish(key, String(n).padEnd(128 * KIB))
  await runUntil(() => closed.page !== 'open', 2_000, LATENCY_MS)
  expect(closed.server).toBeInstanceOf(ChannelOverflowError)
  expect(closed.page).toBeInstanceOf(ChannelOverflowError)
  expect(seen).toEqual(['before'])
})

test('a broadcast whose page was offline while more binary was published than config.channel.bufferLimitBinary holds closes on both ends with ChannelOverflowError at its reconnect', async () => {
  const key = `room:${crypto.randomUUID()}`
  const room = loop.openBroadcast<string>(key)
  const closed = closedWith(room)
  room.page.subscribeBinary(() => {})
  await run(100)
  loop.socket.cut()
  // Offline, the server holds 2 MiB of binary for the page: these are 3 MiB.
  for (let n = 0; n < 6; n++) Broadcast.publishBinary(key, new Uint8Array(512 * KIB))
  await runUntil(() => closed.page !== 'open', 2_000, LATENCY_MS)
  expect(closed.server).toBeInstanceOf(ChannelOverflowError)
  expect(closed.page).toBeInstanceOf(ChannelOverflowError)
})

test('a broadcast whose page was offline while less was published than config.channel.bufferLimit holds gets it all at its reconnect', async () => {
  const key = `room:${crypto.randomUUID()}`
  const room = loop.openBroadcast<string>(key)
  const closed = closedWith(room)
  const seen: string[] = []
  room.page.subscribe((text) => void seen.push(text))
  await run(100)
  loop.socket.cut()
  for (let n = 0; n < 3; n++) Broadcast.publish(key, String(n).padEnd(128 * KIB))
  await runUntil(() => seen.length === 3, 2_000, LATENCY_MS)
  expect(seen).toEqual(Array.from({ length: 3 }, (_, n) => String(n).padEnd(128 * KIB)))
  expect(closed).toEqual({ page: 'open', server: 'open' })
})

// Written in one turn, a burst waits on the wire whatever the page's pace: a bound on it smaller than the largest window
// a page grants refused it to a page that reads at full speed.
test("a burst of sends nobody awaits, past the page's window and within the largest one a page grants, reaches a page that reads", async () => {
  const feed = loop.open<never, string>()
  const page = consume(feed.page)
  await run(100)
  // 30 MiB in one turn, as the playground's push benchmark sends.
  const burst = (n: number) => String(n).padEnd(512 * KIB)
  let error: unknown
  for (let n = 0; n < 60; n++) feed.server.send(burst(n)).catch((err: unknown) => (error ??= err))
  await runUntil(() => page.received.length === 60, 5_000, LATENCY_MS)
  expect(error).toBeUndefined()
  expect(page.received).toEqual(Array.from({ length: 60 }, (_, n) => burst(n)))
})

test("a page that stops reading on a fresh wire has the server hold a channel's sends nobody awaits to its window and the largest window a page grants, on the wire or behind its probe: the next rejects with ChannelOverflowError, and the channel stays open", async () => {
  const feed = loop.open<never, string>()
  const page = consume(feed.page)
  await run(100)
  loop.socket.toPage.hold()
  const { error, sends } = await sendUntilRejected((n) => feed.server.send(message(n)))
  expect(error).toBeInstanceOf(ChannelOverflowError)
  expect(serverHolds(loop.socket)).toBeGreaterThan(CREDIT_WINDOW_INITIAL_BYTES + CREDIT_WINDOW_MAX_BYTES)
  // One message past them, and the headers of each one and of its pieces.
  expect(serverHolds(loop.socket)).toBeLessThanOrEqual(
    CREDIT_WINDOW_INITIAL_BYTES + CREDIT_WINDOW_MAX_BYTES + 192 * KIB,
  )

  // Once the page reads again it gets every message but the refused one, and what is sent after it caught up.
  loop.socket.toPage.release()
  await runUntil(() => page.received.length === sends - 1, 5_000, LATENCY_MS)
  await feed.server.send(message(sends))
  await runUntil(() => page.received.length === sends, 1_000, LATENCY_MS)
  expect(page.received).toEqual([...Array.from({ length: sends - 1 }, (_, n) => message(n)), message(sends)])
})

test("a page that stops reading on a fresh wire has the server hold a channel's ack requests nobody awaits to the largest window a page grants, on the wire or behind its probe: the next rejects with ChannelOverflowError", async () => {
  const feed = loop.open<never, string>()
  feed.page.listen(() => 'ok')
  await run(100)
  loop.socket.toPage.hold()
  const acks: Promise<unknown>[] = []
  const { error, sends } = await sendUntilRejected((n) => {
    const ack = feed.server.send(message(n), { ack: true })
    acks.push(ack)
    return ack
  })
  expect(error).toBeInstanceOf(ChannelOverflowError)
  expect(serverHolds(loop.socket)).toBeGreaterThan(CREDIT_WINDOW_MAX_BYTES)
  expect(serverHolds(loop.socket)).toBeLessThanOrEqual(CREDIT_WINDOW_MAX_BYTES + 192 * KIB)

  loop.socket.toPage.release()
  await run(5_000)
  expect(await Promise.all(acks.slice(0, sends - 1))).toEqual(Array(sends - 1).fill('ok'))
})

test('on a slow link, an awaited stream keeps flowing while a send nobody awaits on the same wire is refused past the largest window a page grants', async () => {
  const stream = loop.open<never, string>()
  const flood = loop.open<never, string>()
  const streamed = consume(stream.page)
  consume(flood.page)
  await run(100)
  loop.socket.toPage.bytesPerMs = 64_000 // 64 MB/s
  let streamError: unknown
  void (async () => {
    while (!stream.server.isClosed) await stream.server.send('x'.repeat(64 * KIB))
  })().catch((err: unknown) => (streamError = err))
  // 256 MB/s.
  let floodError: unknown
  let held = 0
  const flooding = setInterval(() => {
    held = Math.max(held, loop.socket.toPage.bytes)
    if (floodError === undefined) flood.server.send('x'.repeat(256 * KIB)).catch((err: unknown) => (floodError = err))
  }, 1)
  await run(1_000)
  expect(floodError).toBeInstanceOf(ChannelOverflowError)
  const midway = streamed.received.length
  await run(1_000)
  clearInterval(flooding)

  expect(streamError).toBeUndefined()
  expect(midway).toBeGreaterThan(10)
  expect(streamed.received.length - midway).toBeGreaterThan(40)
  // Nothing past the windows and, past the flood's, the largest window a page grants is added.
  expect(held).toBeLessThanOrEqual(
    flowOf(stream.page).byteWindow + flowOf(flood.page).byteWindow + CREDIT_WINDOW_MAX_BYTES + 512 * KIB,
  )
})

test('on a slow link, a chat nobody awaits that the page keeps up with is neither refused nor held up for seconds behind an awaited stream on the same wire', async () => {
  const stream = loop.open<never, string>()
  const chat = loop.open<never, { at: number; text: string }>()
  consume(stream.page)
  const delays: number[] = []
  chat.page.listen(({ at }) => void delays.push(Date.now() - at))
  await run(100)
  loop.socket.toPage.bytesPerMs = 4_000 // 4 MB/s
  let streamError: unknown
  void (async () => {
    while (!stream.server.isClosed) await stream.server.send('x'.repeat(64 * KIB))
  })().catch((err: unknown) => (streamError = err))
  // 1.6 MB/s of 16 KiB messages.
  let chatError: unknown
  let sent = 0
  const chatting = setInterval(() => {
    chat.server.send({ at: Date.now(), text: message(sent++) }).catch((err: unknown) => (chatError ??= err))
  }, 10)
  // By then, probes that counted the stream's own queue as in flight had doubled its window to 8 MiB.
  await run(4_000)
  clearInterval(chatting)
  await runUntil(() => delays.length === sent, 10_000, LATENCY_MS)
  expect(chatError).toBeUndefined()
  expect(streamError).toBeUndefined()
  expect(delays).toHaveLength(sent)
  // Nothing holds it up past the stream's window, drained at the 2.4 MB/s the chat leaves of the link.
  expect(Math.max(...delays)).toBeLessThan(CREDIT_WINDOW_INITIAL_BYTES / 2_400 + 100)
  expect(flowOf(stream.page).byteWindow).toBe(CREDIT_WINDOW_INITIAL_BYTES)
})

test('a send nobody awaits goes out past the window of a page whose listener lags, while the wire holds less than the largest window a page grants', async () => {
  const feed = loop.open<never, string>()
  const page = consume(feed.page, { slow: true })
  await run(100)
  const big = (n: number) => String(n).padEnd(256 * KIB)
  let error: unknown
  let behind = 0
  for (let n = 0; n < 320; n++) {
    feed.server.send(big(n)).catch((err: unknown) => (error = err))
    behind = Math.max(behind, (n + 1 - page.consumed) * (256 * KIB) - flowOf(feed.page).byteWindow)
    await run(1)
  }
  expect(error).toBeUndefined()
  // The page's limit is at most what it consumed and its window, so the server sent this far past it.
  expect(behind).toBeGreaterThan(CREDIT_WINDOW_MAX_BYTES)
  await runUntil(() => page.received.length === 320, 5_000, LATENCY_MS)
  expect(page.received).toEqual(Array.from({ length: 320 }, (_, n) => big(n)))
})

test('a page that stops reading a broadcast on a fresh wire leaves it with ChannelOverflowError on both ends once the server holds its room and a quarter more of publishes for it, on the wire or behind its probe', async () => {
  const key = `room:${crypto.randomUUID()}`
  const room = loop.openBroadcast<string>(key)
  const errors: { server?: Error; page?: Error } = {}
  room.server.onClose((err) => void (errors.server = err))
  room.page.onClose((err) => void (errors.page = err))
  const seen: string[] = []
  room.page.subscribe((text) => void seen.push(text))
  await run(100)
  loop.socket.toPage.hold()
  const publication = (n: number) => String(n).padEnd(256 * KIB)
  let published = 0
  while (!room.server.isClosed && published < 1_000) {
    Broadcast.publish(key, publication(published++))
    await run(0)
  }
  expect(errors.server).toBeInstanceOf(ChannelOverflowError)
  expect(serverHolds(loop.socket)).toBeGreaterThan(CREDIT_WINDOW_MAX_BYTES * 1.25)
  expect(serverHolds(loop.socket)).toBeLessThanOrEqual(CREDIT_WINDOW_MAX_BYTES * 1.25 + 512 * KIB)

  // Once the page reads again it gets every publish sent before the one that found it behind, then the close.
  loop.socket.toPage.release()
  await runUntil(() => errors.page !== undefined, 2_000, LATENCY_MS)
  expect(errors.page).toBeInstanceOf(ChannelOverflowError)
  expect(seen).toEqual(Array.from({ length: published - 1 }, (_, n) => publication(n)))
})

test('on a slow link, a page that keeps up with a broadcast stays in it while an awaited stream fills the wire', async () => {
  const key = `room:${crypto.randomUUID()}`
  const stream = loop.open<never, string>()
  consume(stream.page)
  const room = loop.openBroadcast<string>(key)
  let closed: Error | undefined | null = null
  room.server.onClose((err) => void (closed = err))
  const seen: string[] = []
  room.page.subscribe((text) => void seen.push(text))
  await run(100)
  loop.socket.toPage.bytesPerMs = 4_000 // 4 MB/s
  void (async () => {
    while (!stream.server.isClosed) await stream.server.send('x'.repeat(64 * KIB))
  })().catch(() => {})
  let held = 0
  // 1.6 MB/s of publishes, which wait behind the stream's window on the wire.
  for (let n = 0; n < 300; n++) {
    Broadcast.publish(key, message(n))
    held = Math.max(held, loop.socket.toPage.bytes)
    await run(10)
  }
  await runUntil(() => seen.length === 300, 2_000, LATENCY_MS)
  expect(closed).toBe(null)
  expect(held).toBeGreaterThan(CREDIT_WINDOW_INITIAL_BYTES)
  expect(seen).toEqual(Array.from({ length: 300 }, (_, n) => message(n)))
})

test("a page's consumption of what a broadcast publishes moves the server's limit for it", async () => {
  const key = `room:${crypto.randomUUID()}`
  const room = loop.openBroadcast<string>(key)
  const seen: string[] = []
  room.page.subscribe((text) => void seen.push(text))
  await run(100)
  expect(creditOf(room.server)).toBe(CREDIT_WINDOW_MAX_BYTES - 16)
  const publications = CREDIT_WINDOW_MAX_BYTES / 4 / KIB / KIB + 1
  for (let n = 0; n < publications; n++) {
    Broadcast.publish(key, 'x'.repeat(KIB * KIB))
    await run(1)
  }
  await runUntil(() => seen.length === publications, 1_000, LATENCY_MS)
  await run(100)
  // Sent 17 MiB, of which the last limit leaves less than one not yet counted consumed.
  expect(creditOf(room.server)).toBeGreaterThan(CREDIT_WINDOW_MAX_BYTES - KIB * KIB)
})

test("a broadcast's page acknowledges what it read as a stream's page does, however large its room, so the server's replay for it holds less than a quarter of a stream's window of it", async () => {
  const key = `room:${crypto.randomUUID()}`
  const room = loop.openBroadcast<string>(key)
  const seen: string[] = []
  room.page.subscribe((text) => void seen.push(text))
  await run(100)
  for (let n = 0; n < 12; n++) {
    Broadcast.publish(key, 'x'.repeat(KIB * KIB))
    await run(20)
  }
  await runUntil(() => seen.length === 12, 1_000, LATENCY_MS)
  await run(50)
  const replay = (room.server as unknown as { _replayBuffer: { byteLength: number } })._replayBuffer
  expect(replay.byteLength).toBeLessThan(CREDIT_WINDOW_INITIAL_BYTES / 4)
})

test('on a slow link, producers that await their sends are handed the credit one at a time, and none is refused', async () => {
  const feed = loop.open<never, string>()
  const page = consume(feed.page)
  await run(100)
  loop.socket.toPage.bytesPerMs = 4_000 // 4 MB/s
  let size = KIB
  let error: unknown
  for (let p = 0; p < 8; p++) {
    void (async () => {
      while (!feed.server.isClosed) await feed.server.send('x'.repeat(size))
    })().catch((err: unknown) => (error ??= err))
  }
  // They start small: producers that start together each have a frame out as the credit first runs out, and those count.
  await run(100)
  // Every producer waits on credit by now. From here on each frame is 128 KiB.
  size = 128 * KIB
  let held = 0
  const watch = setInterval(() => (held = Math.max(held, loop.socket.toPage.bytes)), 1)
  const before = page.received.length
  await run(2_000)
  clearInterval(watch)
  expect(error).toBeUndefined()
  expect(held).toBeGreaterThan(CREDIT_WINDOW_INITIAL_BYTES)
  expect(page.received.length - before).toBeGreaterThan(40)
})

// 100 KB/s: a 2 MiB window takes 20 s to go through, twice the deadline a ping or pong has, which waits behind it.
test("on a link slower than a window per ping deadline, a stream's page keeps its wire while the stream keeps arriving ahead of its pong", async () => {
  const feed = loop.open<never, string>()
  const page = consume(feed.page)
  await run(100)
  loop.socket.toPage.bytesPerMs = 100
  produce(feed.server, { message: () => 'x'.repeat(64 * KIB) })
  await run(30_000)
  expect(loop.sockets).toHaveLength(1)
  expect(page.received.length).toBeGreaterThan(30)
})

test('a wire that stops delivering in the middle of a stream is taken for dead one pong deadline after its last frame', async () => {
  const feed = loop.open<never, string>()
  consume(feed.page)
  await run(100)
  loop.socket.toPage.bytesPerMs = 4_000 // 4 MB/s
  produce(feed.server, { message: () => 'x'.repeat(16 * KIB) })
  await run(7_000)
  loop.socket.toPage.hold()
  const stoppedAt = Date.now()
  await runUntil(() => loop.sockets.length === 2, 30_000, LATENCY_MS)
  expect(loop.sockets).toHaveLength(2)
  // The pong deadline is two ping intervals; then the first reconnect waits its delay.
  expect(Date.now() - stoppedAt).toBeLessThanOrEqual(
    2 * CHANNEL_PING_INTERVAL_MS + CHANNEL_RECONNECT_INITIAL_DELAY_MS + 100,
  )
})

test("on an uplink slower than a window per ping deadline, the server doesn't cut an upload's wire while the upload keeps arriving ahead of its ping", async () => {
  const feed = loop.open<string, never>()
  const server = consume(feed.server)
  await run(100)
  loop.socket.toServer.bytesPerMs = 100
  const terminateConnection = vi.spyOn(loop.transport, 'terminateConnection')
  produce(feed.page, { message: () => 'x'.repeat(64 * KIB) })
  await run(30_000)
  expect(terminateConnection).not.toHaveBeenCalled()
  expect(server.received.length).toBeGreaterThan(30)
})

// 100 KB/s, with a ping every second: the page's ping waits 20 s behind its 2 MiB window, and the server's refresh for a
// quarter of it comes every 5 s, both past the page's 2 s pong deadline.
test("on an uplink slower than a quarter window per pong deadline, an upload's page keeps its wire while the upload keeps arriving ahead of its ping", async () => {
  config.channel.pingInterval = 1_000
  const feed = loop.open<string, never>()
  const server = consume(feed.server)
  await run(100)
  loop.socket.toServer.bytesPerMs = 100
  produce(feed.page, { message: () => 'x'.repeat(64 * KIB) })
  await run(30_000)
  expect(loop.sockets).toHaveLength(1)
  expect(server.received.length).toBeGreaterThan(40)
})

test("a download whose path's round trip rises on its wire grows its window to carry the link's rate", async () => {
  const feed = loop.open<never, Uint8Array>()
  let got = 0
  feed.page.listenBinary((data) => void (got += data.byteLength))
  await run(1_000)
  loop.socket.toPage.bytesPerMs = 20_000
  void (async () => {
    while (!feed.server.isClosed) await feed.server.sendBinary(new Uint8Array(64 * KIB))
  })()
  await run(3_000)
  // 20 MB/s over a 200 ms round trip: 4 MB in flight, twice the window the 10 ms round trip measured at attach kept.
  for (const pipe of [loop.socket.toPage, loop.socket.toServer]) pipe.latencyMs = 100
  await run(5_000)
  const before = got
  await run(5_000)
  expect((got - before) / 5_000).toBeGreaterThan(0.9 * 20_000)
  expect(loop.sockets).toHaveLength(1)
})

test("a server that sent a 64 KiB frame every 200 ms on a 10 MB/s link, then sends ~4 KB messages as fast as credit allows, carries them at the link's rate, not at the rate it sent before", async () => {
  const { server, page } = loop.open<never, unknown>()
  let got = 0
  page.listenBinary((data) => void (got += data.byteLength))
  page.listen(() => void (got += 4_000))
  await run(100)
  loop.socket.toPage.bytesPerMs = loop.socket.toServer.bytesPerMs = 10_000
  for (let i = 0; i < 15; i++) {
    void server.sendBinary(new Uint8Array(64 * KIB))
    await run(200)
  }
  await run(500)
  const mark = got
  let stop = false
  void (async () => {
    while (!stop && !server.isClosed) await server.send({ text: 'x'.repeat(4_000) })
  })().catch(() => {})
  await run(3_000)
  stop = true
  expect((got - mark) / 3_000).toBeGreaterThan(0.9 * 10_000)
  expect(loop.sockets).toHaveLength(1)
})

// Node's `ws` reads `bufferedAmount` 0 while the kernel takes what it sends.
test.each([
  ['a socket that reports every byte it holds', false],
  ['a socket that reports nothing under 128 KiB', true],
])(
  "on %s, a server that answered a page's acked request every 200 ms, then sends ~4 KB messages as fast as credit allows, carries them at the link's rate, not at the page's pace",
  async (_, nodeLike) => {
    if (nodeLike)
      loop.transport.bufferedAmount = (socket) => (socket.toPage.bytes > 128 * KIB ? socket.toPage.bytes : 0)
    const rpc = loop.open<string, never>()
    const { server, page } = loop.open<never, unknown>()
    let got = 0
    page.listen(() => void got++)
    rpc.server.listen(() => 'y'.repeat(64 * KIB) as never)
    await run(100)
    loop.socket.toPage.bytesPerMs = loop.socket.toServer.bytesPerMs = 10_000
    for (let i = 0; i < 15; i++) {
      void rpc.page.send('q'.repeat(4_000), { ack: true })
      await run(200)
    }
    await run(500)
    const mark = got
    let stop = false
    void (async () => {
      while (!stop && !server.isClosed) await server.send({ text: 'x'.repeat(4_000) })
    })().catch(() => {})
    await run(3_000)
    stop = true
    // 10 MB/s carries ~2,400 of them a second.
    expect((got - mark) / 3).toBeGreaterThan(2_000)
    expect(loop.sockets).toHaveLength(1)
  },
)

// Deno's WebSocket counts what it was sent until its write completes on the event loop, after the task.
test.each([
  ['with its value', 'ask'],
  ['with a 64 KiB push on another channel', 'echo'],
])(
  "on a socket that reports what it was sent until the task ends, a server that answered a page's 20 KB request every 200 ms %s, then sends ~4 KB messages as fast as credit allows, carries them at the link's rate, not at the page's pace",
  async (_, answer) => {
    let sentThisTask = 0
    const { sendNow } = loop.transport
    loop.transport.sendNow = (socket, frame) => {
      if (sentThisTask === 0) setTimeout(() => void (sentThisTask = 0), 0)
      sentThisTask += frame.byteLength
      sendNow(socket, frame)
    }
    loop.transport.bufferedAmount = () => sentThisTask
    const rpc = loop.open<string, never>()
    const { server, page } = loop.open<never, unknown>()
    let got = 0
    page.listen(() => void got++)
    page.listenBinary(() => {})
    rpc.server.listen(() =>
      answer === 'ask' ? ('y'.repeat(64 * KIB) as never) : void server.sendBinary(new Uint8Array(64 * KIB)),
    )
    await run(100)
    loop.socket.toPage.bytesPerMs = loop.socket.toServer.bytesPerMs = 10_000
    for (let i = 0; i < 15; i++) {
      if (answer === 'ask') void rpc.page.send('q'.repeat(20_000), { ack: true })
      else void rpc.page.send('q'.repeat(20_000))
      await run(200)
    }
    await run(500)
    const mark = got
    let stop = false
    void (async () => {
      while (!stop && !server.isClosed) await server.send({ text: 'x'.repeat(4_000) })
    })().catch(() => {})
    await run(3_000)
    stop = true
    // 10 MB/s carries ~2,400 of them a second.
    expect((got - mark) / 3).toBeGreaterThan(2_000)
    expect(loop.sockets).toHaveLength(1)
  },
)

// What the other channels' messages leave in the server's socket, and deliver just before the ping's ack, is no queue.
test.each([
  ['another channel on it gets a small message every 50 ms', [50]],
  ['another channel on it gets a small message every 250 ms', [250]],
  ['two other channels on it each get a small message every 50 ms, 25 ms apart', [50, 50]],
])(
  "a download whose path's round trip rises on its wire grows its window as far as alone to carry the link's rate while %s",
  async (_, ticks) => {
    const feed = loop.open<never, Uint8Array>()
    const chats = ticks.map(() => loop.open<never, number>())
    for (const { page } of chats) page.listen(() => {})
    let got = 0
    feed.page.listenBinary((data) => void (got += data.byteLength))
    await run(1_000)
    loop.socket.toPage.bytesPerMs = 20_000
    void (async () => {
      while (!feed.server.isClosed) await feed.server.sendBinary(new Uint8Array(64 * KIB))
    })()
    const tickers: ReturnType<typeof setInterval>[] = []
    for (const [i, { server }] of chats.entries()) {
      if (i > 0) await run(ticks[i]! / chats.length)
      tickers.push(setInterval(() => void server.send(Date.now()).catch(() => {}), ticks[i]))
    }
    await run(3_000)
    for (const pipe of [loop.socket.toPage, loop.socket.toServer]) pipe.latencyMs = 100
    await run(5_000)
    const before = got
    await run(5_000)
    for (const ticker of tickers) clearInterval(ticker)
    expect((got - before) / 5_000).toBeGreaterThan(0.9 * 20_000)
    // As far as the spec above, alone on its wire.
    expect(flowOf(feed.page).byteWindow).toBe(8 * 1024 * KIB)
    expect(loop.sockets).toHaveLength(1)
  },
)

// The page's socket here reports nothing it holds, as a browser's does of what its network stack and kernel hold: a
// WebSocket's bufferedAmount reads 0 in Chromium while more than a megabyte of the upload waits below it.
test("on a slow uplink, the server's window for an upload stays at its initial size, however little the page's socket reports it holds", async () => {
  const feed = loop.open<string, never>()
  const server = consume(feed.server)
  await run(100)
  loop.socket.toServer.bytesPerMs = 100
  produce(feed.page, { message: () => 'x'.repeat(64 * KIB) })
  await run(60_000)
  expect(flowOf(feed.server).byteWindow).toBe(CREDIT_WINDOW_INITIAL_BYTES)
  expect(server.received.length).toBeGreaterThan(80)
})

// Two downloads fill a 20 MB/s link whose round trip is 200 ms, and a third channel's messages, sent every 250 ms, wait
// behind what the server's socket holds. A runtime that doesn't report it, as workerd, or reports none of what the
// kernel's send buffer holds, leaves only the page to tell the queue from the path.
test.each([
  ['reports nothing', () => undefined],
  ['reads 0', () => 0],
])(
  "two downloads on one wire whose server's socket %s of what it holds keep their windows at what the path carries in a round trip, and a third channel's messages wait no longer behind them",
  async (_, held) => {
    loop.linkBytesPerMs = 20_000
    loop.transport.bufferedAmount = held
    const a = loop.open<never, Uint8Array>()
    for (const pipe of [loop.socket.toPage, loop.socket.toServer]) pipe.latencyMs = 100
    const b = loop.open<never, Uint8Array>()
    const c = loop.open<never, number>()
    let got = 0
    for (const { page } of [a, b]) page.listenBinary((data) => void (got += data.byteLength))
    const waits: number[] = []
    c.page.listen((sentAt) => void waits.push(Date.now() - sentAt))
    await run(2_000)
    for (const { server } of [a, b]) {
      void (async () => {
        while (!server.isClosed) await server.sendBinary(new Uint8Array(64 * KIB))
      })().catch(() => {})
    }
    const ticker = setInterval(() => void c.server.send(Date.now()).catch(() => {}), 250)
    await run(30_000)
    const before = got
    waits.length = 0
    await run(10_000)
    clearInterval(ticker)
    expect((got - before) / 10_000).toBeGreaterThan(0.9 * 20_000)
    // 4 MB in flight fills the path: a window of 4 MiB each.
    for (const { page } of [a, b]) expect(flowOf(page).byteWindow).toBeLessThanOrEqual(4 * 1024 * KIB)
    expect(Math.max(...waits)).toBeLessThan(400)
    expect(loop.sockets).toHaveLength(1)
  },
)

// 100 KB/s: the RECONCILE naming the new channel waits 20 s behind the 2 MiB window of the upload, twice the time a page
// waits for its RECONCILED on a wire that delivers nothing. The server holds the new channel that long.
test('a channel the page opens while its upload fills a slow uplink attaches on the same wire, however long its RECONCILE waits behind the upload', async () => {
  config.channel.connectTtl = 60_000
  const upload = loop.open<string, never>()
  let uploaded = 0
  upload.server.listen(() => void uploaded++)
  await run(100)
  loop.socket.toServer.bytesPerMs = 100
  produce(upload.page, { message: () => 'x'.repeat(64 * KIB) })
  await run(10_000)
  const late = loop.open<string, never>()
  let arrived = 0
  late.server.listen(() => void arrived++)
  produce(late.page, { message: () => 'y'.repeat(64 * KIB) })
  await run(40_000)
  expect(loop.sockets).toHaveLength(1)
  expect(arrived).toBeGreaterThan(0)
  expect(uploaded).toBeGreaterThan(30)
})

test('a page whose downlink stops while its RECONCILE waits behind its upload takes the wire for dead once it has delivered nothing for the time a page waits for its RECONCILED', async () => {
  config.channel.connectTtl = 60_000
  const upload = loop.open<string, never>()
  consume(upload.server)
  await run(100)
  loop.socket.toServer.bytesPerMs = 100
  produce(upload.page, { message: () => 'x'.repeat(64 * KIB) })
  await run(10_000)
  loop.open<string, never>()
  await run(1_000)
  // The server doesn't cut it: the page finds out on its own, as when the server can't reach it.
  vi.spyOn(loop.transport, 'terminateConnection').mockImplementation(() => {})
  loop.socket.toPage.hold()
  const stoppedAt = Date.now()
  await runUntil(() => loop.sockets.length === 2, 30_000, LATENCY_MS)
  expect(loop.sockets).toHaveLength(2)
  expect(Date.now() - stoppedAt).toBeLessThanOrEqual(RECONCILE_TIMEOUT_MS + CHANNEL_RECONNECT_INITIAL_DELAY_MS + 100)
})

test('on a link too slow for a megabyte in a ping deadline, a megabyte sent at connect, and the next, cross on one wire, either way', async () => {
  config.channel.pingInterval = 1_000
  // 100 KB/s: a megabyte takes 10 s, five pong deadlines.
  loop.linkBytesPerMs = 100
  const { server, page } = loop.open<Uint8Array, Uint8Array>()
  const atServer: number[] = []
  const atPage: number[] = []
  server.listenBinary((data) => void atServer.push(data.byteLength))
  page.listenBinary((data) => void atPage.push(data.byteLength))
  await run(1_000)
  void page.sendBinary(new Uint8Array(1_000_000))
  void server.sendBinary(new Uint8Array(1_000_000))
  await runUntil(() => atServer.length === 1 && atPage.length === 1, 60_000, LATENCY_MS)
  void page.sendBinary(new Uint8Array(1_000_000))
  void server.sendBinary(new Uint8Array(1_000_000))
  await runUntil(() => atServer.length === 2 && atPage.length === 2, 60_000, LATENCY_MS)
  expect(atServer).toEqual([1_000_000, 1_000_000])
  expect(atPage).toEqual([1_000_000, 1_000_000])
  expect(loop.sockets).toHaveLength(1)
})

test.each(['server', 'page'] as const)(
  'on a link too slow for a megabyte in the RECONCILE deadline, a megabyte the %s sends as the page attaches crosses on one wire in the time the link takes',
  async (sender) => {
    // 100 kbit/s: a megabyte takes 80 s, past the 10 s a first wire has to deliver its RECONCILED.
    loop.linkBytesPerMs = 12.5
    const openedAt = Date.now()
    const { server, page } = loop.open<Uint8Array, Uint8Array>()
    let arrivedAt = Number.NaN
    if (sender === 'server') {
      server.onOpen(() => void server.sendBinary(new Uint8Array(1_000_000)))
      page.listenBinary(() => void (arrivedAt = Date.now()))
    } else {
      void page.sendBinary(new Uint8Array(1_000_000))
      server.listenBinary(() => void (arrivedAt = Date.now()))
    }
    await run(90_000)
    expect(arrivedAt - openedAt).toBeLessThanOrEqual(1_000_000 / 12.5 + 1_000)
    expect(loop.sockets).toHaveLength(1)
  },
)

for (const kbit of [10, 5]) {
  test(`on a link of ${kbit} kbit/s, a 40 KiB message crosses on one wire in the time the link takes, either way`, async () => {
    loop.linkBytesPerMs = kbit / 8
    const { server, page } = loop.open<Uint8Array, Uint8Array>()
    const arrived: { page?: number; server?: number } = {}
    page.listenBinary(() => void (arrived.page = Date.now()))
    server.listenBinary(() => void (arrived.server = Date.now()))
    await run(3_000)
    const sentAt = Date.now()
    void page.sendBinary(new Uint8Array(40 * KIB))
    void server.sendBinary(new Uint8Array(40 * KIB))
    const crossMs = (40 * KIB) / (kbit / 8)
    await run(1.2 * crossMs)
    expect(arrived.page! - sentAt).toBeLessThanOrEqual(1.05 * crossMs)
    expect(arrived.server! - sentAt).toBeLessThanOrEqual(1.05 * crossMs)
    expect(loop.sockets).toHaveLength(1)
  })
}

test.each([10, 50, 200].flatMap((rtt) => [8 * KIB, 32 * KIB, 512 * KIB].map((size) => [rtt, size])))(
  'on a fresh wire with a %i ms round trip, a first message of %i B arrives one way later, as a whole message does, either way',
  async (rtt, size) => {
    const { server, page } = loop.open<Uint8Array, Uint8Array>()
    const arrived: { page?: number; server?: number } = {}
    page.listenBinary(() => void (arrived.page = Date.now()))
    server.listenBinary(() => void (arrived.server = Date.now()))
    await run(1_000)
    loop.socket.toServer.latencyMs = loop.socket.toPage.latencyMs = rtt / 2
    const sentAt = Date.now()
    void page.sendBinary(new Uint8Array(size))
    void server.sendBinary(new Uint8Array(size))
    await runUntil(() => arrived.page !== undefined && arrived.server !== undefined, 1_000, LATENCY_MS)
    // Its pieces take a read each, where the in-process socket reads up to `READ_BYTES` of whole messages a tick.
    for (const at of [arrived.page!, arrived.server!]) {
      expect(at - sentAt).toBeLessThanOrEqual(rtt / 2 + size / READ_BYTES)
    }
  },
)

test.each([1.5, 1])(
  'on a fresh wire of %s kbit/s, an 8 KiB message crosses on one wire in the time the link takes, either way',
  async (kbit) => {
    config.channel.connectTtl = 600_000
    config.channel.reconnectTimeout = 600_000
    loop.linkBytesPerMs = kbit / 8
    const { server, page } = loop.open<Uint8Array, Uint8Array>()
    const arrived: { page?: number; server?: number } = {}
    page.listenBinary(() => void (arrived.page = Date.now()))
    server.listenBinary(() => void (arrived.server = Date.now()))
    await run(5_000)
    const sentAt = Date.now()
    void page.sendBinary(new Uint8Array(8 * KIB))
    void server.sendBinary(new Uint8Array(8 * KIB))
    const crossMs = (8 * KIB) / (kbit / 8)
    await run(1.2 * crossMs)
    expect(arrived.page! - sentAt).toBeLessThanOrEqual(1.01 * crossMs)
    expect(arrived.server! - sentAt).toBeLessThanOrEqual(1.01 * crossMs)
    expect(loop.sockets).toHaveLength(1)
  },
)

test.each((['server', 'page'] as const).flatMap((sender) => [2, 1].map((kbit) => [sender, kbit] as const)))(
  'on a fresh wire, a 4000 B message the %s sends at %s kbit/s crosses on one wire in the time the link takes',
  async (sender, kbit) => {
    config.channel.connectTtl = 600_000
    config.channel.reconnectTimeout = 600_000
    loop.linkBytesPerMs = kbit / 8
    const { server, page } = loop.open<Uint8Array, Uint8Array>()
    let arrivedAt: number | undefined
    ;(sender === 'server' ? page : server).listenBinary(() => void (arrivedAt = Date.now()))
    await run(5_000)
    const sentAt = Date.now()
    void (sender === 'server' ? server : page).sendBinary(new Uint8Array(4_000))
    const crossMs = 4_000 / (kbit / 8)
    await run(1.2 * crossMs)
    expect(arrivedAt! - sentAt).toBeLessThanOrEqual(1.01 * crossMs)
    expect(loop.sockets).toHaveLength(1)
  },
)

test.each([
  [4 * KIB, 2_000],
  [32 * KIB, 500],
  [128 * KIB, 200],
])(
  'a fresh wire carries a push of %i B messages nobody awaits as fast as the same wire does once it measured its rate',
  async (size, count) => {
    const { server, page } = loop.open<string, string>()
    let got = 0
    page.listen(() => void got++)
    await run(100)
    const payload = 'x'.repeat(size)
    const push = async () => {
      const [startedAt, until] = [Date.now(), got + count]
      for (let i = 0; i < count; i++) void server.send(payload)
      await runUntil(() => got === until, 10_000, LATENCY_MS)
      return Date.now() - startedAt
    }
    const fresh = await push()
    await run(1_000)
    expect(fresh).toBeLessThanOrEqual(await push())
    expect(loop.sockets).toHaveLength(1)
  },
)

test.each(
  (['server', 'page'] as const).flatMap((sender) =>
    [
      // kbit/s, bytes, when the link dies, the most it may take to find it dead
      [100, 1_000_000, 5_000, 12_000],
      [1_000, 4 * 1024 * KIB, 1_000, 11_000],
      [100, 1_000_000, 40_000, 25_000],
    ].map(([kbit, bytes, diesAfter, withinMs]) => [sender, kbit!, bytes!, diesAfter!, withinMs!] as const),
  ),
)(
  'a fresh wire on which the %s sends at %i kbit/s %i B, whose link dies %i ms in, is found dead within %i ms',
  async (sender, kbit, bytes, diesAfter, withinMs) => {
    config.channel.connectTtl = 600_000
    config.channel.reconnectTimeout = 600_000
    loop.linkBytesPerMs = kbit / 8
    const { server, page } = loop.open<Uint8Array, Uint8Array>()
    await run(3_000)
    // The server doesn't cut it: the page finds out on its own, as when the server can't reach it.
    vi.spyOn(loop.transport, 'terminateConnection').mockImplementation(() => {})
    void (sender === 'server' ? server : page).sendBinary(new Uint8Array(bytes)).catch(() => {})
    await run(diesAfter)
    ;(sender === 'server' ? loop.socket.toPage : loop.socket.toServer).hold()
    const diedAt = Date.now()
    await runUntil(() => loop.sockets.length === 2, 60_000, LATENCY_MS)
    expect(loop.sockets).toHaveLength(2)
    expect(Date.now() - diedAt).toBeLessThanOrEqual(withinMs)
  },
)

test('a push of 4 KiB messages on a fresh wire whose floor carries them within a ping interval goes whole', async () => {
  const { server, page } = loop.open<string, string>()
  let got = 0
  page.listen(() => void got++)
  await run(100)
  const mark = loop.sent.server.length
  for (let i = 0; i < 2_000; i++) void server.send('x'.repeat(4 * KIB))
  await runUntil(() => got === 2_000, 10_000, LATENCY_MS)
  expect(loop.sent.server.slice(mark).filter(([tag]) => tag === TAG.PIECE || tag === TAG.ANNOUNCE)).toEqual([])
})

test('a push of small messages nobody awaits reaches the page in one WebSocket message, on a fresh wire and after', async () => {
  const { server, page } = loop.open<string, string>()
  let got = 0
  page.listen(() => void got++)
  await run(100)
  const push = async () => {
    const mark = loop.sent.server.length
    const until = got + 60
    for (let i = 0; i < 60; i++) void server.send('x'.repeat(64))
    await runUntil(() => got === until, 1_000, LATENCY_MS)
    return new Set(loop.sent.server.slice(mark).flatMap(([tag, , , message]) => (tag === TAG.TEXT ? [message] : [])))
      .size
  }
  expect([await push(), await push()]).toEqual([1, 1])
})

test("a channel's first attach sends no window either way: each side starts with the initial limits", async () => {
  const { page } = loop.open<string, string>()
  let opened = false
  page.onOpen(() => (opened = true))
  await runUntil(() => opened, 1_000, LATENCY_MS)
  await run(100)
  const windows = (sent: [number, number, number, number][]) =>
    sent.filter(([tag]) => tag === TAG.WINDOW || tag === TAG.MSG_WINDOW).map(([tag]) => tag)
  expect([windows(loop.sent.page), windows(loop.sent.server)]).toEqual([[], []])
})

test("a fresh wire's attach measures the path's round trip by the RECONCILED, with no answer to its probe beside it", async () => {
  const { page } = loop.open<string, string>()
  let opened = false
  page.onOpen(() => (opened = true))
  await runUntil(() => opened, 1_000, LATENCY_MS)
  expect(loop.sent.server.filter(([tag]) => tag === TAG.BDP_PING_ACK)).toEqual([])
  expect(
    (page as unknown as { _connection: { transport: { uplink: { link: { pathRtt: number } } } } })._connection.transport
      .uplink.link.pathRtt,
  ).toBe(2 * LATENCY_MS)
})

test.each([16, 64, 256])(
  'a page that sends %i KiB messages, each once the answer to the one before arrived, gets each answer a round trip after it sends it',
  async (kib) => {
    const { server, page } = loop.open<Uint8Array, Uint8Array>()
    server.listenBinary(() => 'ok')
    await run(100)
    const rtts: number[] = []
    for (let i = 0; i < 10; i++) {
      const at = Date.now()
      let answered = false
      void page.sendBinary(new Uint8Array(kib * KIB), { ack: true }).then(() => (answered = true))
      while (!answered) await run(1)
      rtts.push(Date.now() - at)
    }
    expect(rtts.filter((ms) => ms > 2 * LATENCY_MS + 1)).toEqual([])
  },
)

test("the server acknowledges a page's message in the message of its answer", async () => {
  const { server, page } = loop.open<Uint8Array, Uint8Array>()
  server.listenBinary(() => 'ok')
  await run(100)
  const echo = async () => {
    let answered = false
    void page.sendBinary(new Uint8Array(8 * KIB), { ack: true }).then(() => (answered = true))
    while (!answered) await run(1)
  }
  // The fresh wire's first message goes announced, which the server acknowledges at once.
  await echo()
  const before = loop.sent.server.length
  for (let i = 0; i < 10; i++) await echo()
  const sent = loop.sent.server.slice(before)
  const answers = sent.filter(([tag]) => tag === TAG.ACK_RES).map(([, , , message]) => message)
  const acks = sent.filter(([tag]) => tag === TAG.PIECES_ACK).map(([, , , message]) => message)
  expect(acks).toEqual(answers)
  expect(answers).toHaveLength(10)
})

test.each(['page', 'server'] as const)(
  "the %s's requests are acknowledged by their answers: its replay lets them go, and its peer sends no window for them",
  async (asker) => {
    const { server, page } = loop.open<Uint8Array, Uint8Array>()
    const [from, to] = asker === 'page' ? [page, server] : [server, page]
    to.listenBinary(() => 'ok')
    await run(100)
    const before = { page: loop.sent.page.length, server: loop.sent.server.length }
    for (let i = 0; i < 10; i++) {
      let answered = false
      void from.sendBinary(new Uint8Array(256 * KIB), { ack: true }).then(() => (answered = true))
      while (!answered) await run(1)
    }
    const answerer = asker === 'page' ? 'server' : 'page'
    expect(loop.sent[answerer].slice(before[answerer]).filter(([tag]) => tag === TAG.WINDOW)).toEqual([])
    const replay =
      asker === 'page'
        ? [
            ...(
              page as unknown as { _connection: { replayBuffers: Map<number, { byteLength: number }> } }
            )._connection.replayBuffers.values(),
          ][0]!
        : (server as unknown as { _replayBuffer: { byteLength: number } })._replayBuffer
    expect(replay.byteLength).toBe(0)
  },
)

/** The bytes the page's replay holds for `page`'s channel. */
const pageReplayBytes = (page: unknown) =>
  [
    ...(
      page as { _connection: { replayBuffers: Map<number, { byteLength: number }> } }
    )._connection.replayBuffers.values(),
  ][0]!.byteLength

/** The frames of `sent` of `tag` that went in a WebSocket message of their own. */
const alone = (sent: [number, number, number, number][], tag: number) => {
  const frames = new Map<number, number>()
  for (const [, , , message] of sent) frames.set(message, (frames.get(message) ?? 0) + 1)
  return sent.filter(([t, , , message]) => t === tag && frames.get(message) === 1)
}

test("a page acknowledges the answers it receives in the next message it sends, and the server's replay lets them go", async () => {
  const { server, page } = loop.open<string, string>()
  server.listen((text) => text)
  await run(100)
  const before = loop.sent.page.length
  for (let i = 0; i < 10; i++) {
    let answered = false
    void page.send('x'.repeat(256 * KIB), { ack: true }).then(() => (answered = true))
    while (!answered) await run(1)
  }
  const sent = loop.sent.page.slice(before)
  // Each rides the request after it.
  expect(alone(sent, TAG.WINDOW)).toEqual([])
  expect(sent.filter(([tag]) => tag === TAG.WINDOW).length).toBeGreaterThan(0)
  // The last answers' acknowledgement goes with the next heartbeat at the latest.
  await run(CHANNEL_PING_INTERVAL_MS + 100)
  expect((server as unknown as { _replayBuffer: { byteLength: number } })._replayBuffer.byteLength).toBe(0)
})

test("the server acknowledges the page's answers to its ack requests in the next message it sends", async () => {
  const { server, page } = loop.open<string, string>()
  page.listen((text) => text)
  await run(100)
  const before = loop.sent.server.length
  for (let i = 0; i < 10; i++) {
    let answered = false
    void server.send('x'.repeat(256 * KIB), { ack: true }).then(() => (answered = true))
    while (!answered) await run(1)
  }
  const sent = loop.sent.server.slice(before)
  expect(alone(sent, TAG.WINDOW)).toEqual([])
  expect(sent.filter(([tag]) => tag === TAG.WINDOW).length).toBeGreaterThan(0)
})

test("the server acknowledges at its heartbeat in its PONG's message, and the page's replay lets go of what it sent", async () => {
  const { server, page } = loop.open<string, string>()
  server.listen(() => {})
  await run(100)
  const before = loop.sent.server.length
  void page.send('x')
  await run(CHANNEL_PING_INTERVAL_MS + 100)
  const sent = loop.sent.server.slice(before)
  expect(alone(sent, TAG.WINDOW)).toEqual([])
  expect(sent.filter(([tag]) => tag === TAG.WINDOW)).toHaveLength(1)
  expect(pageReplayBytes(page)).toBe(0)
})

test('on a 10 B/ms uplink, a page that idled an hour after an upload has its next send reach the server within seconds', async () => {
  const { server, page } = loop.open<string, never>()
  let got = 0
  server.listen(() => void got++)
  await run(100)
  loop.socket.toServer.bytesPerMs = 10
  let uploading = true
  void (async () => {
    while (uploading) await page.send('x'.repeat(1_000))
  })()
  await run(20_000)
  uploading = false
  for (let minute = 0; minute < 60; minute++) await run(60_000)
  const before = got
  void page.send('x'.repeat(100))
  await runUntil(() => got > before, 5_000, LATENCY_MS)
  expect(got).toBe(before + 1)
  expect(loop.sockets.length).toBe(1)
})

test('a page taking a push of 128 KiB messages acknowledges them only in the messages it sends anyway, once the server measured the rate', async () => {
  const { server, page } = loop.open<Uint8Array, Uint8Array>()
  let got = 0
  page.listenBinary(() => void got++)
  const push = async (n: number) => {
    const until = got + n
    for (let i = 0; i < n; i++) void server.sendBinary(new Uint8Array(128 * KIB))
    await runUntil(() => got === until, 5_000, LATENCY_MS)
  }
  await push(20)
  const before = loop.sent.page.length
  await push(200)
  const sent = loop.sent.page.slice(before)
  expect(alone(sent, TAG.PIECES_ACK)).toEqual([])
  // Its window refreshes carry them.
  expect(sent.filter(([tag]) => tag === TAG.PIECES_ACK).length).toBeGreaterThan(0)
})

test('a fresh wire carrying only small messages stops announcing them once one announcement was acknowledged, either way', async () => {
  const { server, page } = loop.open<Uint8Array, Uint8Array>()
  let got = 0
  page.listenBinary(() => void got++)
  server.listenBinary(() => void got++)
  const announces = () =>
    [loop.sent.page, loop.sent.server].map((sent) => sent.filter(([tag]) => tag === TAG.ANNOUNCE).length)
  const push = async () => {
    for (let i = 0; i < 1_000; i++) {
      void page.sendBinary(new Uint8Array(64))
      void server.sendBinary(new Uint8Array(64))
      await run(1)
    }
  }
  await push()
  const first = announces()
  await push()
  await runUntil(() => got === 4_000, 1_000, LATENCY_MS)
  expect(got).toBe(4_000)
  expect(announces()).toEqual(first)
})

test.each((['server', 'page'] as const).flatMap((sender) => [2, 1].map((kbit) => [sender, kbit] as const)))(
  'a 4000 B message the %s sends as a fresh wire of %s kbit/s opens crosses on that wire',
  async (sender, kbit) => {
    config.channel.connectTtl = 600_000
    config.channel.reconnectTimeout = 600_000
    loop.linkBytesPerMs = kbit / 8
    const { server, page } = loop.open<Uint8Array, Uint8Array>()
    let arrived = false
    ;(sender === 'server' ? page : server).listenBinary(() => void (arrived = true))
    if (sender === 'server') server.onOpen(() => void server.sendBinary(new Uint8Array(4_000)))
    else void page.sendBinary(new Uint8Array(4_000))
    // The handshake and the message, at the link's rate.
    await runUntil(() => arrived, (1.2 * 4_600) / (kbit / 8), LATENCY_MS)
    expect(arrived).toBe(true)
    expect(loop.sockets).toHaveLength(1)
  },
)

test('on a fresh wire with a 600 ms round trip, small messages sent once it opened go unannounced, either way', async () => {
  const { server, page } = loop.open<Uint8Array, Uint8Array>()
  await run(0)
  loop.socket.toServer.latencyMs = loop.socket.toPage.latencyMs = 300
  let got = 0
  page.listenBinary(() => void got++)
  server.listenBinary(() => void got++)
  server.onOpen(() => void server.sendBinary(new Uint8Array(16)))
  void page.sendBinary(new Uint8Array(16))
  await runUntil(() => got === 2, 2_000, LATENCY_MS)
  for (let i = 0; i < 1_000; i++) {
    void page.sendBinary(new Uint8Array(64))
    void server.sendBinary(new Uint8Array(64))
    await run(1)
  }
  await runUntil(() => got === 2_002, 1_000, LATENCY_MS)
  expect(got).toBe(2_002)
  expect([loop.sent.page, loop.sent.server].map((sent) => sent.filter(([tag]) => tag === TAG.ANNOUNCE))).toEqual([
    [],
    [],
  ])
})

test('a WebSocket that cannot be constructed sends the channel to its reconnect loop, it does not throw', async () => {
  config.channel.transports = ['ws']
  vi.stubGlobal('WebSocket', undefined)
  let thrown: unknown
  try {
    loop.open<Uint8Array, Uint8Array>()
    await run(3_000)
  } catch (err) {
    thrown = err
  }
  expect(thrown).toBeUndefined()
})

test('a burst of large frames gets a few PIECES_ACKs, not one each, and the last covers all of it', async () => {
  const { server, page } = loop.open<Uint8Array, Uint8Array>()
  const at = { server: 0, page: 0 }
  server.listenBinary(() => void at.server++)
  page.listenBinary(() => void at.page++)
  await run(100)
  const marks = { page: loop.sent.page.length, server: loop.sent.server.length }
  for (let i = 0; i < 100; i++) {
    void page.sendBinary(new Uint8Array(32_000))
    void server.sendBinary(new Uint8Array(32_000))
  }
  await runUntil(() => at.server === 100 && at.page === 100, 5_000, LATENCY_MS)
  await run(100)
  for (const side of ['page', 'server'] as const) {
    const acks = loop.sent[side].slice(marks[side]).filter(([tag]) => tag === TAG.PIECES_ACK)
    expect(acks.length).toBeGreaterThan(0)
    // The server acknowledges at most every `WIRE_SEND_ACK_GAP_MS`, the page every `WIRE_PIECES_ACK_GAP_MS`.
    expect(acks.length).toBeLessThan(side === 'server' ? 20 : 5)
  }
  const sender = piecesOf(loop.socket)?.room.sender as { unacknowledged: { length: number } }
  expect(sender.unacknowledged).toHaveLength(0)
  expect(loop.sockets).toHaveLength(1)
})

test('a flood of small frames gets a PIECES_ACK from the server, which the page holds back by, and none from the page', async () => {
  const { server, page } = loop.open<Uint8Array, Uint8Array>()
  const at = { server: 0, page: 0 }
  server.listenBinary(() => void at.server++)
  page.listenBinary(() => void at.page++)
  await run(100)
  const marks = { page: loop.sent.page.length, server: loop.sent.server.length }
  for (let i = 0; i < 100; i++) {
    void page.sendBinary(new Uint8Array(1_000))
    void server.sendBinary(new Uint8Array(1_000))
  }
  await runUntil(() => at.server === 100 && at.page === 100, 5_000, LATENCY_MS)
  await run(100)
  const acks = (side: 'page' | 'server') =>
    loop.sent[side].slice(marks[side]).filter(([tag]) => tag === TAG.PIECES_ACK).length
  expect(acks('server')).toBeGreaterThan(0)
  expect(acks('page')).toBe(0)
})

test('a page that aborts its channel right after a message on a fresh wire hands its socket the abort before it lets the wire go idle', async () => {
  config.channel.idleTimeout = 1
  const { page } = loop.open<Uint8Array, Uint8Array>()
  await run(100)
  const socket = loop.socket
  // Within the room the server gives its page, so the abort goes to the wire at once, behind the message's probe.
  void page.sendBinary(new Uint8Array(10_000)).catch(() => {})
  page.abort()
  await runUntil(() => socket.readyState === 3, 1_000, LATENCY_MS)
  expect(socket.readyState).toBe(3)
  expect(loop.sent.page.map(([tag]) => tag)).toContain(TAG.ABORT)
})

test('a page whose every channel on a full connection aborted, then refused one more, still lets the wire go idle', async () => {
  config.channel.idleTimeout = 1
  const pairs = Array.from({ length: MAX_CHANNELS_PER_CONNECTION }, () => loop.open())
  await run(100)
  const socket = loop.socket
  for (const { page } of pairs) page.abort()
  // Their aborts are on the wire, not yet acknowledged: each still holds its place.
  expect(() => loop.open()).toThrow('Too many channels')
  await runUntil(() => socket.readyState === 3, 10_000, LATENCY_MS)
  expect(socket.readyState).toBe(3)
})

test('a page that aborted a thousand channels while its wire was down lets the next wire go idle once their aborts went', async () => {
  config.channel.idleTimeout = 1
  const pairs = Array.from({ length: 1_000 }, () => loop.open())
  await run(100)
  loop.socket.cut()
  for (const { page } of pairs) page.abort()
  // More aborts than the new wire has room for at once: the last of them wait for its room.
  await runUntil(() => loop.sockets.length === 2 && loop.socket.readyState === 3, 60_000, LATENCY_MS)
  expect(loop.sockets).toHaveLength(2)
  expect(loop.socket.readyState).toBe(3)
})

test('a burst of 60 frames of 512 KB a server sends as its channel opens on a fast wire goes at once, whole after its first two', async () => {
  const { server, page } = loop.open<Uint8Array, Uint8Array>()
  let got = 0
  page.listenBinary(() => void got++)
  server.onOpen(() => {
    for (let i = 0; i < 60; i++) void server.sendBinary(new Uint8Array(512_000))
  })
  await runUntil(() => got === 60, 5_000, LATENCY_MS)
  expect(got).toBe(60)
  // The first frame's empty piece, its pair, and its rest in six pieces that at most double; the second in two.
  expect(loop.sent.server.filter(([tag]) => tag === TAG.PIECE)).toHaveLength(11)
  expect(loop.sent.page.filter(([tag]) => tag === TAG.PIECES_ACK).length).toBeLessThanOrEqual(4)
})

test('a server pushing 512 KB frames on a fast wire sends them whole once it measured, however many it queues', async () => {
  config.channel.pingInterval = 1_000
  const { server, page } = loop.open<Uint8Array, Uint8Array>()
  let got = 0
  page.listenBinary(() => void got++)
  await run(100)
  loop.socket.toPage.bytesPerMs = 100_000 // 100 MB/s
  const mark = loop.sent.server.length
  void (async () => {
    for (let i = 0; i < 100; i++) await server.sendBinary(new Uint8Array(512_000))
  })()
  await runUntil(() => got === 100, 10_000, LATENCY_MS)
  const tags = loop.sent.server.slice(mark).map(([tag]) => tag)
  expect(tags.filter((tag) => tag === TAG.BINARY).length).toBeGreaterThan(90)
})

test('on a fresh fast wire, frames of 32 KB and 128 KB go at once, in pieces that grow from a probe pair, either way', async () => {
  const { server, page } = loop.open<Uint8Array, Uint8Array>()
  const atServer: number[] = []
  const atPage: number[] = []
  server.listenBinary((data) => void atServer.push(data.byteLength))
  page.listenBinary((data) => void atPage.push(data.byteLength))
  await run(100)
  const sentAt = Date.now()
  const marks = { page: loop.sent.page.length, server: loop.sent.server.length }
  for (const size of [32_000, 128_000]) {
    void page.sendBinary(new Uint8Array(size))
    void server.sendBinary(new Uint8Array(size))
  }
  await runUntil(() => atServer.length === 2 && atPage.length === 2, 1_000, LATENCY_MS)
  // No round trip: they cross with the latency.
  expect(Date.now() - sentAt).toBeLessThanOrEqual(2 * LATENCY_MS)
  expect(atServer).toEqual([32_000, 128_000])
  expect(atPage).toEqual([32_000, 128_000])
  for (const side of ['page', 'server'] as const) {
    const sent = loop.sent[side].slice(marks[side])
    // A BINARY frame is its 7 B header and its data. Each piece carries at most twice the one before, or 8 KiB; the
    // last two of a frame are cut so the last is the larger.
    expect(sent.filter(([tag]) => tag === TAG.PIECE).map(([, , bytes]) => bytes - PIECE_HEADER_BYTES)).toEqual([
      0, 64, 2_048, 8_192, 7_235, 14_468, 28_936, 33_024, 66_047,
    ])
  }
  expect(loop.sockets).toHaveLength(1)
})

// The page's PINGs wait behind the rest of a frame it sends in pieces: the server answers what arrives once a ping
// interval, so the page knows it arrives, whatever the pieces.
test("on a 100 B/ms uplink, the server answers the pieces of the page's 1 MB frame with a PONG once a ping interval, besides its answers to the page's PINGs", async () => {
  config.channel.pingInterval = 1_000
  loop.linkBytesPerMs = 100
  const { server, page } = loop.open<Uint8Array, never>()
  let got = 0
  server.listenBinary(() => void got++)
  await runUntil(() => loop.sockets.length === 1 && loop.socket.readyState === 1, 1_000, LATENCY_MS)
  // A first frame measures the rate the next is cut by.
  void page.sendBinary(new Uint8Array(20_000))
  await runUntil(() => got === 1, 60_000, LATENCY_MS)
  const marks = { page: loop.sent.page.length, server: loop.sent.server.length }
  const start = Date.now()
  void page.sendBinary(new Uint8Array(1_000_000))
  await runUntil(() => got === 2, 60_000, LATENCY_MS)
  const pongs = loop.sent.server.slice(marks.server).filter(([tag]) => tag === TAG.PONG).length
  const pings = loop.sent.page.slice(marks.page).filter(([tag]) => tag === TAG.PING).length
  expect(pongs).toBeLessThanOrEqual(pings + Math.ceil((Date.now() - start) / 1_000))
  expect(loop.sockets).toHaveLength(1)
})

/** What the page counts as handed to its socket, and as taken from the server. */
const pageCounts = (page: unknown) =>
  (
    page as {
      _connection: {
        transport: { ends: { budget: { sender: { transmittedBytes: number } }; pieces: { took: number } } }
      }
    }
  )._connection.transport.ends

// A budget sees all it puts on a slow uplink only if every byte counts: a piece's header and an announcement too.
test("on a 1 B/ms link, what each end counts as handed and its peer hasn't taken is what the wire holds, headers included", async () => {
  loop.linkBytesPerMs = 1
  const { server, page } = loop.open<Uint8Array, Uint8Array>()
  const at = { server: 0, page: 0 }
  server.listenBinary(() => void at.server++)
  page.listenBinary(() => void at.page++)
  await runUntil(() => loop.sockets.length === 1 && loop.socket.readyState === 1, 1_000, LATENCY_MS)
  const socket = loop.socket
  const serverEnds = () => {
    const pieces = piecesOf(socket)!
    return { sender: pieces.room.sender as { transmittedBytes: number }, receiver: pieces.receiver as { took: number } }
  }
  const off = { up: 0, down: 0 }
  const sampler = setInterval(() => {
    const ends = pageCounts(page)
    off.up = Math.max(
      off.up,
      Math.abs(ends.budget.sender.transmittedBytes - serverEnds().receiver.took - socket.toServer.bytes),
    )
    off.down = Math.max(
      off.down,
      Math.abs(serverEnds().sender.transmittedBytes - ends.pieces.took - socket.toPage.bytes),
    )
  }, 3)
  for (let i = 0; i < 3; i++) {
    void page.sendBinary(new Uint8Array(3_000))
    void server.sendBinary(new Uint8Array(3_000))
  }
  await runUntil(() => at.server === 3 && at.page === 3, 60_000, LATENCY_MS)
  await run(1_000)
  clearInterval(sampler)
  expect(at).toEqual({ server: 3, page: 3 })
  expect(off).toEqual({ up: 0, down: 0 })
  expect(loop.sockets).toHaveLength(1)
})

// What a lost wire held goes again on the next, which starts as any fresh wire does: behind its announcement, at the rate
// its link carries.
test.each((['page', 'server'] as const).flatMap((sender) => (['fast', 'slow'] as const).map((next) => [sender, next])))(
  "after a wire is lost with a frame of the %s's unacknowledged on it, the next wire, on a %s link, carries it and 100 KB each way in the time the link takes",
  async (sender, next) => {
    const { server, page } = loop.open<Uint8Array, Uint8Array>()
    const atServer: number[] = []
    const atPage: number[] = []
    server.listenBinary((data) => void atServer.push(data.byteLength))
    page.listenBinary((data) => void atPage.push(data.byteLength))
    await run(100)
    const lost = loop.socket
    ;(sender === 'page' ? lost.toServer : lost.toPage).bytesPerMs = 1
    void (sender === 'page' ? page : server).sendBinary(new Uint8Array(128_000))
    await run(100)
    if (next === 'slow') loop.linkBytesPerMs = 1
    lost.cut()
    await runUntil(() => loop.sockets.length === 2, 5_000, LATENCY_MS)
    const sentAt = Date.now() + 1_000
    await run(1_000)
    void page.sendBinary(new Uint8Array(100_000))
    void server.sendBinary(new Uint8Array(100_000))
    // At 1 B/ms, the lost frame and the 100 KB behind it.
    const takes = 1.05 * (next === 'slow' ? 228_000 : 0) + 100
    await runUntil(() => atServer.includes(100_000) && atPage.includes(100_000), takes, LATENCY_MS)
    expect(Date.now() - sentAt).toBeLessThanOrEqual(takes)
    expect(sender === 'page' ? atServer : atPage).toEqual([128_000, 100_000])
    expect(sender === 'page' ? atPage : atServer).toEqual([100_000])
    expect(loop.sockets).toHaveLength(2)
  },
)

test('after a page gives up a wire whose pongs stopped, with a frame of its own unacknowledged, the next wire carries it and the next at once', async () => {
  config.channel.pingInterval = 1_000
  const { server, page } = loop.open<Uint8Array, Uint8Array>()
  const atServer: number[] = []
  server.listenBinary((data) => void atServer.push(data.byteLength))
  await run(100)
  // Nothing the server sends reaches the page: its pongs stop, and so does the PIECES_ACK for its frame.
  loop.socket.toPage.hold()
  void page.sendBinary(new Uint8Array(128_000))
  await runUntil(() => loop.sockets.length === 2, 10_000, LATENCY_MS)
  await run(1_000)
  const sentAt = Date.now()
  void page.sendBinary(new Uint8Array(100_000))
  await runUntil(() => atServer.length === 2, 1_000, LATENCY_MS)
  expect(atServer).toEqual([128_000, 100_000])
  expect(Date.now() - sentAt).toBeLessThanOrEqual(100)
  expect(loop.sockets).toHaveLength(2)
})

test("after a page drops a wire the server still holds open, with a frame of the server's unacknowledged on it, the next wire carries it and the page's next at once", async () => {
  config.channel.pingInterval = 1_000
  const { server, page } = loop.open<Uint8Array, Uint8Array>()
  const atServer: number[] = []
  const atPage: number[] = []
  server.listenBinary((data) => void atServer.push(data.byteLength))
  page.listenBinary((data) => void atPage.push(data.byteLength))
  await run(100)
  // The page waits for what the server announced at the rate it arrives: the wire must die to be dropped.
  void server.sendBinary(new Uint8Array(128_000))
  loop.socket.toPage.hold()
  await runUntil(() => loop.sockets.length === 2, 10_000, LATENCY_MS)
  await run(1_000)
  const sentAt = Date.now()
  void page.sendBinary(new Uint8Array(100_000))
  await runUntil(() => atServer.length > 0 && atPage.length > 0, 1_000, LATENCY_MS)
  expect(atServer).toEqual([100_000])
  expect(atPage).toEqual([128_000])
  expect(Date.now() - sentAt).toBeLessThanOrEqual(100)
  expect(loop.sockets).toHaveLength(2)
})

test.each([
  [4600, 500],
  [4200, 900],
  [4800, 300],
  [4900, 400],
])(
  "on a slow uplink, a page whose PING waits behind its probe while it acknowledges the server's download past it keeps its wire (%i ms into a ping interval, the download %i ms later)",
  async (offset, delay) => {
    const { server, page } = loop.open<Uint8Array, Uint8Array>()
    const atServer: number[] = []
    const atPage: number[] = []
    server.listenBinary((data) => void atServer.push(data.byteLength))
    page.listenBinary((data) => void atPage.push(data.byteLength))
    await run(100)
    // A wire lost with the server's probe unacknowledged: the next wire is slow on both ends.
    loop.socket.toPage.bytesPerMs = 1
    void server.sendBinary(new Uint8Array(128_000))
    await run(100)
    loop.socket.cut()
    await runUntil(() => loop.sockets.length === 2 && atPage.length === 1, 10_000, LATENCY_MS)
    loop.socket.toServer.bytesPerMs = 0.25
    let pinged = false
    loop.onSend('page', TAG.PING, () => void (pinged = true))
    await runUntil(() => pinged, 10_000, LATENCY_MS)
    await run(offset)
    void page.sendBinary(new Uint8Array(1_000))
    await run(delay)
    void server.sendBinary(new Uint8Array(20_000))
    await runUntil(() => atServer.length === 1 && atPage.length === 2, 30_000, LATENCY_MS)
    await run(15_000)
    expect(atServer).toEqual([1_000])
    expect(loop.sockets).toHaveLength(2)
  },
)

test('the server holds nothing for empty pieces, takes pieces of any size, and cuts a wire on an empty piece in the middle of a frame', async () => {
  loop.open<Uint8Array, Uint8Array>()
  await run(100)
  const socket = loop.socket
  // The page never reads the PONG each piece gets.
  socket.toPage.hold()
  for (let i = 0; i < 10_000; i++) socket.send(encode.piece(1_000_000, new Uint8Array(0)))
  await run(1_000)
  const receiver = () => piecesOf(socket)!.receiver as { held: number; frame: Uint8Array }
  expect(receiver().held).toBe(0)
  for (let i = 0; i < 3; i++) socket.send(encode.piece(1_000_000, new Uint8Array(1)))
  await run(100)
  expect(socket.readyState).toBe(1)
  expect(receiver().held).toBe(3)
  // What it holds of a frame is one buffer, at most twice what arrived of it.
  expect(receiver().frame.byteLength).toBeLessThanOrEqual(6)
  socket.send(encode.piece(1_000_000, new Uint8Array(0)))
  await run(100)
  expect(socket.readyState).toBe(3)
})

test('pieces of a frame larger than the runtime takes in one message cut the wire, as that message would', async () => {
  setAdapterMaxFrameBytes(1024 * KIB)
  try {
    loop.open<Uint8Array, Uint8Array>()
    await run(100)
    const socket = loop.socket
    socket.send(encode.piece(1024 * KIB + 1, new Uint8Array(0)))
    await run(100)
    expect(socket.readyState).toBe(3)
  } finally {
    setAdapterMaxFrameBytes(undefined)
  }
})

test.each([
  ['naming a floor of 1 B/s', 1, []],
  ['then sending 1 B pieces further and further apart', 0, [10, 150_000]],
])(
  'a peer that opens an announcement it sends nothing more behind, %s, has its wire cut once it was quiet twice the time 8 KiB takes at 64 B per ping deadline, the timer always within what setTimeout takes',
  async (_, floorPerS, gaps) => {
    config.channel.pingInterval = 1_000
    const delays: number[] = []
    const realSetTimeout = globalThis.setTimeout
    const spy = vi.spyOn(globalThis, 'setTimeout').mockImplementation(((fn: () => void, ms?: number) => {
      delays.push(ms ?? 0)
      return realSetTimeout(fn, ms)
    }) as typeof setTimeout)
    // A raw WebSocket to the server, no page behind it.
    const socket = new LoopbackSocket('ws://hostile')
    await run(10)
    socket.send(encode.announce(0, floorPerS))
    for (const gap of gaps) {
      await run(gap)
      socket.send(encode.piece(1_000, new Uint8Array(1)))
    }
    // 2 × 8 KiB at 64 B per 2 s.
    await run(511_000)
    const open = socket.readyState
    await run(2_000)
    spy.mockRestore()
    expect({ open, closed: socket.readyState, overRange: delays.filter((ms) => ms > 2 ** 31 - 1).length }).toEqual({
      open: 1,
      closed: 3,
      overRange: 0,
    })
  },
)

test('a peer that closes an announcement with other bytes than followed it has its wire cut', async () => {
  const socket = new LoopbackSocket('ws://hostile')
  await run(10)
  socket.send(encode.announce(0, 0))
  socket.send(encode.piece(100, new Uint8Array(50)))
  socket.send(encode.announce(100, 0))
  await run(100)
  expect(socket.readyState).toBe(3)
})

test('a page whose uplink stops with its upload queued on it takes the wire for dead within a pong deadline', async () => {
  config.channel.pingInterval = 1_000
  const feed = loop.open<string, never>()
  consume(feed.server)
  await run(100)
  loop.socket.toServer.bytesPerMs = 100
  produce(feed.page, { message: () => 'x'.repeat(64 * KIB) })
  await run(10_000)
  // The server doesn't cut it: the page finds out on its own, as when the server can't reach it either.
  vi.spyOn(loop.transport, 'terminateConnection').mockImplementation(() => {})
  loop.socket.toServer.hold()
  const stoppedAt = Date.now()
  await runUntil(() => loop.sockets.length === 2, 10_000, LATENCY_MS)
  expect(loop.sockets).toHaveLength(2)
  // The pong deadline is two ping intervals; then the first reconnect waits its delay.
  expect(Date.now() - stoppedAt).toBeLessThanOrEqual(2 * 1_000 + CHANNEL_RECONNECT_INITIAL_DELAY_MS + 100)
})

test('on a slow link, a producer that awaits its sends is not refused a message larger than its credit, sent with little credit left', async () => {
  const feed = loop.open<never, string>()
  const page = consume(feed.page)
  await run(100)
  loop.socket.toPage.bytesPerMs = 1_000 // 1 MB/s
  let error: unknown
  let left = Infinity
  void (async () => {
    // 64 KiB messages, so the byte window binds before the message window does.
    while (creditOf(feed.server) > 64 * KIB) await feed.server.send('x'.repeat(64 * KIB))
    left = creditOf(feed.server)
    await feed.server.send('x'.repeat(CREDIT_WINDOW_INITIAL_BYTES))
    for (let n = 0; n < 8; n++) await feed.server.send(message(n))
  })().catch((err: unknown) => (error ??= err))
  await runUntil(() => page.received.length > 0 && page.received.at(-1) === message(7), 10_000, LATENCY_MS)
  expect(left).toBeGreaterThan(0)
  expect(left).toBeLessThanOrEqual(64 * KIB)
  expect(error).toBeUndefined()
  expect(page.received.at(-1)).toBe(message(7))
})

test('on a slow link, a producer that awaits its sends is not refused for the credit another one took with a message larger than it', async () => {
  const feed = loop.open<never, string>()
  const page = consume(feed.page)
  await run(100)
  loop.socket.toPage.bytesPerMs = 1_000 // 1 MB/s
  // 64 KiB messages, 32 of them in the page's 2 MiB window: the byte window binds before the message window does.
  while (creditOf(feed.server) > 64 * KIB) await feed.server.send('x'.repeat(64 * KIB))
  // Both producers' last sends resolved with credit left. One sends a message past it, then the other sends its next.
  const settled = Promise.allSettled([
    feed.server.send('x'.repeat(CREDIT_WINDOW_INITIAL_BYTES)),
    feed.server.send(message(1)),
  ])
  await runUntil(() => page.received.at(-1) === message(1), 10_000, LATENCY_MS)
  expect((await settled).map((result) => result.status)).toEqual(['fulfilled', 'fulfilled'])
  expect(page.received.at(-1)).toBe(message(1))
})

test("a window refresh, a BDP_PING_ACK and a RECONCILE the page sends while an upload fills its slow uplink reach the server within the wire's queue delay, not behind the upload's window, and the upload keeps the uplink full", async () => {
  const upload = loop.open<Uint8Array, Uint8Array>()
  const download = loop.open<Uint8Array, Uint8Array>()
  let uploaded = 0
  upload.server.listenBinary((data) => void (uploaded += data.byteLength))
  download.page.listenBinary(() => {})
  await run(100)
  loop.socket.toServer.bytesPerMs = 1_250 // a 1.25 MB/s uplink
  const chunk = new Uint8Array(64 * 1024)
  let messages = 0
  void (async () => {
    while (!upload.page.isClosed) {
      messages++
      await upload.page.sendBinary(chunk)
    }
  })().catch(() => {})
  void (async () => {
    while (!download.server.isClosed) await download.server.sendBinary(chunk)
  })().catch(() => {})
  await run(3_000)
  const marks = { at: Date.now(), uploaded }
  loop.open() // the page registers a channel: a RECONCILE
  await run(4_000)
  /** How long each frame of `tag` that the page sent after the mark took to reach the server. */
  const waits = (tag: number) => {
    const departures = loop.sent.page.flatMap(([sent], i) => (sent === tag ? [loop.sentAt.page[i]!] : []))
    const arrivals = loop.arrived.flatMap(([arrived, at]) => (arrived === tag ? [at] : []))
    return arrivals.flatMap((at, i) => (departures[i]! >= marks.at ? [at - departures[i]!] : []))
  }
  for (const tag of [TAG.WINDOW, TAG.BDP_PING_ACK, TAG.RECONCILE]) {
    const delays = waits(tag)
    expect(delays.length, `frames of tag ${tag}`).toBeGreaterThan(0)
    expect(Math.max(...delays), `the longest wait of tag ${tag}`).toBeLessThan(600)
  }
  expect(uploaded - marks.uploaded).toBeGreaterThan(0.6 * 4_000 * 1_250)
  // The RECONCILE's round trip didn't have the page send again what it still held back.
  const sentTwice = loop.sent.page.filter(([tag]) => tag === TAG.BINARY).length - messages
  expect(sentTwice).toBeLessThanOrEqual(0)
})

test("a PIECES_ACK and a window refresh the server sends while a download fills its downlink reach the page within a few round trips, not behind the download's window, and both directions keep their link full", async () => {
  const upload = loop.open<Uint8Array, Uint8Array>()
  const download = loop.open<Uint8Array, Uint8Array>()
  const got = { uploaded: 0, downloaded: 0 }
  upload.server.listenBinary((data) => void (got.uploaded += data.byteLength))
  download.page.listenBinary((data) => void (got.downloaded += data.byteLength))
  await run(100)
  const socket = loop.socket
  socket.toServer.bytesPerMs = socket.toPage.bytesPerMs = 1_250 // 1.25 MB/s each way
  const arrived: [number, number][] = []
  const onmessage = socket.onmessage!
  socket.onmessage = (event) => {
    const message = new Uint8Array(event.data)
    for (const frame of isOneFrame(message) ? [message] : messageFrames(message)) arrived.push([frame[0]!, Date.now()])
    onmessage(event)
  }
  const chunk = new Uint8Array(64 * 1024)
  void (async () => {
    while (!upload.page.isClosed) await upload.page.sendBinary(chunk)
  })().catch(() => {})
  void (async () => {
    while (!download.server.isClosed) await download.server.sendBinary(chunk)
  })().catch(() => {})
  await run(3_000)
  const marks = { at: Date.now(), ...got }
  await run(4_000)
  /** How long each frame of `tag` that the server sent after the mark took to reach the page. */
  const waits = (tag: number) => {
    const departures = loop.sent.server.flatMap(([sent], i) => (sent === tag ? [loop.sentAt.server[i]!] : []))
    const arrivals = arrived.flatMap(([frame, at]) => (frame === tag ? [at] : []))
    return arrivals.flatMap((at, i) => (departures[i]! >= marks.at ? [at - departures[i]!] : []))
  }
  for (const tag of [TAG.PIECES_ACK, TAG.WINDOW]) {
    const delays = waits(tag)
    expect(delays.length, `frames of tag ${tag}`).toBeGreaterThan(0)
    expect(Math.max(...delays), `the longest wait of tag ${tag}`).toBeLessThan(600)
  }
  expect(got.uploaded - marks.uploaded).toBeGreaterThan(0.9 * 4_000 * 1_250)
  expect(got.downloaded - marks.downloaded).toBeGreaterThan(0.9 * 4_000 * 1_250)
})

test("after its download paused for 3 s while the page uploads, a server's PIECES_ACKs reach the page within a few round trips once the download goes on, not behind all its window let it send", async () => {
  const upload = loop.open<Uint8Array, Uint8Array>()
  const download = loop.open<Uint8Array, Uint8Array>()
  upload.server.listenBinary(() => {})
  download.page.listenBinary(() => {})
  await run(100)
  const socket = loop.socket
  socket.toServer.bytesPerMs = socket.toPage.bytesPerMs = 1_250 // 1.25 MB/s each way
  const arrived: number[] = []
  const onmessage = socket.onmessage!
  socket.onmessage = (event) => {
    const message = new Uint8Array(event.data)
    for (const frame of isOneFrame(message) ? [message] : messageFrames(message)) {
      if (frame[0] === TAG.PIECES_ACK) arrived.push(Date.now())
    }
    onmessage(event)
  }
  const chunk = new Uint8Array(64 * 1024)
  let resume: (() => void) | null = null
  void (async () => {
    while (!upload.page.isClosed) await upload.page.sendBinary(chunk)
  })().catch(() => {})
  void (async () => {
    while (!download.server.isClosed) {
      if (resume !== null) await new Promise<void>((go) => (resume = go))
      await download.server.sendBinary(chunk)
    }
  })().catch(() => {})
  await run(3_000)
  resume = () => {}
  await run(3_000)
  const mark = Date.now()
  resume()
  resume = null
  await run(2_000)
  const departures = loop.sent.server.flatMap(([tag], i) => (tag === TAG.PIECES_ACK ? [loop.sentAt.server[i]!] : []))
  const waits = arrived.flatMap((at, i) => (departures[i]! >= mark ? [at - departures[i]!] : []))
  expect(waits.length).toBeGreaterThan(0)
  expect(Math.max(...waits)).toBeLessThan(600)
})

test('a channel the page closes right after sends its slow uplink held back delivers them all, in order, before it closes', async () => {
  const { server, page } = loop.open<Uint8Array, never>()
  const got: number[] = []
  server.listenBinary((data) => void got.push(new DataView(data.buffer, data.byteOffset).getUint32(0)))
  const closed = closedWith({ page, server })
  await run(100)
  loop.socket.toServer.bytesPerMs = 600 // 600 KB/s
  const counted = (n: number) => Object.assign(new Uint8Array(64 * 1024), { 0: n & 0xff, 1: n >> 8, 2: 0, 3: 0 })
  for (let n = 0; n < 24; n++) void page.sendBinary(counted(n))
  void page.close()
  await runUntil(() => closed.page !== 'open' && closed.server !== 'open', 30_000, LATENCY_MS)
  expect(got.length).toBe(24)
  expect(got).toEqual([...got.keys()].map((n) => new DataView(counted(n).buffer).getUint32(0)))
  expect(closed).toEqual({ page: undefined, server: undefined })
})

const counted = (n: number) => {
  const message = new Uint8Array(64 * KIB)
  new DataView(message.buffer).setUint32(0, n)
  return message
}
const countOf = (message: Uint8Array) => new DataView(message.buffer, message.byteOffset).getUint32(0)

test.each([
  ['every send was queued before', false],
  ['a producer keeps sending while', true],
])(
  'a page whose slow uplink holds its sends back delivers them all, in order, when %s a channel it opened meanwhile attaches',
  async (_, producer) => {
    const { server, page } = loop.open<Uint8Array, never>()
    const got: number[] = []
    server.listenBinary((data) => void got.push(countOf(data)))
    await run(100)
    loop.socket.toServer.bytesPerMs = 600
    const count = producer ? 60 : 28
    let sent = 0
    if (producer) {
      void (async () => {
        while (sent < count) await page.sendBinary(counted(sent++))
      })().catch(() => {})
    } else {
      for (; sent < count; sent++) void page.sendBinary(counted(sent))
    }
    await run(producer ? 1_000 : 300)
    const late = loop.open<never, number>({ registered: false })
    await run(producer ? 1_500 : 1_000)
    late.register()
    await run(120_000)
    expect(got).toEqual(Array.from({ length: count }, (_, n) => n))
  },
)

/** Counts the entries `page`'s connection reads from the frames it holds back, and from every `Fifo`. */
function countQueueReads(page: unknown) {
  const reads = { count: 0 }
  const connection = (page as { _connection: { sendBuffer: unknown[] } })._connection
  const counting = (queue: unknown[]) =>
    new Proxy(queue, {
      get(target, key, receiver) {
        if (typeof key === 'string' && /^\d+$/.test(key)) reads.count++
        return Reflect.get(target, key, receiver)
      },
    })
  let sendBuffer = counting(connection.sendBuffer)
  Object.defineProperty(connection, 'sendBuffer', {
    get: () => sendBuffer,
    set: (queue: unknown[]) => void (sendBuffer = counting(queue)),
  })
  const fifo = Fifo.prototype as unknown as Record<PropertyKey, (...args: unknown[]) => unknown>
  const methods = { shift: fifo.shift!, peek: fifo.peek!, peekLast: fifo.peekLast! }
  const iterate = fifo[Symbol.iterator]!
  for (const name of ['shift', 'peek', 'peekLast'] as const) {
    fifo[name] = function (this: unknown) {
      reads.count++
      return methods[name].call(this)
    }
  }
  fifo[Symbol.iterator] = function* (this: unknown) {
    for (const item of iterate.call(this) as Iterable<unknown>) {
      reads.count++
      yield item
    }
  }
  const restore = () => Object.assign(fifo, methods, { [Symbol.iterator]: iterate })
  return { reads, restore }
}

test("a page's burst of sends its server acknowledges slower than they come reads each one it holds back a few times, however long the burst", async () => {
  const { server, page } = loop.open<Uint8Array, never>()
  let got = 0
  server.listenBinary(() => void got++)
  await run(100)
  const { reads, restore } = countQueueReads(page)
  try {
    const message = new Uint8Array(2 * KIB)
    for (const n of [10_000, 100_000]) {
      got = 0
      reads.count = 0
      for (let i = 0; i < n; i++) void page.sendBinary(message)
      await runUntil(() => got === n, 600_000, LATENCY_MS)
      expect(got).toBe(n)
      expect(reads.count / n, `reads per message in a burst of ${n}`).toBeLessThan(8)
    }
  } finally {
    restore()
  }
})

/** A download of 64 KiB frames on a 20 MB/s downlink and a 10 ms round trip, with another channel the server sends a
 *  timestamp every 250 ms, while `upload` sends on an uplink of `up` B/ms: over 20 s after 10 s, what the wire carried
 *  down in MB/s and up in B/ms, and how long the other channel's messages took, sorted. */
/** Measures `ms` after 10 s: a whole number of an upload's periods, so the frames on the wire at its ends match. */
async function downloadBeside(up: number, upload: (page: ClientChannel<Uint8Array, never>) => () => void, ms = 20_000) {
  const download = loop.open<never, Uint8Array>()
  const other = loop.open<never, number>()
  const { page, server: uploaded } = loop.open<Uint8Array, never>()
  download.page.listenBinary(() => {})
  const waits: number[] = []
  other.page.listen((sentAt) => void waits.push(Date.now() - sentAt))
  await run(1_000)
  const socket = loop.socket
  socket.toServer.bytesPerMs = up
  socket.toPage.bytesPerMs = 20_000
  const wire = { measuring: false, down: 0, up: 0, payload: 0 }
  uploaded.listenBinary((data) => void (wire.measuring && (wire.payload += data.byteLength)))
  const onmessage = socket.onmessage!
  socket.onmessage = (event) => {
    if (wire.measuring) wire.down += event.data.byteLength
    onmessage(event)
  }
  const receive = loop.receive.bind(loop)
  loop.receive = (from, frame) => {
    if (from === socket && wire.measuring) wire.up += frame.byteLength
    receive(from, frame)
  }
  void (async () => {
    while (!download.server.isClosed) await download.server.sendBinary(new Uint8Array(64 * KIB))
  })().catch(() => {})
  const tick = setInterval(() => void other.server.send(Date.now()).catch(() => {}), 250)
  const stop = upload(page)
  await run(10_000)
  waits.length = 0
  wire.measuring = true
  await run(ms)
  clearInterval(tick)
  stop()
  return {
    down: wire.down / ms / 1_000,
    up: wire.up / ms,
    payload: wire.payload / ms,
    waits: waits.sort((a, b) => a - b),
  }
}

/** What the window's ends can cut off: a 64 KiB frame, in B/ms over the 20 s. */
const CUT_FRAME = (64 * KIB) / 20_000

// Main's growth rule, run in this harness: the wire's MB/s down and B/ms up, and the other channel's median and longest
// wait in ms. Bursts offer `burst / every` at most, which main exceeds by catching up the backlog it built before.
test.each([
  [100, 95, 1_000, { down: 20.0006, up: 97.74, wait: 1_796, longest: 3_313 }],
  [100, 190, 2_000, { down: 20.0006, up: 61.87, wait: 2_621, longest: 3_316 }],
  [1_250, 2_400, 2_000, { down: 19.745, up: 1_229.22, wait: 1_314, longest: 3_313 }],
  [100, 100, 2_000, { down: 20.0006, up: 51.64, wait: 2_637, longest: 3_316 }],
  [1_250, 1_000, 2_000, { down: 20.0006, up: 512.43, wait: 2_726, longest: 3_316 }],
  // Bursts under the page's least send allowance that keep a slow uplink busy.
  [30, 16, 550, { down: 20.0006, up: 29.93, wait: 2_546, longest: 3_207 }],
  [100, 12, 125, { down: 20.0006, up: 98.78, wait: 2_799, longest: 3_227 }],
  [10, 9, 1_000, { down: 20.0006, up: 9.68, wait: 2_476, longest: 3_210 }],
])(
  "on a %i B/ms uplink with bursts of %i KiB every %i ms, a 20 MB/s download carries 0.9 of what it does on main's growth rule, the bursts all they offer, and another channel's messages wait no longer",
  async (up, burst, every, main) => {
    const row = await downloadBeside(
      up,
      (page) => {
        const timer = setInterval(() => void page.sendBinary(new Uint8Array(burst * KIB)).catch(() => {}), every)
        return () => clearInterval(timer)
      },
      Math.round(20_000 / every) * every,
    )
    expect(row.down).toBeGreaterThanOrEqual(0.9 * main.down)
    expect(row.up).toBeGreaterThanOrEqual(Math.min(main.up, (burst * KIB) / every))
    expect(row.waits[row.waits.length >> 1]).toBeLessThanOrEqual(main.wait)
    expect(row.waits.at(-1)).toBeLessThanOrEqual(main.longest)
  },
)

// Main's growth rule, run in this harness, crawls the download beside an upload that awaits each message on a slow uplink:
// the wire's MB/s down and the upload's payload in B/ms. The page's acknowledgements and refreshes of the download take
// up to 1% of the uplink (see `WIRE_ACK_SHARE`).
test.each([
  [10, 16, { down: 2.4154, payload: 9.8304 }],
  [10, 64, { down: 2.7628, payload: 9.8304 }],
  [25, 16, { down: 18.5685, payload: 22.9376 }],
])(
  "on a %i B/ms uplink with an upload that awaits each %i KiB message, a 20 MB/s download carries what it does on main's growth rule or more, the upload within 1% of its payload there, and another channel's messages wait 1.5 s at most",
  async (up, message, main) => {
    const row = await downloadBeside(up, (page) => {
      void (async () => {
        while (!page.isClosed) await page.sendBinary(new Uint8Array(message * KIB))
      })().catch(() => {})
      return () => {}
    })
    expect(row.down).toBeGreaterThanOrEqual(main.down)
    expect(row.payload).toBeGreaterThanOrEqual(0.99 * main.payload)
    expect(row.waits.at(-1)).toBeLessThanOrEqual(1_500)
  },
)

// The server's acknowledgements of the page's upload wait behind its download, so they come late, and in clumps.
test("on a 1250 B/ms uplink with bursts of 2400 KiB every 2000 ms beside a 20 MB/s download, the page's uplink never idles while it holds data back", async () => {
  let idle = 0
  await downloadBeside(1_250, (page) => {
    const timer = setInterval(() => void page.sendBinary(new Uint8Array(2_400 * KIB)).catch(() => {}), 2_000)
    const transport = (
      page as unknown as { _connection: { transport: { ends: { budget: { sender: { heldBytes: number } } } } } }
    )._connection.transport
    const measureFrom = Date.now() + 10_000
    const sampler = setInterval(() => {
      const pipe = loop.socket.toServer as unknown as { queue: unknown[]; lastAt: number }
      if (Date.now() < measureFrom) return
      if (transport.ends.budget.sender.heldBytes > 0 && (pipe.queue.length === 0 || pipe.lastAt < Date.now())) idle++
    }, 1)
    return () => {
      clearInterval(timer)
      clearInterval(sampler)
    }
  })
  expect(idle).toBe(0)
})

// Each end's window covers its refreshes' wait behind what its own socket holds: the page's as its uplink model says, the
// server's as its runtime's buffered amount says.
test('when both ends send 64-byte messages as fast as credit allows, the upload carries what the download does, the server granting the page what the page grants it', async () => {
  const { server, page } = loop.open<string, string>()
  let up = 0
  let down = 0
  server.listen(() => void up++)
  page.listen(() => void down++)
  await run(100)
  for (const end of [server, page]) {
    void (async () => {
      while (!end.isClosed) await end.send('x'.repeat(64))
    })().catch(() => {})
  }
  await run(600)
  const window = (end: unknown) => (end as { _flow: { byteWindow: number } })._flow.byteWindow
  expect(up).toBeGreaterThanOrEqual(0.99 * down)
  expect(window(server)).toBeGreaterThanOrEqual(window(page))
}, 60_000)

// What a page sends about what it receives shares its uplink with what it uploads.
test("on a 10 B/ms uplink the page measured, a 20 MB/s download's acknowledgements and window refreshes take at most 1% of it, and the download keeps its rate", async () => {
  const download = loop.open<never, Uint8Array>()
  const { page, server } = loop.open<Uint8Array, never>()
  download.page.listenBinary(() => {})
  server.listenBinary(() => {})
  await run(1_000)
  const socket = loop.socket
  socket.toServer.bytesPerMs = 10
  socket.toPage.bytesPerMs = 20_000
  await page.sendBinary(new Uint8Array(16 * KIB))
  const wire = { measuring: false, down: 0, control: 0 }
  const onmessage = socket.onmessage!
  socket.onmessage = (event) => {
    if (wire.measuring) wire.down += event.data.byteLength
    onmessage(event)
  }
  const control: number[] = [TAG.PIECES_ACK, TAG.WINDOW, TAG.MSG_WINDOW]
  const receive = loop.receive.bind(loop)
  loop.receive = (from, frame) => {
    if (from === socket && wire.measuring && control.includes(frame[0]!)) wire.control += frame.byteLength
    receive(from, frame)
  }
  void (async () => {
    while (!download.server.isClosed) await download.server.sendBinary(new Uint8Array(64 * KIB))
  })().catch(() => {})
  await run(10_000)
  wire.measuring = true
  await run(20_000)
  expect(wire.control / 20_000).toBeLessThanOrEqual(0.01 * 10)
  expect(wire.down / 20_000 / 1_000).toBeGreaterThanOrEqual(19.9)
})

test.each([
  [100, 16, { down: 19.9055, up: 44.7, wait: 2_583, longest: 3_316 }],
  [100, 64, { down: 19.0699, up: 56.16, wait: 2_210, longest: 3_316 }],
  [1_250, 64, { down: 20.0006, up: 134.78, wait: 2_885, longest: 3_315 }],
  [1_250, 16, { down: 20.0006, up: 79.1, wait: 2_886, longest: 3_316 }],
])(
  "on a %i B/ms uplink with an upload that awaits each %i KiB message, a 20 MB/s download and the upload carry what they do on main's growth rule, and another channel's messages wait no longer",
  async (up, message, main) => {
    const row = await downloadBeside(up, (page) => {
      void (async () => {
        while (!page.isClosed) await page.sendBinary(new Uint8Array(message * KIB))
      })().catch(() => {})
      return () => {}
    })
    expect(row.down).toBeGreaterThanOrEqual(main.down - CUT_FRAME / 1_000)
    expect(row.up).toBeGreaterThanOrEqual(main.up - CUT_FRAME)
    expect(row.waits[row.waits.length >> 1]).toBeLessThanOrEqual(main.wait)
    expect(row.waits.at(-1)).toBeLessThanOrEqual(main.longest)
  },
)

/** A hidden tab: the timers the page's send budget sets wait `every` ms at least. */
function throttlePacing(every: number): () => void {
  const budget = SendBudget.prototype as unknown as { pace: () => void }
  const pace = budget.pace
  budget.pace = function (this: unknown) {
    const setTimer = globalThis.setTimeout
    globalThis.setTimeout = ((fn: () => void, ms = 0) => setTimer(fn, Math.max(every, ms))) as typeof setTimeout
    try {
      pace.call(this)
    } finally {
      globalThis.setTimeout = setTimer
    }
  }
  return () => void (budget.pace = pace)
}

// The burst rows a hidden tab lost, the verifier's and the perturbed ones, with the page's timers at 1 Hz and never firing.
test.each(
  [1_000, 1e9].flatMap((throttle) =>
    (
      [
        [100, 95, 1_000, { down: 20.0006, up: 97.74, wait: 1_796, longest: 3_313 }],
        [1_250, 2_400, 2_000, { down: 19.745, up: 1_229.22, wait: 1_314, longest: 3_313 }],
        [1_250, 1_000, 2_000, { down: 20.0006, up: 512.43, wait: 2_726, longest: 3_316 }],
        [100, 85, 1_000, { down: 20.0006, up: 87.5, wait: 2_548, longest: 3_226 }],
        [1_250, 900, 2_000, { down: 20.0006, up: 461.23, wait: 2_815, longest: 3_316 }],
      ] as const
    ).map(([up, burst, every, main]) => [up, burst, every, throttle, main] as const),
  ),
)(
  "in a hidden tab, on a %i B/ms uplink with bursts of %i KiB every %i ms, the bursts carry all they offer beside a 20 MB/s download, with the page's timers waiting %i ms",
  async (up, burst, every, throttle, main) => {
    const restore = throttlePacing(throttle)
    try {
      const row = await downloadBeside(up, (page) => {
        const timer = setInterval(() => void page.sendBinary(new Uint8Array(burst * KIB)).catch(() => {}), every)
        return () => clearInterval(timer)
      })
      expect(row.down).toBeGreaterThanOrEqual(0.9 * main.down)
      expect(row.up).toBeGreaterThanOrEqual(Math.min(main.up, (burst * KIB) / every) - CUT_FRAME)
      expect(row.waits[row.waits.length >> 1]).toBeLessThanOrEqual(main.wait)
      expect(row.waits.at(-1)).toBeLessThanOrEqual(main.longest)
    } finally {
      restore()
    }
  },
)

// The awaited rows a hidden tab lost, the verifier's and the perturbed ones, with the page's timers never firing.
test.each([
  [100, 64, { down: 19.0699, up: 56.16, wait: 2_210, longest: 3_316 }],
  [100, 48, { down: 16.7234, up: 71.64, wait: 1_619, longest: 3_297 }],
  [1_250, 24, { down: 20.0006, up: 97.52, wait: 2_886, longest: 3_316 }],
])(
  "in a hidden tab, on a %i B/ms uplink with an upload that awaits each %i KiB message, a 20 MB/s download and the upload carry what they do on main's growth rule, with the page's timers never firing",
  async (up, message, main) => {
    const restore = throttlePacing(1e9)
    try {
      const row = await downloadBeside(up, (page) => {
        void (async () => {
          while (!page.isClosed) await page.sendBinary(new Uint8Array(message * KIB))
        })().catch(() => {})
        return () => {}
      })
      expect(row.down).toBeGreaterThanOrEqual(main.down - CUT_FRAME / 1_000)
      expect(row.up).toBeGreaterThanOrEqual(main.up - CUT_FRAME)
      expect(row.waits[row.waits.length >> 1]).toBeLessThanOrEqual(main.wait)
      expect(row.waits.at(-1)).toBeLessThanOrEqual(main.longest)
    } finally {
      restore()
    }
  },
)
