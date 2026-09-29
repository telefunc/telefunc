// Channel flow control end to end: a real ClientChannel and ClientConnection talk to a real ChannelMux and
// ServerChannel over in-process WebSockets with a fixed latency, on fake timers, so each run is deterministic.

import { afterEach, beforeEach, expect, test, vi } from 'vitest'

import { ClientBroadcast, ClientChannel } from './client/channel.js'
import { ServerChannel } from './server/channel.js'
import { Broadcast, ServerBroadcast } from './server/server-broadcast.js'
import { ChannelMux, type ServerTransport } from './server/mux.js'
import {
  CHANNEL_BUFFER_LIMIT_BYTES,
  CHANNEL_TRANSPORT,
  CREDIT_MSG_WINDOW_INITIAL,
  CREDIT_WINDOW_INITIAL_BYTES,
  CREDIT_WINDOW_MAX_BYTES,
} from './constants.js'
import { ChannelOverflowError } from './channel-errors.js'
import { TAG } from './shared-ws.js'
import { NetworkError } from '../shared/NetworkError.js'
import { config as serverConfig } from '../node/server/serverConfig.js'

const LATENCY_MS = 5

/** What a socket read hands over at once; the receiver's microtasks run between two reads. */
const READ_BYTES = 64 * 1024

/** One direction of a wire: delivers in order, `LATENCY_MS` after the send. */
class Pipe {
  private queue: { frame: Uint8Array<ArrayBuffer>; at: number }[] = []
  private timer: ReturnType<typeof setTimeout> | null = null
  private held = false
  /** What the pipe carries, in bytes: the sender's `bufferedAmount`. */
  bytes = 0
  /** A slow link carries a frame only once the one before it has gone through. */
  bytesPerMs = Infinity
  private lastAt = 0
  constructor(private readonly deliver: (frame: Uint8Array<ArrayBuffer>) => void) {}
  push(frame: Uint8Array<ArrayBuffer>): void {
    const at = Math.max(Date.now() + LATENCY_MS, this.lastAt + frame.byteLength / this.bytesPerMs)
    this.lastAt = at
    this.queue.push({ frame, at })
    this.bytes += frame.byteLength
    this.schedule()
  }
  /** What the network still carries stops arriving, until `release`. */
  hold(): void {
    this.held = true
  }
  release(): void {
    this.held = false
    this.schedule()
  }
  /** The wire is cut: what it still carries is lost. */
  clear(): void {
    this.queue.length = 0
    this.bytes = 0
  }
  get length(): number {
    return this.queue.length
  }
  private schedule(): void {
    if (this.timer || this.held || this.queue.length === 0) return
    this.timer = setTimeout(
      () => {
        this.timer = null
        let read = 0
        while (!this.held && read < READ_BYTES && this.queue.length > 0 && this.queue[0]!.at <= Date.now()) {
          const { frame } = this.queue.shift()!
          read += frame.byteLength
          this.bytes -= frame.byteLength
          this.deliver(frame)
        }
        this.schedule()
      },
      Math.max(0, this.queue[0]!.at - Date.now()),
    )
  }
}

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
  readonly toServer = new Pipe((frame) => loop.receive(this, frame))
  readonly toPage = new Pipe((frame) => this.onmessage?.({ data: frame.buffer }))
  constructor(_url: string) {
    loop.sockets.push(this)
    setTimeout(() => {
      if (this.readyState !== 0) return
      loop.mux.onConnectionOpen(this, loop.transport)
      this.readyState = 1
      this.onopen?.()
    }, LATENCY_MS)
  }
  send(frame: Uint8Array): void {
    loop.sends('page', frame)
    this.toServer.push(frame.slice())
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
  readonly mux = new ChannelMux()
  readonly sockets: LoopbackSocket[] = []
  readonly errors: unknown[] = []
  private readonly sessions = new Map<LoopbackSocket, string>()
  readonly transport: ServerTransport<LoopbackSocket> = {
    getSessionId: (socket) => this.sessions.get(socket),
    setSessionId: (socket, id) => void this.sessions.set(socket, id),
    getConnId: () => null,
    sendNow: (socket, frame) => {
      this.sends('server', frame)
      socket.toPage.push(frame.slice())
    },
    bufferedAmount: (socket) => socket.toPage.bytes,
    terminateConnection: (socket) => socket.cut(),
  }
  private readonly connectionKey = crypto.randomUUID()
  private readonly pages: ClientChannel[] = []
  private watch: { from: 'page' | 'server'; tag: number; then: () => void } | null = null
  /** Every frame each side sent, as `[tag, channel ix]`. */
  readonly sent = { page: [] as [number, number][], server: [] as [number, number][] }
  /** Runs `then` once, as `from` sends its next frame of `tag`. */
  onSend(from: 'page' | 'server', tag: number, then: () => void): void {
    this.watch = { from, tag, then }
  }
  sends(from: 'page' | 'server', frame: Uint8Array): void {
    this.sent[from].push([frame[0]!, frame[1]! | (frame[2]! << 8)])
    const watch = this.watch
    if (!watch || watch.from !== from || watch.tag !== frame[0]) return
    this.watch = null
    watch.then()
  }
  receive(socket: LoopbackSocket, frame: Uint8Array<ArrayBuffer>): void {
    this.mux.onConnectionRawMessage(socket, frame).catch((err) => this.errors.push(err))
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

const run = (ms: number) => vi.advanceTimersByTimeAsync(ms)

/** Runs until `done`, in steps of `LATENCY_MS`. A page's window doubles about every 50 ms of a saturated stream,
 *  so runs stay short. */
async function runUntil(done: () => boolean, maxMs: number): Promise<void> {
  for (let elapsed = 0; elapsed < maxMs && !done(); elapsed += LATENCY_MS) await run(LATENCY_MS)
}

/** `for (;;) await channel.send(next)`, the channel page's backpressure loop, counting what it has sent. */
function produce<T = number>(
  channel: { send(data: T): Promise<void>; isClosed: boolean },
  { message = (n) => n as T, onSent }: { message?: (n: number) => T; onSent?: (count: number) => void } = {},
) {
  const sent = { count: 0 }
  void (async () => {
    while (!channel.isClosed) {
      const sending = channel.send(message(sent.count++))
      onSent?.(sent.count)
      await sending
    }
  })().catch(() => {})
  return sent
}

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

const flowOf = (channel: unknown) => (channel as { _flow: { msgWindow: number; byteWindow: number } })._flow

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
  while (error === undefined && sends < 2_000) {
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
  serverConfig.channel = {}
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

test('a stream keeps flowing past its grown window after the page opens another channel, and after a reconnect', async () => {
  const clock = loop.open<never, number>()
  const page = consume(clock.page)
  produce(clock.server)
  await runUntil(() => flowOf(clock.page).msgWindow > 4 * CREDIT_MSG_WINDOW_INITIAL, 1_000)
  let window = flowOf(clock.page).msgWindow
  expect(window).toBeGreaterThan(4 * CREDIT_MSG_WINDOW_INITIAL)

  loop.open<never, number>() // the server attaches the stream's channel again, with the new one
  let before = page.received.length
  await run(100)
  expect(page.received.length - before).toBeGreaterThan(2 * window)

  window = flowOf(clock.page).msgWindow
  loop.socket.cut()
  before = page.received.length
  await runUntil(() => page.received.length - before > 2 * window, 1_000)
  expect(page.received.length - before).toBeGreaterThan(2 * window)
  expect(page.received).toEqual([...page.received.keys()])
})

test('a reattach on the live wire sends no flow-control frames, and one on a new wire repairs with them', async () => {
  const clock = loop.open<never, number>()
  await run(100)
  const flowControl = (from: 'page' | 'server') =>
    loop.sent[from].filter(
      ([tag, ix]) => ix === 0 && (tag === TAG.WINDOW || tag === TAG.MSG_WINDOW || tag === TAG.SENT),
    ).length
  const before = { page: flowControl('page'), server: flowControl('server') }

  loop.open<never, number>() // its RECONCILE attaches the clock again, on the same wire
  await run(100)
  expect(clock.page.isClosed).toBe(false)
  expect({ page: flowControl('page'), server: flowControl('server') }).toEqual(before)

  loop.socket.cut()
  await run(1_000)
  expect(loop.sockets).toHaveLength(2)
  expect({ page: flowControl('page'), server: flowControl('server') }).toEqual({
    page: before.page + 3,
    server: before.server + 3,
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

test("a stream whose reconnect needs more than the server's replay buffer holds ends with NetworkError on both ends, the page having got it in order up to there", async () => {
  serverConfig.channel.serverReplayBuffer = 512
  const clock = loop.open<never, number>()
  const page = consume(clock.page)
  const closed = closedWith(clock)
  produce(clock.server)
  await runUntil(() => flowOf(clock.page).msgWindow > 4 * CREDIT_MSG_WINDOW_INITIAL, 1_000)
  const window = flowOf(clock.page).msgWindow
  expect(window).toBeGreaterThan(4 * CREDIT_MSG_WINDOW_INITIAL)

  // From its next refresh on, the page stops hearing from the server, which sends the window that refresh grants. Then
  // the wire drops, with all of it on the way.
  loop.onSend('page', TAG.MSG_WINDOW, () => loop.socket.toPage.hold())
  await run(50)
  expect(loop.socket.toPage.length).toBeGreaterThanOrEqual(window)
  loop.socket.cut()
  await runUntil(() => closed.page !== 'open', 1_000)
  expect(closed).toEqual({ page: lostOn('server'), server: lostOn('server') })
  expect(page.received).toEqual([...page.received.keys()])
})

test("an upload whose reconnect needs more than the page's replay buffer holds ends with NetworkError on both ends, the server having got it in order up to there", async () => {
  serverConfig.channel.clientReplayBuffer = 512
  const upload = loop.open<number, never>()
  const server = consume(upload.server)
  const closed = closedWith(upload)
  produce(upload.page)
  await runUntil(() => flowOf(upload.server).msgWindow > 4 * CREDIT_MSG_WINDOW_INITIAL, 1_000)
  const window = flowOf(upload.server).msgWindow
  expect(window).toBeGreaterThan(4 * CREDIT_MSG_WINDOW_INITIAL)

  // From its next refresh on, the server stops hearing from the page, which sends the window that refresh grants.
  // Then the wire drops, with all of it on the way.
  loop.onSend('server', TAG.MSG_WINDOW, () => loop.socket.toServer.hold())
  await run(50)
  expect(loop.socket.toServer.length).toBeGreaterThanOrEqual(window)
  loop.socket.cut()
  await runUntil(() => closed.server !== 'open', 1_000)
  expect(closed).toEqual({ page: lostOn('client'), server: lostOn('client') })
  expect(server.received).toEqual([...server.received.keys()])
})

test("a page that opens another channel while more of a stream is on the wire than the server's replay buffer holds keeps the stream whole", async () => {
  serverConfig.channel.serverReplayBuffer = 512
  const clock = loop.open<never, number>()
  const page = consume(clock.page)
  const closed = closedWith(clock)
  produce(clock.server)
  await run(50)
  loop.socket.toPage.hold()
  await run(20)
  expect(loop.socket.toPage.bytes).toBeGreaterThan(512)
  loop.open<never, number>() // its RECONCILE attaches the stream's channel again, on the wire that carries the stream
  await run(50)
  loop.socket.toPage.release()
  const before = page.received.length
  await run(100)
  expect(page.received.length).toBeGreaterThan(before)
  expect(closed).toEqual({ page: 'open', server: 'open' })
  expect(page.received).toEqual([...page.received.keys()])
})

test("a broadcast whose reconnect needs more publishes than the server's replay buffer holds closes on both ends with NetworkError", async () => {
  serverConfig.channel.serverReplayBuffer = 1_024
  const key = `room:${crypto.randomUUID()}`
  const room = loop.openBroadcast<string>(key)
  const closed = closedWith(room)
  const seen: string[] = []
  room.page.subscribe((text) => void seen.push(text))
  await run(100)
  Broadcast.publish(key, 'before')
  await run(50)
  loop.socket.toPage.hold()
  for (let n = 0; n < 8; n++) Broadcast.publish(key, String(n).padEnd(256))
  await run(50)
  loop.socket.cut()
  await runUntil(() => closed.page !== 'open', 1_000)
  expect(closed).toEqual({ page: lostOn('server'), server: lostOn('server') })
  expect(seen).toEqual(['before'])
})

test("a page that stops reading holds a channel's sends nobody awaits to its window and bufferLimit: the next rejects with ChannelOverflowError, and the channel stays open", async () => {
  const feed = loop.open<never, string>()
  const page = consume(feed.page)
  await run(100)
  loop.socket.toPage.hold()
  const { error, sends } = await sendUntilRejected((n) => feed.server.send(message(n)))
  expect(error).toBeInstanceOf(ChannelOverflowError)
  expect(loop.socket.toPage.bytes).toBeLessThanOrEqual(
    CREDIT_WINDOW_INITIAL_BYTES + CHANNEL_BUFFER_LIMIT_BYTES + 32 * KIB,
  )

  // Once the page reads again it gets every message but the refused one, and what is sent after it caught up.
  loop.socket.toPage.release()
  await runUntil(() => page.received.length === sends - 1, 1_000)
  await feed.server.send(message(sends))
  await runUntil(() => page.received.length === sends, 1_000)
  expect(page.received).toEqual([...Array.from({ length: sends - 1 }, (_, n) => message(n)), message(sends)])
})

test("a page that stops reading holds a channel's ack requests nobody awaits to bufferLimit: the next rejects with ChannelOverflowError", async () => {
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
  expect(loop.socket.toPage.bytes).toBeLessThanOrEqual(CHANNEL_BUFFER_LIMIT_BYTES + 32 * KIB)

  loop.socket.toPage.release()
  await run(100)
  expect(await Promise.all(acks.slice(0, sends - 1))).toEqual(Array(sends - 1).fill('ok'))
})

test('on a slow link, an awaited stream keeps flowing while a send nobody awaits on the same wire is refused past bufferLimit', async () => {
  const stream = loop.open<never, string>()
  const flood = loop.open<never, string>()
  const streamed = consume(stream.page)
  consume(flood.page)
  await run(100)
  loop.socket.toPage.bytesPerMs = 4_000 // 4 MB/s
  let streamError: unknown
  void (async () => {
    while (!stream.server.isClosed) await stream.server.send('x'.repeat(64 * KIB))
  })().catch((err: unknown) => (streamError = err))
  let floodError: unknown
  let held = 0
  const flooding = setInterval(() => {
    held = Math.max(held, loop.socket.toPage.bytes)
    if (floodError === undefined) flood.server.send(message(0)).catch((err: unknown) => (floodError = err))
  }, 1)
  await run(1_000)
  expect(floodError).toBeInstanceOf(ChannelOverflowError)
  const midway = streamed.received.length
  await run(1_000)
  clearInterval(flooding)

  expect(streamError).toBeUndefined()
  expect(midway).toBeGreaterThan(10)
  expect(streamed.received.length - midway).toBeGreaterThan(40)
  // The stream alone keeps more than bufferLimit on the wire, and nothing past the windows and bufferLimit is added.
  expect(held).toBeGreaterThan(CHANNEL_BUFFER_LIMIT_BYTES)
  expect(held).toBeLessThanOrEqual(
    flowOf(stream.page).byteWindow + flowOf(flood.page).byteWindow + CHANNEL_BUFFER_LIMIT_BYTES + 128 * KIB,
  )
})

test('a send nobody awaits goes out past the window of a page whose listener lags, while the wire holds less than bufferLimit', async () => {
  const feed = loop.open<never, string>()
  const page = consume(feed.page, { slow: true })
  await run(100)
  let error: unknown
  let behind = 0
  for (let n = 0; n < 400; n++) {
    feed.server.send(message(n)).catch((err: unknown) => (error = err))
    behind = Math.max(behind, (n + 1 - page.consumed) * (16 * KIB) - flowOf(feed.page).byteWindow)
    await run(5)
  }
  expect(error).toBeUndefined()
  // The page's limit is at most what it consumed and its window, so the server sent this far past it.
  expect(behind).toBeGreaterThan(CHANNEL_BUFFER_LIMIT_BYTES)
  await runUntil(() => page.received.length === 400, 5_000)
  expect(page.received).toEqual(Array.from({ length: 400 }, (_, n) => message(n)))
})

test('a page that stops reading a broadcast leaves it with ChannelOverflowError on both ends once the server holds the largest window and bufferLimit of publishes for it', async () => {
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
  expect(loop.socket.toPage.bytes).toBeGreaterThan(CREDIT_WINDOW_MAX_BYTES)
  expect(loop.socket.toPage.bytes).toBeLessThanOrEqual(CREDIT_WINDOW_MAX_BYTES + CHANNEL_BUFFER_LIMIT_BYTES + 512 * KIB)

  // Once the page reads again it gets every publish sent before the one that found it behind, then the close.
  loop.socket.toPage.release()
  await runUntil(() => errors.page !== undefined, 1_000)
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
  await runUntil(() => seen.length === 300, 2_000)
  expect(closed).toBe(null)
  expect(held).toBeGreaterThan(CHANNEL_BUFFER_LIMIT_BYTES)
  expect(seen).toEqual(Array.from({ length: 300 }, (_, n) => message(n)))
})

test("a page's consumption of what a broadcast publishes moves the server's limit for it", async () => {
  const key = `room:${crypto.randomUUID()}`
  const room = loop.openBroadcast<string>(key)
  const seen: string[] = []
  room.page.subscribe((text) => void seen.push(text))
  await run(100)
  expect(creditOf(room.server)).toBe(CREDIT_WINDOW_MAX_BYTES)
  // A limit goes out once a quarter of the window is consumed.
  const publications = CREDIT_WINDOW_MAX_BYTES / 4 / KIB / KIB + 1
  for (let n = 0; n < publications; n++) {
    Broadcast.publish(key, 'x'.repeat(KIB * KIB))
    await run(1)
  }
  await runUntil(() => seen.length === publications, 1_000)
  await run(100)
  // Sent 17 MiB, of which the last limit leaves one not yet counted consumed.
  expect(creditOf(room.server)).toBeGreaterThan(CREDIT_WINDOW_MAX_BYTES - 2 * KIB * KIB)
})

test('on a slow link, producers that await their sends are handed the credit one at a time, so none is refused however far past bufferLimit a frame from each would add up', async () => {
  const feed = loop.open<never, string>()
  const page = consume(feed.page)
  await run(100)
  loop.socket.toPage.bytesPerMs = 4_000 // 4 MB/s
  let size = KIB
  let error: unknown
  for (let p = 0; p < 8; p++)
    void (async () => {
      while (!feed.server.isClosed) await feed.server.send('x'.repeat(size))
    })().catch((err: unknown) => (error ??= err))
  // They start small: producers that start together each have a frame out as the credit first runs out, and those count.
  await run(100)
  // Every producer waits on credit by now. From here on each frame is 128 KiB: 1 MiB from the eight, twice bufferLimit.
  size = 128 * KIB
  let held = 0
  const watch = setInterval(() => (held = Math.max(held, loop.socket.toPage.bytes)), 1)
  const before = page.received.length
  await run(2_000)
  clearInterval(watch)
  expect(error).toBeUndefined()
  expect(held).toBeGreaterThan(CHANNEL_BUFFER_LIMIT_BYTES)
  expect(page.received.length - before).toBeGreaterThan(40)
})

test('on a slow link, a producer that awaits its sends is not refused a message larger than bufferLimit, sent with little credit left', async () => {
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
    await feed.server.send('x'.repeat(CHANNEL_BUFFER_LIMIT_BYTES + 64 * KIB))
    for (let n = 0; n < 8; n++) await feed.server.send(message(n))
  })().catch((err: unknown) => (error ??= err))
  await runUntil(() => page.received.length > 0 && page.received.at(-1) === message(7), 10_000)
  expect(left).toBeGreaterThan(0)
  expect(left).toBeLessThanOrEqual(64 * KIB)
  expect(error).toBeUndefined()
  expect(page.received.at(-1)).toBe(message(7))
})

test('on a slow link, a producer that awaits its sends is not refused for the credit another one took with a message larger than bufferLimit', async () => {
  const feed = loop.open<never, string>()
  const page = consume(feed.page)
  await run(100)
  loop.socket.toPage.bytesPerMs = 1_000 // 1 MB/s
  // 64 KiB messages, 32 of them in the page's 2 MiB window: the byte window binds before the message window does.
  while (creditOf(feed.server) > 64 * KIB) await feed.server.send('x'.repeat(64 * KIB))
  // Both producers' last sends resolved with credit left. One sends a message past it, then the other sends its next.
  const settled = Promise.allSettled([
    feed.server.send('x'.repeat(CHANNEL_BUFFER_LIMIT_BYTES + 64 * KIB)),
    feed.server.send(message(1)),
  ])
  await runUntil(() => page.received.at(-1) === message(1), 10_000)
  expect((await settled).map((result) => result.status)).toEqual(['fulfilled', 'fulfilled'])
  expect(page.received.at(-1)).toBe(message(1))
})
