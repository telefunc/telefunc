// Channel flow control end to end: a real ClientChannel and ClientConnection talk to a real ChannelMux and
// ServerChannel over in-process WebSockets with a fixed latency, on fake timers, so each run is deterministic.

import { afterEach, beforeEach, expect, test, vi } from 'vitest'

import { ClientChannel } from './client/channel.js'
import { ServerChannel } from './server/channel.js'
import { ChannelMux, type ServerTransport } from './server/mux.js'
import { CHANNEL_TRANSPORT, CREDIT_MSG_WINDOW_INITIAL } from './constants.js'
import { TAG } from './shared-ws.js'
import { config as serverConfig } from '../node/server/serverConfig.js'

const LATENCY_MS = 5

/** What a socket read hands over at once; the receiver's microtasks run between two reads. */
const READ_BYTES = 64 * 1024

/** One direction of a wire: delivers in order, `LATENCY_MS` after the send. */
class Pipe {
  private queue: { frame: Uint8Array<ArrayBuffer>; at: number }[] = []
  private timer: ReturnType<typeof setTimeout> | null = null
  private held = false
  constructor(private readonly deliver: (frame: Uint8Array<ArrayBuffer>) => void) {}
  push(frame: Uint8Array<ArrayBuffer>): void {
    this.queue.push({ frame, at: Date.now() + LATENCY_MS })
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
    terminateConnection: (socket) => socket.cut(),
  }
  private readonly connectionKey = crypto.randomUUID()
  private readonly pages: ClientChannel[] = []
  private watch: { from: 'page' | 'server'; tag: number; then: () => void } | null = null
  /** Runs `then` once, as `from` sends its next frame of `tag`. */
  onSend(from: 'page' | 'server', tag: number, then: () => void): void {
    this.watch = { from, tag, then }
  }
  sends(from: 'page' | 'server', frame: Uint8Array): void {
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

test('a stream the page consumes while its first reconcile waits on another channel keeps flowing past 100 messages', async () => {
  const clock = loop.open<never, number>()
  const page = consume(clock.page)
  produce(clock.server)
  // The page opens a second channel whose server side isn't registered yet, so the server holds RECONCILED for it.
  const late = loop.open<never, number>({ registered: false })
  await run(1_000)
  expect(page.received.length).toBeGreaterThanOrEqual(CREDIT_MSG_WINDOW_INITIAL)
  late.register()
  await run(200)
  expect(page.received.length).toBeGreaterThan(10 * CREDIT_MSG_WINDOW_INITIAL)
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

test('a stream keeps flowing after a reconnect that loses more than the replay buffer holds', async () => {
  serverConfig.channel.serverReplayBuffer = 512
  const clock = loop.open<never, number>()
  const page = consume(clock.page)
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
  const before = page.received.length
  await runUntil(() => page.received.length - before > 2 * window, 1_000)
  expect(page.received.length - before).toBeGreaterThan(2 * window)
  // Values were lost, and the ones that arrived are in order.
  expect(page.received.some((n, i) => i > 0 && n > page.received[i - 1]! + 1)).toBe(true)
  expect(page.received.every((n, i) => i === 0 || n > page.received[i - 1]!)).toBe(true)
})

test("an upload keeps flowing after a reconnect that loses more than the page's replay buffer holds", async () => {
  serverConfig.channel.clientReplayBuffer = 512
  const upload = loop.open<number, never>()
  const server = consume(upload.server)
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
  const before = server.received.length
  await runUntil(() => server.received.length - before > 2 * window, 1_000)
  expect(server.received.length - before).toBeGreaterThan(2 * window)
  expect(server.received.some((n, i) => i > 0 && n > server.received[i - 1]! + 1)).toBe(true)
  expect(server.received.every((n, i) => i === 0 || n > server.received[i - 1]!)).toBe(true)
})
