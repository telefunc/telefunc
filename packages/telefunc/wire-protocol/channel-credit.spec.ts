// Channel flow control end to end: a real ClientChannel and ClientConnection talk to a real ChannelMux and
// ServerChannel over in-process WebSockets with a fixed latency, on fake timers, so each run is deterministic.

import { afterEach, beforeEach, expect, test, vi } from 'vitest'

import { ClientBroadcast, ClientChannel } from './client/channel.js'
import { ServerChannel } from './server/channel.js'
import { Broadcast, ServerBroadcast } from './server/server-broadcast.js'
import { ChannelMux, type ServerTransport } from './server/mux.js'
import {
  CHANNEL_PING_INTERVAL_MS,
  CHANNEL_TRANSPORT,
  CREDIT_MSG_WINDOW_INITIAL,
  CREDIT_WINDOW_INITIAL_BYTES,
  CREDIT_WINDOW_MAX_BYTES,
  CHANNEL_RECONNECT_INITIAL_DELAY_MS,
  RECONCILE_TIMEOUT_MS,
} from './constants.js'
import { ChannelOverflowError } from './channel-errors.js'
import { TAG } from './shared-ws.js'
import { NetworkError } from '../shared/NetworkError.js'
import { config as serverConfig } from '../node/server/serverConfig.js'
import { Room } from './room/server/statics.js'
import type { ServerLocalParticipant, ServerRoom } from './room/server/room.js'
import { RoomParticipantStubChannel } from './room/server/stub.js'
import { ClientRoom, ClientStandaloneParticipant } from './room/client.js'

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
    bufferedAmount: (socket) => (this.reportsBacklog ? socket.toPage.bytes : undefined),
    terminateConnection: (socket) => socket.cut(),
  }
  /** Whether the server's runtime tells what waits on a socket, as workerd's doesn't. */
  reportsBacklog = true
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
  /** A room's stub the server has registered, and the page's view of the room through it, on the same connection. */
  async openRoom() {
    const { room, stub, openPage } = await this.roomStub()
    return { room, stub, ...openPage() }
  }
  /** A room's stub the server has registered, and `openPage`, which opens the page's view of the room through it. */
  async roomStub() {
    const room = (await Room.create(`room:${crypto.randomUUID()}`)) as ServerRoom
    const { stub, metadata } = room._openStub({ grants: { selfSuppressed: new Set(), hidden: new Set() } })
    this.mux.registerChannel(stub)
    const openPage = () => {
      const page = new ClientBroadcast({
        channelId: stub.id,
        key: room.id,
        transports: [CHANNEL_TRANSPORT.WS],
        telefuncUrl: 'http://loopback.test/_telefunc',
        connectionKey: this.connectionKey,
      })
      this.pages.push(page as ClientChannel)
      return { page, view: new ClientRoom(page, metadata) }
    }
    return { room, stub, openPage }
  }
  /** A server participant handed to the page: its stub, which the server has registered, and the page's handle. */
  async openParticipant() {
    const { room, participant, stub, openPage } = await this.participantStub()
    return { room, participant, stub, member: openPage() }
  }
  /** A server participant's stub the server has registered, and `openPage`, which opens the page's handle on it. */
  async participantStub() {
    const room = (await Room.create(`room:${crypto.randomUUID()}`)) as ServerRoom
    const participant = (await room.join()) as ServerLocalParticipant
    const stub = new RoomParticipantStubChannel(participant)
    this.mux.registerChannel(stub)
    // As the response handing it to the page carries it.
    const { id, meta, selfDelivery, identity } = participant
    const openPage = () => {
      const page = new ClientChannel({
        channelId: stub.id,
        transports: [CHANNEL_TRANSPORT.WS],
        telefuncUrl: 'http://loopback.test/_telefunc',
        connectionKey: this.connectionKey,
      })
      this.pages.push(page as ClientChannel)
      return new ClientStandaloneParticipant(page, { channelId: stub.id, id, meta, selfDelivery, identity })
    }
    return { room, participant, stub, openPage }
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

test('an upload keeps flowing past its grown window after a reconnect, the server getting it all in order', async () => {
  const upload = loop.open<number, never>()
  const server = consume(upload.server)
  produce(upload.page)
  await runUntil(() => flowOf(upload.server).msgWindow > 4 * CREDIT_MSG_WINDOW_INITIAL, 1_000)
  const window = flowOf(upload.server).msgWindow
  expect(window).toBeGreaterThan(4 * CREDIT_MSG_WINDOW_INITIAL)
  loop.socket.cut() // what it carries is lost, and replays
  const before = server.received.length
  await runUntil(() => server.received.length - before > 2 * window, 1_000)
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
  await runUntil(() => got.length > before + (2 * window) / (256 * KIB), 2_000)
  expect(got.length).toBeGreaterThan(before + (2 * window) / (256 * KIB))
  expect(got).toEqual([...got.keys()])
  expect(closed).toEqual({ page: 'open', server: 'open' })
})

test("sends nobody awaits, past the page's window and more than the server's replay holds, end the channel with NetworkError on both ends at a reconnect, the page having got them in order up to there", async () => {
  serverConfig.channel.serverReplayBuffer = 4 * KIB
  const feed = loop.open<never, string>()
  const page = consume(feed.page)
  const closed = closedWith(feed)
  await run(100)
  loop.socket.toPage.hold()
  const text = (n: number) => String(n).padEnd(256)
  for (let n = 0; n < 64; n++) feed.server.send(text(n)).catch(() => {})
  await run(50)
  loop.socket.cut()
  await runUntil(() => closed.page !== 'open', 1_000)
  expect(closed).toEqual({ page: lostOn('server'), server: lostOn('server') })
  expect(page.received).toEqual(page.received.map((_, n) => text(n)))
})

test("sends nobody awaits, past the server's window and more than the page's replay holds, end the channel with NetworkError on both ends at a reconnect, the server having got them in order up to there", async () => {
  serverConfig.channel.clientReplayBuffer = 4 * KIB
  const upload = loop.open<string, never>()
  const server = consume(upload.server)
  const closed = closedWith(upload)
  await run(100)
  loop.socket.toServer.hold()
  const text = (n: number) => String(n).padEnd(256)
  for (let n = 0; n < 64; n++) upload.page.send(text(n)).catch(() => {})
  await run(50)
  loop.socket.cut()
  await runUntil(() => closed.server !== 'open', 1_000)
  expect(closed).toEqual({ page: lostOn('client'), server: lostOn('client') })
  expect(server.received).toEqual(server.received.map((_, n) => text(n)))
})

test("a page that opens another channel while more of what the server sent is on the wire than the server's replay buffer holds gets it all", async () => {
  serverConfig.channel.serverReplayBuffer = 512
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
  const publication = (n: number) => String(n).padEnd(256)
  for (let n = 0; n < 8; n++) Broadcast.publish(key, publication(n))
  await run(50)
  loop.socket.cut()
  await runUntil(() => closed.page !== 'open', 1_000)
  expect(closed.page).toBeInstanceOf(ChannelOverflowError)
  expect(closed.server).toBeInstanceOf(ChannelOverflowError)
  expect(seen).toEqual(['before', ...seen.slice(1).map((_, n) => publication(n))])
})

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
  await runUntil(() => closed.page !== 'open', 2_000)
  expect(closed.server).toBeInstanceOf(ChannelOverflowError)
  expect(closed.page).toBeInstanceOf(ChannelOverflowError)
  expect(seen).toEqual(['before'])
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
  await runUntil(() => seen.length === 3, 2_000)
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
  await runUntil(() => page.received.length === 60, 5_000)
  expect(error).toBeUndefined()
  expect(page.received).toEqual(Array.from({ length: 60 }, (_, n) => burst(n)))
})

test("a page that stops reading holds a channel's sends nobody awaits to its window and the largest window a page grants: the next rejects with ChannelOverflowError, and the channel stays open", async () => {
  const feed = loop.open<never, string>()
  const page = consume(feed.page)
  await run(100)
  loop.socket.toPage.hold()
  const { error, sends } = await sendUntilRejected((n) => feed.server.send(message(n)))
  expect(error).toBeInstanceOf(ChannelOverflowError)
  expect(loop.socket.toPage.bytes).toBeGreaterThan(CREDIT_WINDOW_INITIAL_BYTES + CREDIT_WINDOW_MAX_BYTES)
  // One message past them, and each one's header.
  expect(loop.socket.toPage.bytes).toBeLessThanOrEqual(CREDIT_WINDOW_INITIAL_BYTES + CREDIT_WINDOW_MAX_BYTES + 64 * KIB)

  // Once the page reads again it gets every message but the refused one, and what is sent after it caught up.
  loop.socket.toPage.release()
  await runUntil(() => page.received.length === sends - 1, 5_000)
  await feed.server.send(message(sends))
  await runUntil(() => page.received.length === sends, 1_000)
  expect(page.received).toEqual([...Array.from({ length: sends - 1 }, (_, n) => message(n)), message(sends)])
})

test("a page that stops reading holds a channel's ack requests nobody awaits to the largest window a page grants: the next rejects with ChannelOverflowError", async () => {
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
  expect(loop.socket.toPage.bytes).toBeGreaterThan(CREDIT_WINDOW_MAX_BYTES)
  expect(loop.socket.toPage.bytes).toBeLessThanOrEqual(CREDIT_WINDOW_MAX_BYTES + 64 * KIB)

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
  await runUntil(() => delays.length === sent, 10_000)
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
  await runUntil(() => page.received.length === 320, 5_000)
  expect(page.received).toEqual(Array.from({ length: 320 }, (_, n) => big(n)))
})

test('a page that stops reading a broadcast leaves it with ChannelOverflowError on both ends once the server holds its room and a quarter more of publishes for it', async () => {
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
  expect(loop.socket.toPage.bytes).toBeGreaterThan(CREDIT_WINDOW_MAX_BYTES * 1.25)
  expect(loop.socket.toPage.bytes).toBeLessThanOrEqual(CREDIT_WINDOW_MAX_BYTES * 1.25 + 512 * KIB)

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
  expect(held).toBeGreaterThan(CREDIT_WINDOW_INITIAL_BYTES)
  expect(seen).toEqual(Array.from({ length: 300 }, (_, n) => message(n)))
})

test("a page's consumption of what a broadcast publishes moves the server's limit for it", async () => {
  const key = `room:${crypto.randomUUID()}`
  const room = loop.openBroadcast<string>(key)
  const seen: string[] = []
  room.page.subscribe((text) => void seen.push(text))
  await run(100)
  expect(creditOf(room.server)).toBe(CREDIT_WINDOW_MAX_BYTES)
  const publications = CREDIT_WINDOW_MAX_BYTES / 4 / KIB / KIB + 1
  for (let n = 0; n < publications; n++) {
    Broadcast.publish(key, 'x'.repeat(KIB * KIB))
    await run(1)
  }
  await runUntil(() => seen.length === publications, 1_000)
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
  await runUntil(() => seen.length === 12, 1_000)
  await run(50)
  const replay = (room.server as unknown as { _replayBuffer: { byteLength: number } })._replayBuffer
  expect(replay.byteLength).toBeLessThan(CREDIT_WINDOW_INITIAL_BYTES / 4)
})

test("a Room member's page gone quiet has its replay, and its room's, let go of what the other end got within a heartbeat", async () => {
  const { room, stub, page, view } = await loop.openRoom()
  const seen: unknown[] = []
  view.subscribe((data) => void seen.push(data))
  const joining = view.join()
  await runUntil(() => view.count === 1, 1_000)
  const me = await joining
  const speaker = await room.join()
  await run(100)
  // Less than a quarter window each way, which no limit acknowledges.
  for (let n = 0; n < 8; n++) {
    void speaker.publish(String(n).padEnd(16 * KIB))
    void me.publish(String(n).padEnd(16 * KIB))
  }
  await runUntil(() => seen.length === 16, 1_000)
  const connection = (page as unknown as { _connection: { replayBuffers: Map<number, { byteLength: number }> } })
    ._connection
  const pageReplay = [...connection.replayBuffers.values()][0]!
  const held = () => [stub._replayBuffer!.byteLength, pageReplay.byteLength]
  expect(held().every((bytes) => bytes > 0)).toBe(true)
  await run(CHANNEL_PING_INTERVAL_MS)
  expect(held()).toEqual([0, 0])
})

test.each([2 ** 31, 2 ** 32])(
  "a Room member's page whose channel passes seq %d gets the room's messages in order, and its publishes answered",
  async (boundary) => {
    const { room, stub, page, view } = await loop.openRoom()
    const seen: string[] = []
    view.subscribe((data) => void seen.push(data as string))
    const joining = view.join()
    await runUntil(() => view.count === 1, 1_000)
    const me = await joining
    const speaker = await room.join()
    await run(100)
    // As if `boundary - 4` more frames had gone each way and arrived.
    const connection = (page as unknown as { _connection: any })._connection
    const ix = connection.channelIndex.get(page)
    for (const replay of [stub._replayBuffer, connection.replayBuffers.get(ix)]) {
      replay._seq += boundary - 4
      replay.pushedSeq += boundary - 4
    }
    stub._lastClientSeq += boundary - 4
    ;(stub as unknown as { _pageLastSeq: number })._pageLastSeq += boundary - 4
    connection.lastSeqByChannel.set(ix, connection.lastSeqByChannel.get(ix) + boundary - 4)
    const receipts: Promise<unknown>[] = []
    for (let n = 0; n < 8; n++) {
      if (n % 2 === 0) receipts.push(me.publish(String(n)))
      else void speaker.publish(String(n))
      await run(10)
    }
    await runUntil(() => seen.length === 8, 1_000)
    expect(seen).toEqual(Array.from({ length: 8 }, (_, n) => String(n)))
    expect(await Promise.all(receipts)).toHaveLength(4)
    expect(stub._replayBuffer!.seq).toBeGreaterThan(boundary)
    expect(connection.replayBuffers.get(ix).seq).toBeGreaterThan(boundary)
    expect(stub.isClosed).toBe(false)
  },
)

test('on a slow link, producers that await their sends are handed the credit one at a time, and none is refused', async () => {
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
  produce(feed.server, { message: () => 'x'.repeat(16 * KIB) })
  await run(7_000)
  loop.socket.toPage.hold()
  const stoppedAt = Date.now()
  await runUntil(() => loop.sockets.length === 2, 30_000)
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

// A Room member's page grants the largest window from the start: all of it can be on the wire ahead of its pong.
test("on a link slower than the room's messages per ping deadline, a Room member's page keeps its wire while they keep arriving ahead of its pong", async () => {
  const { room, view } = await loop.openRoom()
  const seen: string[] = []
  view.subscribe((data) => void seen.push(data as string))
  const speaker = await room.join()
  await run(100)
  loop.socket.toPage.bytesPerMs = 100
  const publication = (n: number) => String(n).padEnd(64 * KIB)
  for (let n = 0; n < 32; n++) void speaker.publish(publication(n))
  await run(30_000)
  expect(loop.sockets).toHaveLength(1)
  expect(seen).toEqual(Array.from({ length: 32 }, (_, n) => publication(n)))
})

test("on an uplink slower than a window per ping deadline, the server doesn't cut the wire of a Room member's page while its publishes keep arriving ahead of its ping", async () => {
  const { view } = await loop.openRoom()
  const joining = view.join()
  await runUntil(() => view.count === 1, 1_000)
  const me = await joining
  await run(100)
  loop.socket.toServer.bytesPerMs = 100
  const terminateConnection = vi.spyOn(loop.transport, 'terminateConnection')
  const receipts: unknown[] = []
  for (let n = 0; n < 32; n++) void me.publish(String(n).padEnd(64 * KIB)).then((receipt) => receipts.push(receipt))
  await run(30_000)
  expect(terminateConnection).not.toHaveBeenCalled()
  expect(receipts).toHaveLength(32)
})

// 100 KB/s, with a ping every second: the page's ping waits 20 s behind its 2 MiB window, and the server's refresh for a
// quarter of it comes every 5 s, both past the page's 2 s pong deadline.
test("on an uplink slower than a quarter window per pong deadline, an upload's page keeps its wire while the upload keeps arriving ahead of its ping", async () => {
  serverConfig.channel.pingInterval = 1_000
  const feed = loop.open<string, never>()
  const server = consume(feed.server)
  await run(100)
  loop.socket.toServer.bytesPerMs = 100
  produce(feed.page, { message: () => 'x'.repeat(64 * KIB) })
  await run(30_000)
  expect(loop.sockets).toHaveLength(1)
  expect(server.received.length).toBeGreaterThan(40)
})

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

// 100 KB/s: the RECONCILE naming the new channel waits 20 s behind the 2 MiB window of the upload, twice the time a page
// waits for its RECONCILED on a wire that delivers nothing. The server holds the new channel that long.
test('a channel the page opens while its upload fills a slow uplink attaches on the same wire, however long its RECONCILE waits behind the upload', async () => {
  serverConfig.channel.connectTtl = 60_000
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
  serverConfig.channel.connectTtl = 60_000
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
  await runUntil(() => loop.sockets.length === 2, 30_000)
  expect(loop.sockets).toHaveLength(2)
  expect(Date.now() - stoppedAt).toBeLessThanOrEqual(RECONCILE_TIMEOUT_MS + CHANNEL_RECONNECT_INITIAL_DELAY_MS + 100)
})

test('a page whose uplink stops with its upload queued on it takes the wire for dead within a pong deadline', async () => {
  serverConfig.channel.pingInterval = 1_000
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
  await runUntil(() => loop.sockets.length === 2, 10_000)
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
  await runUntil(() => page.received.length > 0 && page.received.at(-1) === message(7), 10_000)
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
  await runUntil(() => page.received.at(-1) === message(1), 10_000)
  expect((await settled).map((result) => result.status)).toEqual(['fulfilled', 'fulfilled'])
  expect(page.received.at(-1)).toBe(message(1))
})

test.each([
  ['tells what waits on a socket', true],
  ["can't tell what waits on a socket, as a Durable Object's", false],
])(
  "on a runtime that %s, a Room member's page that stops reading is let go with ChannelOverflowError once the server holds its room and a quarter more of the room's messages for it: its view closes and its member leaves",
  async (_, reportsBacklog) => {
    loop.reportsBacklog = reportsBacklog
    const { room, stub, view } = await loop.openRoom()
    let serverEnd: Error | undefined
    stub.onClose((err) => void (serverEnd = err))
    const seen: string[] = []
    view.subscribe((data) => void seen.push(data as string))
    let viewClosed = false
    view.onClose(() => void (viewClosed = true))
    const joining = view.join()
    await runUntil(() => view.count === 1, 1_000)
    const me = await joining
    const left: unknown[] = []
    me.onLeave((cause) => void left.push(cause))
    const leftOnServer: unknown[] = []
    room.onLeave((member, cause) => void leftOnServer.push([member.id, cause]))
    const speaker = await room.join()
    await run(100)
    loop.socket.toPage.hold()
    const publication = (n: number) => String(n).padEnd(256 * KIB)
    let published = 0
    while (!stub.isClosed && published < 1_000) {
      void speaker.publish(publication(published++))
      await run(0)
    }
    expect(serverEnd).toBeInstanceOf(ChannelOverflowError)
    expect(loop.socket.toPage.bytes).toBeGreaterThan(CREDIT_WINDOW_MAX_BYTES * 1.25)
    expect(loop.socket.toPage.bytes).toBeLessThanOrEqual(CREDIT_WINDOW_MAX_BYTES * 1.25 + 512 * KIB)
    await runUntil(() => leftOnServer.length > 0, 1_000)
    expect(leftOnServer).toEqual([[me.id, { type: 'disconnected' }]])
    const inFlight = me.publish('in flight').catch((err: unknown) => err)

    // Once the page reads again it gets, in order, every message the server sent before the one that found it behind,
    // then the end.
    loop.socket.toPage.release()
    await runUntil(() => viewClosed, 1_000)
    expect(left).toEqual([{ type: 'disconnected' }])
    expect(await inFlight).toBeInstanceOf(ChannelOverflowError)
    expect(seen.length).toBeGreaterThan((CREDIT_WINDOW_MAX_BYTES * 1.25) / (256 * KIB) - 1)
    expect(seen).toEqual(Array.from({ length: seen.length }, (_, n) => publication(n)))
  },
)

test.each([
  ['tells what waits on a socket', true],
  ["can't tell what waits on a socket, as a Durable Object's", false],
])(
  "on a slow link and a runtime that %s, a Room member's page that keeps up with the room stays in through a burst as it attaches, and through more in all than the server holds for a page behind, while an awaited stream fills the wire",
  async (_, reportsBacklog) => {
    loop.reportsBacklog = reportsBacklog
    const stream = loop.open<never, string>()
    consume(stream.page)
    const { room, stub, view } = await loop.openRoom()
    let serverEnd: unknown = 'open'
    stub.onClose((err) => void (serverEnd = err))
    const seen: string[] = []
    view.subscribe((data) => void seen.push(data as string))
    const speaker = await room.join()
    const publication = (n: number) => String(n).padEnd(256 * KIB)
    // 20 MiB as the server attaches the page, before the window the page grants reaches it, then 26 MB/s, 100 MiB in all.
    const burst = 80
    const count = 400
    loop.onSend('server', TAG.RECONCILED, () => {
      loop.socket.toPage.bytesPerMs = 40_000 // 40 MB/s
      for (let n = 0; n < burst; n++) void speaker.publish(publication(n))
    })
    let held = 0
    for (let elapsed = 0; elapsed < 1_000 && seen.length < burst; elapsed++) {
      held = Math.max(held, loop.sockets[0]?.toPage.bytes ?? 0)
      await run(1)
    }
    void (async () => {
      while (!stream.server.isClosed) await stream.server.send('x'.repeat(64 * KIB))
    })().catch(() => {})
    for (let n = burst; n < count; n++) {
      void speaker.publish(publication(n))
      await run(10)
    }
    await runUntil(() => seen.length === count, 2_000)
    expect(serverEnd).toBe('open')
    expect(held).toBeGreaterThan(CREDIT_WINDOW_INITIAL_BYTES + CREDIT_WINDOW_MAX_BYTES / 4)
    expect(count * 256 * KIB).toBeGreaterThan(CREDIT_WINDOW_MAX_BYTES * 1.25)
    expect(seen).toEqual(Array.from({ length: count }, (_, n) => publication(n)))
  },
)

test("a Room member's page whose wire drops with more of the room's messages in flight than a wire holds besides its channels' allowances gets them all as it reconnects, and keeps its new wire", async () => {
  const { room, stub, view } = await loop.openRoom()
  let serverEnd: unknown = 'open'
  stub.onClose((err) => void (serverEnd = err))
  const seen: string[] = []
  view.subscribe((data) => void seen.push(data as string))
  const speaker = await room.join()
  await run(100)
  loop.socket.toPage.hold()
  const publication = (n: number) => String(n).padEnd(256 * KIB)
  // 72 MiB: past the 64 MiB a wire holds besides what its channels' flow control allows, within the 80 MiB a page may be
  // behind.
  const count = 288
  for (let n = 0; n < count; n++) {
    void speaker.publish(publication(n))
    await run(0)
  }
  expect(serverEnd).toBe('open')
  loop.socket.cut()
  await runUntil(() => seen.length === count, 5_000)
  expect(loop.sockets).toHaveLength(2)
  expect(seen).toEqual(Array.from({ length: count }, (_, n) => publication(n)))
  expect(serverEnd).toBe('open')
})

test("a Room member's page cut off further behind than the server's replay buffer holds is let go with ChannelOverflowError, having got the room's messages before it in order: its view closes and its member leaves", async () => {
  serverConfig.channel.serverReplayBuffer = 1_024
  const { room, stub, view } = await loop.openRoom()
  let serverEnd: unknown = 'open'
  stub.onClose((err) => void (serverEnd = err))
  const seen: string[] = []
  view.subscribe((data) => void seen.push(data as string))
  let viewClosed = false
  view.onClose(() => void (viewClosed = true))
  const joining = view.join()
  await runUntil(() => view.count === 1, 1_000)
  const me = await joining
  const left: unknown[] = []
  me.onLeave((cause) => void left.push(cause))
  const speaker = await room.join()
  await run(100)
  void speaker.publish('before')
  await run(50)
  loop.socket.toPage.hold()
  const publication = (n: number) => String(n).padEnd(256)
  for (let n = 0; n < 8; n++) void speaker.publish(publication(n))
  await run(50)
  loop.socket.cut()
  await runUntil(() => viewClosed, 1_000)
  expect(serverEnd).toBeInstanceOf(ChannelOverflowError)
  expect(left).toEqual([{ type: 'disconnected' }])
  expect(seen).toEqual(['before', ...seen.slice(1).map((_, n) => publication(n))])
})

test("a Room member's page offline while more of the room's messages were sent to it than config.channel.bufferLimit holds is let go with ChannelOverflowError at its reconnect: its view closes and its member leaves", async () => {
  const { room, stub, view } = await loop.openRoom()
  let serverEnd: unknown = 'open'
  stub.onClose((err) => void (serverEnd = err))
  const seen: string[] = []
  view.subscribe((data) => void seen.push(data as string))
  let viewClosed = false
  view.onClose(() => void (viewClosed = true))
  const joining = view.join()
  await runUntil(() => view.count === 1, 1_000)
  const me = await joining
  const left: unknown[] = []
  me.onLeave((cause) => void left.push(cause))
  const speaker = await room.join()
  await run(100)
  void speaker.publish('before')
  await run(50)
  loop.socket.cut()
  // Offline, the server holds 512 KiB of text for the page: these are 1 MiB.
  for (let n = 0; n < 8; n++) void speaker.publish(String(n).padEnd(128 * KIB))
  await runUntil(() => viewClosed, 2_000)
  expect(serverEnd).toBeInstanceOf(ChannelOverflowError)
  expect(left).toEqual([{ type: 'disconnected' }])
  expect(seen).toEqual(['before'])
})

test("a Room closed while its page's wire dies reaches the page once it reconnects: its view closes and its member leaves with 'closed'", async () => {
  const { room, stub, view } = await loop.openRoom()
  let serverEnd: unknown = 'open'
  stub.onClose((err) => void (serverEnd = err))
  let viewClosed = false
  view.onClose(() => void (viewClosed = true))
  const joining = view.join()
  await runUntil(() => view.count === 1, 1_000)
  const me = await joining
  const left: unknown[] = []
  me.onLeave((cause) => void left.push(cause))
  await run(100)
  loop.socket.toPage.hold()
  void Room.close(room.id)
  await run(50)
  loop.socket.cut()
  await runUntil(() => viewClosed && serverEnd !== 'open', 5_000)
  expect(left).toEqual([{ type: 'closed' }])
  expect(serverEnd).toBe(undefined)
})

// A stub's close waits the reconnect window, 70 s with the defaults, for a page that attaches only after it.
test("a Room closed before its page attaches, with a connectTtl longer than its stub's close waits, reaches the page as it attaches, after what the room sent it meanwhile: the members it holds leave with 'closed'", async () => {
  serverConfig.channel = { connectTtl: 120_000 }
  const { room, stub, openPage } = await loop.roomStub()
  let serverEnd: unknown = 'open'
  stub.onClose((err) => void (serverEnd = err))
  const other = await room.join()
  const later = await room.join()
  await Room.close(room.id)
  await run(71_000)
  expect((serverEnd as Error).message).toBe('Channel close timed out')

  const { view } = openPage()
  const joined: string[] = []
  view.onJoin((member) => void joined.push(member.id))
  // Returned with the room, as a telefunction may.
  const held = view._reviveRemote({ id: other.id, meta: {}, joinedAt: Date.now(), metaSeq: 0, identity: null })
  const left: unknown[] = []
  held.onLeave((cause) => void left.push(cause))
  await run(1_000)
  expect(joined).toEqual([later.id])
  expect(left).toEqual([{ type: 'closed' }])
})

test.each([
  ['tells what waits on a socket', true],
  ["can't tell what waits on a socket, as a Durable Object's", false],
])(
  'on a runtime that %s, the page of a participant handed to it that stops reading is let go with ChannelOverflowError once the server holds the largest window a page grants past its credit, instead of losing the messages refused past it',
  async (_, reportsBacklog) => {
    loop.reportsBacklog = reportsBacklog
    const { room, participant, stub, member } = await loop.openParticipant()
    let serverEnd: Error | undefined
    stub.onClose((err) => void (serverEnd = err))
    const leftOnServer: unknown[] = []
    participant.onLeave((cause) => void leftOnServer.push(cause))
    const inbox: string[] = []
    member.listen((data) => void inbox.push(data as string))
    const left: unknown[] = []
    member.onLeave((cause) => void left.push(cause))
    const sender = await room.join()
    await run(100)
    loop.socket.toPage.hold()
    const message = (n: number) => String(n).padEnd(256 * KIB)
    let sent = 0
    while (!stub.isClosed && sent < 1_000) {
      void sender.send(participant.id, message(sent++))
      await run(0)
    }
    expect(serverEnd).toBeInstanceOf(ChannelOverflowError)
    expect(loop.socket.toPage.bytes).toBeGreaterThan(CREDIT_WINDOW_MAX_BYTES)
    expect(loop.socket.toPage.bytes).toBeLessThanOrEqual(
      CREDIT_WINDOW_MAX_BYTES + CREDIT_WINDOW_INITIAL_BYTES + 512 * KIB,
    )
    await runUntil(() => leftOnServer.length > 0, 1_000)
    expect(leftOnServer).toEqual([{ type: 'disconnected' }])

    // Once the page reads again it gets, in order, every message the server sent before the one that found it behind,
    // then the end.
    loop.socket.toPage.release()
    await runUntil(() => left.length > 0, 1_000)
    expect(left).toEqual([{ type: 'disconnected' }])
    expect(inbox.length).toBeGreaterThan(CREDIT_WINDOW_MAX_BYTES / (256 * KIB))
    expect(inbox).toEqual(Array.from({ length: inbox.length }, (_, n) => message(n)))
  },
)

test("the page of a participant handed to it that the room removes before the page attaches, with a connectTtl longer than its stub's close waits, gets as it attaches the messages sent to the participant and its meta, then its leave", async () => {
  serverConfig.channel = { connectTtl: 120_000 }
  const { room, participant, stub, openPage } = await loop.participantStub()
  let serverEnd: unknown = 'open'
  stub.onClose((err) => void (serverEnd = err))
  const sender = await room.join()
  await sender.send(participant.id, 'hello')
  await participant.setMeta({ mood: 'away' })
  await Room.removeParticipant(room.id, { id: participant.id, reason: 'kicked' })
  await run(71_000)
  expect((serverEnd as Error).message).toBe('Channel close timed out')

  const member = openPage()
  const inbox: unknown[] = []
  member.listen((data) => void inbox.push(data))
  const left: unknown[] = []
  member.onLeave((cause) => void left.push(cause))
  await run(1_000)
  expect(inbox).toEqual(['hello'])
  expect(member.meta).toEqual({ mood: 'away' })
  expect(left).toEqual([{ type: 'removed', reason: 'kicked' }])
})

test('the page of a participant handed to it, offline while more messages were sent to its participant than config.channel.bufferLimit holds, is let go with ChannelOverflowError at its reconnect, instead of missing them', async () => {
  const { room, participant, stub, member } = await loop.openParticipant()
  let serverEnd: unknown = 'open'
  stub.onClose((err) => void (serverEnd = err))
  const leftOnServer: unknown[] = []
  participant.onLeave((cause) => void leftOnServer.push(cause))
  const inbox: string[] = []
  member.listen((data) => void inbox.push(data as string))
  const left: unknown[] = []
  member.onLeave((cause) => void left.push(cause))
  const sender = await room.join()
  await run(100)
  void sender.send(participant.id, 'before')
  await run(50)
  loop.socket.cut()
  // Offline, the server holds 512 KiB of text for the page: these are 1 MiB.
  for (let n = 0; n < 8; n++) void sender.send(participant.id, String(n).padEnd(128 * KIB))
  await runUntil(() => left.length > 0, 2_000)
  expect(serverEnd).toBeInstanceOf(ChannelOverflowError)
  expect(leftOnServer).toEqual([{ type: 'disconnected' }])
  expect(left).toEqual([{ type: 'disconnected' }])
  expect(inbox).toEqual(['before'])
})

test('the page of a participant handed to it, offline while less was sent to its participant than config.channel.bufferLimit holds, gets it all at its reconnect', async () => {
  const { room, participant, stub, member } = await loop.openParticipant()
  let serverEnd: unknown = 'open'
  stub.onClose((err) => void (serverEnd = err))
  const inbox: string[] = []
  member.listen((data) => void inbox.push(data as string))
  const sender = await room.join()
  await run(100)
  loop.socket.cut()
  const message = (n: number) => String(n).padEnd(128 * KIB)
  for (let n = 0; n < 3; n++) void sender.send(participant.id, message(n))
  await runUntil(() => inbox.length === 3, 2_000)
  expect(inbox).toEqual(Array.from({ length: 3 }, (_, n) => message(n)))
  expect(serverEnd).toBe('open')
})

test.each([
  ['tells what waits on a socket', true],
  ["can't tell what waits on a socket, as a Durable Object's", false],
])(
  'on a slow link and a runtime that %s, the page of a participant handed to it that keeps up stays in through more in all than the server holds for a page behind, while an awaited stream fills the wire',
  async (_, reportsBacklog) => {
    loop.reportsBacklog = reportsBacklog
    const stream = loop.open<never, string>()
    consume(stream.page)
    const { room, participant, stub, member } = await loop.openParticipant()
    let serverEnd: unknown = 'open'
    stub.onClose((err) => void (serverEnd = err))
    const inbox: string[] = []
    member.listen((data) => void inbox.push(data as string))
    const sender = await room.join()
    await run(100)
    loop.socket.toPage.bytesPerMs = 40_000 // 40 MB/s
    void (async () => {
      while (!stream.server.isClosed) await stream.server.send('x'.repeat(64 * KIB))
    })().catch(() => {})
    const message = (n: number) => String(n).padEnd(256 * KIB)
    // 26 MB/s of messages, 100 MiB in all.
    const count = 400
    for (let n = 0; n < count; n++) {
      void sender.send(participant.id, message(n))
      await run(10)
    }
    await runUntil(() => inbox.length === count, 2_000)
    expect(serverEnd).toBe('open')
    expect(count * 256 * KIB).toBeGreaterThan(CREDIT_WINDOW_MAX_BYTES + CREDIT_WINDOW_INITIAL_BYTES)
    expect(inbox).toEqual(Array.from({ length: count }, (_, n) => message(n)))
  },
)
