// A channel across a wire that dies without a word: a stream that awaits its sends resumes after the reconnect without
// loss, the page's close request, close acknowledgement or abort, and the answers that complete a close, replay as its
// data does, and a reconnect that needs what a replay dropped ends the channel on both ends. Past seqs 2^31 and 2^32, a
// channel does all it does before them. These drive the real ClientChannel against the real server over each wire.

import { afterEach, beforeEach, describe, expect, test, vi } from 'vitest'
import type { Peer } from 'crossws'
import '../../node/server/async_hooks.js'

import { ClientChannel } from './channel.js'
import { pumpClientProducerToChannel } from './request/pumpToChannel.js'
import { config } from '../../client/clientConfig.js'
import { ServerChannel } from '../server/channel.js'
import { getChannelMux } from '../server/mux.js'
import { pumpProducerToChannel } from '../server/response/ChannelResponseBody.js'
import { getTelefuncSseChannelHooks } from '../server/sse.js'
import { getTelefuncChannelHooks } from '../server/ws.js'
import { ChannelStreamSource } from '../ChannelStreamSource.js'
import type { ReplayBuffer } from '../replay-buffer.js'
import { TAG, decode, type ReconcilePayload, type SeqReader } from '../shared-ws.js'
import { isAbort } from '../../shared/Abort.js'
import { NetworkError } from '../../shared/NetworkError.js'
import { decodeU32 } from '../frame.js'
import { base64urlToUint8Array } from '../base64url.js'
import { SSE_FLUSH_THROTTLE_MS } from '../constants.js'
import { config as serverConfig } from '../../node/server/serverConfig.js'

/** A receiver with nothing of any channel: each seq reads as its low 32 bits. */
const wireSeqs: SeqReader = { received: () => 0, sent: () => 0 }

type Wire = 'sse' | 'sse-batch' | 'ws'
const WIRES: Wire[] = ['ws', 'sse', 'sse-batch']

// Before the first reconcile, so every RECONCILED offers a page the WebSocket.
getTelefuncChannelHooks()

const advance = (ms: number) => vi.advanceTimersByTimeAsync(ms)

beforeEach(() => {
  vi.useFakeTimers()
  // Each end notices a dead wire 2 s after the last ping.
  serverConfig.channel = { pingInterval: 1_000 }
  ;(getChannelMux() as unknown as { resolvedOptions: unknown }).resolvedOptions = null
})

afterEach(() => {
  vi.useRealTimers()
  vi.unstubAllGlobals()
  delete config.fetch
  serverConfig.channel = {}
  ;(getChannelMux() as unknown as { resolvedOptions: unknown }).resolvedOptions = null
})

/** One way of a link that carries `rate` bytes a second: what is written to it arrives in order, as fast as that. */
class Pipe {
  private busyUntil = 0
  constructor(private readonly rate: number) {}
  /** When `bytes` written now have arrived. */
  arrival(bytes: number): number {
    this.busyUntil = Math.max(Date.now(), this.busyUntil) + (bytes / this.rate) * 1_000
    return this.busyUntil
  }
  /** Bytes written to it that haven't arrived, as a socket's `bufferedAmount` says them. */
  get bufferedAmount(): number {
    return (Math.max(0, this.busyUntil - Date.now()) * this.rate) / 1_000
  }
}

const until = (at: number) => new Promise<void>((resolve) => setTimeout(resolve, at - Date.now()))

/** One wire of the page's connection: a WebSocket, or an SSE downstream and the POSTs that go with it. */
type Link = {
  /** Nothing either end writes to it arrives, and nothing tells either end. */
  dead: boolean
  /** Its batch POSTs stay in flight. */
  holdingPosts: boolean
  /** How it carries what the server writes to the page, and what the page writes to the server: at once if null. */
  down: Pipe | null
  up: Pipe | null
}

/** The network between the page and the server. */
class Net {
  readonly links: Link[] = []
  /** The page's attempts to open a wire fail. */
  refusing = false
  /** Bytes a second each way of the wires the page opens from now on: 0 carries at once. */
  readonly rate = { down: 0, up: 0 }
  private readonly byConnId = new Map<string, Link>()
  private readonly losing = new Set<number>()
  private readonly onPageGets = new Map<number, (frame: Uint8Array) => void>()
  private readonly onPageSends = new Map<number, (frame: Uint8Array) => void>()
  private readonly onServerSends = new Map<number, (frame: Uint8Array) => void>()
  open(connId?: string): Link {
    const link = {
      dead: false,
      holdingPosts: false,
      down: this.rate.down ? new Pipe(this.rate.down) : null,
      up: this.rate.up ? new Pipe(this.rate.up) : null,
    }
    this.links.push(link)
    if (connId) this.byConnId.set(connId, link)
    return link
  }
  linkOf(connId: string): Link | undefined {
    return this.byConnId.get(connId)
  }
  /** Every wire goes silent at once, as in a network blip. What the page opens afterwards gets through. */
  die(): void {
    for (const link of this.links) link.dead = true
  }
  /** Every wire goes silent, and the page's attempts to open another fail, until `heal`. */
  cut(): void {
    this.refusing = true
    this.die()
  }
  heal(): void {
    this.refusing = false
  }
  /** Runs `then` once, as the page receives its next frame of `tag`: that frame still arrives. */
  whenPageGets(tag: number, then: () => void): void {
    this.onPageGets.set(tag, then)
  }
  /** Runs `then` once, as the page writes its next frame of `tag` to the wire: that frame is lost if it kills it. */
  whenPageSends(tag: number, then: (frame: Uint8Array) => void): void {
    this.onPageSends.set(tag, then)
  }
  /** Runs `then` once, as the server writes its next frame of `tag` to the page: that frame is lost if it kills it. */
  whenServerSends(tag: number, then: () => void): void {
    this.onServerSends.set(tag, then)
  }
  pageGets(frame: Uint8Array): void {
    this.fire(this.onPageGets, frame)
  }
  pageSends(frame: Uint8Array): void {
    this.fire(this.onPageSends, frame)
  }
  serverSends(frame: Uint8Array): void {
    this.fire(this.onServerSends, frame)
  }
  /** The page's next frame of `tag` is lost, and its wire lives on: a loss no replay repairs. */
  losePageFrame(tag: number): void {
    this.losing.add(tag)
  }
  loses(frame: Uint8Array): boolean {
    return this.losing.delete(frame[0]!)
  }
  private fire(hooks: Map<number, (frame: Uint8Array) => void>, frame: Uint8Array): void {
    const then = hooks.get(frame[0]!)
    if (!then) return
    hooks.delete(frame[0]!)
    then(frame)
  }
}

/** A page on `wire`, and the server it talks to. `upgrade`: an SSE page that may move to a WebSocket. */
function page(wire: Wire, { upgrade = false } = {}) {
  const net = new Net()
  if (wire === 'ws' || upgrade) vi.stubGlobal('WebSocket', webSocketTo(net))
  if (wire !== 'ws') config.fetch = sseServer(net, wire === 'sse-batch')
  const telefuncUrl = `http://${crypto.randomUUID()}.test/_telefunc`
  const connectionKey = crypto.randomUUID()
  return {
    net,
    /** A channel the page opens. */
    channel<ClientToServer = unknown, ServerToClient = unknown>(channelId: string) {
      return new ClientChannel<ClientToServer, ServerToClient>({
        channelId,
        ack: true,
        transports: wire === 'ws' ? ['ws'] : upgrade ? ['sse', 'ws'] : ['sse'],
        telefuncUrl,
        connectionKey,
      })
    },
  }
}

/** What the server registers when a call's request arrives. */
function register<ClientToServer = unknown, ServerToClient = unknown>() {
  const channel = new ServerChannel<ClientToServer, ServerToClient>({ ack: true })
  getChannelMux().registerChannel(channel)
  return channel
}

/** What a promise settled with: `'pending'` until it does. */
function settled<T>(promise: Promise<T>) {
  const result: { value: unknown } = { value: 'pending' }
  promise.then(
    (value) => void (result.value = value),
    (err: unknown) => void (result.value = err),
  )
  return result
}

/** Where a channel's `onClose` leaves what it got: `'open'` until it fires. */
function closedWith(channel: { onClose(callback: (err?: Error) => void): void }) {
  const closed: { err: unknown } = { err: 'open' }
  channel.onClose((err) => void (closed.err = err))
  return closed
}

const flowOf = (channel: unknown) => (channel as { _flow: { byteWindow: number } })._flow

/** Expects the error a channel ends with when a reconnect needs messages `side`'s replay buffer dropped. */
function expectLost(err: unknown, side: 'server' | 'client') {
  expect(err).toBeInstanceOf(NetworkError)
  expect((err as NetworkError).message).toBe(
    `Channel closed: a reconnect needed messages the ${side}'s replay buffer had dropped to stay within its size. Raise config.channel.${side}ReplayBuffer, or ${side}ReplayBufferBinary for binary messages and streams.`,
  )
}

function sseServer(net: Net, refuseUpload: boolean): typeof fetch {
  const sse = getTelefuncSseChannelHooks()
  return (async (url: string, init: RequestInit) => {
    const aborted = new Promise<never>((_resolve, reject) =>
      init.signal?.addEventListener('abort', () =>
        reject(new DOMException('The operation was aborted.', 'AbortError')),
      ),
    )
    aborted.catch(() => {})
    const body = init.body as Blob | ReadableStream<Uint8Array>
    if (net.refusing) throw new TypeError('fetch failed')
    if (!(body instanceof Blob)) {
      // A browser that can't stream a request body (Firefox, Safari) gets a 400 and the page sends batch POSTs.
      if (refuseUpload) return new Response('', { status: 400 })
      const request = new Request(url, {
        method: 'POST',
        body: body.pipeThrough(uploadThrough(net)),
        duplex: 'half',
      } as RequestInit)
      const response = (await Promise.race([sse.handleRequest(request), aborted]))!
      return new Response(response.body as string, { status: response.statusCode })
    }
    // Bytes rather than the Blob: a Request reads a Blob body on a later event-loop turn, which fake timers don't give.
    const bytes = new Uint8Array(await body.arrayBuffer())
    const [metadata, ...frames] = lengthPrefixed(bytes).map(({ frame }) => frame)
    const { connId, streamResponse } = JSON.parse(new TextDecoder().decode(metadata)) as {
      connId: string
      streamResponse?: true
    }
    const link = streamResponse ? net.open(connId) : net.linkOf(connId)!
    for (const frame of frames) {
      if (link.dead) break
      net.pageSends(frame)
    }
    if (link.dead || (link.holdingPosts && !streamResponse)) return await aborted
    const request = link.up
      ? new Request(url, { method: 'POST', body: carried(bytes, link, link.up), duplex: 'half' } as RequestInit)
      : new Request(url, { method: 'POST', body: bytes })
    // A body cut by the link's death fails the server's read, which nothing tells the page.
    const response = await sse.handleRequest(request).catch(() => null)
    if (link.dead || !response) return await aborted
    const responseBody =
      response.body instanceof ReadableStream ? downstream(response.body, link, net) : (response.body as string)
    return new Response(responseBody, {
      status: response.statusCode,
      headers: { 'Content-Type': response.contentType },
    })
  }) as typeof fetch
}

/** `[u32 length][bytes]` chunks, each `prefixed` and its `frame`: an SSE POST's metadata header, then its frames. */
function lengthPrefixed(bytes: Uint8Array): { prefixed: Uint8Array; frame: Uint8Array }[] {
  const chunks: { prefixed: Uint8Array; frame: Uint8Array }[] = []
  let offset = 0
  while (offset + 4 <= bytes.length) {
    const length = decodeU32(bytes.subarray(offset, offset + 4) as Uint8Array<ArrayBuffer>)
    chunks.push({
      prefixed: bytes.subarray(offset, offset + 4 + length),
      frame: bytes.subarray(offset + 4, offset + 4 + length),
    })
    offset += 4 + length
  }
  return chunks
}

/** A POST's body on its way to the server over `pipe`: each chunk once the link carried it, and none once it died. */
function carried(bytes: Uint8Array, link: Link, pipe: Pipe): ReadableStream<Uint8Array> {
  const chunks = lengthPrefixed(bytes).map(({ prefixed }) => ({ prefixed, at: pipe.arrival(prefixed.byteLength) }))
  let next = 0
  return new ReadableStream<Uint8Array>({
    async pull(controller) {
      await until(chunks[next]!.at)
      if (link.dead) return controller.error(new TypeError('network error'))
      controller.enqueue(chunks[next++]!.prefixed)
      if (next === chunks.length) controller.close()
    },
  })
}

/** An upload request's body on its way to the server: what the page writes once its wire died is lost. */
function uploadThrough(net: Net) {
  let pending = new Uint8Array(0)
  let link: Link | undefined
  return new TransformStream<Uint8Array, Uint8Array>({
    async transform(chunk, controller) {
      const joined = new Uint8Array(pending.length + chunk.length)
      joined.set(pending)
      joined.set(chunk, pending.length)
      let offset = 0
      while (offset + 4 <= joined.length) {
        const length = decodeU32(joined.subarray(offset, offset + 4) as Uint8Array<ArrayBuffer>)
        if (offset + 4 + length > joined.length) break
        const prefixed = joined.slice(offset, offset + 4 + length)
        const frame = prefixed.subarray(4)
        offset += 4 + length
        if (!link) {
          link = net.linkOf((JSON.parse(new TextDecoder().decode(frame)) as { connId: string }).connId)
        } else {
          if (!link.dead) net.pageSends(frame)
          if (link.up) await until(link.up.arrival(prefixed.byteLength))
          if (link.dead) continue
        }
        controller.enqueue(prefixed)
      }
      pending = joined.slice(offset)
    },
  })
}

/** An SSE downstream on its way to the page: once its wire died nothing arrives, not even its end. */
function downstream(body: ReadableStream<Uint8Array>, link: Link, net: Net): ReadableStream<Uint8Array> {
  const reader = body.getReader()
  const decoder = new TextDecoder()
  const encoder = new TextEncoder()
  let text = ''
  return new ReadableStream<Uint8Array>({
    async pull(controller) {
      for (;;) {
        const { done, value } = await reader.read()
        if (done) {
          if (link.dead) return await new Promise<void>(() => {})
          controller.close()
          return
        }
        text += decoder.decode(value, { stream: true })
        let delivered = false
        let end: number
        while ((end = text.indexOf('\n\n')) !== -1) {
          const event = text.slice(0, end)
          text = text.slice(end + 2)
          const frame = event.startsWith('data: ') ? base64urlToUint8Array(event.slice('data: '.length)) : null
          if (frame && !link.dead) net.serverSends(frame)
          if (link.down) await until(link.down.arrival(event.length + 2))
          if (link.dead) continue
          if (frame) net.pageGets(frame)
          controller.enqueue(encoder.encode(`${event}\n\n`))
          delivered = true
        }
        if (delivered) return
      }
    },
    cancel: (reason) => (link.dead ? undefined : reader.cancel(reason)),
  })
}

/** A `WebSocket` whose far end is the server's crossws hooks. */
function webSocketTo(net: Net) {
  const hooks = getTelefuncChannelHooks()
  return class {
    static readonly OPEN = 1
    readyState = 0
    binaryType = 'blob'
    onopen: (() => void) | null = null
    onmessage: ((event: { data: ArrayBuffer }) => void) | null = null
    onclose: (() => void) | null = null
    onerror: (() => void) | null = null
    private readonly link = net.open()
    private readonly peer = {
      context: {},
      // It hands each frame on as it is sent, or holds what its link hasn't carried.
      websocket: this.link.down ?? { bufferedAmount: 0 },
      send: (frame: Uint8Array) => {
        if (!this.link.dead) net.serverSends(frame)
        if (this.link.dead) return
        const data = frame.slice().buffer
        if (this.link.down) {
          void until(this.link.down.arrival(frame.byteLength)).then(() => {
            if (this.link.dead) return
            net.pageGets(new Uint8Array(data))
            this.onmessage?.({ data })
          })
          return
        }
        net.pageGets(frame)
        queueMicrotask(() => this.onmessage?.({ data }))
      },
      // A socket the server destroys closes with 1006, without a close frame.
      terminate: () => this.end(1006),
    } as unknown as Peer
    constructor(_url: string) {
      queueMicrotask(async () => {
        if (net.refusing) {
          this.readyState = 3
          this.onclose?.()
          return
        }
        await hooks.open!(this.peer)
        this.readyState = 1
        this.onopen?.()
      })
    }
    get bufferedAmount(): number {
      return this.link.up?.bufferedAmount ?? 0
    }
    send(data: Uint8Array) {
      const frame = data.slice()
      if (!this.link.dead) net.pageSends(frame)
      if (this.link.dead || net.loses(frame)) return
      if (this.link.up) {
        void until(this.link.up.arrival(frame.byteLength)).then(() => {
          if (!this.link.dead) void hooks.message!(this.peer, { uint8Array: () => frame } as never)
        })
        return
      }
      void hooks.message!(this.peer, { uint8Array: () => frame } as never)
    }
    close() {
      this.end(1000)
    }
    private end(code: number) {
      if (this.readyState === 3) return
      this.readyState = 3
      if (this.link.dead) return
      void hooks.close!(this.peer, { code } as never)
      // The page hears of it once its link carried what went before.
      if (this.link.down) void until(this.link.down.arrival(0)).then(() => this.onclose?.())
      else this.onclose?.()
    }
  }
}

describe.each(WIRES)('over %s', (wire) => {
  test('a server close whose acknowledgement goes down with a dying wire ends gracefully on the server, as on the page (#483)', async () => {
    const { net, channel } = page(wire)
    channel(register().id) // another channel on the page
    const server = register()
    const pageClosed = closedWith(channel(server.id))
    const serverClosed = closedWith(server)
    await advance(500)
    net.whenPageGets(TAG.CLOSE, () => net.die()) // the page acknowledges into a wire that has just died
    const closing = settled(server.close({ timeout: 20_000 }))
    await advance(10_000)
    expect(pageClosed.err).toBeUndefined()
    expect(serverClosed.err).toBeUndefined()
    expect(closing.value).toBe(0)
  })

  test("a page's answer that completes its close, written into a dying wire, reaches the server, and both ends close gracefully (#484)", async () => {
    const { net, channel } = page(wire)
    channel(register().id) // another channel on the page
    const server = register<never, string>()
    const pageChannel = channel<never, string>(server.id)
    let answer!: () => void
    pageChannel.listen(() => new Promise<string>((resolve) => (answer = () => resolve('reply'))))
    const serverClosed = closedWith(server)
    await advance(500)
    const asked = settled(server.send('question', { ack: true }))
    await advance(100)
    const closing = settled(pageChannel.close({ timeout: 20_000 }))
    await advance(100) // the server acknowledges the close
    net.die()
    answer() // written into the dead wire, it completes the page's close
    await advance(10_000)
    expect(closing.value).toBe(0)
    expect(asked.value).toBe('reply')
    expect(serverClosed.err).toBeUndefined()
  })

  test("a page's answer that completes its close, held behind a registration whose RECONCILE goes down with a dying wire, reaches the server (#484)", async () => {
    const { net, channel } = page(wire)
    channel(register().id) // another channel on the page
    const server = register<never, string>()
    const pageChannel = channel<never, string>(server.id)
    let answer!: () => void
    pageChannel.listen(() => new Promise<string>((resolve) => (answer = () => resolve('reply'))))
    const serverClosed = closedWith(server)
    await advance(500)
    const asked = settled(server.send('question', { ack: true }))
    await advance(100)
    const closing = settled(pageChannel.close({ timeout: 20_000 }))
    await advance(100)
    net.whenPageSends(TAG.RECONCILE, () => net.die())
    channel(register().id) // the listener opens a channel, whose registration holds the answer
    answer()
    await advance(15_000) // a wire awaiting its RECONCILED is dropped at the reconcile timeout
    expect(closing.value).toBe(0)
    expect(asked.value).toBe('reply')
    expect(serverClosed.err).toBeUndefined()
  })

  test('a page close whose request goes down with a dying wire completes on both ends after the reconnect', async () => {
    const { net, channel } = page(wire)
    channel(register().id) // another channel on the page
    const server = register()
    const pageChannel = channel(server.id)
    const serverClosed = closedWith(server)
    await advance(500)
    net.whenPageSends(TAG.CLOSE, () => net.die())
    const closing = settled(pageChannel.close({ timeout: 20_000 }))
    await advance(10_000)
    expect(closing.value).toBe(0)
    expect(serverClosed.err).toBeUndefined()
  })

  test('a page close whose request goes down with a dying wire behind a message over the replay budget ends the channel with NetworkError on both ends after the reconnect', async () => {
    serverConfig.channel = { pingInterval: 1_000, clientReplayBuffer: 1_024 }
    ;(getChannelMux() as unknown as { resolvedOptions: unknown }).resolvedOptions = null
    const { net, channel } = page(wire)
    channel(register().id) // another channel on the page
    const server = register<string, never>()
    const pageChannel = channel<string, never>(server.id)
    const pageClosed = closedWith(pageChannel)
    const serverClosed = closedWith(server)
    await advance(500)
    net.whenPageSends(TAG.TEXT, () => net.die())
    void pageChannel.send('x'.repeat(2_000), { ack: false }) // lost with the wire, and too big to replay
    const closing = settled(pageChannel.close({ timeout: 20_000 }))
    await advance(10_000)
    expect(closing.value).toBe(1)
    expectLost(pageClosed.err, 'client')
    expectLost(serverClosed.err, 'client')
  })

  test('a page close that times out as its request goes down with a dying wire ends the channel on the server, as the request asked', async () => {
    const { net, channel } = page(wire)
    channel(register().id) // another channel on the page
    const server = register()
    const pageChannel = channel(server.id)
    const serverClosed = closedWith(server)
    await advance(500)
    net.whenPageSends(TAG.CLOSE, () => net.die())
    const closing = settled(pageChannel.close({ timeout: 1_000 }))
    await advance(10_000)
    expect(closing.value).toBe(1)
    expect(serverClosed.err).toBeUndefined()
  })

  test('an abort the page queues behind a registration reaches the server', async () => {
    const { channel } = page(wire)
    const server = register()
    const pageChannel = channel(server.id)
    const serverClosed = closedWith(server)
    await advance(500)
    channel(register().id) // its RECONCILE holds the page's sends
    pageChannel.abort()
    await advance(1_000)
    expect(serverClosed.err).toBeUndefined()
  })

  test('a close acknowledgement the page queues while its RECONCILE is in flight reaches the server', async () => {
    const { net, channel } = page(wire)
    const server = register()
    channel(server.id)
    const serverClosed = closedWith(server)
    await advance(500)
    net.whenPageSends(TAG.RECONCILE, () => void server.close({ timeout: 20_000 }))
    channel(register().id)
    await advance(1_000)
    expect(serverClosed.err).toBeUndefined()
  })

  test("a page's answer that completes its close, held behind a registration on a live wire, reaches the server", async () => {
    const { channel } = page(wire)
    const server = register<never, string>()
    const pageChannel = channel<never, string>(server.id)
    let answer!: () => void
    pageChannel.listen(() => new Promise<string>((resolve) => (answer = () => resolve('reply'))))
    const serverClosed = closedWith(server)
    await advance(500)
    const asked = settled(server.send('question', { ack: true }))
    await advance(100)
    const closing = settled(pageChannel.close({ timeout: 20_000 }))
    await advance(100)
    channel(register().id) // the listener opens a channel, whose registration holds the answer
    answer()
    await advance(1_000)
    expect(closing.value).toBe(0)
    expect(asked.value).toBe('reply')
    expect(serverClosed.err).toBeUndefined()
  })

  test('a page goes idle once its last channel closes, after the server gave up on one the page aborted before the server had it', async () => {
    idleWithAbortedCallback()
    const { channel } = page(wire)
    const pageChannel = channel(register().id)
    await advance(500)
    await expectIdleAfterAbortedCallback(channel, pageChannel)
  })
})

test('over sse upgraded to a WebSocket, a page goes idle once its last channel closes, after the server gave up on one the page aborted before the server had it', async () => {
  idleWithAbortedCallback()
  const { channel } = page('sse', { upgrade: true })
  const pageChannel = channel(register().id)
  const connection = (pageChannel as any)._connection
  for (let waited = 0; waited < 5_000 && connection.transport.type !== 'ws'; waited += 5) await advance(5)
  await advance(500)
  expect(connection.transport.type).toBe('ws')
  await expectIdleAfterAbortedCallback(channel, pageChannel)
})

/** The server gives up on a channel it hasn't registered after 1 s, and a connection goes 300 ms after its last channel. */
function idleWithAbortedCallback() {
  serverConfig.channel = { pingInterval: 1_000, connectTtl: 1_000, idleTimeout: 300 }
  ;(getChannelMux() as unknown as { resolvedOptions: unknown }).resolvedOptions = null
}

/** The page aborts a call's callback before the call reaches the server, which never registers it, then closes
 *  `pageChannel`, its last channel, once the server gave up on the callback. */
async function expectIdleAfterAbortedCallback(
  channel: (channelId: string) => ClientChannel,
  pageChannel: ClientChannel,
) {
  const connection = (pageChannel as unknown as { _connection: { closed: boolean } })._connection
  channel(crypto.randomUUID()).abort()
  await advance(2_000)
  void pageChannel.close()
  await advance(2_000)
  expect(connection.closed).toBe(true)
}

describe.each(WIRES)('over %s, from the server', (wire) => {
  test('a page close whose acknowledgement goes down with a dying wire completes gracefully on both ends', async () => {
    const { net, channel } = page(wire)
    channel(register().id) // another channel on the page
    const server = register()
    const pageChannel = channel(server.id)
    const pageClosed = closedWith(pageChannel)
    const serverClosed = closedWith(server)
    await advance(500)
    net.whenServerSends(TAG.CLOSE_ACK, () => net.die())
    const closing = settled(pageChannel.close({ timeout: 20_000 }))
    await advance(10_000)
    expect(closing.value).toBe(0)
    expect(pageClosed.err).toBeUndefined()
    expect(serverClosed.err).toBeUndefined()
  })

  test('a server abort that goes down with a dying wire reaches the page as the abort', async () => {
    const { net, channel } = page(wire)
    channel(register().id) // another channel on the page
    const server = register()
    const pageClosed = closedWith(channel(server.id))
    await advance(500)
    net.whenServerSends(TAG.ABORT, () => net.die())
    server.abort('gone')
    await advance(10_000)
    expect(isAbort(pageClosed.err)).toBe(true)
    expect((pageClosed.err as { abortValue: unknown }).abortValue).toBe('gone')
  })

  test("a server's answer that completes its close, written into a dying wire, reaches the page", async () => {
    const { net, channel } = page(wire)
    channel(register().id) // another channel on the page
    const server = register<string, never>()
    let answer!: () => void
    server.listen(() => new Promise<string>((resolve) => (answer = () => resolve('reply'))))
    const pageChannel = channel<string, never>(server.id)
    const pageClosed = closedWith(pageChannel)
    const serverClosed = closedWith(server)
    await advance(500)
    const asked = settled(pageChannel.send('question', { ack: true }))
    await advance(100)
    const closing = settled(server.close({ timeout: 20_000 }))
    await advance(100) // the page acknowledges the close
    net.whenServerSends(TAG.ACK_RES, () => net.die())
    answer() // written into the dead wire, it completes the server's close
    await advance(10_000)
    expect(asked.value).toBe('reply')
    expect(closing.value).toBe(0)
    expect(pageClosed.err).toBeUndefined()
    expect(serverClosed.err).toBeUndefined()
  })

  test("a server's answer that completes the page's close, written into a dying wire, reaches the page", async () => {
    const { net, channel } = page(wire)
    channel(register().id) // another channel on the page
    const server = register<string, never>()
    let answer!: () => void
    server.listen(() => new Promise<string>((resolve) => (answer = () => resolve('reply'))))
    const pageChannel = channel<string, never>(server.id)
    const pageClosed = closedWith(pageChannel)
    const serverClosed = closedWith(server)
    await advance(500)
    const asked = settled(pageChannel.send('question', { ack: true }))
    await advance(100)
    const closing = settled(pageChannel.close({ timeout: 20_000 }))
    await advance(100) // the server acknowledges the close
    net.whenServerSends(TAG.ACK_RES, () => net.die())
    answer()
    await advance(10_000)
    expect(asked.value).toBe('reply')
    expect(closing.value).toBe(0)
    expect(pageClosed.err).toBeUndefined()
    expect(serverClosed.err).toBeUndefined()
  })

  test("a server's answer made while its page is away, which completes its close, reaches the page once it's back", async () => {
    const { net, channel } = page(wire)
    channel(register().id) // another channel on the page
    const server = register<string, never>()
    let answer!: () => void
    server.listen(() => new Promise<string>((resolve) => (answer = () => resolve('reply'))))
    const pageChannel = channel<string, never>(server.id)
    const pageClosed = closedWith(pageChannel)
    const serverClosed = closedWith(server)
    await advance(500)
    const asked = settled(pageChannel.send('question', { ack: true }))
    await advance(100)
    const closing = settled(server.close({ timeout: 20_000 }))
    await advance(100) // the page acknowledges the close
    net.cut()
    await advance(3_000) // the server notices the page is gone
    answer() // it completes the server's close
    await advance(100)
    net.heal()
    await advance(10_000)
    expect(asked.value).toBe('reply')
    expect(closing.value).toBe(0)
    expect(pageClosed.err).toBeUndefined()
    expect(serverClosed.err).toBeUndefined()
  })

  test('a server abort made while its page is away reaches the page as the abort once it is back', async () => {
    const { net, channel } = page(wire)
    channel(register().id) // another channel on the page
    const server = register()
    const pageClosed = closedWith(channel(server.id))
    await advance(500)
    net.cut()
    await advance(3_000) // the server notices the page is gone
    server.abort('gone')
    await advance(100)
    net.heal()
    await advance(10_000)
    expect(isAbort(pageClosed.err)).toBe(true)
    expect((pageClosed.err as { abortValue: unknown }).abortValue).toBe('gone')
  })

  test('a server close that times out while its page is away gets the page what the server sent meanwhile, then closes it gracefully', async () => {
    const { net, channel } = page(wire)
    channel(register().id) // another channel on the page
    const server = register<never, string>()
    const pageChannel = channel<never, string>(server.id)
    const got: string[] = []
    pageChannel.listen((message) => void got.push(message))
    const pageClosed = closedWith(pageChannel)
    const serverClosed = closedWith(server)
    await advance(500)
    void server.send('m0', { ack: false })
    await advance(100)
    net.cut()
    await advance(3_000) // the server notices the page is gone
    const sent = [settled(server.send('m1', { ack: false })), settled(server.send('m2', { ack: false }))]
    const closing = settled(server.close())
    await advance(6_000)
    expect(closing.value).toBe(1)
    expect((serverClosed.err as Error).message).toBe('Channel close timed out')
    net.heal()
    await advance(10_000)
    expect(got).toEqual(['m0', 'm1', 'm2'])
    expect(pageClosed.err).toBeUndefined()
    expect(sent.map(({ value }) => value)).toEqual([undefined, undefined])
  })

  test('a channel the server sends on and closes before its page attaches, whose close times out first, gets the page what it sent, then closes it gracefully', async () => {
    const { channel } = page(wire)
    const server = register<never, string>()
    const sent = [settled(server.send('m1', { ack: false })), settled(server.send('m2', { ack: false }))]
    const closing = settled(server.close({ timeout: 300 }))
    await advance(1_000) // the page's connection is slow to open
    expect(closing.value).toBe(1)
    const pageChannel = channel<never, string>(server.id)
    const got: string[] = []
    pageChannel.listen((message) => void got.push(message))
    const pageClosed = closedWith(pageChannel)
    await advance(1_000)
    expect(got).toEqual(['m1', 'm2'])
    expect(pageClosed.err).toBeUndefined()
    expect(sent.map(({ value }) => value)).toEqual([undefined, undefined])
  })

  test('a server abort made while its page is away gets the page what the server sent before it, then the abort', async () => {
    const { net, channel } = page(wire)
    channel(register().id) // another channel on the page
    const server = register<never, string>()
    const pageChannel = channel<never, string>(server.id)
    const got: string[] = []
    pageChannel.listen((message) => void got.push(message))
    const pageClosed = closedWith(pageChannel)
    await advance(500)
    net.cut()
    await advance(3_000) // the server notices the page is gone
    const sent = settled(server.send('m1', { ack: false }))
    server.abort('gone')
    await advance(100)
    net.heal()
    await advance(10_000)
    expect(got).toEqual(['m1'])
    expect(isAbort(pageClosed.err)).toBe(true)
    expect(sent.value).toBeUndefined()
  })

  test('an ack request a server makes while its page is away rejects as its close times out, as no answer can reach it, and still reaches the page', async () => {
    const { net, channel } = page(wire)
    channel(register().id) // another channel on the page
    const server = register<never, string>()
    const pageChannel = channel<never, string>(server.id)
    const got: string[] = []
    pageChannel.listen((message) => void got.push(message))
    await advance(500)
    net.cut()
    await advance(3_000) // the server notices the page is gone
    const asked = settled(server.send('question', { ack: true }))
    const closing = settled(server.close())
    await advance(6_000)
    expect(closing.value).toBe(1)
    expect((asked.value as Error).message).toBe('Channel close timed out')
    net.heal()
    await advance(10_000)
    expect(got).toEqual(['question'])
  })

  test('a page whose channels the server closes on a healthy wire lets each go within a ping round trip, and its next RECONCILE lists only the open ones', async () => {
    const { net, channel } = page(wire)
    const kept = channel(register().id)
    const servers = Array.from({ length: 20 }, () => register())
    const closes = servers.map((server) => closedWith(channel(server.id)))
    await advance(500)
    for (const server of servers) void server.close()
    await advance(3_000)
    expect(closes.every((closed) => closed.err === undefined)).toBe(true)
    const connection = (kept as unknown as { _connection: { channels: Map<number, unknown> } })._connection
    expect(connection.channels.size).toBe(1)
    expect(servers.filter((server) => getChannelMux()['channels'].has(server.id))).toEqual([])
    let listed: unknown[] = []
    net.whenPageSends(TAG.RECONCILE, (frame) => {
      listed = (decode(frame, wireSeqs) as { payload: ReconcilePayload }).payload.open.map(({ id }) => id)
    })
    net.die()
    await advance(5_000)
    expect(listed).toEqual([kept.id])
  })

  test('the server lets a channel it ended go once the page has its last frames, and one whose page never comes back after the reconnect window', async () => {
    const { net, channel } = page(wire)
    channel(register().id) // another channel on the page
    const serverClosed = register()
    const pageClosed = register()
    const aborted = register()
    channel(serverClosed.id)
    const pageChannel = channel(pageClosed.id)
    channel(aborted.id)
    await advance(500)
    const names = new Map([
      [serverClosed.id, 'server-closed'],
      [pageClosed.id, 'page-closed'],
      [aborted.id, 'aborted'],
    ])
    const held = () => [...names].filter(([id]) => getChannelMux()['channels'].has(id)).map(([, name]) => name)
    // The page's acknowledgement of a close request tells the server the page has it.
    const closing = settled(serverClosed.close())
    await advance(100)
    expect(closing.value).toBe(0)
    expect(held()).toEqual(['page-closed', 'aborted'])
    // The server's acknowledgement of the page's close request waits for a PING to say the page has it.
    const pageClosing = settled(pageChannel.close())
    await advance(100)
    expect(pageClosing.value).toBe(0)
    expect(held()).toEqual(['page-closed', 'aborted'])
    await advance(1_500)
    expect(held()).toEqual(['aborted'])
    net.cut()
    aborted.abort()
    await advance(30_000)
    expect(held()).toEqual(['aborted'])
    await advance(40_000)
    expect(held()).toEqual([])
  })

  test('a page whose last channel closed goes away with its wire once the server has all it sent, and reconnects for it before', async () => {
    const { net, channel } = page(wire)
    const server = register()
    const pageChannel = channel(server.id)
    const connection = (pageChannel as unknown as { _connection: { closed: boolean } })._connection
    await advance(500)
    void pageChannel.close()
    await advance(1_500) // a ping round trip
    let reconciles = 0
    net.whenPageSends(TAG.RECONCILE, () => reconciles++)
    net.die()
    await advance(5_000)
    expect(reconciles).toBe(0)
    expect(connection.closed).toBe(true)

    const again = page(wire)
    const aborted = register()
    const abortedClosed = closedWith(aborted)
    const unconfirmed = again.channel(aborted.id)
    await advance(500)
    let reconciled = 0
    again.net.whenPageSends(TAG.CLOSE, () => {
      again.net.die()
      again.net.whenPageSends(TAG.RECONCILE, () => reconciled++)
    })
    unconfirmed.abort() // its abort goes down with the wire
    await advance(5_000)
    expect(reconciled).toBe(1)
    expect(abortedClosed.err).toBeUndefined()
  })
})

/** Awaits each of `count` sends, one every `everyMs`, as a stream that awaits its sends does. */
function produce(send: (n: number) => Promise<unknown>, count: number, everyMs: number) {
  void (async () => {
    for (let n = 0; n < count; n++) {
      await send(n)
      await new Promise((resolve) => setTimeout(resolve, everyMs))
    }
  })().catch(() => {})
}
/** A 16 KiB message that names its place in the stream. */
const text = (n: number) => String(n).padEnd(16 * 1_024)
/** A 1 MiB binary message that names its place in the stream: two of them take the page's 2 MiB window. */
const chunk = (n: number) => new Uint8Array(1_024 * 1_024).fill(n)
const inOrder = (count: number) => Array.from({ length: count }, (_, n) => n)
/** The wire dies once `sender` may send more than half of `receiver`'s window, which then goes into the dead wire. */
async function dieWithCredit(net: Net, sender: unknown, receiver: unknown) {
  const flow = (sender as { _flow: { _limitBytes: number; _sentBytes: number } })._flow
  while (flow._limitBytes - flow._sentBytes <= flowOf(receiver).byteWindow / 2) await advance(1)
  net.die()
}

describe.each(WIRES)('over %s, a stream that awaits its sends, whose wire drops with its window in flight,', (wire) => {
  test('from the server, as text, resumes after the reconnect without loss', async () => {
    const { net, channel } = page(wire)
    const server = register<never, string>()
    const pageChannel = channel<never, string>(server.id)
    const got: number[] = []
    pageChannel.listen((message) => void got.push(Number.parseInt(message)))
    const closed = [closedWith(pageChannel), closedWith(server)]
    await advance(500)
    produce((n) => server.send(text(n), { ack: false }), 400, 1)
    await advance(100)
    await dieWithCredit(net, server, pageChannel)
    await advance(10_000)
    expect(got).toEqual(inOrder(400))
    expect(closed.map(({ err }) => err)).toEqual(['open', 'open'])
  })

  test('from the server, as binary, resumes after the reconnect without loss', async () => {
    const { net, channel } = page(wire)
    const server = register()
    const pageChannel = channel(server.id)
    const got: number[] = []
    pageChannel.listenBinary((data) => void got.push(data[0]!))
    const closed = [closedWith(pageChannel), closedWith(server)]
    await advance(500)
    produce((n) => server.sendBinary(chunk(n)), 24, 10)
    await advance(100)
    await dieWithCredit(net, server, pageChannel)
    await advance(10_000)
    expect(got).toEqual(inOrder(24))
    expect(closed.map(({ err }) => err)).toEqual(['open', 'open'])
  })

  test('from the page, as text, resumes after the reconnect without loss', async () => {
    const { net, channel } = page(wire)
    const server = register<string, never>()
    const got: number[] = []
    server.listen((message) => void got.push(Number.parseInt(message)))
    const pageChannel = channel<string, never>(server.id)
    const closed = [closedWith(pageChannel), closedWith(server)]
    await advance(500)
    produce((n) => pageChannel.send(text(n), { ack: false }), 400, 1)
    await advance(100)
    await dieWithCredit(net, pageChannel, server)
    await advance(10_000)
    expect(got).toEqual(inOrder(400))
    expect(closed.map(({ err }) => err)).toEqual(['open', 'open'])
  })

  test('from the page, as binary, resumes after the reconnect without loss', async () => {
    const { net, channel } = page(wire)
    const server = register()
    const got: number[] = []
    server.listenBinary((data) => void got.push(data[0]!))
    const pageChannel = channel(server.id)
    const closed = [closedWith(pageChannel), closedWith(server)]
    await advance(500)
    produce((n) => pageChannel.sendBinary(chunk(n)), 24, 10)
    await advance(100)
    await dieWithCredit(net, pageChannel, server)
    await advance(10_000)
    expect(got).toEqual(inOrder(24))
    expect(closed.map(({ err }) => err)).toEqual(['open', 'open'])
  })

  test("the server's replay lets go of what the page acknowledges, holding the page's window and a message at most", async () => {
    const { channel } = page(wire)
    const server = register<never, string>()
    const pageChannel = channel<never, string>(server.id)
    let got = 0
    pageChannel.listen(() => void got++)
    await advance(500)
    const held = () => server._replayBuffer!.byteLength
    let most = 0
    let shrank = 0
    const dispatchCtrl = server._dispatchCtrl.bind(server)
    server._dispatchCtrl = (frame) => {
      const before = held()
      dispatchCtrl(frame)
      if (frame.tag === TAG.WINDOW && held() < before) shrank++
    }
    produce((n) => server.send(text(n), { ack: false }), 400, 1)
    const watch = setInterval(() => (most = Math.max(most, held())), 1)
    await advance(2_000)
    clearInterval(watch)
    expect(got).toBe(400)
    expect(shrank).toBeGreaterThan(0)
    expect(most).toBeLessThanOrEqual(flowOf(pageChannel).byteWindow + 16 * 1_024)
  })

  test("the page's replay lets go of what the server acknowledges, holding the server's window and a message at most", async () => {
    const { channel } = page(wire)
    const server = register<string, never>()
    let got = 0
    server.listen(() => void got++)
    const pageChannel = channel<string, never>(server.id)
    await advance(500)
    const connection = (pageChannel as any)._connection
    const held = () => connection.replayBuffers.get(connection.channelIndex.get(pageChannel)).byteLength as number
    let most = 0
    let shrank = 0
    const dispatchFrame = connection.dispatchFrame.bind(connection)
    connection.dispatchFrame = (frame: { tag: number }) => {
      const before = held()
      dispatchFrame(frame)
      if (frame.tag === TAG.WINDOW && held() < before) shrank++
    }
    produce((n) => pageChannel.send(text(n), { ack: false }), 400, 1)
    const watch = setInterval(() => (most = Math.max(most, held())), 1)
    await advance(2_000)
    clearInterval(watch)
    expect(got).toBe(400)
    expect(shrank).toBeGreaterThan(0)
    expect(most).toBeLessThanOrEqual(flowOf(server).byteWindow + 16 * 1_024)
  })
})

/** A 64 KiB binary message that names its place in the stream. */
const block = (n: number) => new Uint8Array(64 * 1_024).fill(n)
/** Bytes a second of a link that carries a message a second. */
const SLOW = 64 * 1_024

function reconnectTimeout5s() {
  serverConfig.channel = { pingInterval: 1_000, reconnectTimeout: 5_000 }
  ;(getChannelMux() as unknown as { resolvedOptions: unknown }).resolvedOptions = null
}

describe.each(WIRES)(
  'over %s, a reconnect whose replay takes longer than the ping deadline to cross the link',
  (wire) => {
    beforeEach(reconnectTimeout5s)

    test('keeps its wire, and the stream resumes without loss', async () => {
      const { net, channel } = page(wire)
      net.rate.down = SLOW
      const server = register()
      const pageChannel = channel(server.id)
      const got: number[] = []
      pageChannel.listenBinary((data) => void got.push(data[0]!))
      const closed = [closedWith(pageChannel), closedWith(server)]
      await advance(500)
      produce((n) => server.sendBinary(block(n)), 20, 0)
      await advance(1_000)
      net.die() // with most of the stream in flight, which the page reconnects for over a link as slow
      await advance(40_000)
      expect(closed.map(({ err }) => (err instanceof Error ? err.message : err))).toEqual(['open', 'open'])
      expect(got).toEqual(inOrder(20))
      expect(net.links).toHaveLength(2)
    })
  },
)

describe.each(WIRES)("over %s, a stream that awaits its sends, begun before its page's first RECONCILED,", (wire) => {
  test("from the page, on a channel it passes to the server, resumes without loss when its wire drops with it in flight, however small the page's replay", async () => {
    serverConfig.channel = { pingInterval: 1_000, clientReplayBuffer: 64 * 1_024 }
    ;(getChannelMux() as unknown as { resolvedOptions: unknown }).resolvedOptions = null
    const { net, channel } = page(wire)
    // The first wire dies as its RECONCILED arrives, and the next as the server attaches the channel.
    net.whenPageGets(TAG.RECONCILED, () => net.die())
    net.whenPageGets(TAG.ATTACH_RESULT, () => net.die())
    const pageChannel = channel<string, never>(crypto.randomUUID())
    void (async () => {
      for (let n = 0; n < 40; n++) await pageChannel.send(text(n), { ack: false })
    })()
    await advance(5_000)
    // The call passing it reaches the server once the first wire's loss was noticed on both ends.
    const server = new ServerChannel<string, never>({ id: pageChannel.id })
    getChannelMux().registerChannel(server)
    const got: number[] = []
    server.listen((message) => void got.push(Number.parseInt(message)))
    const closed = [closedWith(pageChannel), closedWith(server)]
    await advance(20_000)
    expect(got).toEqual(inOrder(40))
    expect(closed.map(({ err }) => err)).toEqual(['open', 'open'])
  })

  test("from the server, after the page read what the server sent it before, resumes without loss when its wire drops with it in flight, however small the server's replay", async () => {
    serverConfig.channel = {
      pingInterval: 1_000,
      serverReplayBuffer: 256 * 1_024,
      serverReplayBufferBinary: 256 * 1_024,
    }
    ;(getChannelMux() as unknown as { resolvedOptions: unknown }).resolvedOptions = null
    const { net, channel } = page(wire)
    const server = register<never, string>()
    void server.sendBinary(new Uint8Array(1_024 * 1_024)) // nobody awaits it: the page reads it as it attaches
    const pageChannel = channel<never, string>(server.id)
    let read = 0
    pageChannel.listenBinary((data) => void (read += data.byteLength))
    const got: number[] = []
    pageChannel.listen((message) => void got.push(Number.parseInt(message)))
    const closed = [closedWith(pageChannel), closedWith(server)]
    await advance(500)
    expect(read).toBe(1_024 * 1_024)
    produce((n) => server.send(text(n), { ack: false }), 400, 1)
    await advance(100)
    await dieWithCredit(net, server, pageChannel)
    await advance(20_000)
    expect(got).toEqual(inOrder(400))
    expect(closed.map(({ err }) => err)).toEqual(['open', 'open'])
  })
})

test("over ws, a stream that awaits its sends from the page on a channel the server returned, begun before the page's first RECONCILED, resumes without loss when its wire drops with it in flight, however small the page's replay", async () => {
  // The server waits for the page past the first wire's reconcile timeout.
  serverConfig.channel = { pingInterval: 1_000, connectTtl: 20_000, clientReplayBuffer: 64 * 1_024 }
  ;(getChannelMux() as unknown as { resolvedOptions: unknown }).resolvedOptions = null
  const { net, channel } = page('ws')
  const server = register<string, never>()
  const got: number[] = []
  server.listen((message) => void got.push(Number.parseInt(message)))
  // Its first RECONCILE goes down with the wire, and the next wire dies as the page gets its RECONCILED.
  net.whenPageSends(TAG.RECONCILE, () => {
    net.die()
    net.whenPageGets(TAG.RECONCILED, () => net.die())
  })
  const pageChannel = channel<string, never>(server.id)
  const closed = [closedWith(pageChannel), closedWith(server)]
  void (async () => {
    for (let n = 0; n < 40; n++) await pageChannel.send(text(n), { ack: false })
  })()
  await advance(30_000)
  expect(got).toEqual(inOrder(40))
  expect(closed.map(({ err }) => err)).toEqual(['open', 'open'])
})

describe.each(WIRES)('over %s, a channel gone quiet', (wire) => {
  test("has each end's replay let go of what the other end got within a heartbeat, and a heartbeat with nothing new sends no WINDOW", async () => {
    const { net, channel } = page(wire)
    const server = register<string, string>()
    server.listen(() => {})
    const pageChannel = channel<string, string>(server.id)
    pageChannel.listen(() => {})
    await advance(500)
    // Less than a quarter window each way, which no limit acknowledges.
    for (let n = 0; n < 8; n++) {
      void server.send(String(n).padEnd(16 * 1_024), { ack: false })
      void pageChannel.send(String(n).padEnd(16 * 1_024), { ack: false })
    }
    await advance(100)
    const connection = (pageChannel as any)._connection
    const pageReplay = connection.replayBuffers.get(connection.channelIndex.get(pageChannel))
    const held = () => [server._replayBuffer!.byteLength, pageReplay.byteLength]
    expect(held().every((bytes) => bytes > 0)).toBe(true)
    await advance(1_000) // a heartbeat
    expect(held()).toEqual([0, 0])

    const windows = { page: 0, server: 0 }
    const countPage = () => {
      windows.page++
      net.whenPageSends(TAG.WINDOW, countPage)
    }
    const countServer = () => {
      windows.server++
      net.whenServerSends(TAG.WINDOW, countServer)
    }
    net.whenPageSends(TAG.WINDOW, countPage)
    net.whenServerSends(TAG.WINDOW, countServer)
    await advance(5_000)
    expect(windows).toEqual({ page: 0, server: 0 })
  })
})

describe.each(WIRES)('over %s, past the replay', (wire) => {
  /** Chunks of a stream of `size` bytes each: the first, then, once `resume` is called, eight more. */
  function chunks(size: number) {
    let resume!: () => void
    const resumed = new Promise<void>((resolve) => (resume = resolve))
    const chunk = (n: number) => new Uint8Array(size).fill(n) as Uint8Array<ArrayBuffer>
    const producer = {
      chunks: (async function* () {
        yield chunk(0)
        await resumed
        for (let n = 1; n <= 8; n++) yield chunk(n)
      })(),
      cancel: () => {},
    }
    const all = new Uint8Array(9 * size)
    for (let n = 0; n <= 8; n++) all.set(chunk(n), n * size)
    return { producer, resume, all }
  }

  test("a reconnect that needs messages the server's replay dropped ends the channel with NetworkError on both ends, and the page gets none after them", async () => {
    serverConfig.channel = { pingInterval: 1_000, serverReplayBuffer: 1_024 }
    const { net, channel } = page(wire)
    const server = register<never, string>()
    const pageChannel = channel<never, string>(server.id)
    const got: string[] = []
    pageChannel.listen((message) => void got.push(message))
    const pageClosed = closedWith(pageChannel)
    const serverClosed = closedWith(server)
    await advance(500)
    void server.send('before', { ack: false })
    await advance(100)
    net.die()
    // Sent into the dead wire, nobody awaiting them: past the page's window, and more than the server's replay holds.
    for (let n = 0; n < 8; n++) void server.send(String(n).padEnd(256), { ack: false })
    await advance(10_000)
    expect(got).toEqual(['before'])
    expectLost(pageClosed.err, 'server')
    expectLost(serverClosed.err, 'server')
  })

  test("a reconnect that needs messages the page's replay dropped ends the channel with NetworkError on both ends, and the server gets none after them", async () => {
    serverConfig.channel = { pingInterval: 1_000, clientReplayBuffer: 1_024 }
    ;(getChannelMux() as unknown as { resolvedOptions: unknown }).resolvedOptions = null
    const { net, channel } = page(wire)
    const server = register<string, never>()
    const got: string[] = []
    server.listen((message) => void got.push(message))
    const pageChannel = channel<string, never>(server.id)
    const pageClosed = closedWith(pageChannel)
    const serverClosed = closedWith(server)
    await advance(500)
    void pageChannel.send('before', { ack: false })
    await advance(100)
    net.die()
    // Sent into the dead wire, nobody awaiting them: past the server's window, and more than the page's replay holds.
    for (let n = 0; n < 8; n++) void pageChannel.send(String(n).padEnd(256), { ack: false })
    await advance(10_000)
    expect(got).toEqual(['before'])
    expectLost(pageClosed.err, 'client')
    expectLost(serverClosed.err, 'client')
  })

  test('a reconnect whose gap the replays fill resumes without loss, past a message larger than them that arrived before', async () => {
    serverConfig.channel = { pingInterval: 1_000, serverReplayBuffer: 1_024, clientReplayBuffer: 1_024 }
    ;(getChannelMux() as unknown as { resolvedOptions: unknown }).resolvedOptions = null
    const { net, channel } = page(wire)
    const server = register<string, string>()
    const serverGot: string[] = []
    server.listen((message) => void serverGot.push(message.slice(0, 8)))
    const pageChannel = channel<string, string>(server.id)
    const pageGot: string[] = []
    pageChannel.listen((message) => void pageGot.push(message.slice(0, 8)))
    const pageClosed = closedWith(pageChannel)
    const serverClosed = closedWith(server)
    await advance(500)
    void server.send('large'.padEnd(2_000), { ack: false })
    void pageChannel.send('large'.padEnd(2_000), { ack: false })
    await advance(100)
    net.die()
    for (let n = 0; n < 3; n++) {
      void server.send(`s${n}`.padEnd(256), { ack: false })
      void pageChannel.send(`p${n}`.padEnd(256), { ack: false })
    }
    await advance(10_000)
    void server.send('after', { ack: false })
    void pageChannel.send('after', { ack: false })
    // Over SSE batch POSTs, a page's send waits for the next flush, at most one each SSE_FLUSH_THROTTLE_MS.
    await advance(SSE_FLUSH_THROTTLE_MS + 100)
    expect(pageGot.map((message) => message.trim())).toEqual(['large', 's0', 's1', 's2', 'after'])
    expect(serverGot.map((message) => message.trim())).toEqual(['large', 'p0', 'p1', 'p2', 'after'])
    expect(pageClosed.err).toBe('open')
    expect(serverClosed.err).toBe('open')
  })

  /** A stream over the 'channel' transport, from the server, of chunks of `size` bytes, and what the page reads of it. */
  function download(wire: Wire, size: number) {
    const { net, channel } = page(wire)
    const { producer, resume, all } = chunks(size)
    const channelId = pumpProducerToChannel(() => producer, {
      context: {} as never,
      requestContext: { responseAbort: { errorPromise: new Promise(() => {}), abort: () => {} } } as never,
      telefunctionName: 'onDownload',
      telefuncFilePath: '/download.telefunc.ts',
    })
    return { net, resume, all, read: settled(ChannelStreamSource.create(channel(channelId)).bytes()) }
  }

  /** An upload over the 'channel' transport of chunks of `size` bytes, and what the server reads of it. */
  function upload(wire: Wire, size: number) {
    const { net } = page(wire)
    const { producer, resume, all } = chunks(size)
    const upload = pumpClientProducerToChannel(
      () => producer,
      wire === 'ws' ? ['ws'] : ['sse'],
      `http://${crypto.randomUUID()}.test/_telefunc`,
    )
    const server = new ServerChannel({ id: upload.metadata.channelId })
    getChannelMux().registerChannel(server)
    return { net, resume, all, read: settled(ChannelStreamSource.create(server).bytes()) }
  }

  test("a stream over the 'channel' transport whose wire drops mid-stream completes after the reconnect, however small the server's replay", async () => {
    serverConfig.channel = { pingInterval: 1_000, serverReplayBufferBinary: 4_096 }
    const { net, resume, all, read } = download(wire, 1_024)
    await advance(500)
    net.die()
    resume() // the rest of the stream, and its end, go into the dead wire
    await advance(10_000)
    expect(read.value).toEqual(all)
  })

  test("an upload over the 'channel' transport whose wire drops mid-stream completes after the reconnect, however small the page's replay", async () => {
    serverConfig.channel = { pingInterval: 1_000, clientReplayBufferBinary: 4_096 }
    ;(getChannelMux() as unknown as { resolvedOptions: unknown }).resolvedOptions = null
    const { net, resume, all, read } = upload(wire, 1_024)
    await advance(500)
    net.die()
    resume() // the rest of the upload, and its end, go into the dead wire
    await advance(10_000)
    expect(read.value).toEqual(all)
  })

  test("a stream over the 'channel' transport that a reconnect needs a chunk of larger than the server's replay errors on the page rather than completing short", async () => {
    serverConfig.channel = { pingInterval: 1_000, serverReplayBufferBinary: 4_096 }
    const { net, resume, read } = download(wire, 8_192)
    await advance(500)
    net.die()
    resume() // the rest of the stream, and its end, go into the dead wire
    await advance(10_000)
    expectLost(read.value, 'server')
  })

  test("an upload over the 'channel' transport that a reconnect needs a chunk of larger than the page's replay errors on the server rather than completing short", async () => {
    serverConfig.channel = { pingInterval: 1_000, clientReplayBufferBinary: 4_096 }
    ;(getChannelMux() as unknown as { resolvedOptions: unknown }).resolvedOptions = null
    const { net, resume, read } = upload(wire, 8_192)
    await advance(500)
    net.die()
    resume() // the rest of the upload, and its end, go into the dead wire
    await advance(10_000)
    expectLost(read.value, 'client')
  })
})

test('on SSE batch POSTs, a close acknowledgement held behind an upgrade barrier that a dying wire never lets out reaches the server (#485)', async () => {
  const { net, channel } = page('sse-batch', { upgrade: true })
  const other = register<string, never>()
  const pageOther = channel<string, never>(other.id)
  const server = register()
  const pageClosed = closedWith(channel(server.id))
  const serverClosed = closedWith(server)
  const connection = (pageOther as any)._connection
  // When the barrier is due, a batch POST is still in flight, so it waits for it.
  const enterUpgradeCommitting = connection.enterUpgradeCommitting.bind(connection)
  connection.enterUpgradeCommitting = (...args: unknown[]) => {
    connection.enterUpgradeCommitting = enterUpgradeCommitting
    for (const link of net.links) link.holdingPosts = true
    void pageOther.send('in flight', { ack: false })
    enterUpgradeCommitting(...args)
  }
  for (let waited = 0; waited < 3_000 && !connection.committing; waited += 5) await advance(5)
  expect(connection.committing).not.toBe(null)
  const closing = settled(server.close({ timeout: 20_000 }))
  await advance(50) // the page acknowledges it behind the barrier
  expect(pageClosed.err).toBeUndefined()
  net.die()
  await advance(10_000)
  expect(serverClosed.err).toBeUndefined()
  expect(closing.value).toBe(0)
})

test("over sse, an upgrade whose barrier finds more on the old wire than the server's replay buffer holds keeps the channel, which the page gets all of in order", async () => {
  serverConfig.channel = { pingInterval: 1_000, serverReplayBuffer: 1_024 }
  const { net, channel } = page('sse', { upgrade: true })
  const server = register<never, string>()
  const pageChannel = channel<never, string>(server.id)
  const got: string[] = []
  pageChannel.listen((message) => void got.push(message.trim()))
  const pageClosed = closedWith(pageChannel)
  const serverClosed = closedWith(server)
  // As the page writes its barrier, the server sends the old wire more than its replay holds.
  net.whenPageSends(TAG.BARRIER, () => {
    for (let n = 0; n < 8; n++) void server.send(String(n).padEnd(256), { ack: false })
  })
  await advance(5_000)
  expect((pageChannel as any)._connection.transport.type).toBe('ws')
  expect(got).toEqual(['0', '1', '2', '3', '4', '5', '6', '7'])
  expect(pageClosed.err).toBe('open')
  expect(serverClosed.err).toBe('open')
})

test('over sse, a stream that awaits its sends keeps whole across the upgrade to a WebSocket, and resumes without loss when that drops with its window in flight', async () => {
  const { net, channel } = page('sse', { upgrade: true })
  const server = register<never, string>()
  const pageChannel = channel<never, string>(server.id)
  const got: number[] = []
  pageChannel.listen((message) => void got.push(Number.parseInt(message)))
  const closed = [closedWith(pageChannel), closedWith(server)]
  produce((n) => server.send(text(n), { ack: false }), 600, 5)
  await advance(1_500)
  expect((pageChannel as any)._connection.transport.type).toBe('ws')
  await dieWithCredit(net, server, pageChannel)
  await advance(10_000)
  expect(got).toEqual(inOrder(600))
  expect(closed.map(({ err }) => err)).toEqual(['open', 'open'])
})

test('over ws, a page lets go of a closed channel whose close the server never got once nothing of it is left to replay', async () => {
  const { net, channel } = page('ws')
  const kept = channel(register().id)
  const pageChannel = channel(register().id)
  await advance(500)
  net.losePageFrame(TAG.CLOSE)
  const closing = settled(pageChannel.close({ timeout: 1_000 }))
  await advance(5_000)
  expect(closing.value).toBe(1)
  const connection = (kept as unknown as { _connection: { channels: Map<number, unknown> } })._connection
  expect(connection.channels.size).toBe(2) // the server may lack its close
  await advance(65_000) // its replay's age passes
  expect(connection.channels.size).toBe(1)
})

/** Moves a quiet channel's seqs on by `count` each way, as if `count` more frames had gone each way and arrived. */
function skipSeqs(
  server: { _replayBuffer: ReplayBuffer | null; _lastClientSeq: number },
  pageChannel: unknown,
  count: number,
) {
  const connection = (pageChannel as any)._connection
  const ix = connection.channelIndex.get(pageChannel)
  for (const replay of [server._replayBuffer, replayOf(pageChannel)] as any[]) {
    replay._seq += count
    replay.pushedSeq += count
  }
  server._lastClientSeq += count
  ;(server as any)._pageLastSeq += count
  connection.lastSeqByChannel.set(ix, (connection.lastSeqByChannel.get(ix) ?? 0) + count)
}
const replayOf = (pageChannel: unknown) => {
  const connection = (pageChannel as any)._connection
  return connection.replayBuffers.get(connection.channelIndex.get(pageChannel)) as ReplayBuffer
}
/** The highest seq the page has of what the server sent on the channel. */
const pageHas = (pageChannel: unknown) => {
  const connection = (pageChannel as any)._connection
  return connection.lastSeqByChannel.get(connection.channelIndex.get(pageChannel)) as number
}

describe.each(WIRES)('over %s, a channel whose seqs pass 2^31 and 2^32', (wire) => {
  test.each([2 ** 31, 2 ** 32])(
    'delivers each way in order across %d, answers its ack requests, and has each replay let go of what the other end got',
    async (boundary) => {
      const { channel } = page(wire)
      const server = register<string, string>()
      const serverGot: number[] = []
      server.listen((message) => {
        serverGot.push(Number.parseInt(message))
        return 'server'
      })
      const pageChannel = channel<string, string>(server.id)
      const pageGot: number[] = []
      pageChannel.listen((message) => {
        pageGot.push(Number.parseInt(message))
        return 'page'
      })
      const closed = [closedWith(pageChannel), closedWith(server)]
      await advance(500)
      skipSeqs(server, pageChannel, boundary - 4)
      const answers: { value: unknown }[] = []
      for (let n = 0; n < 8; n++) {
        if (n % 3 === 0) {
          answers.push(
            settled(server.send(String(n), { ack: true })),
            settled(pageChannel.send(String(n), { ack: true })),
          )
          continue
        }
        void server.send(String(n), { ack: false })
        void pageChannel.send(String(n), { ack: false })
      }
      await advance(100)
      expect(pageGot).toEqual(inOrder(8))
      expect(serverGot).toEqual(inOrder(8))
      expect(answers.map(({ value }) => value)).toEqual(['page', 'server', 'page', 'server', 'page', 'server'])
      expect(server._replayBuffer!.seq).toBeGreaterThan(boundary)
      expect(replayOf(pageChannel).seq).toBeGreaterThan(boundary)
      await advance(1_000) // a heartbeat
      expect([server._replayBuffer!.byteLength, replayOf(pageChannel).byteLength]).toEqual([0, 0])
      expect(closed.map(({ err }) => err)).toEqual(['open', 'open'])
    },
  )

  // The wire drops as the sender reaches `dieAt`, with its window in flight: from before the boundary, the reconnect
  // replays across it; from past 2^32, the RECONCILE or RECONCILED names a lastSeq past it.
  const drops = [
    [2 ** 31, 2 ** 31 - 10],
    [2 ** 32, 2 ** 32 - 10],
    [2 ** 32, 2 ** 32 + 10],
  ]

  test.each(drops)(
    'resumes a stream from the server across %d, whose wire drops at %d with its window in flight, without loss',
    async (boundary, dieAt) => {
      const { net, channel } = page(wire)
      const server = register<never, string>()
      const pageChannel = channel<never, string>(server.id)
      const got: number[] = []
      pageChannel.listen((message) => void got.push(Number.parseInt(message)))
      const closed = [closedWith(pageChannel), closedWith(server)]
      await advance(500)
      skipSeqs(server, pageChannel, boundary - 100)
      produce((n) => server.send(text(n), { ack: false }), 400, 1)
      while (server._replayBuffer!.seq < dieAt) await advance(1)
      await dieWithCredit(net, server, pageChannel)
      await advance(50)
      const lost = [pageHas(pageChannel), server._replayBuffer!.seq]
      await advance(10_000)
      // What the reconnect replays starts before the boundary and passes it, or starts past it.
      expect(lost[0]! < boundary).toBe(dieAt < boundary)
      expect(lost[1]!).toBeGreaterThan(Math.max(lost[0]!, boundary))
      expect(got).toEqual(inOrder(400))
      expect(closed.map(({ err }) => err)).toEqual(['open', 'open'])
    },
  )

  test.each(drops)(
    'resumes a stream from the page across %d, whose wire drops at %d with its window in flight, without loss',
    async (boundary, dieAt) => {
      const { net, channel } = page(wire)
      const server = register<string, never>()
      const got: number[] = []
      server.listen((message) => void got.push(Number.parseInt(message)))
      const pageChannel = channel<string, never>(server.id)
      const closed = [closedWith(pageChannel), closedWith(server)]
      await advance(500)
      skipSeqs(server, pageChannel, boundary - 100)
      produce((n) => pageChannel.send(text(n), { ack: false }), 400, 1)
      while (replayOf(pageChannel).seq < dieAt) await advance(1)
      await dieWithCredit(net, pageChannel, server)
      await advance(50)
      const lost = [server._lastClientSeq, replayOf(pageChannel).seq]
      await advance(10_000)
      // What the reconnect replays starts before the boundary and passes it, or starts past it.
      expect(lost[0]! < boundary).toBe(dieAt < boundary)
      expect(lost[1]!).toBeGreaterThan(Math.max(lost[0]!, boundary))
      expect(got).toEqual(inOrder(400))
      expect(closed.map(({ err }) => err)).toEqual(['open', 'open'])
    },
  )

  test('settles an ack request with its answer, from either end, after its requester sent 2^32 more frames', async () => {
    const { channel } = page(wire)
    const server = register<string, string>()
    let serverAnswers!: () => void
    server.listen(() => new Promise<string>((resolve) => (serverAnswers = () => resolve('server'))))
    const pageChannel = channel<string, string>(server.id)
    let pageAnswers!: () => void
    pageChannel.listen(() => new Promise<string>((resolve) => (pageAnswers = () => resolve('page'))))
    await advance(500)
    const asked = [settled(server.send('?', { ack: true })), settled(pageChannel.send('?', { ack: true }))]
    await advance(100)
    skipSeqs(server, pageChannel, 2 ** 32)
    pageAnswers()
    serverAnswers()
    await advance(1_000)
    expect(asked.map(({ value }) => value)).toEqual(['page', 'server'])
  })

  test.each([2 ** 31, 2 ** 32])(
    'closes gracefully from either end at %d, and the server lets each go within a ping round trip',
    async (boundary) => {
      const { channel } = page(wire)
      channel(register().id) // another channel on the page
      const servers = [register(), register()]
      const pages = servers.map((server) => channel(server.id))
      const closed = [...servers, ...pages].map(closedWith)
      await advance(500)
      for (const [n, server] of servers.entries()) skipSeqs(server, pages[n], boundary - 1)
      // The first closes from the server, the second from the page.
      const closing = [settled(servers[0]!.close()), settled(pages[1]!.close())]
      await advance(100)
      expect(closing.map(({ value }) => value)).toEqual([0, 0])
      expect(closed.map(({ err }) => err)).toEqual([undefined, undefined, undefined, undefined])
      await advance(1_500)
      expect(servers.filter((server) => getChannelMux()['channels'].has(server.id))).toEqual([])
    },
  )
})

test.each([2 ** 32 - 4, 2 ** 32 + 4])(
  'over sse, a channel at seq %d keeps whole across the upgrade to a WebSocket, each way in order',
  async (seq) => {
    const { net, channel } = page('sse', { upgrade: true })
    const server = register<string, string>()
    const serverGot: number[] = []
    server.listen((message) => void serverGot.push(Number.parseInt(message)))
    const pageChannel = channel<string, string>(server.id)
    const pageGot: number[] = []
    pageChannel.listen((message) => void pageGot.push(Number.parseInt(message)))
    const closed = [closedWith(pageChannel), closedWith(server)]
    // Once the channel is attached over SSE, as the page opens its WebSocket.
    net.whenPageSends(TAG.PREPARE, () => skipSeqs(server, pageChannel, seq))
    // As the page writes its barrier, each end sends: the server on the old wire, the page once the barrier commits.
    net.whenPageSends(TAG.BARRIER, () => {
      for (let n = 0; n < 8; n++) {
        void server.send(String(n), { ack: false })
        void pageChannel.send(String(n), { ack: false })
      }
    })
    await advance(5_000)
    expect((pageChannel as any)._connection.transport.type).toBe('ws')
    for (let n = 8; n < 16; n++) {
      void server.send(String(n), { ack: false })
      void pageChannel.send(String(n), { ack: false })
    }
    await advance(100)
    expect(pageGot).toEqual(inOrder(16))
    expect(serverGot).toEqual(inOrder(16))
    expect(closed.map(({ err }) => err)).toEqual(['open', 'open'])
  },
)
