// A page registers a new channel by naming it in a RECONCILE: the server may not have registered it yet (a call's callback
// whose request is still on its way, or never arrives), and the page may be moving to a WebSocket. These drive the real
// ClientChannel against the real server over each wire.

import { afterEach, beforeEach, describe, expect, test, vi } from 'vitest'
import type { Peer } from 'crossws'

import { ClientChannel } from './channel.js'
import { config } from '../../client/clientConfig.js'
import { ServerChannel } from '../server/channel.js'
import { getChannelMux } from '../server/mux.js'
import { getTelefuncSseChannelHooks } from '../server/sse.js'
import { getTelefuncChannelHooks } from '../server/ws.js'
import { TAG } from '../shared-ws.js'
import { decodeU32 } from '../frame.js'
import { base64urlToUint8Array } from '../base64url.js'
import { getServerConfig } from '../../node/server/serverConfig.js'

type Wire = 'sse' | 'sse-batch' | 'ws'
const WIRES: Wire[] = ['sse', 'sse-batch', 'ws']

/** What crossed the wire: the page's requests (a WebSocket counts one), and each direction's frame tags in order. */
type Traffic = { requests: number; toServer: number[]; toPage: number[] }

// Before the first reconcile, so every RECONCILED offers a page the WebSocket.
getTelefuncChannelHooks()
const { connectTtl } = getServerConfig().channel

beforeEach(() => {
  vi.useFakeTimers()
})

afterEach(() => {
  vi.useRealTimers()
  vi.unstubAllGlobals()
  delete config.fetch
})

/** A page on `wire`, and the server it talks to. `upgrade`: an SSE page that may move to a WebSocket, whose READY
 *  reaches the page `readyAfter` ms after the server sent it. */
function page(wire: Wire, { upgrade = false, readyAfter = 0 } = {}) {
  const traffic: Traffic = { requests: 0, toServer: [], toPage: [] }
  /** Ends a wire as a network drop does, the latest last. */
  const cuts: (() => void)[] = []
  if (wire === 'ws' || upgrade) vi.stubGlobal('WebSocket', webSocketTo(traffic, cuts, readyAfter))
  if (wire !== 'ws') config.fetch = sseServer(traffic, wire === 'sse-batch', cuts)
  const telefuncUrl = `http://${crypto.randomUUID()}.test/_telefunc`
  const connectionKey = crypto.randomUUID()
  return {
    traffic,
    cut: () => cuts.at(-1)!(),
    /** A channel the page opens, such as a call's callback. */
    channel<ClientToServer = unknown, ServerToClient = unknown>(channelId: string = crypto.randomUUID()) {
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
function register<ClientToServer = unknown, ServerToClient = unknown>(id: string = crypto.randomUUID()) {
  const channel = new ServerChannel<ClientToServer, ServerToClient>({ id, ack: true })
  getChannelMux().registerChannel(channel)
  return channel
}

function sseServer(traffic: Traffic, refuseUpload: boolean, cuts: (() => void)[]): typeof fetch {
  const sse = getTelefuncSseChannelHooks()
  return (async (url: string, init: RequestInit) => {
    traffic.requests++
    const body = init.body as Blob | ReadableStream<Uint8Array>
    // A browser that can't stream a request body (Firefox, Safari) gets a 400 and the page sends batch POSTs.
    if (!(body instanceof Blob) && refuseUpload) return new Response('', { status: 400 })
    // Bytes rather than the Blob: a Request reads a Blob body on a later event-loop turn, which fake timers don't give.
    let logged: Uint8Array | ReadableStream<Uint8Array>
    if (body instanceof Blob) {
      logged = new Uint8Array(await body.arrayBuffer())
      for (const frame of lengthPrefixed(logged)) traffic.toServer.push(frame[0]!)
    } else {
      logged = body.pipeThrough(framesThrough((frame) => traffic.toServer.push(frame[0]!)))
    }
    const request = new Request(url, { method: 'POST', body: logged, duplex: 'half' } as RequestInit)
    const response = (await sse.handleRequest(request))!
    const responseBody =
      response.body instanceof ReadableStream
        ? response.body.pipeThrough(eventsThrough((frame) => traffic.toPage.push(frame[0]!), cuts))
        : (response.body as string)
    return new Response(responseBody, {
      status: response.statusCode,
      headers: { 'Content-Type': response.contentType },
    })
  }) as typeof fetch
}

/** The frames after an SSE POST's metadata header. */
function lengthPrefixed(bytes: Uint8Array): Uint8Array[] {
  const frames: Uint8Array[] = []
  let offset = 0
  while (offset + 4 <= bytes.length) {
    const length = decodeU32(bytes.subarray(offset, offset + 4) as Uint8Array<ArrayBuffer>)
    frames.push(bytes.subarray(offset + 4, offset + 4 + length))
    offset += 4 + length
  }
  return frames.slice(1)
}

/** Passes an upload POST's body through, calling `onFrame` with each frame after its metadata header. */
function framesThrough(onFrame: (frame: Uint8Array) => void) {
  let pending = new Uint8Array(0)
  let seenMetadata = false
  return new TransformStream<Uint8Array, Uint8Array>({
    transform(chunk, controller) {
      controller.enqueue(chunk)
      const joined = new Uint8Array(pending.length + chunk.length)
      joined.set(pending)
      joined.set(chunk, pending.length)
      let offset = 0
      while (offset + 4 <= joined.length) {
        const length = decodeU32(joined.subarray(offset, offset + 4) as Uint8Array<ArrayBuffer>)
        if (offset + 4 + length > joined.length) break
        if (seenMetadata) onFrame(joined.subarray(offset + 4, offset + 4 + length))
        seenMetadata = true
        offset += 4 + length
      }
      pending = joined.slice(offset)
    },
  })
}

/** Passes an SSE downstream through, calling `onFrame` with each frame it carries. */
function eventsThrough(onFrame: (frame: Uint8Array) => void, cuts: (() => void)[]) {
  const decoder = new TextDecoder()
  let text = ''
  return new TransformStream<Uint8Array, Uint8Array>({
    start: (controller) => void cuts.push(() => controller.terminate()),
    transform(chunk, controller) {
      controller.enqueue(chunk)
      text += decoder.decode(chunk, { stream: true })
      let end: number
      while ((end = text.indexOf('\n\n')) !== -1) {
        const event = text.slice(0, end)
        text = text.slice(end + 2)
        if (event.startsWith('data: ')) onFrame(base64urlToUint8Array(event.slice('data: '.length)))
      }
    },
  })
}

/** A `WebSocket` whose far end is the server's crossws hooks. */
function webSocketTo(traffic: Traffic, cuts: (() => void)[], readyAfter: number) {
  const hooks = getTelefuncChannelHooks()
  return class {
    static readonly OPEN = 1
    readyState = 0
    binaryType = 'blob'
    onopen: (() => void) | null = null
    onmessage: ((event: { data: ArrayBuffer }) => void) | null = null
    onclose: (() => void) | null = null
    onerror: (() => void) | null = null
    private readonly peer = {
      context: {},
      send: (frame: Uint8Array) => {
        traffic.toPage.push(frame[0]!)
        const data = frame.slice().buffer
        const deliver = () => this.onmessage?.({ data })
        if (frame[0] === TAG.READY && readyAfter > 0) setTimeout(deliver, readyAfter)
        else queueMicrotask(deliver)
      },
      terminate: () => this.close(),
    } as unknown as Peer
    constructor(_url: string) {
      traffic.requests++
      cuts.push(() => this.end(1006))
      queueMicrotask(async () => {
        await hooks.open!(this.peer)
        this.readyState = 1
        this.onopen?.()
      })
    }
    send(data: Uint8Array) {
      traffic.toServer.push(data[0]!)
      const frame = data.slice()
      void hooks.message!(this.peer, { uint8Array: () => frame } as never)
    }
    close() {
      this.end(1000)
    }
    private end(code: number) {
      if (this.readyState === 3) return
      this.readyState = 3
      void hooks.close!(this.peer, { code } as never)
      this.onclose?.()
    }
  }
}

describe.each(WIRES)('over %s', (wire) => {
  test("while an aborted call's callback never registers, the next call's callback opens at once and stays open", async () => {
    const { channel } = page(wire)
    // A search box: the first keystroke's call is aborted before its request leaves, so the server never registers
    // its callback.
    channel().abort()
    await vi.advanceTimersByTimeAsync(100)
    // The next keystroke's call reaches the server.
    const server = register()
    let closedWith: unknown = 'open'
    server.onClose((err) => void (closedWith = err))
    const callback = channel(server.id)
    const createdAt = Date.now()
    let openedAfter = -1
    callback.onOpen(() => void (openedAfter = Date.now() - createdAt))
    await vi.advanceTimersByTimeAsync(3 * connectTtl)
    expect(openedAfter).toBeGreaterThanOrEqual(0)
    expect(openedAfter).toBeLessThan(connectTtl / 10)
    expect(closedWith).toBe('open')
  })

  test("a callback whose call reaches the server after the RECONCILE naming it attaches, and doesn't hold the channel named with it", async () => {
    const { channel } = page(wire)
    const returned = register() // one a call returned: the server registered it before the page saw it
    let returnedOpen = false
    channel(returned.id).onOpen(() => void (returnedOpen = true))
    const callbackId = crypto.randomUUID()
    const callback = channel<string, string>(callbackId)
    callback.listen((question) => `answer to ${question}`)
    void callback.send('sent before its call arrived')
    await vi.advanceTimersByTimeAsync(200)
    expect(returnedOpen).toBe(true)
    // Its call arrives, and the telefunction calls the callback.
    const server = register<string, string>(callbackId)
    const received: string[] = []
    server.listen((message) => void received.push(message))
    const answer = server.send('question', { ack: true })
    await vi.advanceTimersByTimeAsync(200)
    expect(await answer).toBe('answer to question')
    expect(received).toEqual(['sent before its call arrived'])
  })

  test('a callback whose call never reaches the server is released with the same error as before, connectTtl after the RECONCILE naming it', async () => {
    const { channel } = page(wire)
    // A call lost in a drop, then the next one, whose request is lost too.
    channel()
    await vi.advanceTimersByTimeAsync(100)
    const callback = channel()
    const createdAt = Date.now()
    let closed: { err: unknown; after: number } | null = null
    callback.onClose((err) => void (closed = { err, after: Date.now() - createdAt }))
    await vi.advanceTimersByTimeAsync(3 * connectTtl)
    expect(closed!.err).toMatchObject({
      name: 'NetworkError',
      message: 'Channel not acknowledged by server after reconnect',
      isChannel: true,
    })
    expect(closed!.after).toBeGreaterThanOrEqual(connectTtl)
    expect(closed!.after).toBeLessThan(connectTtl + connectTtl / 10)
  })

  test("an aborted call's callback whose request reaches the server after the RECONCILE naming it gets the abort", async () => {
    const { channel } = page(wire)
    channel(register().id)
    await vi.advanceTimersByTimeAsync(200)
    const callbackId = crypto.randomUUID()
    // Its call is aborted once its request has left.
    channel(callbackId).abort()
    await vi.advanceTimersByTimeAsync(200)
    const server = register(callbackId)
    let closedWith: unknown = 'open'
    server.onClose((err) => void (closedWith = err))
    await vi.advanceTimersByTimeAsync(200)
    expect(closedWith).toBeUndefined()
  })

  test('a callback the page aborts while the server awaits it gets the abort once its call arrives', async () => {
    const { channel } = page(wire)
    channel(register().id)
    await vi.advanceTimersByTimeAsync(200)
    const callbackId = crypto.randomUUID()
    const callback = channel(callbackId)
    await vi.advanceTimersByTimeAsync(200)
    callback.abort()
    channel(register().id) // the page's next RECONCILE
    await vi.advanceTimersByTimeAsync(200)
    const server = register(callbackId)
    let closedWith: unknown = 'open'
    server.onClose((err) => void (closedWith = err))
    await vi.advanceTimersByTimeAsync(200)
    expect(closedWith).toBeUndefined()
  })

  test('what a callback sends before and after a reconnect reaches it in order once its call arrives', async () => {
    const { channel, cut } = page(wire)
    channel(register().id)
    await vi.advanceTimersByTimeAsync(200)
    const callbackId = crypto.randomUUID()
    const callback = channel<string, string>(callbackId)
    void callback.send('before the drop', { ack: false })
    await vi.advanceTimersByTimeAsync(200)
    cut()
    await vi.advanceTimersByTimeAsync(2_000)
    void callback.send('after the drop', { ack: false })
    channel(register().id) // its RECONCILE carries what the page queued
    await vi.advanceTimersByTimeAsync(200)
    const server = register<string, string>(callbackId)
    const received: string[] = []
    server.listen((message) => void received.push(message))
    await vi.advanceTimersByTimeAsync(200)
    expect(received).toEqual(['before the drop', 'after the drop'])
  })
})

describe.each(['sse', 'sse-batch'] as const)('over %s', (wire) => {
  test('a page moves to a WebSocket while the server awaits a callback, and the abort it holds reaches the callback there', async () => {
    const { channel } = page(wire, { upgrade: true })
    const server = register<string, string>()
    const open = channel<string, string>(server.id)
    const received: string[] = []
    open.listen((message) => void received.push(message))
    const callbackId = crypto.randomUUID()
    channel(callbackId).abort()
    await vi.advanceTimersByTimeAsync(1_000)
    const connection = (open as any)._connection
    expect(connection.transport.type).toBe('ws')
    const callback = register(callbackId)
    let closedWith: unknown = 'open'
    callback.onClose((err) => void (closedWith = err))
    await vi.advanceTimersByTimeAsync(200)
    expect(closedWith).toBeUndefined()
    void server.send('over the WebSocket', { ack: false })
    await vi.advanceTimersByTimeAsync(200)
    expect(received).toEqual(['over the WebSocket'])
  })

  test("a search box whose aborted calls' callbacks never register doesn't keep the page off the WebSocket", async () => {
    const { channel } = page(wire, { upgrade: true })
    const connection = (channel(register().id) as any)._connection
    // A keystroke a second, each aborting its call before its request leaves.
    for (let keystroke = 0; keystroke < 8; keystroke++) {
      channel().abort()
      await vi.advanceTimersByTimeAsync(1_000)
    }
    expect(connection.transport.type).toBe('ws')
  })
})

describe('with every channel registered, a page sends and gets the frames it did before', () => {
  const tags = (list: number[]) => list.filter((tag) => tag !== TAG.PING && tag !== TAG.PONG)
  // A returned channel the page writes to at once, the server's reply, then a second returned channel.
  async function run(wire: Wire) {
    const { channel, traffic } = page(wire)
    const first = register<string, string>()
    const received: string[] = []
    first.listen((message) => void received.push(message))
    const pageFirst = channel<string, string>(first.id)
    void pageFirst.send('hi', { ack: false })
    await vi.advanceTimersByTimeAsync(200)
    void first.send('hello', { ack: false })
    const second = register()
    channel(second.id)
    await vi.advanceTimersByTimeAsync(1_000)
    expect(received).toEqual(['hi'])
    return { requests: traffic.requests, toServer: tags(traffic.toServer), toPage: tags(traffic.toPage) }
  }
  test.each(WIRES)('over %s', async (wire) => {
    expect(await run(wire)).toEqual(EXPECTED_TRAFFIC[wire])
  })
})

const { RECONCILE, RECONCILED, TEXT, WINDOW, MSG_WINDOW, SENT, BDP_PING, BDP_PING_ACK, STREAM_REQUEST_OPEN_ACK } = TAG
/** As recorded before initial channels the server hasn't registered were answered at once. */
const EXPECTED_TRAFFIC: Record<Wire, Traffic> = {
  sse: {
    requests: 2,
    toServer: [
      RECONCILE,
      TEXT,
      TEXT,
      BDP_PING_ACK,
      WINDOW,
      MSG_WINDOW,
      SENT,
      RECONCILE,
      BDP_PING,
      WINDOW,
      MSG_WINDOW,
      SENT,
    ],
    toPage: [
      STREAM_REQUEST_OPEN_ACK,
      WINDOW,
      MSG_WINDOW,
      SENT,
      BDP_PING,
      RECONCILED,
      TEXT,
      WINDOW,
      MSG_WINDOW,
      SENT,
      RECONCILED,
      BDP_PING_ACK,
    ],
  },
  'sse-batch': {
    requests: 5,
    toServer: [
      RECONCILE,
      TEXT,
      TEXT,
      BDP_PING_ACK,
      WINDOW,
      WINDOW,
      MSG_WINDOW,
      SENT,
      RECONCILE,
      BDP_PING,
      WINDOW,
      WINDOW,
      MSG_WINDOW,
      SENT,
    ],
    toPage: [WINDOW, MSG_WINDOW, SENT, BDP_PING, RECONCILED, TEXT, WINDOW, MSG_WINDOW, SENT, BDP_PING_ACK, RECONCILED],
  },
  ws: {
    requests: 1,
    toServer: [RECONCILE, TEXT, WINDOW, MSG_WINDOW, SENT, BDP_PING_ACK, RECONCILE, BDP_PING, WINDOW, MSG_WINDOW, SENT],
    toPage: [WINDOW, MSG_WINDOW, SENT, RECONCILED, BDP_PING, TEXT, WINDOW, MSG_WINDOW, SENT, RECONCILED, BDP_PING_ACK],
  },
}
