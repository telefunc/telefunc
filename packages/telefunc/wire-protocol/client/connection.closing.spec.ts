// A channel's end across a wire that dies without a word: the page's close request, close acknowledgement or abort, and
// the answers that complete a close, replay after the reconnect as its data does. These drive the real ClientChannel
// against the real server over each wire.

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
import { config as serverConfig } from '../../node/server/serverConfig.js'

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

/** One wire of the page's connection: a WebSocket, or an SSE downstream and the POSTs that go with it. */
type Link = {
  /** Nothing either end writes to it arrives, and nothing tells either end. */
  dead: boolean
  /** Its batch POSTs stay in flight. */
  holdingPosts: boolean
}

/** The network between the page and the server. */
class Net {
  readonly links: Link[] = []
  private readonly byConnId = new Map<string, Link>()
  private readonly onPageGets = new Map<number, () => void>()
  private readonly onPageSends = new Map<number, () => void>()
  open(connId?: string): Link {
    const link = { dead: false, holdingPosts: false }
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
  /** Runs `then` once, as the page receives its next frame of `tag`: that frame still arrives. */
  whenPageGets(tag: number, then: () => void): void {
    this.onPageGets.set(tag, then)
  }
  /** Runs `then` once, as the page writes its next frame of `tag` to the wire: that frame is lost if it kills it. */
  whenPageSends(tag: number, then: () => void): void {
    this.onPageSends.set(tag, then)
  }
  pageGets(frame: Uint8Array): void {
    this.fire(this.onPageGets, frame)
  }
  pageSends(frame: Uint8Array): void {
    this.fire(this.onPageSends, frame)
  }
  private fire(hooks: Map<number, () => void>, frame: Uint8Array): void {
    const then = hooks.get(frame[0]!)
    if (!then) return
    hooks.delete(frame[0]!)
    then()
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
    const [metadata, ...frames] = lengthPrefixed(bytes)
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
    const response = (await sse.handleRequest(new Request(url, { method: 'POST', body: bytes })))!
    const responseBody =
      response.body instanceof ReadableStream ? downstream(response.body, link, net) : (response.body as string)
    return new Response(responseBody, {
      status: response.statusCode,
      headers: { 'Content-Type': response.contentType },
    })
  }) as typeof fetch
}

/** `[u32 length][bytes]` chunks: an SSE POST's metadata header, then its frames. */
function lengthPrefixed(bytes: Uint8Array): Uint8Array[] {
  const chunks: Uint8Array[] = []
  let offset = 0
  while (offset + 4 <= bytes.length) {
    const length = decodeU32(bytes.subarray(offset, offset + 4) as Uint8Array<ArrayBuffer>)
    chunks.push(bytes.subarray(offset + 4, offset + 4 + length))
    offset += 4 + length
  }
  return chunks
}

/** An upload request's body on its way to the server: what the page writes once its wire died is lost. */
function uploadThrough(net: Net) {
  let pending = new Uint8Array(0)
  let link: Link | undefined
  return new TransformStream<Uint8Array, Uint8Array>({
    transform(chunk, controller) {
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
          if (link.dead) continue
          if (event.startsWith('data: ')) net.pageGets(base64urlToUint8Array(event.slice('data: '.length)))
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
      send: (frame: Uint8Array) => {
        if (this.link.dead) return
        net.pageGets(frame)
        const data = frame.slice().buffer
        queueMicrotask(() => this.onmessage?.({ data }))
      },
      terminate: () => this.end(),
    } as unknown as Peer
    constructor(_url: string) {
      queueMicrotask(async () => {
        await hooks.open!(this.peer)
        this.readyState = 1
        this.onopen?.()
      })
    }
    send(data: Uint8Array) {
      const frame = data.slice()
      if (!this.link.dead) net.pageSends(frame)
      if (this.link.dead) return
      void hooks.message!(this.peer, { uint8Array: () => frame } as never)
    }
    close() {
      this.end()
    }
    private end() {
      if (this.readyState === 3) return
      this.readyState = 3
      if (this.link.dead) return
      void hooks.close!(this.peer, { code: 1000 } as never)
      this.onclose?.()
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

  test('a page close whose request goes down with a dying wire behind a message over the replay budget completes on both ends after the reconnect', async () => {
    serverConfig.channel = { pingInterval: 1_000, clientReplayBuffer: 1_024 }
    ;(getChannelMux() as unknown as { resolvedOptions: unknown }).resolvedOptions = null
    const { net, channel } = page(wire)
    channel(register().id) // another channel on the page
    const server = register<string, never>()
    const pageChannel = channel<string, never>(server.id)
    const serverClosed = closedWith(server)
    await advance(500)
    net.whenPageSends(TAG.TEXT, () => net.die())
    void pageChannel.send('x'.repeat(2_000), { ack: false }) // lost with the wire, and too big to replay
    const closing = settled(pageChannel.close({ timeout: 20_000 }))
    await advance(10_000)
    expect(closing.value).toBe(0)
    expect(serverClosed.err).toBeUndefined()
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
