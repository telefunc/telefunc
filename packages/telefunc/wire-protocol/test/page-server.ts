export { Net, lengthPrefixed, page, postBody, register }
export type { Traffic, Wire }

import type { Peer } from 'crossws'
import { vi } from 'vitest'

import { ClientChannel } from '../client/channel.js'
import { config } from '../../client/clientConfig.js'
import { ServerChannel } from '../server/channel.js'
import { getChannelMux } from '../server/mux.js'

import { getTelefuncSseChannelHooks } from '../server/sse.js'
import { getTelefuncChannelHooks } from '../server/ws.js'
import { TAG } from '../shared-ws.js'
import { decodeU32 } from '../frame.js'
import { base64urlToUint8Array } from '../base64url.js'

/** One way of a link that carries `rate` bytes a second: what is written to it arrives in order, as fast as that. */
class Rate {
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

/** What one read of the server's event stream hands the page at most, as a socket read does. */
const EVENT_READ_BYTES = 16 * 1024

const until = (at: number) => new Promise<void>((resolve) => setTimeout(resolve, at - Date.now()))

/** What crossed the wire: the page's requests (a WebSocket counts one), and each direction's frame tags in order. */
type Traffic = { requests: number; toServer: number[]; toPage: number[] }

/** One wire of the page's connection: a WebSocket, or an SSE downstream and the POSTs that go with it. */
type Link = {
  /** Nothing either end writes to it arrives, and nothing tells either end. */
  dead: boolean
  /** Its batch POSTs stay in flight. */
  holdingPosts: boolean
  /** How it carries what the server writes to the page, and what the page writes to the server: at once if null. */
  down: Rate | null
  up: Rate | null
  /** Until when what the server writes to the page waits behind a frame `Net.delays` held. */
  delayedUntil: number
  /** Ends it as a network drop does, both ends hearing of it: set by what carries it. */
  drop: () => void
}

/** The network between the page and the server. */
class Net {
  /** The wires the page opened, the latest last. */
  readonly links: Link[] = []
  readonly traffic: Traffic = { requests: 0, toServer: [], toPage: [] }
  /** The page's attempts to open a wire fail. */
  refusing = false
  /** Bytes a second each way of the wires the page opens from now on: 0 carries at once. */
  readonly rate = { down: 0, up: 0 }
  /** How long a frame of a tag takes to reach the page, the ones after it on its wire waiting behind it. */
  delays: Partial<Record<number, number>> = {}
  /** Milliseconds after the page makes it that an upload request reaches the server, as one that opens a connection of
   *  its own does. */
  uploadLag = 0
  /** How long an SSE-batch page's upload request takes to be refused. */
  refusedAfter = 0
  /** How long each batch POST's body takes to end after its frames, which the server reads at once. */
  postMs = 0
  /** The next batch POST carrying a RECONCILE fails, before or after the server reads it. */
  failNextReconcilePost: 'read' | 'unread' | null = null
  /** The body of the next batch POST carrying a RECONCILE ends this many ms after its frames. */
  holdNextReconcilePost: number | null = null
  /** The upload request loses what follows the next RECONCILE it carries, and ends 50 ms later. */
  endUploadAfterNextReconcile = false
  private readonly byConnId = new Map<string, Link>()
  private readonly losing = new Set<number>()
  private readonly onPageGets = new Map<number, (frame: Uint8Array) => void>()
  private readonly onPageSends = new Map<number, (frame: Uint8Array) => void>()
  private readonly onServerSends = new Map<number, (frame: Uint8Array) => void>()
  open(connId?: string): Link {
    const link = {
      dead: false,
      holdingPosts: false,
      down: this.rate.down ? new Rate(this.rate.down) : null,
      up: this.rate.up ? new Rate(this.rate.up) : null,
      delayedUntil: 0,
      drop: () => {},
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
    this.traffic.toServer.push(frame[0]!)
    this.fire(this.onPageSends, frame)
  }
  serverSends(frame: Uint8Array): void {
    this.traffic.toPage.push(frame[0]!)
    this.fire(this.onServerSends, frame)
  }
  /** When `bytes` the server writes on `link` now reach the page: behind what it wrote before, and a frame of `tag` as
   *  long as `delays` says. */
  arrival(link: Link, bytes: number, tag: number | undefined): number {
    const delay = tag === undefined ? 0 : (this.delays[tag] ?? 0)
    link.delayedUntil = Math.max(link.down ? link.down.arrival(bytes) : Date.now(), link.delayedUntil) + delay
    return link.delayedUntil
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

/** `sse-late-upload`: SSE whose upload request reaches the server 500 ms after the request that opens the wire, as one
 *  that opens a connection of its own may. */
type Wire = 'sse' | 'sse-batch' | 'ws' | 'sse-late-upload'

/** A page on `wire`, and the server it talks to. `upgrade`: an SSE page that may move to a WebSocket. */
function page(wire: Wire, { upgrade = false } = {}) {
  const net = new Net()
  if (wire === 'ws' || upgrade) vi.stubGlobal('WebSocket', webSocketTo(net))
  if (wire !== 'ws') config.fetch = sseFetch(net, wire === 'sse-batch')
  if (wire === 'sse-late-upload') net.uploadLag = 500
  const telefuncUrl = `http://${crypto.randomUUID()}.test/_telefunc`
  const connectionKey = crypto.randomUUID()
  return {
    net,
    /** A channel the page opens, such as a call's callback. `maxFrameBytes` as the page's reviver gives it. */
    channel<ClientToServer = unknown, ServerToClient = unknown>(
      channelId: string = crypto.randomUUID(),
      { maxFrameBytes }: { maxFrameBytes?: number } = {},
    ) {
      return new ClientChannel<ClientToServer, ServerToClient>({
        channelId,
        ack: true,
        maxFrameBytes,
        transports: wire === 'ws' ? ['ws'] : upgrade ? ['sse', 'ws'] : ['sse'],
        telefuncUrl,
        connectionKey,
      })
    },
  }
}

/** What the server registers when a call's request arrives. */
function register<ClientToServer = unknown, ServerToClient = unknown>(id?: string) {
  const channel = new ServerChannel<ClientToServer, ServerToClient>({ id, ack: true })
  getChannelMux().registerChannel(channel)
  return channel
}

/** A `fetch` whose far end is the server's SSE hooks, over `net`. `refuseUpload`: a browser that can't stream a request
 *  body (Firefox, Safari) gets a 400 and the page sends batch POSTs. */
function sseFetch(net: Net, refuseUpload: boolean): typeof fetch {
  const sse = getTelefuncSseChannelHooks()
  return (async (url: string, init: RequestInit) => {
    net.traffic.requests++
    const aborted = new Promise<never>((_resolve, reject) =>
      init.signal?.addEventListener('abort', () =>
        reject(new DOMException('The operation was aborted.', 'AbortError')),
      ),
    )
    aborted.catch(() => {})
    const body = init.body as Blob | ReadableStream<Uint8Array>
    if (net.refusing) throw new TypeError('fetch failed')
    if (!(body instanceof Blob)) {
      if (refuseUpload) {
        if (net.refusedAfter) await until(Date.now() + net.refusedAfter)
        return new Response('', { status: 400 })
      }
      if (net.uploadLag) await until(Date.now() + net.uploadLag)
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
    const reconcilePost = !streamResponse && frames.some((frame) => frame[0] === TAG.RECONCILE)
    const fails = reconcilePost ? net.failNextReconcilePost : null
    if (fails) net.failNextReconcilePost = null
    if (fails === 'unread') throw new TypeError('fetch failed')
    if (link.dead || (link.holdingPosts && !streamResponse)) return await aborted
    const held = reconcilePost ? net.holdNextReconcilePost : null
    if (held !== null) net.holdNextReconcilePost = null
    const endsAfter = held ?? (streamResponse ? 0 : net.postMs)
    const request = new Request(url, {
      method: 'POST',
      body: link.up ? carried(bytes, link, link.up) : endsAfter > 0 ? endingAfter(bytes, endsAfter) : bytes,
      duplex: 'half',
    } as RequestInit)
    // A body cut by the link's death fails the server's read, which nothing tells the page.
    const response = await sse.handleRequest(request).catch(() => null)
    if (fails === 'read') throw new TypeError('fetch failed')
    if (link.dead || !response) return await aborted
    const responseBody =
      response.body instanceof ReadableStream ? downstream(response.body, link, net) : (response.body as string)
    return new Response(responseBody, {
      status: response.statusCode,
      headers: { 'Content-Type': response.contentType },
    })
  }) as typeof fetch
}

/** A POST's body on its way to the server over `pipe`: each chunk once the link carried it, and none once it died. */
function carried(bytes: Uint8Array, link: Link, pipe: Rate): ReadableStream<Uint8Array> {
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

/** `bytes` as a body that ends `ms` after them. */
function endingAfter(bytes: Uint8Array, ms: number): ReadableStream<Uint8Array> {
  return new ReadableStream<Uint8Array>({
    start: (controller) => controller.enqueue(bytes),
    pull: async (controller) => {
      await until(Date.now() + ms)
      controller.close()
    },
  })
}

/** An upload request's body on its way to the server: what the page writes once its wire died is lost. */
function uploadThrough(net: Net) {
  let pending = new Uint8Array(0)
  let link: Link | undefined
  let losing = false
  return new TransformStream<Uint8Array, Uint8Array>({
    async transform(chunk, controller) {
      if (losing) return
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
        if (net.endUploadAfterNextReconcile && frame[0] === TAG.RECONCILE) {
          net.endUploadAfterNextReconcile = false
          losing = true
          setTimeout(() => controller.terminate(), 50)
          return
        }
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
  let dropped = false
  return new ReadableStream<Uint8Array>({
    start(controller) {
      link.drop = () => {
        if (dropped) return
        dropped = true
        controller.close()
        void reader.cancel()
      }
    },
    async pull(controller) {
      for (;;) {
        const { done, value } = await reader.read()
        if (dropped) return
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
          const bytes = encoder.encode(`${event}\n\n`)
          for (let offset = 0; offset < bytes.length; offset += EVENT_READ_BYTES) {
            const last = offset + EVENT_READ_BYTES >= bytes.length
            const chunk = bytes.subarray(offset, offset + EVENT_READ_BYTES)
            const at = net.arrival(link, chunk.length, last ? frame?.[0] : undefined)
            if (at > Date.now()) await until(at)
            if (link.dead || dropped) break
            if (last && frame) net.pageGets(frame)
            controller.enqueue(chunk)
            delivered = true
          }
        }
        if (delivered) return
      }
    },
    cancel: (reason) => (link.dead ? undefined : reader.cancel(reason)),
  })
}

/** A `WebSocket` whose far end is the server's crossws hooks, over `net`. */
function webSocketTo(net: Net): new (url: string) => unknown {
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
        const at = net.arrival(this.link, frame.byteLength, frame[0])
        if (at > Date.now()) {
          void until(at).then(() => {
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
      net.traffic.requests++
      this.link.drop = () => this.end(1006)
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

/** An SSE POST's Blob body: its metadata header, then its frames. */
async function postBody(
  blob: Blob,
): Promise<{ metadata: { connId: string; streamResponse?: boolean }; frames: Uint8Array[] }> {
  const [metadata, ...frames] = lengthPrefixed(new Uint8Array(await blob.arrayBuffer())).map(({ frame }) => frame)
  return { metadata: JSON.parse(new TextDecoder().decode(metadata)), frames }
}
