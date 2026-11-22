// The real Node server and the real page, with a fetch wrapper that stands for what is between them, for the twins of
// the scripted server's rows (ClientConnection.marked.spec.ts): a page that breaks the protocol, and a proxy that
// rewrites the URL or a refusal's `Content-Type`. Each spec file serves one set of transports (`useRealServer`).

export {
  cutIn,
  findText,
  framesOf,
  framesStart,
  open,
  opens,
  pair,
  sends,
  textOf,
  until,
  delay,
  uploads,
  useRealServer,
  rewriting,
  withholding,
  withoutWire,
  world,
}

import { afterAll, afterEach, beforeAll } from 'vitest'
import { createServer, type Server } from 'node:http'
import { Telefunc } from '../../../serve/node.js'
import { config as serverConfig, type ChannelConfigUser } from '../../../node/server/serverConfig.js'
import { base64urlToUint8Array, uint8ArrayToBase64url } from '../../wire/base64url.js'
import { decodeU32 } from '../../wire/envelope.js'
import { decode } from '../../wire/frame.js'
import { SEQ_OFFSET, readU16, readU32 } from '../../wire/header.js'
import { TAG } from '../../wire/tags.js'
import { getChannelMux } from '../server/ChannelMux.js'
import { ServerChannel } from '../server/ServerChannel.js'
import { ClientChannel } from './ClientChannel.js'

type Kind = 'open' | 'post' | 'upload'
/** What the wrapper does to one request of `kind` that carries `text`, or a frame tagged `carries`, where given, and to
 *  its answer. */
type Hook = {
  kind: Kind
  text?: string
  carries?: number
  url?: (url: string) => string
  body?: (bytes: Uint8Array<ArrayBuffer>) => BlobPart[]
  upload?: (body: ReadableStream<Uint8Array>) => ReadableStream<Uint8Array>
  contentType?: (contentType: string) => string
  /** The event stream as the page reads it. */
  stream?: (body: ReadableStream<Uint8Array>) => ReadableStream<Uint8Array>
  /** The request reaches the server, and its answer never reaches the page. */
  held?: true
  /** The request waits this long before it reaches the server, as on a slow link. */
  delayMs?: number
}
const world: {
  hooks: Hook[]
  /** Whether the page's streaming upload is taken: Node's fetch can, a browser over HTTP/1.1 can't. */
  uploads: boolean
  requests: { kind: Kind; bytes: number; status: number; contentType: string | null }[]
  /** Each request as it went, by its kind: a streaming upload is answered only at its end. */
  started: Kind[]
  /** The text messages batch POSTs carried, as they went. */
  posted: string[]
  /** Each batch POST a hook changed, as it went: `again` sends it once more, past the hooks, whatever the page aborted;
   *  `status` is its own answer's, once it came. */
  hooked: { again: () => Promise<Response>; status: number | null }[]
  sockets: number
  /** Each request whose body streamed (chunked), as its connection closed: the status the server had written, null
   *  where it had written none. */
  closedStreaming: (number | null)[]
  /** What the page's WebSockets send in place of each message, where set. */
  wsSends: ((message: Uint8Array<ArrayBuffer>) => Uint8Array<ArrayBuffer>[]) | null
  telefuncUrl: string
} = {
  hooks: [],
  uploads: false,
  requests: [],
  started: [],
  posted: [],
  hooked: [],
  sockets: 0,
  closedStreaming: [],
  wsSends: null,
  telefuncUrl: '',
}

/** `frame`'s channel and seq, where it carries `text`. */
function textOf(frame: Uint8Array<ArrayBuffer>, text: string): { ix: number; seq: number } | null {
  if (frame[0] !== TAG.TEXT) return null
  const decoded = decode(frame)
  if (decoded.tag !== TAG.TEXT || decoded.text !== JSON.stringify(text)) return null
  return { ix: readU16(frame, 1), seq: readU32(frame, SEQ_OFFSET) }
}

/** The text messages a batch body carries. */
function textsOf(body: Uint8Array<ArrayBuffer>): string[] {
  return Array.from(framesOf(body)).flatMap(({ frame }) => {
    if (frame[0] !== TAG.TEXT) return []
    const decoded = decode(frame)
    return decoded.tag === TAG.TEXT ? [String(JSON.parse(decoded.text))] : []
  })
}

/** Where a batch body's frame that carries `text` ends, and its channel and seq; null where none does. */
function findText(body: Uint8Array<ArrayBuffer>, text: string): { end: number; ix: number; seq: number } | null {
  for (const { frame, end } of framesOf(body)) {
    const found = textOf(frame, text)
    if (found !== null) return { end, ...found }
  }
  return null
}

/** A batch body's frames, each with where it ends in the body. */
function* framesOf(body: Uint8Array<ArrayBuffer>): Generator<{ frame: Uint8Array<ArrayBuffer>; end: number }> {
  let at = 4 + decodeU32(body.subarray(0, 4))
  while (at + 4 <= body.length) {
    const end = at + 4 + decodeU32(body.subarray(at, at + 4))
    yield { frame: body.subarray(at + 4, end), end }
    at = end
  }
}

/** Where a batch body's frames start, after its metadata. */
const framesStart = (body: Uint8Array<ArrayBuffer>) => 4 + decodeU32(body.subarray(0, 4))

/** Whether a batch body carries a frame tagged `tag`. */
const carriesTag = (body: Uint8Array<ArrayBuffer>, tag: number) =>
  Array.from(framesOf(body)).some(({ frame }) => frame[0] === tag)

/** The hook for this request, taken from the queue: the first of its kind, of its text where it names one. */
async function takeHook(
  kind: Kind,
  body: unknown,
): Promise<{ hook: Hook; bytes: Uint8Array<ArrayBuffer> | null; taken: boolean } | null> {
  const bytes = body instanceof Blob ? new Uint8Array(await body.arrayBuffer()) : null
  const at = world.hooks.findIndex(
    (hook) =>
      hook.kind === kind &&
      (hook.text === undefined || (bytes !== null && findText(bytes, hook.text) !== null)) &&
      (hook.carries === undefined || (bytes !== null && carriesTag(bytes, hook.carries))),
  )
  const hook = world.hooks[at]
  if (hook === undefined) return bytes === null ? null : { hook: { kind }, bytes, taken: false }
  world.hooks.splice(at, 1)
  return { hook, bytes, taken: true }
}

const realFetch = globalThis.fetch
async function proxyFetch(input: RequestInfo | URL, init?: RequestInit): Promise<Response> {
  const body = init?.body
  const kind: Kind =
    new Headers(init?.headers).get('accept') === 'text/event-stream'
      ? 'open'
      : body instanceof ReadableStream
        ? 'upload'
        : 'post'
  if (kind === 'upload' && !world.uploads) throw new TypeError('Failed to fetch')
  const taken = await takeHook(kind, body)
  const hook = taken?.hook ?? { kind }
  const bytes = taken?.bytes ?? null
  const url = String(input instanceof Request ? input.url : input)
  const sent =
    bytes !== null
      ? new Blob(hook.body ? hook.body(bytes) : [bytes])
      : body instanceof ReadableStream && hook.upload
        ? hook.upload(body)
        : body
  world.started.push(kind)
  if (kind === 'post' && bytes !== null) world.posted.push(...textsOf(bytes))
  const target = hook.url ? hook.url(url) : url
  const hooked: (typeof world.hooked)[number] | null =
    taken?.taken && sent instanceof Blob
      ? { again: () => realFetch(target, { ...init, body: sent, signal: null }), status: null }
      : null
  if (hooked !== null) world.hooked.push(hooked)
  if (hook.held) return heldAnswer(realFetch(target, { ...init, body: sent, signal: null }), init?.signal)
  if (hook.delayMs !== undefined) await delay(hook.delayMs)
  const response = await realFetch(target, { ...init, body: sent })
  if (hooked !== null) hooked.status = response.status
  const contentType = response.headers.get('content-type')
  world.requests.push({ kind, bytes: bytes?.byteLength ?? 0, status: response.status, contentType })
  if (hook.stream && response.body) {
    return new Response(hook.stream(response.body), { status: response.status, headers: response.headers })
  }
  if (!hook.contentType || contentType === null) return response
  return new Response(response.body, {
    status: response.status,
    headers: { 'content-type': hook.contentType(contentType) },
  })
}

/** A request whose answer the page never gets: it settles only as the page aborts it. */
function heldAnswer(sent: Promise<Response>, signal: AbortSignal | null | undefined): Promise<Response> {
  void sent.catch(() => {})
  return new Promise((_, reject) =>
    signal?.addEventListener('abort', () => reject(new DOMException('aborted', 'AbortError')), { once: true }),
  )
}

/** Counts the WebSockets the page makes, and sends what `world.wsSends` makes of each message. */
const RealWebSocket = globalThis.WebSocket
class CountedWebSocket extends RealWebSocket {
  constructor(url: string | URL, protocols?: string | string[]) {
    super(url, protocols)
    world.sockets++
  }
  override send(data: string | Blob | ArrayBufferLike | ArrayBufferView): void {
    if (typeof data === 'string' || data instanceof Blob) return super.send(data)
    const message = ArrayBuffer.isView(data)
      ? new Uint8Array(data.buffer, data.byteOffset, data.byteLength).slice()
      : new Uint8Array(data).slice()
    for (const part of world.wsSends?.(message) ?? [message]) super.send(part)
  }
}

/** The real Node server for this spec file, serving `transports`, and the wrapper and the counted WebSocket in place;
 *  call it at the file's top, before any request: the server's channel config is set once. */
function useRealServer(transports: ('sse' | 'ws')[], channel: Omit<ChannelConfigUser, 'transports'> = {}): void {
  serverConfig.channel = { ...channel, transports }
  let server: Server
  beforeAll(async () => {
    globalThis.fetch = proxyFetch
    globalThis.WebSocket = CountedWebSocket
    const telefunc = new Telefunc()
    server = createServer((req, res) => {
      if (req.headers['transfer-encoding'] === 'chunked') {
        res.on('close', () => void world.closedStreaming.push(res.headersSent ? res.statusCode : null))
      }
      void telefunc.serve({ req, res })
    })
    telefunc.installWebSocket(server)
    await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', resolve))
    const address = server.address()
    if (address === null || typeof address === 'string') throw new Error('no port')
    world.telefuncUrl = `http://127.0.0.1:${address.port}/_telefunc`
  })
  afterEach(() => {
    Object.assign(world, {
      hooks: [],
      uploads: false,
      requests: [],
      started: [],
      posted: [],
      hooked: [],
      sockets: 0,
      closedStreaming: [],
      wsSends: null,
    })
  })
  afterAll(() => {
    globalThis.fetch = realFetch
    globalThis.WebSocket = RealWebSocket
    server.closeAllConnections()
    server.close()
  })
}

/** Resolves once `done()` holds, checked every 10 ms; rejects after `ms`. */
async function until(done: () => boolean, ms = 5_000): Promise<void> {
  const deadline = performance.now() + ms
  while (!done()) {
    if (performance.now() > deadline) throw new Error('timed out waiting')
    await new Promise((resolve) => setTimeout(resolve, 10))
  }
}

const delay = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms))

/** A server channel and the page's end of it, on the page's connection `connectionKey`, over `transports`. */
function pair(connectionKey: string, transports: ('sse' | 'ws')[] = ['sse']) {
  const channel = new ServerChannel<string, string>()
  getChannelMux().registerChannel(channel)
  const got: string[] = []
  channel.listen((message) => void got.push(message))
  const page = new ClientChannel<string, string>({
    channelId: channel.id,
    connection: { telefuncUrl: world.telefuncUrl, transports, idleTimeout: 0, connectionKey },
  })
  void page.opened.catch(() => {})
  const ended: Error[] = []
  page.onClose((err) => void ended.push(err ?? new Error('closed with no error')))
  return { channel, page, got, ended }
}

/** `count` channels on one connection over SSE, opened. */
async function open(count: number) {
  const connectionKey = crypto.randomUUID()
  const pairs = Array.from({ length: count }, () => pair(connectionKey))
  await Promise.all(pairs.map(({ page }) => page.opened))
  return pairs
}

const opens = () => world.started.filter((kind) => kind === 'open').length
const uploads = () => world.started.filter((kind) => kind === 'upload').length
const sends = (page: ClientChannel<string, string>, message: string) => void page.send(message).catch(() => {})

/** An event stream hook that keeps from the page each event whose frame `withheld` names, and `cut()`, which ends the
 *  stream as a dropped connection does. */
function withholding(withheld: (frame: Uint8Array<ArrayBuffer>) => boolean) {
  return rewriting((frame) => (withheld(frame) ? null : frame))
}

/** An event stream hook that hands the page, for each event, the frame `handed` gives for its frame, or none for null,
 *  and `cut()`, which ends the stream as a dropped connection does. */
function rewriting(handed: (frame: Uint8Array<ArrayBuffer>) => Uint8Array<ArrayBuffer> | null) {
  let cut = () => {}
  const stream = (body: ReadableStream<Uint8Array>) => {
    const decoder = new TextDecoder()
    const encoder = new TextEncoder()
    let partial = ''
    return body.pipeThrough(
      new TransformStream<Uint8Array, Uint8Array>({
        start: (controller) => {
          cut = () => controller.error(new TypeError('network error'))
        },
        transform: (chunk, controller) => {
          const events = (partial + decoder.decode(chunk, { stream: true })).split('\n\n')
          partial = events.pop() ?? ''
          const kept = events.flatMap((event) => {
            const data = event.split('\n').find((line) => line.startsWith('data: '))
            if (data === undefined) return [event]
            const frame = handed(base64urlToUint8Array(data.slice(6)))
            return frame === null ? [] : [`data: ${uint8ArrayToBase64url(frame)}`]
          })
          if (kept.length > 0) controller.enqueue(encoder.encode(kept.map((event) => `${event}\n\n`).join('')))
        },
      }),
    )
  }
  return { stream, cut: () => cut() }
}

/** The URL without its wire version, as a proxy that rewrites the query may leave it. */
const withoutWire = (url: string) => url.replace(/([?&])wire=\d+&?/, '$1')
/** The batch body cut in the middle of the frame that carries `text`: the server reads it to its end mid-frame. */
const cutIn = (text: string) => (bytes: Uint8Array<ArrayBuffer>) => [
  bytes.subarray(0, (findText(bytes, text)?.end ?? bytes.length) - 3),
]
