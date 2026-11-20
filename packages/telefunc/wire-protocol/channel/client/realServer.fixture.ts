// The real Node server and the real page, with a fetch wrapper that stands for what is between them, for the twins of
// the scripted server's rows (ClientConnection.marked.spec.ts): a page that breaks the protocol, and a proxy that
// rewrites the URL or a refusal's `Content-Type`. Each spec file serves one set of transports (`useRealServer`).

export {
  cutIn,
  findText,
  open,
  opens,
  pair,
  sends,
  textOf,
  until,
  delay,
  uploads,
  useRealServer,
  withholding,
  withoutWire,
  world,
}

import { afterAll, afterEach, beforeAll } from 'vitest'
import { createServer, type Server } from 'node:http'
import { Telefunc } from '../../../serve/node.js'
import { config as serverConfig, type ChannelConfigUser } from '../../../node/server/serverConfig.js'
import { base64urlToUint8Array } from '../../wire/base64url.js'
import { decodeU32 } from '../../wire/envelope.js'
import { decode } from '../../wire/frame.js'
import { SEQ_OFFSET, readU16, readU32 } from '../../wire/header.js'
import { TAG } from '../../wire/tags.js'
import { getChannelMux } from '../server/ChannelMux.js'
import { ServerChannel } from '../server/ServerChannel.js'
import { ClientChannel } from './ClientChannel.js'

type Kind = 'open' | 'post' | 'upload'
/** What the wrapper does to one request of `kind` that carries `text`, where given, and to its answer. */
type Hook = {
  kind: Kind
  text?: string
  url?: (url: string) => string
  body?: (bytes: Uint8Array<ArrayBuffer>) => BlobPart[]
  upload?: (body: ReadableStream<Uint8Array>) => ReadableStream<Uint8Array>
  contentType?: (contentType: string) => string
  /** The event stream as the page reads it. */
  stream?: (body: ReadableStream<Uint8Array>) => ReadableStream<Uint8Array>
}
const world: {
  hooks: Hook[]
  /** Whether the page's streaming upload is taken: Node's fetch can, a browser over HTTP/1.1 can't. */
  uploads: boolean
  requests: { kind: Kind; bytes: number; status: number; contentType: string | null }[]
  /** Each request as it went, by its kind: a streaming upload is answered only at its end. */
  started: Kind[]
  sockets: number
  telefuncUrl: string
} = { hooks: [], uploads: false, requests: [], started: [], sockets: 0, telefuncUrl: '' }

/** `frame`'s channel and seq, where it carries `text`. */
function textOf(frame: Uint8Array<ArrayBuffer>, text: string): { ix: number; seq: number } | null {
  if (frame[0] !== TAG.TEXT) return null
  const decoded = decode(frame)
  if (decoded.tag !== TAG.TEXT || decoded.text !== JSON.stringify(text)) return null
  return { ix: readU16(frame, 1), seq: readU32(frame, SEQ_OFFSET) }
}

/** Where a batch body's frame that carries `text` ends, and its channel and seq; null where none does. */
function findText(body: Uint8Array<ArrayBuffer>, text: string): { end: number; ix: number; seq: number } | null {
  let at = 4 + decodeU32(body.subarray(0, 4))
  while (at + 4 <= body.length) {
    const end = at + 4 + decodeU32(body.subarray(at, at + 4))
    const found = textOf(body.subarray(at + 4, end), text)
    if (found !== null) return { end, ...found }
    at = end
  }
  return null
}

/** The hook for this request, taken from the queue: the first of its kind, of its text where it names one. */
async function takeHook(
  kind: Kind,
  body: unknown,
): Promise<{ hook: Hook; bytes: Uint8Array<ArrayBuffer> | null } | null> {
  const bytes = body instanceof Blob ? new Uint8Array(await body.arrayBuffer()) : null
  const at = world.hooks.findIndex(
    (hook) =>
      hook.kind === kind && (hook.text === undefined || (bytes !== null && findText(bytes, hook.text) !== null)),
  )
  const hook = world.hooks[at]
  if (hook === undefined) return bytes === null ? null : { hook: { kind }, bytes }
  world.hooks.splice(at, 1)
  return { hook, bytes }
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
  const response = await realFetch(hook.url ? hook.url(url) : url, { ...init, body: sent })
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

/** Counts the WebSockets the page makes. */
const RealWebSocket = globalThis.WebSocket
class CountedWebSocket extends RealWebSocket {
  constructor(url: string | URL, protocols?: string | string[]) {
    super(url, protocols)
    world.sockets++
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
    server = createServer((req, res) => void telefunc.serve({ req, res }))
    telefunc.installWebSocket(server)
    await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', resolve))
    const address = server.address()
    if (address === null || typeof address === 'string') throw new Error('no port')
    world.telefuncUrl = `http://127.0.0.1:${address.port}/_telefunc`
  })
  afterEach(() => {
    Object.assign(world, { hooks: [], uploads: false, requests: [], started: [], sockets: 0 })
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
          const kept = events.filter((event) => {
            const data = event.split('\n').find((line) => line.startsWith('data: '))
            return data === undefined || !withheld(base64urlToUint8Array(data.slice(6)))
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
