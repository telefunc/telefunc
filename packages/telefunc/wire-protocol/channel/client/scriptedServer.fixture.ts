// A scripted Telefunc server for the connection's specs, on a virtual clock: a REAL ClientChannel and its connection
// reach it over a fake WebSocket or the page's own `fetch`. As Telefunc's server does, it confirms each channel's data in
// a WINDOW it sends ahead of its PONG; a test makes it drop, cut or refuse what it gets.

export {
  PING_INTERVAL_MS,
  RECONNECT_TIMEOUT_MS,
  FakeWebSocket,
  world,
  serveSse,
  openChannel,
  advance,
  holdPage,
  windowOf,
  useScriptedServer,
}
export type { Script, PostAnswer, UploadScript }

import { afterEach, beforeEach, vi } from 'vitest'
import { WIRE_MAX_RAW_FRAME_BYTES } from '../../constants.js'
import { uint8ArrayToBase64url } from '../../wire/base64url.js'
import { decodeU32, parseSseRequestMetadata, type SseRequestMetadata } from '../../wire/envelope.js'
import { decode, encode, isOneFrame, isSequencedFrame, messageFrames, type DecodedFrame } from '../../wire/frame.js'
import type { ReconciledPayload } from '../../wire/payloads.js'
import { reconnectWindowOf } from '../../wire/pingDeadline.js'
import { TAG } from '../../wire/tags.js'
import { ClientChannel } from './ClientChannel.js'

const PING_INTERVAL_MS = 2_000
const RECONNECT_TIMEOUT_MS = 10_000
const encoder = new TextEncoder()

/** The server: what it has confirmed of each channel, which channels' data it drops, and what its RECONCILED says. */
class Server {
  readonly confirmed = new Map<number, number>()
  /** The channels whose data it takes and never confirms, as when something in front of it answers for it. */
  readonly drops = new Set<number>()
  /** The channels whose data cuts the wire it comes on, as a proxy that cuts a request at its limit does, before the
   *  server reads it: over SSE the POST that carries it fails as a network error, and over WebSocket the socket goes. */
  readonly cuts = new Set<number>()
  /** The channels whose POSTs something in front of it answers 413 only after it dropped the wire they went on: the
   *  page reads the refusal for a wire already lost. */
  readonly lateRefusals = new Set<number>()
  /** The channels a RECONCILED leaves out, as when the server dropped them. */
  readonly omits = new Set<number>()
  /** How many seqs behind what it got it confirms, as when the page's newest frames are still on their way. */
  lags = 0
  /** Each data frame it got: the channel, the seq, its tag, the wire it came on (the event streams opened by then), and
   *  when. */
  readonly got: { ix: number; seq: number; tag: number; wire: number; at: number }[] = []
  settings: Partial<ReconciledPayload> = {}

  /** The frames it sends down in answer to `frame`. */
  answer(frame: DecodedFrame): Uint8Array<ArrayBuffer>[] {
    if (frame.tag === TAG.RECONCILE) {
      const open = frame.payload.open.filter(({ ix }) => !this.omits.has(ix))
      for (const { ix } of open) if (!this.confirmed.has(ix)) this.confirmed.set(ix, 0)
      return [this.reconciled(open.map(({ ix }) => ix)), ...this.messageWindows()]
    }
    if (frame.tag === TAG.PREPARE) return [encode.ready({ upgradeId: frame.payload.upgradeId })]
    if (frame.tag === TAG.PING) {
      const windows = [...this.confirmed].map(([ix, lastSeq]) =>
        encode.window(ix, 1_000_000, Math.max(0, lastSeq - this.lags)),
      )
      return [...windows, ...this.messageWindows(), encode.pong()]
    }
    if (isSequencedFrame(frame)) {
      const wire = world.streams.length + FakeWebSocket.made.length
      this.got.push({ ix: frame.index, seq: frame.seq, tag: frame.tag, wire, at: performance.now() })
      if (!this.drops.has(frame.index)) this.confirmed.set(frame.index, frame.seq)
    }
    return []
  }

  /** Room for as many messages as a test sends, on each channel it has. */
  private messageWindows(): Uint8Array<ArrayBuffer>[] {
    return [...this.confirmed.keys()].map((ix) => encode.msgWindow(ix, 1_000_000))
  }

  reconciled(ixes: number[], upgradeId?: string): Uint8Array<ArrayBuffer> {
    return encode.reconciled({
      sessionId: 'session',
      open: ixes.map((ix) => ({ ix, lastSeq: this.confirmed.get(ix) ?? 0 })),
      reconnectTimeout: RECONNECT_TIMEOUT_MS,
      idleTimeout: 600_000,
      pingInterval: PING_INTERVAL_MS,
      maxFrameBytes: WIRE_MAX_RAW_FRAME_BYTES,
      creditWindow: 1_000_000,
      sseFlushThrottle: 50,
      ssePostIdleFlushDelay: 10,
      transports: ['sse', 'ws'],
      ...this.settings,
      ...(upgradeId === undefined ? {} : { upgradeId }),
    })
  }
}

/** What a socket the page makes does: answers as the server does, or never opens (`hangs`), or opens and never
 *  answers a RECONCILE on its own (`mute`). */
type Script = 'answers' | 'hangs' | 'mute'

class FakeWebSocket {
  static OPEN = 1
  static scripts: Script[] = []
  static made: FakeWebSocket[] = []
  onopen: (() => void) | null = null
  onclose: ((event: { code: number }) => void) | null = null
  onerror: (() => void) | null = null
  onmessage: ((event: { data: ArrayBuffer }) => void) | null = null
  binaryType = 'blob'
  readyState = 0
  bufferedAmount = 0
  readonly script: Script

  constructor(url: string) {
    this.script = FakeWebSocket.scripts.shift() ?? 'answers'
    // A page an earlier test left behind reaches nothing.
    if (!url.includes(new URL(world.telefuncUrl).pathname)) {
      this.script = 'hangs'
      queueMicrotask(() => this.drop())
      return
    }
    FakeWebSocket.made.push(this)
    if (this.script === 'hangs') return
    queueMicrotask(() => {
      this.readyState = 1
      this.onopen?.()
    })
  }
  send(data: ArrayBuffer | Uint8Array): void {
    const message = data instanceof Uint8Array ? data.slice() : new Uint8Array(data)
    for (const raw of isOneFrame(message) ? [message] : messageFrames(message)) {
      const frame = decode(raw)
      if (this.script === 'mute' && frame.tag === TAG.RECONCILE) continue
      // Cut before the server reads it, as over SSE: the socket goes, and the server never has the frame.
      if (isSequencedFrame(frame) && world.server.cuts.has(frame.index)) {
        setTimeout(() => this.drop(), 0)
        return
      }
      for (const down of world.server.answer(frame)) {
        const deliver = () => this.deliver(down)
        if (frame.tag === TAG.PREPARE && world.holdReady) world.heldReady.push(deliver)
        else setTimeout(deliver, 0)
      }
    }
  }
  close(): void {
    this.readyState = 3
  }
  /** The wire goes, as when a proxy restarts. */
  drop(): void {
    if (this.readyState === 3) return
    this.readyState = 3
    this.onclose?.({ code: 1006 })
  }
  deliver(frame: Uint8Array<ArrayBuffer>): void {
    if (this.readyState === 1) this.onmessage?.({ data: frame.slice().buffer })
  }
  /** The latest socket the page made that is open. */
  static get open(): FakeWebSocket | undefined {
    return FakeWebSocket.made.findLast((ws) => ws.readyState === 1)
  }
}

/** An SSE event stream the test writes the server's frames to, and ends as a server or a proxy that drops it does. One
 *  `held` carries nothing, as a server that hasn't answered the open writes nothing on it, till `answer`. */
function eventStream() {
  let controller: ReadableStreamDefaultController<Uint8Array> | null = null
  let held = false
  const stream = new ReadableStream<Uint8Array>({
    start: (c) => {
      controller = c
    },
    cancel: () => {
      controller = null
    },
  })
  return {
    openedAt: performance.now(),
    response: new Response(stream, { headers: { 'Content-Type': 'text/event-stream' } }),
    frame: (frame: Uint8Array<ArrayBuffer>) => {
      if (!held) controller?.enqueue(encoder.encode(`data: ${uint8ArrayToBase64url(frame)}\n\n`))
    },
    hold: () => {
      held = true
    },
    answer: () => {
      held = false
    },
    end: () => {
      controller?.close()
      controller = null
    },
  }
}

/** The frames of an SSE POST's body, its metadata first, read as the server reads them: a body that ends inside a
 *  frame or its length is no request the server takes. */
async function postFrames(body: Blob): Promise<{ metadata: SseRequestMetadata; frames: DecodedFrame[] }> {
  const buf = new Uint8Array(await body.arrayBuffer())
  const raw: Uint8Array<ArrayBuffer>[] = []
  let off = 0
  while (off < buf.length) {
    if (off + 4 > buf.length) throw new Error('The POST body ends inside a frame length')
    const len = decodeU32(buf.subarray(off, off + 4))
    if (off + 4 + len > buf.length) throw new Error('The POST body ends inside a frame')
    raw.push(buf.slice(off + 4, off + 4 + len))
    off += 4 + len
  }
  const [head, ...rest] = raw
  const metadata = parseSseRequestMetadata(JSON.parse(new TextDecoder().decode(head)))
  return { metadata, frames: rest.map((frame) => decode(frame)) }
}

/** How the server's side answers a batch POST: at once with a 200 (`taken`), after `ms` with a 200 (`slow`), with no
 *  answer once it took the frames (`lost`), with `status` and an HTML page before taking them (`refused`), or with
 *  `status` and the `Content-Type` values `contentType` before taking them (`typed`, as Telefunc's own refusals are); a
 *  refusal's `pongAfterMs` has the wire the POST went on carry a PONG that long after it, as one already on its way. */
type PostAnswer =
  | { tag: 'taken' }
  | { tag: 'slow'; ms: number }
  | { tag: 'lost' }
  | { tag: 'refused'; status: number; pongAfterMs?: number }
  | { tag: 'typed'; status: number; contentType: string[] }

/** An answer with `status` and each of `contentType` as a `Content-Type` value of its own. */
function typedResponse({ status, contentType }: { status: number; contentType: string[] }): Response {
  const headers = new Headers()
  for (const value of contentType) headers.append('Content-Type', value)
  return new Response('Refused', { status, headers })
}

/** What a streaming upload meets: the server takes it (`taken`, acknowledged on its wire's event stream, `ackAfterMs`
 *  after it went where given, or when the test calls `world.heldAcks` with `ack: 'held'`); it fails to fetch `afterMs`
 *  after it went, unacknowledged, its frames read and answered `answersAfterMs` after each where given (as a body that
 *  reached the server whose acknowledgement never reached the page), else unread (as HTTP/1.1 or a network change);
 *  something in front holds its body for as long as it is open (`held`), as a proxy that forwards a request only once
 *  its body ended: it settles only as the page aborts it; or something in front answers it with `status` and an HTML
 *  page, unacknowledged (`answered`). */
type UploadScript =
  | { tag: 'taken'; ackAfterMs?: number; ack?: 'held' }
  | { tag: 'fails'; afterMs: number; answersAfterMs?: number }
  | { tag: 'held' }
  | { tag: 'answered'; status: number }

/** The test's server and what it saw; `useScriptedServer` makes a new one for each test. */
type World = {
  server: Server
  telefuncUrl: string
  streams: ReturnType<typeof eventStream>[]
  postAnswers: PostAnswer[]
  controlAnswers: PostAnswer[]
  /** How each open is answered before it opens, in order; an event stream once none is left. */
  openAnswers: { status: number; contentType: string[] }[]
  /** While set, a streaming upload is taken: acknowledged on the latest event stream, its frames answered as a batch
   *  POST's are. Unset, it fails to fetch, and the page sends in batch POSTs. */
  uploads: boolean
  /** What each streaming upload meets, in order; once none is left, `taken` where `uploads` is set, else it fails to
   *  fetch at once. */
  uploadScripts: UploadScript[]
  /** How each streaming upload is answered once it carried data, in order; once none is left, it is read till it ends. */
  uploadAnswers: { status: number; contentType: string[] }[]
  /** How many streaming uploads the page sent, taken or not. */
  uploadPosts: number
  /** Each data frame a taken upload carried to the server: the upload (as `uploadPosts` counted it), the channel, the
   *  seq. */
  uploaded: { upload: number; ix: number; seq: number }[]
  /** The acknowledgements of uploads taken with `ack: 'held'`, in order: a test calls one to send it. */
  heldAcks: (() => void)[]
  postLimit: number
  holdOpens: boolean
  heldOpens: (() => void)[]
  holdReady: boolean
  heldReady: (() => void)[]
  posts: { bytes: number; status: number; data: boolean; wire: number }[]
  /** How many opens the page sent. */
  opens: number
}

const world: World = {
  server: new Server(),
  telefuncUrl: '',
  /** The page's SSE event streams, the latest last. */
  streams: [],
  /** How the next batch POSTs that carry data are answered, one each; once they run out, `taken`. */
  postAnswers: [],
  /** How the next batch POSTs that carry no data are answered, one each; once they run out, `taken`. */
  controlAnswers: [],
  openAnswers: [],
  uploads: false,
  uploadScripts: [],
  uploadAnswers: [],
  uploadPosts: 0,
  uploaded: [],
  heldAcks: [],
  /** The largest batch POST body something in front of the server takes: a larger one is answered 413, unread. */
  postLimit: Infinity,
  /** While set, the server answers no open, and writes nothing on its stream: each waits in `heldOpens`, which a test
   *  calls to answer it. */
  holdOpens: false,
  heldOpens: [],
  /** While set, the server's READY to a WebSocket's PREPARE waits in `heldReady`, which a test calls to send it: the
   *  page stays on SSE meanwhile. */
  holdReady: false,
  heldReady: [],
  /** Each batch POST: its body's size, its status (0 for none), whether it carried data, and the wire it went on. */
  posts: [],
  opens: 0,
}

/** The page's `fetch` against the server over SSE: an open gets an event stream, any other POST its frames' answer. */
function serveSse(): void {
  vi.stubGlobal('fetch', async (url: string, init: RequestInit) => {
    if (String(url).startsWith(world.telefuncUrl) && init.body instanceof ReadableStream) {
      const wire = ++world.uploadPosts
      const script = world.uploadScripts.shift() ?? (world.uploads ? { tag: 'taken' } : { tag: 'fails', afterMs: 0 })
      return meetUpload(script, wire, init.body, init.signal)
    }
    // No streaming upload: the page's frames go in batch POSTs, which the server reads.
    if (!String(url).startsWith(world.telefuncUrl) || !(init.body instanceof Blob))
      throw new TypeError('Failed to fetch')
    const bytes = init.body.size
    const { metadata, frames } = await postFrames(init.body)
    const { server } = world
    if (metadata.kind === 'streamResponse') {
      world.opens++
      const refusal = world.openAnswers.shift()
      if (refusal !== undefined) return typedResponse(refusal)
      const stream = eventStream()
      world.streams.push(stream)
      const answer = () => {
        stream.answer()
        for (const frame of frames) for (const down of server.answer(frame)) stream.frame(down)
      }
      if (world.holdOpens) {
        stream.hold()
        world.heldOpens.push(answer)
      } else answer()
      return stream.response
    }
    const data = frames.some((frame) => isSequencedFrame(frame))
    const post = { bytes, status: 0, data, wire: world.streams.length }
    world.posts.push(post)
    if (frames.some((frame) => isSequencedFrame(frame) && server.cuts.has(frame.index)))
      throw new TypeError('Failed to fetch')
    if (frames.some((frame) => isSequencedFrame(frame) && server.lateRefusals.has(frame.index))) {
      world.streams.at(-1)?.end()
      post.status = 413
      return new Response('<h1>413 Request Entity Too Large</h1>', {
        status: 413,
        headers: { 'Content-Type': 'text/html' },
      })
    }
    if (bytes > world.postLimit) {
      post.status = 413
      return new Response('<h1>413 Request Entity Too Large</h1>', {
        status: 413,
        headers: { 'Content-Type': 'text/html' },
      })
    }
    const stream = world.streams.at(-1)
    const answer = (data ? world.postAnswers.shift() : world.controlAnswers.shift()) ?? { tag: 'taken' }
    if (answer.tag === 'refused') {
      post.status = answer.status
      const { pongAfterMs } = answer
      if (pongAfterMs !== undefined) setTimeout(() => stream?.frame(encode.pong()), pongAfterMs)
      return new Response('<h1>Refused</h1>', { status: answer.status, headers: { 'Content-Type': 'text/html' } })
    }
    if (answer.tag === 'typed') {
      post.status = answer.status
      return typedResponse(answer)
    }
    for (const frame of frames) {
      if (frame.tag !== TAG.BARRIER) continue
      // The upgrade commits: the channels attach on the new wire, and the old one's FIN ends its data.
      const { upgradeId, open } = frame.payload
      const ixes = open.map(({ ix }) => ix)
      setTimeout(() => FakeWebSocket.open?.deliver(server.reconciled(ixes, upgradeId)), 0)
      setTimeout(() => stream?.frame(encode.fin()), 0)
    }
    if (answer.tag === 'slow') await new Promise((resolve) => setTimeout(resolve, answer.ms))
    for (const frame of frames) for (const down of server.answer(frame)) stream?.frame(down)
    if (answer.tag === 'lost') throw new TypeError('Failed to fetch')
    post.status = 200
    return new Response(null)
  })
}

/** The streaming upload of the page's `wire`th wire (each wire opens one of each), as `script` says. */
async function meetUpload(
  script: UploadScript,
  wire: number,
  body: ReadableStream<Uint8Array>,
  signal: AbortSignal | null | undefined,
): Promise<Response> {
  if (script.tag === 'taken') return takeUpload(body, wire, script)
  if (script.tag === 'answered') {
    return new Response('<h1>Refused</h1>', { status: script.status, headers: { 'Content-Type': 'text/html' } })
  }
  if (script.tag === 'held') {
    return new Promise((_, reject) =>
      signal?.addEventListener('abort', () => reject(new DOMException('aborted', 'AbortError')), { once: true }),
    )
  }
  const { afterMs, answersAfterMs } = script
  // The page's body fails once its fetch did: nothing more is read.
  if (answersAfterMs !== undefined) void answerUpload(body, wire, answersAfterMs).catch(() => {})
  if (afterMs > 0) await new Promise((resolve) => setTimeout(resolve, afterMs))
  throw new TypeError('Failed to fetch')
}

/** The event stream of the page's `wire`th wire, once its open made it: the page sends a wire's open and its upload
 *  together. */
async function streamOf(wire: number) {
  while (world.streams.length < wire) await new Promise((resolve) => setTimeout(resolve, 1))
  return world.streams[wire - 1]
}

/** An upload's frames read as the server reads them, and answered `afterMs` after each on its wire's event stream,
 *  with no acknowledgement. */
async function answerUpload(body: ReadableStream<Uint8Array>, wire: number, afterMs: number): Promise<void> {
  const stream = await streamOf(wire)
  for await (const frame of uploadFrames(body.getReader())) {
    const answers = world.server.answer(frame)
    setTimeout(() => {
      for (const down of answers) stream?.frame(down)
    }, afterMs)
  }
}

/** A streaming upload, acknowledged on its wire's event stream as `script` says: its frames are read only then, as
 *  Telefunc's server reads them after the acknowledgement, and answered as they come, as a batch POST's are, until it
 *  carried data while an answer waits in `uploadAnswers`, or its body ended. */
async function takeUpload(
  body: ReadableStream<Uint8Array>,
  wire: number,
  { ackAfterMs, ack }: { ackAfterMs?: number; ack?: 'held' },
): Promise<Response> {
  const stream = await streamOf(wire)
  if (ackAfterMs !== undefined) await new Promise((resolve) => setTimeout(resolve, ackAfterMs))
  if (ack === 'held') await new Promise<void>((resolve) => world.heldAcks.push(resolve))
  stream?.frame(encode.streamRequestOpenAck())
  const reader = body.getReader()
  for await (const frame of uploadFrames(reader)) {
    const refusal = isSequencedFrame(frame) ? world.uploadAnswers.shift() : undefined
    if (refusal !== undefined) {
      void reader.cancel().catch(() => {})
      return typedResponse(refusal)
    }
    if (isSequencedFrame(frame)) world.uploaded.push({ upload: wire, ix: frame.index, seq: frame.seq })
    for (const down of world.server.answer(frame)) stream?.frame(down)
  }
  return new Response(null)
}

/** An upload's frames after its metadata, as the server reads them, till its body ends; a failed read throws. */
async function* uploadFrames(reader: ReadableStreamDefaultReader<Uint8Array>): AsyncGenerator<DecodedFrame> {
  let pending = new Uint8Array(0)
  let metadataRead = false
  while (true) {
    const read = await reader.read()
    if (read.done) return
    const joined = new Uint8Array(pending.length + read.value.length)
    joined.set(pending)
    joined.set(read.value, pending.length)
    pending = joined
    while (pending.length >= 4 && pending.length >= 4 + decodeU32(pending.subarray(0, 4))) {
      const len = decodeU32(pending.subarray(0, 4))
      const raw = pending.slice(4, 4 + len)
      pending = pending.slice(4 + len)
      if (metadataRead) yield decode(raw)
      metadataRead = true
    }
  }
}

/** A channel the page opens over `transports`, and when and how it ended. */
function openChannel(transports: ('sse' | 'ws')[]) {
  const channel = new ClientChannel({
    channelId: crypto.randomUUID(),
    connection: { telefuncUrl: world.telefuncUrl, transports, connectionKey: 'key' },
  })
  void channel.opened.catch(() => {})
  const ended: { at: number; err: Error | null }[] = []
  channel.onClose((err) => void ended.push({ at: performance.now(), err }))
  return { channel, ended }
}

/** Advances the virtual clock by `ms`, in steps of a tenth of a second. */
async function advance(ms: number): Promise<void> {
  for (let left = ms; left > 0; left -= 100) await vi.advanceTimersByTimeAsync(Math.min(100, left))
}

/** The page's event loop is held for `ms`, as a tab whose script ran that long or that the browser paused: no timer
 *  runs meanwhile, then each that came due runs once (the fake clock's `jump`). */
async function holdPage(ms: number): Promise<void> {
  const clock: unknown = Reflect.get(globalThis.setTimeout, 'clock')
  if (!canJump(clock)) throw new Error('holdPage needs the fake timers installed')
  clock.jump(ms)
  await vi.advanceTimersByTimeAsync(0)
}

function canJump(clock: unknown): clock is { jump(ms: number): number } {
  return typeof clock === 'object' && clock !== null && typeof Reflect.get(clock, 'jump') === 'function'
}

const windowOf = (transport: 'sse' | 'ws', reconnectTimeout = RECONNECT_TIMEOUT_MS) =>
  reconnectWindowOf(PING_INTERVAL_MS, reconnectTimeout, transport)

/** A new server for each test of the file, on a virtual clock, and each reconnect delay 0.9 of its most. */
function useScriptedServer(): void {
  beforeEach(() => {
    vi.useFakeTimers({ toFake: ['setTimeout', 'clearTimeout', 'setInterval', 'clearInterval', 'Date', 'performance'] })
    vi.spyOn(Math, 'random').mockReturnValue(0.9)
    world.server = new Server()
    world.telefuncUrl = `http://telefunc.invalid/${crypto.randomUUID()}/_telefunc`
    world.streams = []
    world.postAnswers = []
    world.controlAnswers = []
    world.postLimit = Infinity
    world.posts = []
    world.openAnswers = []
    world.opens = 0
    world.uploads = false
    world.uploadScripts = []
    world.uploadAnswers = []
    world.uploadPosts = 0
    world.uploaded = []
    world.heldAcks = []
    world.holdOpens = false
    world.heldOpens = []
    world.holdReady = false
    world.heldReady = []
    FakeWebSocket.made = []
    FakeWebSocket.scripts = []
    vi.stubGlobal('WebSocket', FakeWebSocket)
  })
  afterEach(() => {
    for (const ws of FakeWebSocket.made) ws.drop()
    vi.clearAllTimers()
    vi.unstubAllGlobals()
    vi.restoreAllMocks()
    vi.useRealTimers()
  })
}
