// What a page does with the answers to its SSE requests. A REAL ClientChannel and its connection open over the page's
// own `fetch` against a scripted HTTP server: each open the server gets is answered by the next answer a test lists, and
// once those run out, with a wire; a batch POST larger than `postLimit` is refused as a proxy's body limit refuses it.

import { afterEach, beforeEach, expect, onTestFinished, test, vi } from 'vitest'
import { createServer, type IncomingMessage, type Server, type ServerResponse } from 'node:http'
import { connect } from 'node:net'
import { once } from 'node:events'
import { ClientChannel } from './ClientChannel.js'
import { decode, encode } from '../../wire/frame.js'
import { TAG } from '../../wire/tags.js'
import { decodeU32, parseSseRequestMetadata } from '../../wire/envelope.js'
import { uint8ArrayToBase64url } from '../../wire/base64url.js'
import { WIRE_MAX_RAW_FRAME_BYTES } from '../../constants.js'

/** An answer to an open of the channels `ixes`. */
type Answer = (res: ServerResponse, ixes: number[]) => void

let server: Server
let url: string
let answers: Answer[]
let opens: number
let wires: ServerResponse[]
let channels: ClientChannel[]
/** The upload POST is held, neither acknowledged nor answered, as by a proxy that buffers a request's body. */
let holdUploads: boolean
/** The largest batch POST body taken: a larger one is answered 413, unread. */
let postLimit: number
/** Each batch POST's body size and status, and the number of opens before it: the wire it went on. */
let posts: { bytes: number; status: number; wire: number }[]
/** The data frames batch POSTs carried, by tag and size. */
let received: { tag: number; bytes: number }[]
/** The channels the RECONCILEs that batch POSTs carried released. */
let released: number[]

const delay = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms))

/** The request's body as its frames, the metadata first; null where its page left before sending all of it. */
async function framesOf(req: IncomingMessage): Promise<Uint8Array<ArrayBuffer>[] | null> {
  const chunks: Buffer[] = []
  try {
    for await (const chunk of req) {
      if (!Buffer.isBuffer(chunk)) throw new Error('Expected the request body in Buffers')
      chunks.push(chunk)
    }
  } catch (err) {
    if (req.readableAborted) return null
    throw err
  }
  const buf = new Uint8Array(Buffer.concat(chunks))
  const frames: Uint8Array<ArrayBuffer>[] = []
  for (let off = 0; off + 4 <= buf.length; ) {
    const len = decodeU32(buf.slice(off, off + 4))
    frames.push(buf.slice(off + 4, off + 4 + len))
    off += 4 + len
  }
  return frames
}

/** A wire: the event stream, with the RECONCILED for the channels the open's RECONCILE names. `contentType`: its
 *  `Content-Type` values, each on a line of its own. */
function wire({ pingInterval = 100_000, reconnectTimeout = 60_000, contentType = ['text/event-stream'] } = {}): Answer {
  return (res, ixes) => {
    if (contentType.length > 0) res.setHeader('Content-Type', contentType)
    res.writeHead(200)
    const frame = encode.reconciled({
      sessionId: crypto.randomUUID(),
      open: ixes.map((ix) => ({ ix, lastSeq: 0 })),
      reconnectTimeout,
      idleTimeout: 60_000,
      pingInterval,
      maxFrameBytes: WIRE_MAX_RAW_FRAME_BYTES,
      creditWindow: 1_000_000,
      sseFlushThrottle: 300,
      ssePostIdleFlushDelay: 50,
      transports: ['sse'],
    })
    res.write(`: open\n\ndata: ${uint8ArrayToBase64url(frame)}\n\n`)
    wires.push(res)
  }
}

function html(status: number): (res: ServerResponse) => void {
  return (res) => res.writeHead(status, { 'Content-Type': 'text/html' }).end('<!doctype html><title>Sign in</title>')
}

beforeEach(async () => {
  answers = []
  opens = 0
  wires = []
  channels = []
  holdUploads = false
  postLimit = Infinity
  posts = []
  received = []
  released = []
  server = createServer((req, res) => {
    // A page a redirect sent here.
    if (req.method === 'GET') return html(200)(res)
    // The upload POST that would stream: ended at once, its frames go in batch POSTs.
    if (req.headers['transfer-encoding'] === 'chunked') return holdUploads ? undefined : void res.end()
    const bytes = Number(req.headers['content-length'])
    if (bytes > postLimit) {
      posts.push({ bytes, status: 413, wire: opens })
      req.resume()
      req.on('end', () =>
        res.writeHead(413, { 'Content-Type': 'text/html' }).end('<h1>413 Request Entity Too Large</h1>'),
      )
      return
    }
    void framesOf(req).then((frames) => {
      if (frames === null) return
      const metadata = parseSseRequestMetadata(JSON.parse(new TextDecoder().decode(frames[0])))
      if (metadata.kind !== 'streamResponse') {
        posts.push({ bytes, status: 200, wire: opens })
        for (const raw of frames.slice(1)) {
          if (raw[0] === TAG.TEXT || raw[0] === TAG.BINARY) received.push({ tag: raw[0], bytes: raw.byteLength })
          const frame = decode(raw)
          if (frame.tag === TAG.RECONCILE) released.push(...(frame.payload.released ?? []))
        }
        return void res.end()
      }
      opens++
      const reconcile = frames
        .slice(1)
        .map((raw) => decode(raw))
        .find((frame) => frame.tag === TAG.RECONCILE)
      const ixes = reconcile?.tag === TAG.RECONCILE ? reconcile.payload.open.map(({ ix }) => ix) : []
      ;(answers.shift() ?? wire())(res, ixes)
    })
  })
  await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', resolve))
  const address = server.address()
  if (address === null || typeof address === 'string') throw new Error('no port')
  url = `http://127.0.0.1:${address.port}`
})
afterEach(async () => {
  for (const channel of channels) channel.abort()
  for (const res of wires) res.end()
  server.closeAllConnections()
  await new Promise((resolve) => server.close(resolve))
})

/** `connectionKey`: channels given the same one share a connection. */
function openChannel(connectionKey = crypto.randomUUID()) {
  const channel = new ClientChannel({
    channelId: crypto.randomUUID(),
    connection: { telefuncUrl: `${url}/_telefunc`, transports: ['sse'], connectionKey },
  })
  channels.push(channel)
  return channel
}

/** `value`'s message, which the test expects of an Error. */
function messageOf(value: unknown): string {
  if (!(value instanceof Error)) throw new Error(`Expected an Error, got ${String(value)}`)
  return value.message
}

/** What the channel's opening came to: its error, or `'opened'`. */
function openingOf(channel: ClientChannel) {
  return channel.opened.then(
    (): 'opened' => 'opened',
    (err: unknown) => err,
  )
}

// A page that goes away sends the rest of no request it started: the fixture then has no request to answer.
test('a request whose page leaves in the middle of its body is left unanswered, with no error', async () => {
  const unhandled: unknown[] = []
  const onUnhandled = (err: unknown) => unhandled.push(err)
  process.on('unhandledRejection', onUnhandled)
  onTestFinished(() => {
    process.off('unhandledRejection', onUnhandled)
  })
  const closed = new Promise<void>((resolve) => server.once('connection', (socket) => socket.once('close', resolve)))
  const { port } = new URL(url)
  const page = connect(Number(port), '127.0.0.1')
  await once(page, 'connect')
  page.write(`POST /_telefunc HTTP/1.1\r\nHost: 127.0.0.1:${port}\r\nContent-Length: 1000\r\n\r\n`)
  page.write(new Uint8Array(10))
  await delay(100)
  page.destroy()
  await closed
  await delay(100)
  expect(unhandled).toEqual([])
})

test('a first open answered with a page of HTML ends the channel at once, naming what it got', async () => {
  answers.push(html(200))
  const channel = openChannel()
  const opening = await openingOf(channel)
  expect(opening).toBeInstanceOf(Error)
  expect(messageOf(opening)).toBe('The server answered the SSE connection with 200 text/html, not an event stream')
  expect(opens).toBe(1)
})

test('a first open redirected to a page of HTML ends the channel at once, naming what it got', async () => {
  answers.push((res) => res.writeHead(302, { Location: '/login' }).end())
  const channel = openChannel()
  const opening = await openingOf(channel)
  expect(messageOf(opening)).toBe('The server answered the SSE connection with 200 text/html, not an event stream')
  expect(opens).toBe(1)
})

// Fetch extracts a response's media type from all its Content-Type values: the last that parses and isn't */* wins
// (https://fetch.spec.whatwg.org/#concept-header-extract-mime-type).
test.each([
  { sent: ['text/plain', 'text/event-stream'] },
  { sent: ['nonsense', 'text/event-stream'] },
  { sent: ['text/event-stream; charset=utf-8, text/event-stream;x="a,b"'] },
])(
  'a first open whose Content-Type values are $sent opens, as its media type is text/event-stream',
  async ({ sent }) => {
    answers.push(wire({ contentType: sent }))
    expect(await openingOf(openChannel())).toBe('opened')
    expect(opens).toBe(1)
  },
)

test.each([
  { sent: ['text/event-stream', 'text/plain'], answer: '200 text/event-stream, text/plain' },
  { sent: ['application/json'], answer: '200 application/json' },
  { sent: [], answer: '200 and no Content-Type' },
])(
  'a first open whose Content-Type values are $sent ends the channel at once, naming them',
  async ({ sent, answer }) => {
    answers.push(wire({ contentType: sent }))
    const opening = await openingOf(openChannel())
    expect(opening instanceof Error && opening.message).toBe(
      `The server answered the SSE connection with ${answer}, not an event stream`,
    )
    expect(opens).toBe(1)
  },
)

// Its reconnects wait up to 0.5 s, 1 s and 2 s.
test('during a recovery, an open answered with a page of HTML is tried again', { timeout: 15_000 }, async () => {
  const channel = openChannel()
  expect(await openingOf(channel)).toBe('opened')
  answers.push(html(200), html(502))
  wires.shift()?.end()
  while (opens < 4) await delay(50)
  expect({ opens, closed: channel.isClosed }).toEqual({ opens: 4, closed: false })
})

test('a first open refused ends the channel at once, naming the status that refused it', async () => {
  answers.push(html(404))
  const notFound = await openingOf(openChannel())
  answers.push(html(502))
  const badGateway = await openingOf(openChannel())
  expect([messageOf(notFound), messageOf(badGateway)]).toEqual([
    'Server rejected SSE connection with 404 text/html',
    'Server rejected SSE connection with 502 text/html',
  ])
  expect(opens).toBe(2)
})

// The page waits up to 3 s for its upload's acknowledgement.
test(
  'a wire that ends while its upload waits for an acknowledgement is followed by another',
  { timeout: 15_000 },
  async () => {
    holdUploads = true
    const channel = openChannel()
    expect(await openingOf(channel)).toBe('opened')
    wires.shift()?.end()
    while (opens < 2) await delay(50)
    expect(channel.isClosed).toBe(false)
  },
)

// The old open's upload settles late, by its 3 s wait, where a successor holds the transport by then. Its fetch ignores
// the abort its wire's end sends, so that it is still pending when the successor opens.
test(
  'the upload of a wire that ended settles late without ending the wire after it, or opening another',
  { timeout: 20_000 },
  async () => {
    holdUploads = true
    const realFetch = globalThis.fetch
    let held = false
    vi.stubGlobal('fetch', (input: RequestInfo | URL, init?: RequestInit) => {
      if (held || !(init?.body instanceof ReadableStream)) return realFetch(input, init)
      held = true
      return new Promise<Response>(() => {})
    })
    onTestFinished(() => {
      vi.unstubAllGlobals()
    })
    const channel = openChannel()
    expect(await openingOf(channel)).toBe('opened')
    wires.shift()?.end()
    while (opens < 2 || wires.length === 0) await delay(50)
    let ended = false
    wires[0]?.on('close', () => {
      ended = true
    })
    await delay(4000)
    expect({ opens, ended, closed: channel.isClosed }).toEqual({ opens: 2, ended: false, closed: false })
  },
)

// Its window is the ping deadline, two and a half intervals over SSE, and then `reconnectTimeout`: 12.5 s, room for
// attempts after a first handshake that took seconds, as the page starts none that couldn't reconcile in time.
test.each([
  {
    answer: 'a page of HTML',
    status: 200,
    message: 'The server answered the SSE connection with 200 text/html, not an event stream',
  },
  { answer: "a proxy's error page", status: 502, message: 'Server rejected SSE connection with 502 text/html' },
])(
  'a recovery answered only with $answer ends at its deadline, naming it',
  { timeout: 30_000 },
  async ({ status, message }) => {
    answers.push(
      wire({ pingInterval: 1000, reconnectTimeout: 10_000 }),
      ...Array.from({ length: 20 }, () => html(status)),
    )
    const channel = openChannel()
    expect(await openingOf(channel)).toBe('opened')
    wires.shift()?.end()
    const closed = await channel.closed
    expect(opens, 'the page tried again before its deadline').toBeGreaterThan(1)
    expect(closed).toBeInstanceOf(Error)
    expect(closed?.message).toBe(message)
  },
)

// The page's first POST is sized to 64 KiB, so two messages of 60 KiB go in one POST of about 120 KiB.
test('a POST of several messages refused for its size goes again in smaller POSTs, on the same wire', async () => {
  postLimit = 100 * 1024
  const channel = openChannel()
  expect(await openingOf(channel)).toBe('opened')
  await Promise.all([0, 1, 2].map(() => channel.sendBinary(new Uint8Array(60 * 1024))))
  while (received.length < 3) await delay(20)
  expect(posts.filter(({ status }) => status === 413).length).toBe(1)
  expect(posts.every(({ bytes, status }) => status === 413 || bytes <= postLimit)).toBe(true)
  expect({ opens, closed: channel.isClosed }).toEqual({ opens: 1, closed: false })
})

// A first reconnect waits up to 0.5 s.
test(
  'a message refused for its size alone ends its channel after one reconnect, naming the answer',
  { timeout: 15_000 },
  async () => {
    postLimit = 1024 * 1024
    const connectionKey = crypto.randomUUID()
    const channel = openChannel(connectionKey)
    const other = openChannel(connectionKey)
    expect(await Promise.all([openingOf(channel), openingOf(other)])).toEqual(['opened', 'opened'])
    void channel.sendBinary(new Uint8Array(2 * 1024 * 1024)).catch(() => {})
    const closing = await channel.closed
    expect(messageOf(closing)).toBe(
      "Channel closed: what the page sent on it was refused with 413 text/html, again after a reconnect. Something between the page and the server refuses it, such as a proxy's limit on the size of a request.",
    )
    // The message alone is refused once on each wire; a POST that carried it with another frame was refused before
    // that, uncounted, and split.
    const refused = posts.filter(({ status }) => status === 413)
    const lone = Math.min(...refused.map(({ bytes }) => bytes))
    const alone = refused.filter(({ bytes }) => bytes === lone)
    expect(alone.map(({ wire }) => wire)).toEqual([1, 2])
    expect(refused.length - alone.length).toBeLessThanOrEqual(1)
    // The server is told on the wire the reconnect opened, which the second refusal doesn't end.
    while (released.length < 1) await delay(20)
    expect(released).toEqual([0])
    await delay(1500)
    expect(opens).toBe(2)
    await other.send('hello')
    while (received.length < 1) await delay(20)
    expect(other.isClosed).toBe(false)
  },
)
