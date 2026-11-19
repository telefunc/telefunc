// What a page does with the answer to its SSE open. A REAL ClientChannel and its connection open over the page's own
// `fetch` against a scripted HTTP server: each open the server gets is answered by the next answer a test lists, and
// once those run out, with a wire.

import { afterEach, beforeEach, expect, onTestFinished, test, vi } from 'vitest'
import { createServer, type IncomingMessage, type Server, type ServerResponse } from 'node:http'
import { ClientChannel } from './ClientChannel.js'
import { decode, encode } from '../../wire/frame.js'
import { TAG } from '../../wire/tags.js'
import { decodeU32 } from '../../wire/envelope.js'
import { uint8ArrayToBase64url } from '../../wire/base64url.js'
import { WIRE_MAX_RAW_FRAME_BYTES } from '../../constants.js'

/** An answer to an open of the channel `ix`. */
type Answer = (res: ServerResponse, ix: number) => void

let server: Server
let url: string
let answers: Answer[]
let opens: number
let wires: ServerResponse[]
let channels: ClientChannel[]
/** The upload POST is held, neither acknowledged nor answered, as by a proxy that buffers a request's body. */
let holdUploads: boolean

const delay = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms))

/** The request's body as its frames: the metadata first. */
async function framesOf(req: IncomingMessage): Promise<Uint8Array<ArrayBuffer>[]> {
  const chunks: Buffer[] = []
  for await (const chunk of req) chunks.push(chunk as Buffer)
  const buf = new Uint8Array(Buffer.concat(chunks))
  const frames: Uint8Array<ArrayBuffer>[] = []
  for (let off = 0; off + 4 <= buf.length; ) {
    const len = decodeU32(buf.slice(off, off + 4))
    frames.push(buf.slice(off + 4, off + 4 + len))
    off += 4 + len
  }
  return frames
}

/** A wire: the event stream, with the RECONCILED for the channel the open's RECONCILE names. */
function wire({ pingInterval = 100_000, reconnectTimeout = 60_000 } = {}): Answer {
  return (res, ix) => {
    res.writeHead(200, { 'Content-Type': 'text/event-stream' })
    const frame = encode.reconciled({
      sessionId: crypto.randomUUID(),
      open: [{ ix, lastSeq: 0 }],
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
  server = createServer((req, res) => {
    // A page a redirect sent here.
    if (req.method === 'GET') return html(200)(res)
    // The upload POST that would stream: ended at once, its frames go in batch POSTs.
    if (req.headers['transfer-encoding'] === 'chunked') return holdUploads ? undefined : void res.end()
    void framesOf(req).then((frames) => {
      const metadata = JSON.parse(new TextDecoder().decode(frames[0]))
      if (!metadata.streamResponse) return void res.end()
      opens++
      const reconcile = frames
        .slice(1)
        .map((raw) => decode(raw))
        .find((frame) => frame.tag === TAG.RECONCILE)
      const ix = reconcile?.tag === TAG.RECONCILE ? (reconcile.payload.open[0]?.ix ?? 0) : 0
      ;(answers.shift() ?? wire())(res, ix)
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

function openChannel() {
  const channel = new ClientChannel({
    channelId: crypto.randomUUID(),
    connection: { telefuncUrl: `${url}/_telefunc`, transports: ['sse'], connectionKey: crypto.randomUUID() },
  })
  channels.push(channel)
  return channel
}

/** What the channel's opening came to: its error, or `'opened'`. */
function openingOf(channel: ClientChannel) {
  return channel.opened.then(
    () => 'opened' as const,
    (err: unknown) => err,
  )
}

/** What the channel's end came to: its error, or `'closed'`. */
function closingOf(channel: ClientChannel) {
  return channel.closed.then(
    () => 'closed' as const,
    (err: unknown) => err,
  )
}

test('a first open answered with a page of HTML ends the channel at once, naming what it got', async () => {
  answers.push(html(200))
  const channel = openChannel()
  const opening = await openingOf(channel)
  expect(opening).toBeInstanceOf(Error)
  expect((opening as Error).message).toBe(
    'The server answered the SSE connection with 200 text/html, not an event stream',
  )
  expect(opens).toBe(1)
})

test('a first open redirected to a page of HTML ends the channel at once, naming what it got', async () => {
  answers.push((res) => res.writeHead(302, { Location: '/login' }).end())
  const channel = openChannel()
  const opening = await openingOf(channel)
  expect((opening as Error).message).toBe(
    'The server answered the SSE connection with 200 text/html, not an event stream',
  )
  expect(opens).toBe(1)
})

// Its reconnects wait up to 0.5 s, 1 s and 2 s.
test('during a recovery, an open answered with a page of HTML is tried again', { timeout: 15_000 }, async () => {
  const channel = openChannel()
  expect(await openingOf(channel)).toBe('opened')
  answers.push(html(200), html(502))
  wires.shift()?.end()
  while (opens < 4) await delay(50)
  expect({ opens, closed: channel.isClosed }).toEqual({ opens: 4, closed: false })
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
test(
  'a recovery answered only with pages of HTML ends at its deadline, naming the last answer',
  { timeout: 30_000 },
  async () => {
    answers.push(wire({ pingInterval: 1000, reconnectTimeout: 10_000 }), ...Array.from({ length: 20 }, () => html(200)))
    const channel = openChannel()
    expect(await openingOf(channel)).toBe('opened')
    wires.shift()?.end()
    const closing = await closingOf(channel)
    expect(opens, 'the page tried again before its deadline').toBeGreaterThan(1)
    expect(closing).toBeInstanceOf(Error)
    expect((closing as Error).message).toBe(
      'The server answered the SSE connection with 200 text/html, not an event stream',
    )
  },
)
