// What a page does with the answer to its SSE open. A REAL ClientChannel and its connection open over the page's own
// `fetch` against a scripted HTTP server: each open the server gets is answered by the next answer a test lists, and
// once those run out, with a wire.

import { afterEach, beforeEach, expect, onTestFinished, test, vi } from 'vitest'
import { createServer, type IncomingMessage, type Server, type ServerResponse } from 'node:http'
import { connect } from 'node:net'
import { once } from 'node:events'
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
/** The upload POST is held until the wire's RECONCILED went, then ended: the page's frames go in batch POSTs, and a
 *  RECONCILE among them is answered on the wire. */
let endUploadsAfterWire: boolean
let heldUploads: ServerResponse[]
/** The wire's session and settings, which a RECONCILED on it names. */
let session: string
let settings: { pingInterval?: number; reconnectTimeout?: number }

const delay = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms))

/** The request's body as its frames, the metadata first; null where its page left before sending all of it. */
async function framesOf(req: IncomingMessage): Promise<Uint8Array<ArrayBuffer>[] | null> {
  const chunks: Buffer[] = []
  try {
    for await (const chunk of req) chunks.push(chunk as Buffer)
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

/** The event of a RECONCILED for the channel `ix`. */
function reconciled(ix: number, { pingInterval = 100_000, reconnectTimeout = 60_000 } = {}): string {
  const frame = encode.reconciled({
    sessionId: session,
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
  return `data: ${uint8ArrayToBase64url(frame)}\n\n`
}

/** A wire: the event stream, with the RECONCILED for the channel the open's RECONCILE names. */
function wire(ofWire: { pingInterval?: number; reconnectTimeout?: number } = {}): Answer {
  return (res, ix) => {
    res.writeHead(200, { 'Content-Type': 'text/event-stream' })
    session = crypto.randomUUID()
    settings = ofWire
    res.write(`: open\n\n${reconciled(ix, ofWire)}`)
    wires.push(res)
    if (endUploadsAfterWire) setTimeout(() => void heldUploads.splice(0).forEach((upload) => upload.end()), 100)
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
  endUploadsAfterWire = false
  heldUploads = []
  server = createServer((req, res) => {
    // A page a redirect sent here.
    if (req.method === 'GET') return html(200)(res)
    // The upload POST that would stream: ended at once, its frames go in batch POSTs.
    if (req.headers['transfer-encoding'] === 'chunked') {
      if (endUploadsAfterWire) return void heldUploads.push(res)
      return holdUploads ? undefined : void res.end()
    }
    void framesOf(req).then((frames) => {
      if (frames === null) return
      const metadata = JSON.parse(new TextDecoder().decode(frames[0]))
      if (!metadata.streamResponse) {
        for (const frame of frames.slice(1).map((raw) => decode(raw))) {
          if (frame.tag === TAG.PING) wires.at(-1)?.write(`data: ${uint8ArrayToBase64url(encode.pong())}\n\n`)
          if (frame.tag === TAG.RECONCILE)
            for (const { ix } of frame.payload.open) wires.at(-1)?.write(reconciled(ix, settings))
        }
        return void res.end()
      }
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

function openChannel(connectionKey = crypto.randomUUID()) {
  const channel = new ClientChannel({
    channelId: crypto.randomUUID(),
    connection: { telefuncUrl: `${url}/_telefunc`, transports: ['sse'], connectionKey },
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

test('a first open refused ends the channel at once, naming the status that refused it', async () => {
  answers.push(html(404))
  const notFound = await openingOf(openChannel())
  answers.push(html(502))
  const badGateway = await openingOf(openChannel())
  expect([(notFound as Error).message, (badGateway as Error).message]).toEqual([
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

// SSE's RECONCILED comes before its open, which waits for it to confirm its upload: the first RECONCILED takes the
// handshake's sample, and a later one on the same wire takes none. The recovery's window: 2.5 pings of 1 s, then 3 s.
test(
  'a RECONCILED after the first on a wire leaves the handshake the first measured, which a recovery then fits',
  { timeout: 30_000 },
  async () => {
    endUploadsAfterWire = true
    answers.push(wire({ pingInterval: 1000, reconnectTimeout: 3000 }), ...Array.from({ length: 20 }, () => html(502)))
    const connectionKey = crypto.randomUUID()
    const channel = openChannel(connectionKey)
    expect(await openingOf(channel)).toBe('opened')
    await delay(6_000)
    expect(await openingOf(openChannel(connectionKey))).toBe('opened')
    const opened = opens
    wires.shift()?.end()
    const closed = await channel.closed
    // A handshake of some ms leaves room for attempts in the 5.5 s; one of 6 s, measured from the open, for none.
    expect(opens, 'the page tried again before its deadline').toBeGreaterThan(opened)
    expect(closed?.message).toBe('Server rejected SSE connection with 502 text/html')
  },
)
