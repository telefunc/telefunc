// What a page does when its SSE wire ends as it opens. A REAL ClientChannel and its connection open over the page's own
// `fetch` against a scripted HTTP server, which answers each open with a wire.

import { afterEach, beforeEach, expect, onTestFinished, test, vi } from 'vitest'
import { createServer, type IncomingMessage, type Server, type ServerResponse } from 'node:http'
import { ClientChannel } from './ClientChannel.js'
import { decode, encode } from '../../wire/frame.js'
import { TAG } from '../../wire/tags.js'
import { decodeU32 } from '../../wire/envelope.js'
import { uint8ArrayToBase64url } from '../../wire/base64url.js'
import { WIRE_MAX_RAW_FRAME_BYTES } from '../../constants.js'

let server: Server
let url: string
let opens: number
let wires: ServerResponse[]
/** The connId of each open, in order. */
let streams: string[]
/** The frames each batch POST carried, with the connId it named. */
let posts: { connId: string; frames: ReturnType<typeof decode>[] }[]
let channels: ClientChannel[]
/** The upload POST is held, neither acknowledged nor answered, as by a proxy that buffers a request's body. */
let holdUploads: boolean

const delay = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms))

/** Waits until `done`, failing after `ms` with what didn't come. */
async function within(ms: number, what: string, done: () => boolean) {
  const until = performance.now() + ms
  while (!done()) {
    if (performance.now() > until) throw new Error(`No ${what} within ${ms} ms`)
    await delay(50)
  }
}

/** The request's body as its frames: the metadata first. */
async function framesOf(req: IncomingMessage): Promise<Uint8Array<ArrayBuffer>[]> {
  const chunks: Uint8Array[] = []
  for await (const chunk of req) chunks.push(chunk)
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
function wire(res: ServerResponse, ix: number): void {
  res.writeHead(200, { 'Content-Type': 'text/event-stream' })
  const frame = encode.reconciled({
    sessionId: crypto.randomUUID(),
    open: [{ ix, lastSeq: 0 }],
    reconnectTimeout: 60_000,
    idleTimeout: 60_000,
    pingInterval: 100_000,
    maxFrameBytes: WIRE_MAX_RAW_FRAME_BYTES,
    creditWindow: 1_000_000,
    sseFlushThrottle: 300,
    ssePostIdleFlushDelay: 50,
    transports: ['sse'],
  })
  res.write(`: open\n\ndata: ${uint8ArrayToBase64url(frame)}\n\n`)
  wires.push(res)
}

/** The request's connId, and whether it opens a wire, from its metadata frame. */
function metadataOf(frame: Uint8Array | undefined): { connId: string; opensWire: boolean } {
  const metadata: unknown = JSON.parse(new TextDecoder().decode(frame))
  if (typeof metadata !== 'object' || metadata === null) throw new Error('A request without metadata')
  if (!('connId' in metadata) || typeof metadata.connId !== 'string') throw new Error('A request without a connId')
  return { connId: metadata.connId, opensWire: 'streamResponse' in metadata && metadata.streamResponse === true }
}

/** The scripted server: it ends or holds an upload POST, records a batch POST, and answers an open with a wire. */
async function answer(req: IncomingMessage, res: ServerResponse): Promise<void> {
  // The upload POST that would stream: ended at once, its frames go in batch POSTs.
  if (req.headers['transfer-encoding'] === 'chunked') {
    if (!holdUploads) res.end()
    return
  }
  const frames = await framesOf(req)
  const { connId, opensWire } = metadataOf(frames[0])
  const sent = frames.slice(1).map((raw) => decode(raw))
  if (!opensWire) {
    posts.push({ connId, frames: sent })
    res.end()
    return
  }
  opens++
  streams.push(connId)
  const reconcile = sent.find((frame) => frame.tag === TAG.RECONCILE)
  wire(res, reconcile?.tag === TAG.RECONCILE ? (reconcile.payload.open[0]?.ix ?? 0) : 0)
}

beforeEach(async () => {
  opens = 0
  wires = []
  streams = []
  posts = []
  channels = []
  holdUploads = false
  server = createServer((req, res) => void answer(req, res))
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
async function openingOf(channel: ClientChannel): Promise<unknown> {
  try {
    await channel.opened
    return 'opened'
  } catch (err) {
    return err
  }
}

// The page waits up to 3 s for its upload's acknowledgement; past it, what it sends goes in batch POSTs.
test(
  'a wire that ends while its upload waits for an acknowledgement is followed by another, which the channel sends on',
  { timeout: 20_000 },
  async () => {
    holdUploads = true
    const channel = openChannel()
    expect(await openingOf(channel)).toBe('opened')
    let reconnected = false
    channel.onReconnect(() => {
      reconnected = true
    })
    wires.shift()?.end()
    await within(5_000, 'second open after the wire ended', () => streams.length >= 2)
    await within(5_000, "reconnect of the channel on the second wire's RECONCILED", () => reconnected)
    void channel.send('after')
    const successor = streams[1]
    await within(6_000, 'message in a batch POST of the second wire', () =>
      posts.some(
        (post) =>
          post.connId === successor &&
          post.frames.some((frame) => frame.tag === TAG.TEXT && frame.text.includes('after')),
      ),
    )
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
    await within(5_000, 'second open after the wire ended', () => opens >= 2 && wires.length > 0)
    let ended = false
    wires[0]?.on('close', () => {
      ended = true
    })
    await delay(4000)
    expect({ opens, ended, closed: channel.isClosed }).toEqual({ opens: 2, ended: false, closed: false })
  },
)
