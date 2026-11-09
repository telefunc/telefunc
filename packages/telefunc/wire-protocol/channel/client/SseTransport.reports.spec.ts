// The page reports what its SSE event stream brought, for the server's path estimate, in the batch POSTs it makes
// anyway: a POST that went for a report alone would hold the next frames' POST for its round trip.

import { describe, expect, test } from 'vitest'
import { stringify } from '@brillout/json-serializer/stringify'

import { ClientConnection } from './ClientConnection.js'
import { decode, encode } from '../../wire/frame.js'
import { TAG } from '../../wire/tags.js'
import { decodeU32 } from '../../wire/envelope.js'
import { uint8ArrayToBase64url } from '../../wire/base64url.js'
import { WIRE_MAX_RAW_FRAME_BYTES } from '../../constants.js'

const delay = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms))

const channel = {
  id: crypto.randomUUID(),
  isClosed: false,
  _maxFrameBytes: WIRE_MAX_RAW_FRAME_BYTES,
  _onTransportOpen() {},
  _dispatchFrame() {},
  _onEnded() {},
  _attach: {
    state: () => ({}),
    receiveCredit: { onProbeAnswered() {}, acknowledge() {}, answered() {} },
    capWindows() {},
    onBatched() {},
    onUpgradeJoined() {},
  },
}

/** A batch POST's body: [u32 len][metadata json][u32 len][frame]... */
async function parseBody(blob: Blob): Promise<{ metadata: { streamResponse?: boolean }; frames: Uint8Array[] }> {
  const bytes = new Uint8Array(await blob.arrayBuffer())
  let offset = 0
  const next = (): Uint8Array => {
    const length = decodeU32(bytes.subarray(offset, offset + 4) as Uint8Array<ArrayBuffer>)
    offset += 4
    return bytes.subarray(offset, (offset += length))
  }
  const metadata = JSON.parse(new TextDecoder().decode(next()))
  const frames: Uint8Array[] = []
  while (offset < bytes.length) frames.push(next())
  return { metadata, frames }
}

/** A server that refuses streaming uploads, so that frames go in batch POSTs, and records each one's frame tags. */
function createServer() {
  const posts: number[][] = []
  let events!: ReadableStreamDefaultController<Uint8Array>
  let ix = 0
  const push = (frame: Uint8Array) =>
    events.enqueue(new TextEncoder().encode(`data: ${uint8ArrayToBase64url(frame as Uint8Array<ArrayBuffer>)}\n\n`))
  const fetchImpl = (async (_url: string, init: RequestInit) => {
    if (!(init.body instanceof Blob)) throw new TypeError('no streaming uploads')
    const { metadata, frames } = await parseBody(init.body)
    if (!metadata.streamResponse) {
      posts.push(frames.map((frame) => frame[0]!))
      return new Response('', { status: 200 })
    }
    for (const raw of frames) {
      const frame = decode(raw as Uint8Array<ArrayBuffer>)
      if (frame.tag === TAG.RECONCILE) ix = frame.payload.open[0]!.ix
    }
    const stream = new ReadableStream<Uint8Array>({ start: (controller) => void (events = controller) })
    queueMicrotask(() => {
      events.enqueue(new TextEncoder().encode(': open\n\n'))
      push(
        encode.reconciled({
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
        }),
      )
    })
    return new Response(stream, { status: 200, headers: { 'Content-Type': 'text/event-stream' } })
  }) as unknown as typeof fetch
  return { fetchImpl, posts, sendText: (seq: number) => push(encode.text(ix, stringify(seq), seq)) }
}

describe('SSE path reports', () => {
  test('ride in the POSTs the page makes anyway, and never go alone', async () => {
    const server = createServer()
    const connection = ClientConnection.getOrCreate('http://test.local/_telefunc', channel as never, {
      transports: ['sse'],
      fetchImpl: server.fetchImpl,
      connectionKey: crypto.randomUUID(),
    })
    const alone = () => server.posts.filter((tags) => tags.every((tag) => tag === TAG.PIECES_ACK))
    await delay(120)
    for (let seq = 1; seq <= 3; seq++) server.sendText(seq)
    // Past the POST throttle: a report would have gone by now.
    await delay(500)
    expect(alone()).toEqual([])
    for (let seq = 4; seq <= 6; seq++) server.sendText(seq)
    await delay(20)
    connection.sendSequenced(channel as never, 'data', encode.text, stringify(1))
    await delay(500)
    expect(alone()).toEqual([])
    expect(server.posts.at(-1)).toEqual([TAG.TEXT, TAG.PIECES_ACK])
  })
})
