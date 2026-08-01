// Regression: SSE client→server reconnect must preserve seq order, so the server's seq-dedup
// (server/channel.ts: `seq <= _lastClientSeq` ⇒ drop) never drops a real message.
//
// The hazard: a fire-and-forget send (seq 1) goes out on the streamRequest wire and is lost (a
// transient disconnect before the server reads it). A second send (seq 2) is buffered during the
// reconnect. If the client emits seq 2 in the reconnect's initial batch — ahead of the replay of
// the older seq 1 — the server dispatches seq 2 first (advancing _lastClientSeq to 2) and then
// drops the replayed seq 1 as a "duplicate" (1 <= 2). seq 1 would be lost forever.
//
// Drives the REAL ClientConnection (send/buffer/replay/reconnect) against a mock SSE server that
// uses the REAL ServerChannel for seq-dedup. Only the network + RECONCILED plumbing is modelled,
// faithfully mirroring server/sse.ts runStreamResponse + server/mux.ts:313 (RECONCILED.lastSeq is
// captured from _lastClientSeq at reconcile time, before the initial-batch data frames dispatch).

import { describe, expect, test } from 'vitest'
import { stringify } from '@brillout/json-serializer/stringify'

import { ClientConnection } from './connection.js'
import { ServerChannel } from '../server/channel.js'
import { decode, encode, TAG, type DecodedFrame } from '../shared-ws.js'
import { decodeU32, concat } from '../frame.js'
import { uint8ArrayToBase64url } from '../base64url.js'

const delay = (ms: number) => new Promise((r) => setTimeout(r, ms))

type Bytes = Uint8Array<ArrayBuffer>
type TextFrame = Extract<DecodedFrame, { tag: typeof TAG.TEXT }>
type FetchStub = typeof fetch
type SsePostMetadata = { streamResponse?: boolean }

function createChannel(id = crypto.randomUUID()) {
  return { id, isClosed: false, _onTransportOpen() {}, _dispatchFrame() {}, _onTransportClose() {} }
}

/** SSE downstream response stream the client reads (server→client). */
function makeSseDownstream() {
  let controller!: ReadableStreamDefaultController<Bytes>
  const enc = new TextEncoder()
  const stream = new ReadableStream<Bytes>({ start: (c) => (controller = c) })
  return {
    stream,
    pushFrame: (frame: Bytes) => controller.enqueue(enc.encode(`data: ${uint8ArrayToBase64url(frame)}\n\n`)),
    open: () => controller.enqueue(enc.encode(`: open\n\n`)),
    close: () => {
      try {
        controller.close()
      } catch {}
    },
  }
}

/** Parse a one-shot Blob body: [u32 len][metadata json][u32 len][frame]... */
async function parseBlobBody(blob: Blob): Promise<{ metadata: SsePostMetadata; frames: Bytes[] }> {
  const bytes = new Uint8Array(await blob.arrayBuffer())
  let off = 0
  const next = (): Bytes => {
    const len = decodeU32(bytes.subarray(off, off + 4))
    off += 4
    const chunk = bytes.subarray(off, off + len)
    off += len
    return chunk
  }
  const metadata: SsePostMetadata = JSON.parse(new TextDecoder().decode(next()))
  const frames: Bytes[] = []
  while (off < bytes.length) frames.push(next())
  return { metadata, frames }
}

/** Incrementally yield length-prefixed chunks from a ReadableStream (streamRequest body). */
async function* readLengthPrefixed(stream: ReadableStream<Bytes>): AsyncGenerator<Bytes> {
  const reader = stream.getReader()
  let buf = new Uint8Array(0)
  const pull = async (): Promise<boolean> => {
    const { value, done } = await reader.read()
    if (done) return false
    buf = buf.length === 0 ? value : concat(buf, value)
    return true
  }
  const ensure = async (n: number): Promise<boolean> => {
    while (buf.length < n) if (!(await pull())) return false
    return true
  }
  while (true) {
    if (!(await ensure(4))) return
    const len = decodeU32(buf.subarray(0, 4))
    buf = buf.subarray(4)
    if (!(await ensure(len))) return
    yield buf.subarray(0, len)
    buf = buf.subarray(len)
  }
}

function decodeTextFrames(frames: Bytes[]): TextFrame[] {
  const textFrames: TextFrame[] = []
  for (const raw of frames) {
    const frame = decode(raw)
    if (frame.tag === TAG.TEXT) textFrames.push(frame)
  }
  return textFrames
}

/**
 * Connect, send seq 1, sever the wire (optionally losing seq 1 in flight), send seq 2 while
 * reconnecting, then let the reconnect settle. Returns what the server channel actually received.
 */
async function runScenario(loseSeq1: boolean): Promise<{ received: number[]; wire2Upstream: number[] }> {
  const received: number[] = []
  const serverCh = new ServerChannel<(n: number) => void, never>()
  serverCh.listen((n) => void received.push(n))

  const upstreamSeqsByPost: number[][] = []
  let streamReqCount = 0
  let dropUpstream = loseSeq1 // wire 1's upstream is "lost" only in the lost-frame case
  let wire = 0 // count of SSE downstream POSTs (connect attempts)
  let downstream: ReturnType<typeof makeSseDownstream> | null = null

  // Faithful model of server/sse.ts runStreamResponse + mux reconcile:
  // capture lastSeq BEFORE dispatching the batch's data frames, then dispatch, then RECONCILED.
  const handleInitialBatch = (frames: Bytes[], sse: ReturnType<typeof makeSseDownstream>) => {
    let ix = 0
    for (const raw of frames) {
      const f = decode(raw)
      if (f.tag === TAG.RECONCILE) ix = f.payload.open[0]!.ix
    }
    const lastSeqAtReconcile = serverCh._lastClientSeq // mux.ts:313 — captured at reconcile time
    for (const f of decodeTextFrames(frames)) serverCh._dispatchFrame(f) // bumps _lastClientSeq, real dedup
    sse.open()
    sse.pushFrame(encode.streamRequestOpenAck())
    sse.pushFrame(
      encode.reconciled({
        sessionId: crypto.randomUUID(),
        open: [{ ix, lastSeq: lastSeqAtReconcile }],
        reconnectTimeout: 60_000,
        idleTimeout: 60_000,
        pingInterval: 100_000,
        clientReplayBuffer: 1_000_000,
        clientReplayBufferBinary: 2_000_000,
        sseFlushThrottle: 300,
        ssePostIdleFlushDelay: 50,
        transports: ['sse'],
      }),
    )
  }

  const handleSsePost = async (body: Blob): Promise<Response> => {
    const { metadata, frames } = await parseBlobBody(body)
    if (metadata.streamResponse) {
      wire += 1
      const sse = makeSseDownstream()
      downstream = sse
      handleInitialBatch(frames, sse)
      return new Response(sse.stream, { status: 200, headers: { 'Content-Type': 'text/event-stream' } })
    }
    for (const frame of decodeTextFrames(frames)) serverCh._dispatchFrame(frame)
    return new Response('', { status: 200 })
  }

  const handleStreamRequest = (stream: ReadableStream<Bytes>, signal?: AbortSignal | null): Promise<Response> => {
    const postIx = ++streamReqCount
    const seen: number[] = []
    upstreamSeqsByPost[postIx] = seen
    return new Promise<Response>((resolve, reject) => {
      signal?.addEventListener('abort', () => reject(new DOMException('aborted', 'AbortError')), { once: true })
      ;(async () => {
        let first = true
        for await (const chunk of readLengthPrefixed(stream)) {
          if (first) {
            first = false
            continue
          } // metadata
          const f = decode(chunk)
          if (f.tag === TAG.TEXT) {
            seen.push(f.seq)
            if (!dropUpstream) serverCh._dispatchFrame(f) // wire 1 dropped in the lost-frame case
          }
        }
        resolve(new Response('', { status: 200 }))
      })().catch(reject)
    })
  }

  const fetchImpl = (async (_input, init) => {
    const body = init?.body
    if (body instanceof Blob) return handleSsePost(body)
    if (body instanceof ReadableStream) return handleStreamRequest(body, init?.signal)
    throw new TypeError('Expected an SSE Blob or byte stream request body')
  }) satisfies FetchStub

  const channel = createChannel()
  const conn = ClientConnection.getOrCreate('http://test.local/_telefunc', channel as never, {
    transports: ['sse'],
    fetchImpl,
    connectionKey: crypto.randomUUID(),
  })

  // 1) First connect + reconcile.
  await delay(120)
  expect(wire).toBe(1)

  // 2) Send seq 1 — goes onto wire 1's streamRequest (delivered, or "lost" in flight).
  conn.send(channel as never, stringify(1))
  await delay(40)
  expect(upstreamSeqsByPost[1]).toContain(1) // client always emits seq 1 on wire 1

  // 3) Sever wire 1 → client reconnects. From here upstream is delivered to the server.
  dropUpstream = false
  downstream!.close()
  await delay(20)

  // 4) Send seq 2 while reconnecting → buffered, picked up by the reconnect.
  conn.send(channel as never, stringify(2))

  // 5) Wait out the reconnect (CHANNEL_RECONNECT_INITIAL_DELAY_MS = 500ms) + settle.
  await delay(900)
  expect(wire).toBe(2)
  await delay(50)

  return { received, wire2Upstream: upstreamSeqsByPost[2] ?? [] }
}

describe('SSE reconnect preserves client→server seq order (exactly-once)', () => {
  test('an earlier send that survived the wire, then a buffered send → both delivered, in order', async () => {
    const { received } = await runScenario(/* loseSeq1 */ false)
    expect(received).toEqual([1, 2])
  })

  test('an earlier send LOST on the wire is redelivered ahead of a newer buffered send', async () => {
    // Regression guard. Before the fix the client emitted seq 2 in the reconnect's initial batch
    // ahead of seq 1's replay; the server advanced _lastClientSeq to 2 and dup-dropped the
    // replayed seq 1, yielding [2]. The fix defers reconnect data to the post-RECONCILED replay,
    // so seq 1 (older) lands before seq 2 (newer) and both are delivered exactly once.
    const { received, wire2Upstream } = await runScenario(/* loseSeq1 */ true)
    expect(wire2Upstream).toContain(1) // client replays the lost seq 1 on the new wire…
    expect(received).toEqual([1, 2]) // …and the server now accepts it (no spurious dup-drop)
  })
})
