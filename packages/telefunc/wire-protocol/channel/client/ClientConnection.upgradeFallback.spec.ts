// The SSE→WS handoff's fallback: a WebSocket that closes after the flip sends the page back to SSE, and the old wire's
// frames the join held reach the page's channels first. This spec drives a REAL ClientChannel and its connection against
// a scripted SSE server and a fake WebSocket, as ClientConnection.upgradeJoin.spec.ts does with a stand-in channel: a
// real one answers what it receives with flow-control frames, which a stand-in doesn't send.

import { afterEach, beforeEach, describe, expect, test, vi } from 'vitest'

import { ClientChannel } from './ClientChannel.js'
import { decode, encode, isOneFrame, messageFrames } from '../../wire/frame.js'
import { TAG } from '../../wire/tags.js'
import type { DecodedFrame } from '../../wire/frame.js'
import { decodeU32 } from '../../wire/envelope.js'
import { uint8ArrayToBase64url } from '../../wire/base64url.js'
import { WIRE_MAX_RAW_FRAME_BYTES } from '../../constants.js'

// This spec pins the upgrade's order, not its deadlines: a loaded runner must not reach one between steps.
vi.mock('../../constants.js', async (importOriginal) => ({
  ...(await importOriginal<typeof import('../../constants.js')>()),
  WS_PROBE_TIMEOUT_MS: 60_000,
  UPGRADE_ATTEMPT_TIMEOUT_MS: 60_000,
  UPGRADE_HANDOFF_JOIN_TIMEOUT_MS: 60_000,
  RECONCILE_TIMEOUT_MS: 60_000,
}))
vi.mock('../../wire/pingDeadline.js', async (importOriginal) => ({
  ...(await importOriginal<typeof import('../../wire/pingDeadline.js')>()),
  pingDeadline: () => 60_000,
}))

const delay = (ms: number) => new Promise((r) => setTimeout(r, ms))
/** Long enough for the connect POST, the probe handshake and any chained microtasks to drain. */
const settle = async () => {
  for (let i = 0; i < 20; i++) await delay(5)
}

const bytes = (frame: Uint8Array) => frame as Uint8Array<ArrayBuffer>

class FakeWebSocket {
  static OPEN = 1
  static last: FakeWebSocket | null = null
  onopen: (() => void) | null = null
  onclose: ((event: { code: number }) => void) | null = null
  onerror: (() => void) | null = null
  onmessage: ((ev: { data: ArrayBuffer }) => void) | null = null
  binaryType = 'blob'
  readyState = 1
  /** Frames the client sent on this socket, decoded. */
  closedSocketSends = 0
  readonly sent: DecodedFrame[] = []
  private onSent: ((frame: DecodedFrame) => void) | null = null

  constructor(_url: string) {
    FakeWebSocket.last = this
    queueMicrotask(() => this.onopen?.())
  }
  send(data: ArrayBuffer | Uint8Array): void {
    if (this.readyState !== 1) {
      this.closedSocketSends++
      return
    }
    const raw = new Uint8Array(data)
    for (const bytes of isOneFrame(raw) ? [raw] : messageFrames(raw)) {
      const frame = decode(bytes)
      this.sent.push(frame)
      this.onSent?.(frame)
    }
  }
  close(): void {
    if (this.readyState === 3) return
    this.readyState = 3
    this.onclose?.({ code: 1006 })
  }
  /** Server→client on this socket. */
  deliver(frame: Uint8Array): void {
    this.onmessage?.({ data: bytes(frame).buffer })
  }
  answer(handler: (frame: DecodedFrame) => void): void {
    this.onSent = handler
  }
}

function reconciled(extra: { ix: number; sessionId: string; upgradeId?: string; pingInterval?: number }) {
  return encode.reconciled({
    sessionId: extra.sessionId,
    open: [{ ix: extra.ix, lastSeq: 0 }],
    reconnectTimeout: 60_000,
    idleTimeout: 60_000,
    pingInterval: extra.pingInterval ?? 100_000,
    maxFrameBytes: WIRE_MAX_RAW_FRAME_BYTES,
    creditWindow: 1_000_000,
    sseFlushThrottle: 300,
    ssePostIdleFlushDelay: 50,
    transports: ['sse', 'ws'],
    ...(extra.upgradeId === undefined ? {} : { upgradeId: extra.upgradeId }),
  })
}

/** SSE downstream the client reads (server→client on the old wire). */
function makeDownstream() {
  let controller!: ReadableStreamDefaultController<Uint8Array>
  const enc = new TextEncoder()
  const stream = new ReadableStream<Uint8Array>({ start: (c) => (controller = c) })
  return {
    stream,
    open: () => controller.enqueue(enc.encode(': open\n\n')),
    push: (frame: Uint8Array) => controller.enqueue(enc.encode(`data: ${uint8ArrayToBase64url(bytes(frame))}\n\n`)),
    end: () => controller.close(),
  }
}

function parseLengthPrefixed(buf: Uint8Array): Uint8Array[] {
  const out: Uint8Array[] = []
  let off = 0
  while (off + 4 <= buf.length) {
    const len = decodeU32(bytes(buf.subarray(off, off + 4)))
    off += 4
    out.push(buf.subarray(off, off + len))
    off += len
  }
  return out
}

type Harness = {
  ix: number
  probe: FakeWebSocket
  /** Resolves once the client has emitted its barrier RECONCILE on the old wire. */
  barrierSent: Promise<{ upgradeId: string }>
  pushOld: (frame: Uint8Array) => void
  endOld: () => void
  received: { length: number; first: number; last: number }[]
  opens(): number
  dispose(): void
  reconciles: { ix: number; lastSeq: number }[][]
  /** The tags of what the page sent the scripted SSE server, in order. */
  upstream: number[]
  pushCurrent: (frame: Uint8Array) => void
}

/** Connect over SSE, reconcile, let the client probe WS, answer PREPARE with READY, and stop at
 *  the moment the barrier has been emitted — the start of the join, before either completion frame lands. */
async function upgradeToBarrier(): Promise<Harness> {
  const received: Harness['received'] = []
  const sessionId = crypto.randomUUID()
  let ix = 0
  const oldDownstream = makeDownstream()
  let downstream = oldDownstream
  let opens = 0
  const reconciles: { ix: number; lastSeq: number }[][] = []
  const upstream: number[] = []

  let resolveBarrier!: (value: { upgradeId: string }) => void
  const barrierSent = new Promise<{ upgradeId: string }>((resolve) => {
    resolveBarrier = resolve
  })

  const onUpstreamFrame = (frame: DecodedFrame) => {
    upstream.push(frame.tag)
    if (frame.tag === TAG.BARRIER) resolveBarrier({ upgradeId: frame.payload.upgradeId })
  }

  const fetchImpl = (async (_url: string, init: RequestInit) => {
    const body = init.body as unknown
    if (body instanceof Blob) {
      const frames = parseLengthPrefixed(new Uint8Array(await body.arrayBuffer()))
      const metadata = JSON.parse(new TextDecoder().decode(frames[0]!))
      for (const raw of frames.slice(1)) {
        const frame = decode(bytes(raw))
        if (frame.tag === TAG.RECONCILE) {
          ix = frame.payload.open[0]?.ix ?? 0
          reconciles.push(
            frame.payload.open.map((e: { ix: number; lastSeq: number }) => ({ ix: e.ix, lastSeq: e.lastSeq })),
          )
        }
        onUpstreamFrame(frame)
      }
      if (!metadata.streamResponse) return new Response('', { status: 200 })
      if (opens++ > 0) downstream = makeDownstream()
      downstream.open()
      downstream.push(reconciled({ ix, sessionId }))
      return new Response(downstream.stream as BodyInit, {
        status: 200,
        headers: { 'Content-Type': 'text/event-stream' },
      })
    }
    // Long-lived upstream POST: ack it (that is what keeps the client in duplex mode), then hold
    // the response open and read frames as the client pushes them, as the real server does.
    downstream.push(encode.streamRequestOpenAck())
    const stream = body as ReadableStream<Uint8Array>
    return await new Promise<Response>((resolve, reject) => {
      init.signal?.addEventListener('abort', () => reject(new DOMException('aborted', 'AbortError')), { once: true })
      void (async () => {
        const reader = stream.getReader()
        let buf = new Uint8Array(0)
        let sawMetadata = false
        while (true) {
          const { value, done } = await reader.read()
          if (done) break
          const next = new Uint8Array(buf.length + value.length)
          next.set(buf)
          next.set(value, buf.length)
          buf = next
          while (buf.length >= 4) {
            const len = decodeU32(bytes(buf.subarray(0, 4)))
            if (buf.length < 4 + len) break
            const raw = buf.subarray(4, 4 + len)
            buf = buf.subarray(4 + len)
            if (!sawMetadata) {
              sawMetadata = true
              continue
            }
            onUpstreamFrame(decode(bytes(raw)))
          }
        }
        resolve(new Response('', { status: 200 }))
      })().catch(reject)
    })
  }) as unknown as typeof fetch

  const realFetch = globalThis.fetch
  globalThis.fetch = fetchImpl
  let channel: ClientChannel
  try {
    channel = new ClientChannel({
      channelId: crypto.randomUUID(),
      connection: {
        telefuncUrl: 'http://test.local/_telefunc',
        transports: ['sse', 'ws'],
        connectionKey: crypto.randomUUID(),
      },
    })
  } finally {
    globalThis.fetch = realFetch
  }
  channel.listenBinary((data) => {
    received.push({ length: data.length, first: data[0] ?? -1, last: data.at(-1) ?? -1 })
  })

  await settle()
  const probe = FakeWebSocket.last
  if (probe === null) throw new Error('the client should have probed a WebSocket')

  // Probe handshake: PING→PONG proves the wire, then PREPARE→READY stages the upgrade.
  probe.answer((frame) => {
    if (frame.tag === TAG.PING) probe.deliver(encode.pong())
    if (frame.tag === TAG.PREPARE) probe.deliver(encode.ready({ upgradeId: frame.payload.upgradeId }))
  })
  probe.deliver(encode.pong())

  const barrier = await Promise.race([barrierSent, settle().then(() => null)])
  expect(barrier, 'the client should have emitted its barrier').not.toBeNull()

  return {
    ix,
    probe,
    barrierSent,
    pushOld: oldDownstream.push,
    endOld: oldDownstream.end,
    received,
    opens: () => opens,
    reconciles,
    upstream,
    pushCurrent: (frame: Uint8Array) => downstream.push(frame),
    dispose: () => channel.abort(),
  }
}

const realWebSocket = globalThis.WebSocket

beforeEach(() => {
  FakeWebSocket.last = null
  ;(globalThis as unknown as { WebSocket: unknown }).WebSocket = FakeWebSocket
})
afterEach(() => {
  ;(globalThis as unknown as { WebSocket: unknown }).WebSocket = realWebSocket
})

// Case 1: the new WebSocket closes after its RECONCILED, while the old SSE wire's FIN is still outstanding and a
// data frame of the old wire waits in the join's buffer. The page goes back to SSE.
// Expected: the buffered message reaches the app once, no internal assert fires, the page reconnects over SSE, and
// the reconnect's RECONCILE reports only what the app got.
/** The two wires of a join, either of which may go between the new one's RECONCILED and the old one's FIN. */
const ENDS: Record<string, (h: Harness) => void> = {
  'the WebSocket closes': (h) => h.probe.close(),
  'the old SSE wire ends': (h) => h.endOld(),
}

for (const [ending, end] of Object.entries(ENDS)) {
  test(`${ending} before the old wire's FIN: the page falls back to SSE, and its channel gets the data the join held, then the next`, async () => {
    // What a frame's handling throws is thrown from a microtask (`dispatchFrame`): collect it here.
    const uncaught: unknown[] = []
    const realQueueMicrotask = globalThis.queueMicrotask
    globalThis.queueMicrotask = (cb) =>
      realQueueMicrotask(() => {
        try {
          cb()
        } catch (err) {
          uncaught.push(err)
        }
      })
    const h = await upgradeToBarrier()
    try {
      const { upgradeId } = await h.barrierSent
      // Its receipt starts a flow-control probe, which the page sends at once where its wire is open.
      const payload = new Uint8Array(512 * 1024).fill(42)
      h.pushOld(encode.binary(h.ix, payload, 1))
      h.probe.deliver(reconciled({ ix: h.ix, sessionId: crypto.randomUUID(), upgradeId }))
      await settle()
      expect(h.received, 'RECONCILED arrived but FIN is still outstanding').toEqual([])
      const reconcilesBefore = h.reconciles.length
      const handedBefore = h.probe.sent.length
      const upstreamBefore = h.upstream.length

      end(h)
      // The reconnect's initial backoff is 500 ms; 2 s bounds the wait for it.
      const limit = Date.now() + 2_000
      while (h.opens() < 2 && Date.now() < limit) await delay(20)
      await settle()
      // The server's next message, seq 2, on the new SSE wire.
      h.pushCurrent(encode.binary(h.ix, new Uint8Array(1024).fill(7), 2))
      await settle()

      expect({
        uncaught: uncaught.map((err) => (err instanceof Error ? err.message : err)),
        received: h.received,
        sseOpens: h.opens(),
        reconnectReconciles: h.reconciles.slice(reconcilesBefore),
        handedTheWebSocketAfter: h.probe.sent.slice(handedBefore).map((frame) => frame.tag),
        // The flow-control probe the held data started goes once the page is back, on the new SSE wire; on a slow run
        // the next message may start another, once the probe interval passed.
        flowControlProbeAfter: h.upstream.slice(upstreamBefore).some((tag) => tag === TAG.BDP_PING),
        closedSocketSends: h.probe.closedSocketSends,
      }).toEqual({
        uncaught: [],
        received: [
          { length: payload.length, first: 42, last: 42 },
          { length: 1024, first: 7, last: 7 },
        ],
        sseOpens: 2,
        reconnectReconciles: [[{ ix: h.ix, lastSeq: 1 }]],
        handedTheWebSocketAfter: [],
        flowControlProbeAfter: true,
        closedSocketSends: 0,
      })
    } finally {
      globalThis.queueMicrotask = realQueueMicrotask
      h.dispose()
    }
  })
}
