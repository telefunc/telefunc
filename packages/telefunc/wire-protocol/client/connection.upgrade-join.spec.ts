// The SSE→WS handoff's join: the client flips to the new wire, then waits for BOTH limbs — FIN on
// the old wire, RECONCILED on the new one — before it resumes dispatching. The limbs cross two
// sockets, so they can arrive in either order, and anything that lands mid-join must be held and
// replayed in the right order afterwards.
//
// The browser e2e proves an upgrade happens and delivers exactly-once; it cannot pin these
// orderings. This spec drives the REAL ClientConnection against a scripted SSE server and a fake
// WebSocket, so each interleaving is chosen rather than raced.

import { afterEach, beforeEach, describe, expect, test } from 'vitest'

import { ClientConnection } from './connection.js'
import { decode, encode, TAG } from '../shared-ws.js'
import type { DecodedFrame } from '../shared-ws.js'
import { decodeU32 } from '../frame.js'
import { uint8ArrayToBase64url } from '../base64url.js'

const delay = (ms: number) => new Promise((r) => setTimeout(r, ms))
/** Long enough for the connect POST, the probe handshake and any chained microtasks to drain. */
const settle = async () => {
  for (let i = 0; i < 20; i++) await delay(5)
}

const bytes = (frame: Uint8Array) => frame as Uint8Array<ArrayBuffer>

/** The frames the connection let through to the channel, in dispatch order. */
function createChannel(dispatched: DecodedFrame[]) {
  return {
    id: crypto.randomUUID(),
    isClosed: false,
    _onTransportOpen() {},
    _dispatchFrame(frame: DecodedFrame) {
      dispatched.push(frame)
    },
    _onTransportClose() {},
  }
}

class FakeWebSocket {
  static last: FakeWebSocket | null = null
  onopen: (() => void) | null = null
  onclose: (() => void) | null = null
  onerror: (() => void) | null = null
  onmessage: ((ev: { data: ArrayBuffer }) => void) | null = null
  binaryType = 'blob'
  readyState = 1
  /** Frames the client sent on this socket, decoded. */
  readonly sent: DecodedFrame[] = []
  private onSent: ((frame: DecodedFrame) => void) | null = null

  constructor(_url: string) {
    FakeWebSocket.last = this
    queueMicrotask(() => this.onopen?.())
  }
  send(data: ArrayBuffer | Uint8Array): void {
    const frame = decode(new Uint8Array(data instanceof Uint8Array ? data : new Uint8Array(data)))
    this.sent.push(frame)
    this.onSent?.(frame)
  }
  close(): void {
    if (this.readyState === 3) return
    this.readyState = 3
    this.onclose?.()
  }
  /** Server→client on this socket. */
  deliver(frame: Uint8Array): void {
    this.onmessage?.({ data: bytes(frame).buffer })
  }
  answer(handler: (frame: DecodedFrame) => void): void {
    this.onSent = handler
  }
}

function reconciled(extra: { ix: number; sessionId: string; upgradeId?: string }) {
  return encode.reconciled({
    sessionId: extra.sessionId,
    open: [{ ix: extra.ix, lastSeq: 0 }],
    reconnectTimeout: 60_000,
    idleTimeout: 60_000,
    pingInterval: 100_000,
    clientReplayBuffer: 1_000_000,
    clientReplayBufferBinary: 2_000_000,
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
  dispatched: DecodedFrame[]
  connection: ClientConnection
  channel: ReturnType<typeof createChannel>
}

/** Connect over SSE, reconcile, let the client probe WS, answer PREPARE with READY, and stop at
 *  the moment the barrier has been emitted — the start of the join, before either limb lands. */
async function upgradeToBarrier(): Promise<Harness> {
  const dispatched: DecodedFrame[] = []
  const channel = createChannel(dispatched)
  const sessionId = crypto.randomUUID()
  let ix = 0
  const downstream = makeDownstream()

  let resolveBarrier!: (value: { upgradeId: string }) => void
  const barrierSent = new Promise<{ upgradeId: string }>((resolve) => {
    resolveBarrier = resolve
  })

  const onUpstreamFrame = (frame: DecodedFrame) => {
    if (frame.tag === TAG.BARRIER) resolveBarrier({ upgradeId: frame.payload.upgradeId })
  }

  const fetchImpl = (async (_url: string, init: RequestInit) => {
    const body = init.body as unknown
    if (body instanceof Blob) {
      const frames = parseLengthPrefixed(new Uint8Array(await body.arrayBuffer()))
      const metadata = JSON.parse(new TextDecoder().decode(frames[0]!))
      for (const raw of frames.slice(1)) {
        const frame = decode(bytes(raw))
        if (frame.tag === TAG.RECONCILE) ix = frame.payload.open[0]?.ix ?? 0
        onUpstreamFrame(frame)
      }
      if (!metadata.streamResponse) return new Response('', { status: 200 })
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

  const connection = ClientConnection.getOrCreate('http://test.local/_telefunc', channel as never, {
    transports: ['sse', 'ws'],
    fetchImpl,
    connectionKey: crypto.randomUUID(),
  })

  await settle()
  const probe = FakeWebSocket.last
  expect(probe, 'the client should have probed a WebSocket').not.toBeNull()

  // Probe handshake: PING→PONG proves the wire, then PREPARE→READY stages the upgrade.
  probe!.answer((frame) => {
    if (frame.tag === TAG.PING) probe!.deliver(encode.pong())
    if (frame.tag === TAG.PREPARE) probe!.deliver(encode.ready({ upgradeId: frame.payload.upgradeId }))
  })
  probe!.deliver(encode.pong())

  const barrier = await Promise.race([barrierSent, settle().then(() => null)])
  expect(barrier, 'the client should have emitted its barrier').not.toBeNull()

  return { ix, probe: probe!, barrierSent, pushOld: downstream.push, dispatched, connection, channel }
}

const realWebSocket = globalThis.WebSocket

beforeEach(() => {
  FakeWebSocket.last = null
  ;(globalThis as unknown as { WebSocket: unknown }).WebSocket = FakeWebSocket
})
afterEach(() => {
  ;(globalThis as unknown as { WebSocket: unknown }).WebSocket = realWebSocket
})

describe('SSE→WS handoff join', () => {
  test('COMMITTED then FIN: held frames replay old wire first, in order', async () => {
    const h = await upgradeToBarrier()
    const { upgradeId } = await h.barrierSent

    // Both wires keep talking across the join; nothing may reach the channel yet.
    h.pushOld(encode.text(h.ix, '"old-1"', 1))
    h.probe.deliver(encode.text(h.ix, '"new-1"', 2))
    await settle()
    expect(h.dispatched).toEqual([])

    h.probe.deliver(reconciled({ ix: h.ix, sessionId: crypto.randomUUID(), upgradeId }))
    await settle()
    expect(h.dispatched, 'RECONCILED alone must not end the join — FIN is still outstanding').toEqual([])

    h.pushOld(encode.fin())
    await settle()
    expect(h.dispatched.map((f) => (f as { text: string }).text)).toEqual(['"old-1"', '"new-1"'])
  })

  test('FIN then COMMITTED: the reordered limbs still complete the join', async () => {
    const h = await upgradeToBarrier()
    const { upgradeId } = await h.barrierSent

    h.pushOld(encode.text(h.ix, '"old-1"', 1))
    h.pushOld(encode.fin())
    await settle()
    expect(h.dispatched, 'FIN alone must not end the join — COMMITTED is still outstanding').toEqual([])

    h.probe.deliver(encode.text(h.ix, '"new-1"', 2))
    h.probe.deliver(reconciled({ ix: h.ix, sessionId: crypto.randomUUID(), upgradeId }))
    await settle()
    expect(h.dispatched.map((f) => (f as { text: string }).text)).toEqual(['"old-1"', '"new-1"'])
  })

  test("a RECONCILED for another upgrade's id does not settle this one", async () => {
    const h = await upgradeToBarrier()
    await h.barrierSent

    h.probe.deliver(reconciled({ ix: h.ix, sessionId: crypto.randomUUID(), upgradeId: 'someone-elses' }))
    h.pushOld(encode.fin())
    h.probe.deliver(encode.text(h.ix, '"new-1"', 1))
    await settle()
    expect(h.dispatched, 'the join must still be waiting for its own COMMITTED').toEqual([])
  })
})
