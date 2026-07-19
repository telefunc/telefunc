// Client-side driver for a REAL `ClientConnection` through a REAL SSE→WS upgrade, up to and
// including the handoff window.
//
// Two seams are needed and neither costs a production change:
//   - SSE: `fetchImpl` is already an option (`connection.ts:148`), exercised by `connection.spec.ts`.
//   - WS: `connection.ts:1301` / `:1385` do a bare `new WebSocket(this.wsUrl)` and
//     `TRANSPORT_REGISTRY` (`:1948-1955`) is a module const, not an option — so a
//     `globalThis.WebSocket` stub is the only seam. No spec in the repo did this before.
//
// Real timers throughout. Fake timers and real `ReadableStream`s do not mix — stream-reader
// wakeups are not timer-driven — which is why `connection.reconnect-ordering.spec.ts` avoids `vi`
// entirely. Waits here are condition-polled with a deadline rather than fixed sleeps, so they are
// both faster and not timing-pinned.

export { installWebSocketStub, createUpgradeHarness, waitUntil, reconciledPayload }
export type { StubWebSocket, UpgradeHarness, HarnessClientChannel }

import { parse } from '@brillout/json-serializer/parse'

import { uint8ArrayToBase64url } from './base64url.js'
import { ClientConnection } from './client/connection.js'
import { concat, decodeU32 } from './frame.js'
import { TAG, decode, encode, type ChannelFrame, type DecodedFrame, type ReconciledPayload } from './shared-ws.js'

// ── globalThis.WebSocket stub ────────────────────────────────────────────────────────────────

class StubWebSocket {
  static readonly CONNECTING = 0
  static readonly OPEN = 1
  static readonly CLOSING = 2
  static readonly CLOSED = 3

  readyState = StubWebSocket.CONNECTING
  binaryType = 'blob'
  onopen: ((ev: unknown) => void) | null = null
  onmessage: ((ev: { data: ArrayBuffer }) => void) | null = null
  onerror: ((ev: unknown) => void) | null = null
  onclose: ((ev: unknown) => void) | null = null

  /** Every frame the client wrote to this socket, decoded, in call order. */
  readonly sent: DecodedFrame[] = []
  /** Answer PINGs with PONGs, as any live server does — this is what makes `probe()` succeed. */
  autoPong = true

  constructor(readonly url: string) {
    setTimeout(() => {
      if (this.readyState !== StubWebSocket.CONNECTING) return
      this.readyState = StubWebSocket.OPEN
      this.onopen?.({})
    }, 0)
  }

  send(data: ArrayBufferView | ArrayBuffer): void {
    const bytes =
      data instanceof ArrayBuffer ? new Uint8Array(data) : new Uint8Array(data.buffer, data.byteOffset, data.byteLength)
    this.sent.push(decode(bytes as Uint8Array<ArrayBuffer>))
    if (this.autoPong && bytes[0] === TAG.PING) setTimeout(() => this.emit(encode.pong()), 0)
  }

  close(_code?: number): void {
    if (this.readyState === StubWebSocket.CLOSED) return
    this.readyState = StubWebSocket.CLOSED
    this.onclose?.({})
  }

  /** Server→client frame. Deliberately still fires after `close()`: `ProbeWire.close`
   *  (`connection.ts:1352-1357`) does NOT null `ws.onmessage`, so a late frame really can land
   *  on a closed probe socket. A stub that silently swallowed it would hide that. */
  emit(frame: Uint8Array): void {
    this.onmessage?.({ data: new Uint8Array(frame).buffer })
  }
}

/** Installs the stub on `globalThis` and returns every socket the code under test opens.
 *  Call `restore()` in `afterEach`. */
function installWebSocketStub(): { sockets: StubWebSocket[]; restore: () => void } {
  const sockets: StubWebSocket[] = []
  const previous = (globalThis as Record<string, unknown>).WebSocket
  const Stub = class extends StubWebSocket {
    constructor(url: string) {
      super(url)
      sockets.push(this)
    }
  }
  ;(globalThis as Record<string, unknown>).WebSocket = Stub
  return {
    sockets,
    restore: () => {
      if (previous === undefined) delete (globalThis as Record<string, unknown>).WebSocket
      else (globalThis as Record<string, unknown>).WebSocket = previous
    },
  }
}

// ── Waiting ──────────────────────────────────────────────────────────────────────────────────

/** Poll `predicate` until it holds. Throws with `label` on timeout, so a harness that never
 *  reaches the state under test fails loudly instead of asserting against a half-built world. */
async function waitUntil(predicate: () => boolean, label: string, timeoutMs = 3_000): Promise<void> {
  const deadline = Date.now() + timeoutMs
  while (!predicate()) {
    if (Date.now() > deadline) throw new Error(`Timed out waiting for: ${label}`)
    await new Promise((resolve) => setTimeout(resolve, 5))
  }
}

// ── Channels ─────────────────────────────────────────────────────────────────────────────────

type ReceivedPayload = { kind: 'text'; value: unknown } | { kind: 'binary'; bytes: Uint8Array }

/** The shape `handoffBuffered()` reads out of the connection's private upgrade state. */
type UpgradeStateShape = { tag: string; bufferedFrames: number; bufferedBytes: number }

type HarnessClientChannel = {
  readonly id: string
  isClosed: boolean
  /** Data payloads as the application would see them, captured at `_dispatchFrame` — i.e. BELOW
   *  `ClientConnection.dispatchFrame`'s `trackSeq` dedup (`connection.ts:695`), which is the
   *  coalescer that silently drops. Observing above it would prove nothing. */
  readonly received: ReceivedPayload[]
  /** Per-channel ctrl frames that reached the channel. */
  readonly ctrl: ChannelFrame[]
  /** Every error handed to `_onTransportClose` — the abort-value oracle. */
  readonly closeErrors: (Error | undefined)[]
  /** `_onTransportOpen` calls, with the `batched` flag each carried. */
  readonly opens: boolean[]
}

function createHarnessChannel(id: string): HarnessClientChannel {
  return {
    id,
    isClosed: false,
    received: [],
    ctrl: [],
    closeErrors: [],
    opens: [],
    _onTransportOpen(batched: boolean) {
      this.opens.push(batched)
    },
    _dispatchFrame(frame: ChannelFrame) {
      if (frame.tag === TAG.TEXT || frame.tag === TAG.TEXT_ACK_REQ) {
        this.received.push({ kind: 'text', value: parse(frame.text) })
      } else if (frame.tag === TAG.BINARY || frame.tag === TAG.BINARY_ACK_REQ) {
        this.received.push({ kind: 'binary', bytes: frame.data })
      } else {
        this.ctrl.push(frame)
      }
    },
    _onTransportClose(err?: Error) {
      this.isClosed = true
      this.closeErrors.push(err)
    },
  } as HarnessClientChannel
}

// ── RECONCILED payloads ──────────────────────────────────────────────────────────────────────

/** Server settings the client is happy with and that keep timers out of the way: a 100 s ping
 *  interval means no heartbeat fires inside any test's budget. */
function reconciledPayload(open: ReconciledPayload['open'], sessionId = crypto.randomUUID()): ReconciledPayload {
  return {
    sessionId,
    open,
    reconnectTimeout: 60_000,
    idleTimeout: 60_000,
    pingInterval: 100_000,
    clientReplayBuffer: 1_000_000,
    clientReplayBufferBinary: 2_000_000,
    sseFlushThrottle: 300,
    ssePostIdleFlushDelay: 50,
    transports: ['sse', 'ws'],
  }
}

// ── The SSE mock server + the upgrade driver ─────────────────────────────────────────────────

/** SSE downstream response body the client reads (server→client). */
function makeSseDownstream() {
  let controller!: ReadableStreamDefaultController<Uint8Array>
  const enc = new TextEncoder()
  const stream = new ReadableStream<Uint8Array>({ start: (c) => (controller = c) })
  let open = true
  return {
    stream,
    comment: () => controller.enqueue(enc.encode(`: open\n\n`)),
    pushFrame: (frame: Uint8Array) => {
      if (!open) return
      controller.enqueue(enc.encode(`data: ${uint8ArrayToBase64url(frame as Uint8Array<ArrayBuffer>)}\n\n`))
    },
    close: () => {
      if (!open) return
      open = false
      try {
        controller.close()
      } catch {}
    },
  }
}

/** Incrementally yield length-prefixed chunks from the streamRequest body. */
async function* readLengthPrefixed(
  stream: ReadableStream<Uint8Array<ArrayBuffer>>,
): AsyncGenerator<Uint8Array<ArrayBuffer>> {
  const reader = stream.getReader()
  let buf: Uint8Array<ArrayBuffer> = new Uint8Array(0)
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

async function parseBlobBody(blob: Blob): Promise<{ metadata: Record<string, unknown>; frames: Uint8Array[] }> {
  const bytes = new Uint8Array(await blob.arrayBuffer())
  let off = 0
  const next = (): Uint8Array => {
    const len = decodeU32(bytes.subarray(off, off + 4) as Uint8Array<ArrayBuffer>)
    off += 4
    const chunk = bytes.subarray(off, off + len)
    off += len
    return chunk
  }
  const metadata = JSON.parse(new TextDecoder().decode(next())) as Record<string, unknown>
  const frames: Uint8Array[] = []
  while (off < bytes.length) frames.push(next())
  return { metadata, frames }
}

type UpgradeHarness = {
  readonly channels: HarnessClientChannel[]
  /** Old wire. `pushFrame` is the server→client direction; `upstream` is what the client sent. */
  readonly sse: {
    pushFrame(frame: Uint8Array): void
    close(): void
    readonly upstream: DecodedFrame[]
  }
  /** New wire — the probe socket, adopted as the transport at handoff. */
  readonly ws: {
    readonly socket: StubWebSocket
    pushFrame(frame: Uint8Array): void
    readonly sent: DecodedFrame[]
  }
  /** True once the client has committed to the handoff: it emits its handoff RECONCILE with
   *  `upgrade: true` from `_onTransportOpen(ws)` (`connection.ts:1096`, `:667-669`). */
  inHandoff(): boolean
  /** True once the handoff has COMMITTED and the buffer has been drained.
   *
   *  `tryCompleteUpgradeHandoff` (`connection.ts:821-831`) disposes the old transport and then
   *  drains the buffer, all in one synchronous block, and disposing aborts the SSE downstream
   *  fetch (`:1816-1817`). Any asynchronous observer therefore sees the abort strictly after the
   *  drain. This matters: the obvious observable — a second `_onTransportOpen` — fires from
   *  `handleReconciled` (`:811`) which runs BEFORE the drain and even when FIN has not arrived,
   *  so waiting on it reads "done" while the buffer is still full. */
  handoffDrained(): boolean
  /** How many SSE downstream (`streamResponse`) POSTs the client has opened. Exactly 1 through a
   *  successful handoff — the transport flips to WS and never reconnects. A SECOND one is the
   *  unambiguous signal that the upgrade fell back: `abortUpgradeAndReconnectSse` installs a fresh
   *  SSE transport and reconnects. `handoffDrained()` cannot discriminate here, because the old
   *  wire's fetch is aborted on both the success and the fallback path. */
  sseConnects(): number
  /** Queue a client→server send on one of the harness channels. During the handoff `reconciling`
   *  is true, so `canSendImmediately()` is false and the frame lands in `sendBuffer` — which is
   *  the state the settlement-cleanup tests are about. */
  send(channelIndex: number, data: string): void
  /** Number of entries still queued in the connection's private `sendBuffer`.
   *
   *  Reaching into a private field is deliberate. `sendBuffer` retention is a pure memory leak with
   *  NO behavioural surface: a stale entry's `channelIx` can never match a future channel (indexes
   *  are monotonic and never reused), so it is silently dropped at the next compaction and never
   *  mis-sent. There is therefore nothing to observe from outside, and the honest options are to
   *  read the real field or to not gate the fix at all. Reading the REAL array — never a mirrored
   *  tally — is what stops this from becoming a co-set proxy for the bug it exists to catch. */
  bufferedSendCount(): number
  /** Live handoff-buffer accounting, or null once the handoff has ended (committed or fallen back).
   *
   *  These are the SAME two fields the cap is enforced against — read straight off the state, never
   *  a tally maintained beside it. A parallel counter would stay correct exactly when the
   *  enforcement broke, which is the one case the assertion exists to catch. */
  handoffBuffered(): { frames: number; bytes: number } | null
  dispose(): void
}

/**
 * Drive a real `ClientConnection` from a fresh SSE connect all the way into the upgrade handoff
 * window, then hand back both wires so a test can control the interleaving across them.
 *
 * On return: the transport has flipped to WS, the old SSE downstream is still open, the client has
 * sent its handoff RECONCILE on the WS wire, and neither FIN nor the WS RECONCILED has arrived —
 * so every frame pushed on either wire lands in `state.upgrade.buffer` (`connection.ts:734`).
 */
async function createUpgradeHarness(channelIds: string[] = ['A']): Promise<UpgradeHarness> {
  const stub = installWebSocketStub()
  const channels = channelIds.map(createHarnessChannel)
  const upstream: DecodedFrame[] = []
  let downstream: ReturnType<typeof makeSseDownstream> | null = null
  let sseTornDown = false
  let sseConnects = 0

  const fetchImpl = (async (_url: string, init: RequestInit) => {
    const body = init.body as unknown

    if (body instanceof Blob) {
      const { metadata, frames } = await parseBlobBody(body)
      for (const raw of frames) upstream.push(decode(raw as Uint8Array<ArrayBuffer>))
      if (metadata.streamResponse !== true) return new Response('', { status: 200 })
      sseConnects += 1
      const sse = makeSseDownstream()
      downstream = sse
      init.signal?.addEventListener('abort', () => (sseTornDown = true), { once: true })
      sse.comment()
      sse.pushFrame(encode.streamRequestOpenAck())
      return new Response(sse.stream as BodyInit, {
        status: 200,
        headers: { 'Content-Type': 'text/event-stream' },
      })
    }

    // ReadableStream body → the long-lived streamRequest upload POST. It must resolve when the
    // client closes the body, because `forceDrain` (`connection.ts:1560-1569`) awaits exactly that.
    const stream = body as ReadableStream<Uint8Array<ArrayBuffer>>
    return await new Promise<Response>((resolve, reject) => {
      init.signal?.addEventListener('abort', () => reject(new DOMException('aborted', 'AbortError')), { once: true })
      void (async () => {
        let first = true
        for await (const chunk of readLengthPrefixed(stream)) {
          if (first) {
            first = false // metadata header
            continue
          }
          upstream.push(decode(chunk))
        }
        resolve(new Response('', { status: 200 }))
      })().catch(reject)
    })
  }) as unknown as typeof fetch

  // Same `connectionKey` for every channel ⇒ `getOrCreate` returns the one instance and registers
  // each channel on it, exactly as `ClientChannel`'s constructor does in production.
  const connectionKey = crypto.randomUUID()
  let connection!: ClientConnection
  for (const channel of channels) {
    connection = ClientConnection.getOrCreate('http://upgrade.test.local/_telefunc', channel as never, {
      transports: ['sse', 'ws'],
      fetchImpl,
      connectionKey,
    })
  }

  const firstReconcile = (): (DecodedFrame & { tag: typeof TAG.RECONCILE }) | undefined =>
    upstream.find((f): f is DecodedFrame & { tag: typeof TAG.RECONCILE } => f.tag === TAG.RECONCILE)

  // 1) First reconcile settles on SSE. `transports: ['sse','ws']` in the reply is what arms
  //    `maybeStartUpgrade` (`connection.ts:887`).
  await waitUntil(() => downstream !== null, 'SSE downstream opened')
  await waitUntil(() => firstReconcile() !== undefined, 'client sent its first RECONCILE')
  const open = firstReconcile()!.payload.open
  if (open.length !== channels.length) {
    throw new Error(`Harness expected ${channels.length} channel(s) in the first RECONCILE, got ${open.length}`)
  }
  downstream!.pushFrame(encode.reconciled(reconciledPayload(open.map((entry) => ({ ix: entry.ix, lastSeq: 0 })))))

  // 2) The upgrade probe opens a WS. The stub auto-pongs, so `probe()` resolves.
  await waitUntil(() => stub.sockets.length > 0, 'WS probe socket opened')
  const socket = stub.sockets[0]!

  // 3) `drainOldWire` closes the streamRequest body and awaits the POST; then the transport flips
  //    and `_onTransportOpen(ws)` emits the handoff RECONCILE (`upgrade: true`).
  await waitUntil(
    () => socket.sent.some((f) => f.tag === TAG.RECONCILE && f.payload.upgrade === true),
    'client entered the upgrade handoff',
  )

  return {
    channels,
    sse: {
      pushFrame: (frame) => downstream!.pushFrame(frame),
      close: () => downstream!.close(),
      upstream,
    },
    ws: {
      socket,
      pushFrame: (frame) => socket.emit(frame),
      sent: socket.sent,
    },
    inHandoff: () => socket.sent.some((f) => f.tag === TAG.RECONCILE && f.payload.upgrade === true),
    handoffDrained: () => sseTornDown,
    sseConnects: () => sseConnects,
    send: (channelIndex, data) => {
      connection.send(channels[channelIndex] as never, data)
    },
    bufferedSendCount: () => (connection as unknown as { sendBuffer: unknown[] }).sendBuffer.length,
    handoffBuffered: () => {
      const state = (connection as unknown as { state: { tag: string; upgrade?: UpgradeStateShape } }).state
      const upgrade = state.tag === 'open' ? state.upgrade : undefined
      if (upgrade?.tag !== 'handoff') return null
      return { frames: upgrade.bufferedFrames, bytes: upgrade.bufferedBytes }
    },
    dispose: () => {
      downstream?.close()
      socket.close()
      stub.restore()
    },
  }
}
