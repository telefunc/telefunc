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
export type { StubWebSocket, UpgradeHarness, UpgradeHarnessOptions, HarnessClientChannel }

import { parse } from '@brillout/json-serializer/parse'

import { uint8ArrayToBase64url } from './base64url.js'
import { ClientConnection } from './client/connection.js'
import { concat, decodeU32 } from './frame.js'
import {
  TAG,
  decode,
  encode,
  type ChannelFrame,
  type DecodedFrame,
  type PreparePayload,
  type ReconcilePayload,
  type ReconciledPayload,
} from './shared-ws.js'

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
  /** Server-side reaction to a client frame, invoked synchronously from `send`. Assigned at
   *  CONSTRUCTION (via `installWebSocketStub`) rather than after the socket surfaces in `sockets`:
   *  the barrier client sends its `PREPARE` the moment `probe()` resolves, which is sooner than any
   *  awaiting harness can reach in and attach a listener. */
  onSent: ((socket: StubWebSocket, frame: DecodedFrame) => void) | null = null

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
    const frame = decode(bytes as Uint8Array<ArrayBuffer>)
    this.sent.push(frame)
    if (this.autoPong && bytes[0] === TAG.PING) setTimeout(() => this.emit(encode.pong()), 0)
    this.onSent?.(this, frame)
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
function installWebSocketStub(onSent?: (socket: StubWebSocket, frame: DecodedFrame) => void): {
  sockets: StubWebSocket[]
  restore: () => void
} {
  const sockets: StubWebSocket[] = []
  const previous = (globalThis as Record<string, unknown>).WebSocket
  const Stub = class extends StubWebSocket {
    constructor(url: string) {
      super(url)
      this.onSent = onSent ?? null
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
type UpgradeStateShape = { tag: string; bufferedFrames: number; bufferedBytes: number; finReceived: boolean }

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
function reconciledPayload(
  open: ReconciledPayload['open'],
  sessionId = crypto.randomUUID(),
  /** Capability advertisement. Absent by default, so every spec written before the barrier flow
   *  existed keeps driving the LEGACY upgrade — the byte-for-byte wire it was written against. */
  barrierUpgrade = false,
): ReconciledPayload {
  return {
    sessionId,
    open,
    ...(barrierUpgrade ? { barrierUpgrade: true as const } : undefined),
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

/** Mock-server behaviour. Every default reproduces the pre-barrier harness exactly, so the specs
 *  written against it keep driving the legacy flow on the legacy wire. */
type UpgradeHarnessOptions = {
  /** Advertise `barrierUpgrade` on every RECONCILED — this alone is what puts the client on the
   *  barrier flow, which is the whole point of the capability gate. */
  barrierUpgrade?: boolean
  /** What the server does with the client's `PREPARE`.
   *  - `ready`: stage it and answer `READY` (synchronously — a server may legally answer in the
   *    same turn, and a client that only listens afterwards would miss it).
   *  - `withhold`: accept it and say nothing. The client has no wire event to react to; only its
   *    attempt deadline can end this.
   *  - `terminate`: close the wire, as an OLD server does on decoding an unknown tag. */
  prepare?: 'ready' | 'withhold' | 'terminate'
  /** What the server does with the client's barrier.
   *  - `commit`: `FIN` on the old wire, `COMMITTED` on the staged one.
   *  - `refuse`: NOTHING — the real silent refusal a stale barrier gets. Nothing is terminated,
   *    nothing rotates, and no frame ever tells the client. Deliberately indistinguishable on the
   *    wire from a server that simply stopped, because server-side it is.
   *  - `fin-only` / `committed-only`: one limb of the join, to exercise the other's deadline. */
  barrier?: 'commit' | 'refuse' | 'fin-only' | 'committed-only'
  /** Which limb of the commit goes first. The real server runs the old session's finalizer inside
   *  `reconcile` and delivers the reconciled after it, so `fin-first` is the production ordering. */
  commitOrder?: 'fin-first' | 'committed-first'
  /** Fail the streamRequest upload POST so the client falls back to outbox+batch POSTs — the real
   *  production mode whenever duplex `fetch` is unavailable, and the one where the barrier has to
   *  quiesce before it can be emitted. */
  batchMode?: boolean
  /** Answer ordinary RECONCILEs arriving on the SSE downstream AFTER the first one.
   *
   *  Off by default because it changes the wire every existing spec was written against. It is what
   *  closes T2's declared gap: without it a post-fallback reconnect never settles, so `maybeStartUpgrade`
   *  is never reached, and any assertion that `upgradeDisabled` suppressed a second attempt would be
   *  a gate that cannot fail. */
  autoReconcile?: boolean
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
  /** Every WS socket the client has opened, in order. A SECOND one after a fallback is the
   *  unambiguous signal that `upgradeDisabled` failed to stick. */
  readonly sockets: StubWebSocket[]
  /** `PREPARE` payloads the server received, in order. Empty is the legacy-flow expectation — and
   *  on its own a vacuous assertion, so every spec that checks it pairs it with proof that the
   *  legacy upgrade DID happen. */
  readonly prepares: PreparePayload[]
  /** Barrier reconcile payloads the server received, in order. */
  readonly barriers: ReconcilePayload[]
  /** The connection's live upgrade tag — `'none' | 'probing' | 'preparing' | 'draining' |
   *  'handoff'` — read straight off the state machine, never mirrored. */
  upgradeTag(): string
  /** Whether the handoff has taken its FIN yet, read off the same field the join reads.
   *
   *  The point is to have something a test can WAIT for. Pushing a frame onto the SSE downstream
   *  only queues it; a test that asserts immediately afterwards is asserting against a client that
   *  has not seen it, which passes no matter what the client would have done. */
  handoffFinReceived(): boolean
  /** Register another channel on the SAME connection, exactly as a `ClientChannel` constructor
   *  does. Used to open one mid-attempt, when registrations are deferred. */
  register(id: string): HarnessClientChannel
  /** Answer the recorded `PREPARE` by hand. Pairs with `prepare: 'withhold'`, which is what holds
   *  the client in `preparing` long enough for a test to act inside the staging window. */
  sendReady(upgradeId?: string): void
  /** Frames of each batch-mode upstream POST (the connect POST excluded), in arrival order. */
  readonly batchPosts: DecodedFrame[][]
  /** Make subsequent batch-mode upstream POSTs never resolve. Their frames are still recorded
   *  first, so a hung POST is observable — a server that read the body and then stopped answering.
   *  This is what leaves `flushing` stuck true, which is the state the attempt deadline has to be
   *  able to interrupt. */
  setBatchPostsHang(hang: boolean): void
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
async function createUpgradeHarness(
  channelIds: string[] = ['A'],
  options: UpgradeHarnessOptions = {},
): Promise<UpgradeHarness> {
  const opts = {
    barrierUpgrade: false,
    prepare: 'ready' as const,
    barrier: 'commit' as const,
    commitOrder: 'fin-first' as const,
    autoReconcile: false,
    batchMode: false,
    ...options,
  }
  const channels = channelIds.map(createHarnessChannel)
  const upstream: DecodedFrame[] = []
  const prepares: PreparePayload[] = []
  const barriers: ReconcilePayload[] = []
  const batchPosts: DecodedFrame[][] = []
  let hangBatchPosts = false
  let downstream: ReturnType<typeof makeSseDownstream> | null = null
  let sseTornDown = false
  let sseConnects = 0
  /** The probe wire the server staged, i.e. the one the COMMITTED belongs on. */
  let stagedSocket: StubWebSocket | null = null
  /** Armed only after the harness has answered the FIRST reconcile itself, so `autoReconcile`
   *  cannot double-answer it. */
  let autoReconcileArmed = false

  const openOf = (open: { ix: number; lastSeq: number }[]) => open.map((e) => ({ ix: e.ix, lastSeq: e.lastSeq }))

  // ── Server: the WS (staged probe) direction ──
  const stub = installWebSocketStub((socket, frame) => {
    if (frame.tag !== TAG.PREPARE) return
    prepares.push(frame.payload)
    if (opts.prepare === 'terminate') {
      // What an OLD server does with tag 0x07: `decode` asserts, the wire is terminated
      // permanently. The old SSE session is untouched — that is the property under test.
      socket.close()
      return
    }
    if (opts.prepare === 'withhold') return
    stagedSocket = socket
    socket.emit(encode.ready({ upgradeId: frame.payload.upgradeId }))
  })

  // ── Server: the SSE (old wire) upstream direction ──
  const onUpstreamFrame = (frame: DecodedFrame): void => {
    if (frame.tag !== TAG.RECONCILE) return
    const ctrl = frame.payload
    if (ctrl.barrier) {
      barriers.push(ctrl)
      if (opts.barrier === 'refuse') return
      const commit = () => {
        if (!stagedSocket) return
        stagedSocket.emit(
          encode.reconciled({
            ...reconciledPayload(openOf(ctrl.open), undefined, opts.barrierUpgrade),
            upgradeId: ctrl.upgradeId,
          }),
        )
      }
      const fin = () => downstream?.pushFrame(encode.fin())
      if (opts.barrier === 'fin-only') return void fin()
      if (opts.barrier === 'committed-only') return void commit()
      if (opts.commitOrder === 'fin-first') {
        fin()
        commit()
      } else {
        commit()
        fin()
      }
      return
    }
    if (!autoReconcileArmed || !opts.autoReconcile) return
    downstream?.pushFrame(encode.reconciled(reconciledPayload(openOf(ctrl.open), undefined, opts.barrierUpgrade)))
  }

  const fetchImpl = (async (_url: string, init: RequestInit) => {
    const body = init.body as unknown

    if (body instanceof Blob) {
      const { metadata, frames } = await parseBlobBody(body)
      const decoded = frames.map((raw) => decode(raw as Uint8Array<ArrayBuffer>))
      for (const frame of decoded) upstream.push(frame)
      if (metadata.streamResponse !== true) {
        batchPosts.push(decoded)
        for (const frame of decoded) onUpstreamFrame(frame)
        // Recorded first, then hung: a server that read the body and went quiet, rather than one
        // that never received it. `flushing` stays true for as long as this never resolves.
        //
        // It DOES reject once its controller is aborted, exactly as a real `fetch` would. That
        // settlement is load-bearing: it is what makes a stalled POST outlive the transport that
        // issued it and report failure against a wire that has since been replaced — the
        // double-teardown hazard the recovery path has to be immune to.
        if (hangBatchPosts) {
          return await new Promise<Response>((_resolve, reject) => {
            init.signal?.addEventListener('abort', () => reject(new DOMException('aborted', 'AbortError')), {
              once: true,
            })
          })
        }
        return new Response('', { status: 200 })
      }
      sseConnects += 1
      const sse = makeSseDownstream()
      downstream = sse
      init.signal?.addEventListener('abort', () => (sseTornDown = true), { once: true })
      sse.comment()
      // Withholding the open-ack is not enough on its own — the client would wait out
      // `STREAM_REQUEST_HANDSHAKE_TIMEOUT_MS` first. The upload POST failing (below) is what makes
      // the fallback immediate.
      if (!opts.batchMode) sse.pushFrame(encode.streamRequestOpenAck())
      // Reacted to only once this POST's OWN downstream exists. A connect POST carries the
      // connection's initial RECONCILE, so answering it any earlier would push the RECONCILED onto
      // the PREVIOUS (already aborted) stream — the client would never settle, and would reconnect
      // forever. Invisible until something reconnects, which is exactly the fallback path.
      for (const frame of decoded) onUpstreamFrame(frame)
      return new Response(sse.stream as BodyInit, {
        status: 200,
        headers: { 'Content-Type': 'text/event-stream' },
      })
    }

    // ReadableStream body → the long-lived streamRequest upload POST. It must resolve when the
    // client closes the body, because `forceDrain` (`connection.ts:1560-1569`) awaits exactly that.
    const stream = body as ReadableStream<Uint8Array<ArrayBuffer>>
    // Rejecting it is what a server without duplex support effectively does. The client marks
    // `streamRequest` failed — sticky — and lives on outbox+batch POSTs from here.
    if (opts.batchMode) throw new Error('streamRequest upload not supported by this server')
    return await new Promise<Response>((resolve, reject) => {
      init.signal?.addEventListener('abort', () => reject(new DOMException('aborted', 'AbortError')), { once: true })
      void (async () => {
        let first = true
        for await (const chunk of readLengthPrefixed(stream)) {
          if (first) {
            first = false // metadata header
            continue
          }
          const frame = decode(chunk)
          upstream.push(frame)
          onUpstreamFrame(frame)
        }
        resolve(new Response('', { status: 200 }))
      })().catch(reject)
    })
  }) as unknown as typeof fetch

  // Same `connectionKey` for every channel ⇒ `getOrCreate` returns the one instance and registers
  // each channel on it, exactly as `ClientChannel`'s constructor does in production.
  const connectionKey = crypto.randomUUID()
  const telefuncUrl = 'http://upgrade.test.local/_telefunc'
  const registerChannel = (channel: HarnessClientChannel): ClientConnection =>
    ClientConnection.getOrCreate(telefuncUrl, channel as never, {
      transports: ['sse', 'ws'],
      fetchImpl,
      connectionKey,
    })
  let connection!: ClientConnection
  for (const channel of channels) connection = registerChannel(channel)

  const upgradeTag = (): string => {
    const state = (connection as unknown as { state: { tag: string; upgrade?: { tag: string } } }).state
    if (state.tag !== 'open') return state.tag
    return state.upgrade?.tag ?? 'none'
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
  downstream!.pushFrame(
    encode.reconciled(
      reconciledPayload(
        open.map((entry) => ({ ix: entry.ix, lastSeq: 0 })),
        undefined,
        opts.barrierUpgrade,
      ),
    ),
  )
  autoReconcileArmed = true

  // 2) The upgrade probe opens a WS. The stub auto-pongs, so `probe()` resolves.
  await waitUntil(() => stub.sockets.length > 0, 'WS probe socket opened')
  const socket = stub.sockets[0]!

  // 3) Drive to the point each flow makes both wires observable.
  if (!opts.barrierUpgrade) {
    // Legacy: `drainOldWire` closes the streamRequest body and awaits the POST; then the transport
    // flips and `_onTransportOpen(ws)` emits the handoff RECONCILE (`upgrade: true`).
    await waitUntil(
      () => socket.sent.some((f) => f.tag === TAG.RECONCILE && f.payload.upgrade === true),
      'client entered the upgrade handoff',
    )
  } else if (opts.prepare !== 'ready') {
    // No READY is coming, so the attempt cannot progress past `preparing`. Stop at the PREPARE.
    await waitUntil(() => prepares.length > 0, 'client sent its PREPARE')
  } else {
    // Barrier: the old wire's FINAL frame is the barrier itself; the transport flip follows it
    // synchronously, so this is also the point at which the client is in the handoff.
    await waitUntil(() => barriers.length > 0, 'client emitted its upgrade barrier')
  }

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
    // Read off the live state machine rather than inferred from the wire: on the barrier flow the
    // new wire never sends a RECONCILE at all, so the legacy wire-signal would read false forever.
    inHandoff: () => upgradeTag() === 'handoff',
    sockets: stub.sockets,
    prepares,
    barriers,
    upgradeTag,
    handoffFinReceived: () => {
      const state = (connection as unknown as { state: { tag: string; upgrade?: UpgradeStateShape } }).state
      const upgrade = state.tag === 'open' ? state.upgrade : undefined
      return upgrade?.tag === 'handoff' && upgrade.finReceived
    },
    register: (id) => {
      const channel = createHarnessChannel(id)
      channels.push(channel)
      registerChannel(channel)
      return channel
    },
    batchPosts,
    setBatchPostsHang: (hang) => {
      hangBatchPosts = hang
    },
    sendReady: (upgradeId) => {
      const id = upgradeId ?? prepares.at(-1)?.upgradeId
      if (id === undefined) throw new Error('sendReady() called before the client sent a PREPARE')
      stagedSocket = socket
      socket.emit(encode.ready({ upgradeId: id }))
    },
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
      // Tear down the CONNECTION, not just its wires. A harness whose upgrade was still in flight
      // otherwise keeps reconnecting after its test ends, and the WS sockets it opens land in the
      // NEXT test's stub array — which is precisely the observable the stickiness and
      // socket-identity specs assert on. Reaching for the private method is deliberate: there is no
      // public teardown, and leaving the leak in place would make those assertions read the wrong
      // connection's behaviour.
      ;(connection as unknown as { dispose: () => void }).dispose()
      stub.restore()
    },
  }
}
