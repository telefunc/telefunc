// Client-side driver for a REAL `ClientConnection` through a REAL SSE→WS barrier upgrade, up to and
// including the handoff window.
//
// Two seams are needed and neither costs a production change:
//   - SSE: `fetchImpl` is already an option, exercised by `connection.spec.ts`.
//   - WS: `connection.ts` does a bare `new WebSocket(this.wsUrl)` and `TRANSPORT_REGISTRY` is a
//     module const, not an option — so a `globalThis.WebSocket` stub is the only seam.
//
// Real timers throughout. Fake timers and real `ReadableStream`s do not mix — stream-reader wakeups
// are not timer-driven — so waits are condition-polled with a deadline rather than fixed sleeps.

export { createUpgradeHarness, waitUntil }
export type { UpgradeHarness }

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
   *  CONSTRUCTION rather than after the socket surfaces in `sockets`: the client sends its `PREPARE`
   *  the moment `probe()` resolves, sooner than any awaiting harness can attach a listener. */
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

  /** Server→client frame. Deliberately still fires after `close()`: `ProbeWire.close` does NOT null
   *  `ws.onmessage`, so a late frame really can land on a closed probe socket. */
  emit(frame: Uint8Array): void {
    this.onmessage?.({ data: new Uint8Array(frame).buffer })
  }
}

/** Installs the stub on `globalThis` and returns every socket the code under test opens. */
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

/** Poll `predicate` until it holds. Throws with `label` on timeout, so a harness that never reaches
 *  the state under test fails loudly instead of asserting against a half-built world. */
async function waitUntil(predicate: () => boolean, label: string, timeoutMs = 3_000): Promise<void> {
  const deadline = Date.now() + timeoutMs
  while (!predicate()) {
    if (Date.now() > deadline) throw new Error(`Timed out waiting for: ${label}`)
    await new Promise((resolve) => setTimeout(resolve, 5))
  }
}

// ── Channels ─────────────────────────────────────────────────────────────────────────────────

type ReceivedPayload = { kind: 'text'; value: unknown } | { kind: 'binary'; bytes: Uint8Array }

type UpgradeStateShape = {
  tag: string
  bufferedFrames: number
  bufferedBytes: number
  finReceived: boolean
  to: unknown
}

type ConnectionShape = { state: { tag: string; upgrade?: UpgradeStateShape }; transport: unknown }

type HarnessClientChannel = {
  readonly id: string
  isClosed: boolean
  /** Data payloads as the application would see them, captured at `_dispatchFrame` — i.e. BELOW
   *  `ClientConnection.dispatchFrame`'s `trackSeq` dedup, which is the coalescer that silently
   *  drops. Observing above it would prove nothing. */
  readonly received: ReceivedPayload[]
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
      }
    },
    _onTransportClose(err?: Error) {
      this.isClosed = true
      this.closeErrors.push(err)
    },
  } as HarnessClientChannel
}

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

type UpgradeHarnessOptions = {
  /** `ready` answers READY synchronously (a server may legally answer in the same turn, and a client
   *  that only listens afterwards would miss it); `withhold` says nothing, so only the attempt
   *  deadline can end it; `terminate` closes the wire, as an OLD server does on an unknown tag. */
  prepare?: 'ready' | 'withhold' | 'terminate'
  /** `commit` = FIN then COMMITTED (the production order). `refuse` = NOTHING, the real silent
   *  refusal a stale barrier gets — also what a test picks to own the FIN/COMMITTED interleaving
   *  itself. `fin-only`/`committed-only` = one limb, to exercise the other's deadline. */
  barrier?: 'commit' | 'refuse' | 'fin-only' | 'committed-only'
  /** Fail the streamRequest upload POST so the client falls back to outbox+batch POSTs — the real
   *  mode whenever duplex `fetch` is unavailable, and the one where the barrier must quiesce first. */
  batchMode?: boolean
  /** Answer ordinary RECONCILEs after the first. Off by default; without it a post-fallback reconnect
   *  never settles, `maybeStartUpgrade` is never reached, and any "no second attempt" assertion is a
   *  gate that cannot fail. */
  autoReconcile?: boolean
  /** Batch mode only: hold the response to the POST that CARRIES the barrier until
   *  `releaseHungPosts()`. Freezes the client in `committing` — barrier on the wire, transport not yet
   *  flipped — which is the only window in which a test can drive the old wire ahead of the flip.
   *  Scoped to that one POST rather than `setBatchPostsHang`, which would also hang the ordinary
   *  traffic that has to keep flowing to get the client there. */
  hangBarrierPost?: boolean
}

type UpgradeHarness = {
  readonly channels: HarnessClientChannel[]
  /** Old wire. `pushFrame` is server→client; `upstream` is what the client sent. */
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
  /** A COMMITTED for the in-flight attempt: a RECONCILED that ECHOES the barrier's upgradeId, the
   *  only thing discriminating the commit from a stale ordinary reconciled. Pairs with
   *  `barrier: 'refuse'`, where the test drives both limbs of the join by hand. */
  committedFrame(open?: ReconciledPayload['open'], overrides?: Partial<ReconciledPayload>): Uint8Array
  /** The probe has been adopted as the transport. The flip is an EVENT inside `committing`, not a
   *  state of its own, so the only honest reading is transport identity — the same comparison the
   *  connection's own routing makes. */
  flipped(): boolean
  /** Handoff COMMITTED and buffer drained. `tryCompleteUpgrade` disposes the old transport
   *  then drains, in one synchronous block, and disposing aborts the SSE fetch — so an async observer
   *  sees the abort strictly after the drain. The obvious alternative, a second `_onTransportOpen`,
   *  fires BEFORE the drain and even when FIN has not arrived. */
  handoffDrained(): boolean
  /** SSE downstream POSTs opened. Exactly 1 through a successful handoff; a SECOND is the unambiguous
   *  fallback signal. `handoffDrained()` cannot discriminate — the fetch aborts on both paths. */
  sseConnects(): number
  send(channelIndex: number, data: string): void
  /** Entries still in the private `sendBuffer`. Reaching into a private field is deliberate:
   *  retention there is a pure memory leak with NO behavioural surface, so the honest options are the
   *  REAL array or no gate at all. Never a mirrored tally. */
  bufferedSendCount(): number
  /** Live handoff-buffer accounting, null once the handoff ended. The SAME two fields the cap is
   *  enforced against — a parallel counter would stay correct exactly when enforcement broke. */
  handoffBuffered(): { frames: number; bytes: number } | null
  /** Every WS socket opened. A SECOND after a fallback means `upgradeDisabled` failed to stick. */
  readonly sockets: StubWebSocket[]
  readonly prepares: PreparePayload[]
  readonly barriers: ReconcilePayload[]
  /** `'none' | 'staging' | 'committing'`, off the state machine, never mirrored. */
  upgradeTag(): string
  /** FIN consumed, read off the field the join reads. The point is to have something a test can WAIT
   *  for: pushing onto the SSE downstream only queues it, so asserting immediately afterwards passes
   *  no matter what the client would have done. */
  handoffFinReceived(): boolean
  register(id: string): HarnessClientChannel
  /** Answer the recorded `PREPARE` by hand. Pairs with `prepare: 'withhold'`. */
  sendReady(upgradeId?: string): void
  /** Frames of each batch-mode upstream POST (the connect POST excluded), in arrival order. */
  readonly batchPosts: DecodedFrame[][]
  /** Make subsequent batch POSTs never resolve. Frames are recorded first, so this models a server
   *  that READ the body and went quiet — what leaves `flushing` stuck true. */
  setBatchPostsHang(hang: boolean): void
  /** Answer hung POSTs with a 200 — a slow server that came back, as opposed to `dispose()` aborting
   *  it. That distinction IS the finding: a POST settling SUCCESSFULLY leaves the wire usable. */
  releaseHungPosts(): void
  dispose(): void
}

/**
 * Drive a real `ClientConnection` from a fresh SSE connect into the barrier upgrade's handoff
 * window, then hand back both wires so a test can control the interleaving across them.
 */
async function createUpgradeHarness(
  channelIds: string[] = ['A'],
  options: UpgradeHarnessOptions = {},
): Promise<UpgradeHarness> {
  const opts = {
    prepare: 'ready' as const,
    barrier: 'commit' as const,
    autoReconcile: false,
    batchMode: false,
    ...options,
  }
  const channels = channelIds.map(createHarnessChannel)
  const upstream: DecodedFrame[] = []
  const prepares: PreparePayload[] = []
  const barriers: ReconcilePayload[] = []
  const batchPosts: DecodedFrame[][] = []
  const hungPostResolvers: Array<(response: Response) => void> = []
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
      // What an OLD server does with tag 0x07: `decode` asserts and the wire is terminated. The old
      // SSE session is untouched — that is the property under test.
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
        stagedSocket.emit(encode.reconciled({ ...reconciledPayload(openOf(ctrl.open)), upgradeId: ctrl.upgradeId }))
      }
      const fin = () => downstream?.pushFrame(encode.fin())
      if (opts.barrier === 'fin-only') return void fin()
      if (opts.barrier === 'committed-only') return void commit()
      fin()
      commit()
      return
    }
    if (!autoReconcileArmed || !opts.autoReconcile) return
    downstream?.pushFrame(encode.reconciled(reconciledPayload(openOf(ctrl.open))))
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
        // that never received it. It DOES reject once its controller is aborted, exactly as a real
        // `fetch` would — that settlement is what makes a stalled POST outlive the transport that
        // issued it, the double-teardown hazard the recovery path has to be immune to.
        const carriesBarrier = decoded.some((frame) => frame.tag === TAG.RECONCILE && frame.payload.barrier === true)
        if (hangBatchPosts || (opts.hangBarrierPost && carriesBarrier)) {
          return await new Promise<Response>((resolve, reject) => {
            hungPostResolvers.push(resolve)
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
      // `STREAM_REQUEST_HANDSHAKE_TIMEOUT_MS` first. The upload POST failing is what makes the
      // batch-mode fallback immediate.
      if (!opts.batchMode) sse.pushFrame(encode.streamRequestOpenAck())
      // Reacted to only once this POST's OWN downstream exists. A connect POST carries the
      // connection's initial RECONCILE, so answering any earlier would push the RECONCILED onto the
      // PREVIOUS (already aborted) stream and the client would reconnect forever.
      for (const frame of decoded) onUpstreamFrame(frame)
      return new Response(sse.stream as BodyInit, {
        status: 200,
        headers: { 'Content-Type': 'text/event-stream' },
      })
    }

    // ReadableStream body → the long-lived streamRequest upload POST. Rejecting it is what a server
    // without duplex support effectively does: the client marks `streamRequest` failed — sticky —
    // and lives on outbox+batch POSTs from here.
    const stream = body as ReadableStream<Uint8Array<ArrayBuffer>>
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

  const readConnection = (): ConnectionShape => connection as unknown as ConnectionShape

  const upgradeTag = (): string => {
    const { state } = readConnection()
    if (state.tag !== 'open') return state.tag
    return state.upgrade?.tag ?? 'none'
  }

  const committingUpgrade = (): UpgradeStateShape | null => {
    const { state } = readConnection()
    const upgrade = state.tag === 'open' ? state.upgrade : undefined
    return upgrade?.tag === 'committing' ? upgrade : null
  }

  const firstReconcile = (): (DecodedFrame & { tag: typeof TAG.RECONCILE }) | undefined =>
    upstream.find((f): f is DecodedFrame & { tag: typeof TAG.RECONCILE } => f.tag === TAG.RECONCILE)

  // 1) First reconcile settles on SSE. `transports: ['sse','ws']` in the reply arms `maybeStartUpgrade`.
  await waitUntil(() => downstream !== null, 'SSE downstream opened')
  await waitUntil(() => firstReconcile() !== undefined, 'client sent its first RECONCILE')
  const open = firstReconcile()!.payload.open
  if (open.length !== channels.length) {
    throw new Error(`Harness expected ${channels.length} channel(s) in the first RECONCILE, got ${open.length}`)
  }
  downstream!.pushFrame(encode.reconciled(reconciledPayload(open.map((entry) => ({ ix: entry.ix, lastSeq: 0 })))))
  autoReconcileArmed = true

  // 2) The upgrade probe opens a WS. The stub auto-pongs, so `probe()` resolves.
  await waitUntil(() => stub.sockets.length > 0, 'WS probe socket opened')
  const socket = stub.sockets[0]!

  // 3) Drive to the point both wires are observable. With no READY coming the attempt cannot get
  //    past `staging`, so stop at the PREPARE; otherwise the barrier is the old wire's final frame
  //    and the transport flip follows it synchronously.
  if (opts.prepare !== 'ready') {
    await waitUntil(() => prepares.length > 0, 'client sent its PREPARE')
  } else {
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
    committedFrame: (open = [{ ix: 0, lastSeq: 0 }], overrides) => {
      const barrier = barriers.at(-1)
      if (!barrier) throw new Error('committedFrame() called before the client emitted a barrier')
      return encode.reconciled({ ...reconciledPayload(open), upgradeId: barrier.upgradeId, ...overrides })
    },
    // Read off the live state machine rather than inferred from the wire: on the barrier flow the
    // new wire never sends a RECONCILE at all, so any wire-derived signal would read false forever.
    flipped: () => {
      const upgrade = committingUpgrade()
      return upgrade !== null && readConnection().transport === upgrade.to
    },
    sockets: stub.sockets,
    prepares,
    barriers,
    upgradeTag,
    handoffFinReceived: () => committingUpgrade()?.finReceived === true,
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
    releaseHungPosts: () => {
      for (const resolve of hungPostResolvers.splice(0)) resolve(new Response('', { status: 200 }))
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
      const upgrade = committingUpgrade()
      if (upgrade === null) return null
      return { frames: upgrade.bufferedFrames, bytes: upgrade.bufferedBytes }
    },
    dispose: () => {
      downstream?.close()
      socket.close()
      // Tear down the CONNECTION, not just its wires. A harness whose upgrade was still in flight
      // otherwise keeps reconnecting after its test ends, and the WS sockets it opens land in the
      // NEXT test's stub array — precisely what the stickiness and socket-identity specs assert on.
      ;(connection as unknown as { dispose: () => void }).dispose()
      stub.restore()
    },
  }
}
