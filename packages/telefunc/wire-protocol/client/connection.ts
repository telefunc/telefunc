export { ClientConnection }
export type { MuxChannel, MuxConnection }

import { parse } from '@brillout/json-serializer/parse'
import { makeAbortError, makeBugError } from '../../client/remoteTelefunctionCall/errors.js'
import { assert } from '../../utils/assert.js'
import { ChannelClosedError } from '../channel-errors.js'
import { NetworkError } from '../../shared/NetworkError.js'
import { base64urlToUint8Array } from '../base64url.js'
import {
  CHANNEL_CLIENT_REPLAY_BUFFER_BYTES,
  CHANNEL_CLIENT_REPLAY_BUFFER_BINARY_BYTES,
  CHANNEL_IDLE_TIMEOUT_MS,
  CHANNEL_PING_INTERVAL_MS,
  CHANNEL_RECONNECT_INITIAL_DELAY_MS,
  CHANNEL_RECONNECT_MAX_DELAY_MS,
  CHANNEL_RECONNECT_TIMEOUT_MS,
  CHANNEL_TRANSPORT,
  RECONCILE_TIMEOUT_MS,
  SSE_FLUSH_THROTTLE_MS,
  SSE_POST_IDLE_FLUSH_DELAY_MS,
  SSE_RECONCILE_DEADLINE_MS,
  STREAM_REQUEST_HANDSHAKE_TIMEOUT_MS,
  TELEFUNC_SESSION_HEADER,
  UPGRADE_HANDOFF_BUFFER_BYTES,
  UPGRADE_HANDOFF_BUFFER_FRAMES,
  UPGRADE_HANDOFF_JOIN_TIMEOUT_MS,
  UPGRADE_ATTEMPT_TIMEOUT_MS,
  UPGRADE_DRAIN_TIMEOUT_MS,
  WS_PROBE_TIMEOUT_MS,
  type ChannelTransport,
  type ChannelTransports,
} from '../constants.js'
import { encodeU32, encodeLengthPrefixedFrames } from '../frame.js'
import { createPushReadableStream, type PushReadableStream } from '../push-readable-stream.js'
import { ReplayBuffer } from '../replay-buffer.js'
import { REQUEST_KIND, REQUEST_KIND_HEADER, getMarkedRequestUrl } from '../request-kind.js'
import { ACK_STATUS, TAG, decode, encode, isChannelDataFrame, payloadBytes } from '../shared-ws.js'
import type {
  AckResultStatus,
  ChannelFrame,
  DecodedFrame,
  PreparePayload,
  ReadyPayload,
  ReconcilePayload,
  ReconciledPayload,
} from '../shared-ws.js'
import { encodeSseRequest, encodeSseRequestMetadata } from '../sse-request.js'
import { DeadlineScheduler } from './deadlineScheduler.js'

type BufferedFrame = {
  frame: Uint8Array<ArrayBuffer>
  channelIx: number
  seq?: number
}

/** Probe wire returned by `WsTransport.probe`. Liveness is the consumer's responsibility
 *  until the swap commits — typically driven via a transient Heartbeat.
 *
 *  `send`/`onFrame` exist so the barrier upgrade can exchange `PREPARE`/`READY` on the probed
 *  socket WITHOUT that socket becoming the transport: nothing about the connection may move to the
 *  new wire until the server has committed the barrier, so adopting the `WsTransport` early — the
 *  obvious alternative — would put a sessionless wire in `this.transport` for a whole round-trip. */
type ProbeWire = {
  ping: () => void
  onPong: (cb: () => void) => void
  onClose: (cb: () => void) => void
  /** Raw frame onto the probed socket. Best-effort: a dead socket is a failed attempt, which the
   *  attempt deadline and `onClose` already handle. */
  send: (frame: Uint8Array<ArrayBuffer>) => void
  /** Every non-PONG frame the probed socket receives, with the raw byte length the handoff buffer
   *  accounts against. PONG stays with `onPong` — it is liveness, not protocol. */
  onFrame: (cb: (frame: DecodedFrame, byteLength: number) => void) => void
  close: () => void
}

type HeldProbeFrame = { frame: DecodedFrame; byteLength: number }

/** What `stageUpgrade` hands to the flip. `heldFrames` is LIVE: the probe's `onFrame` keeps
 *  appending to it until the attempt ends, and the flip replays whatever it holds at that moment. */
type StagedProbe = {
  upgradeId: string
  heartbeat: Heartbeat
  heldFrames: HeldProbeFrame[]
}

type AttemptWatchdog = { timer: ReturnType<typeof setTimeout> | null }

/** Ping-then-pong-deadline loop. Each transport owns one for its wire; the upgrade probe
 *  flow constructs a transient instance for the probed wire until the swap commits. */
class Heartbeat {
  private pingTimer: ReturnType<typeof setInterval> | null = null
  private pongTimer: ReturnType<typeof setTimeout> | null = null

  constructor(
    private readonly intervalMs: number,
    private readonly pongTimeoutMs: number,
    private readonly send: () => void,
    private readonly onDead: () => void,
  ) {}

  start(): void {
    if (this.pingTimer) return
    this.send()
    this.resetPong()
    this.pingTimer = setInterval(this.send, this.intervalMs)
  }

  resetPong(): void {
    if (this.pongTimer) clearTimeout(this.pongTimer)
    this.pongTimer = setTimeout(this.onDead, this.pongTimeoutMs)
  }

  stop(): void {
    if (this.pingTimer) {
      clearInterval(this.pingTimer)
      this.pingTimer = null
    }
    if (this.pongTimer) {
      clearTimeout(this.pongTimer)
      this.pongTimer = null
    }
  }
}

/** Resolve when `promise` settles OR `signal` aborts, whichever comes first.
 *
 *  The promise is NOT cancelled — nothing here can cancel an in-flight fetch — the caller merely
 *  stops waiting on it. That distinction is the whole point: an emitter that cannot stop waiting
 *  makes every deadline above it advisory, because the abort fires and nothing observes it. */
function raceAbort(promise: Promise<unknown>, signal: AbortSignal): Promise<void> {
  if (signal.aborted) return Promise.resolve()
  return new Promise<void>((resolve) => {
    const settle = (): void => {
      signal.removeEventListener('abort', settle)
      resolve()
    }
    signal.addEventListener('abort', settle, { once: true })
    promise.then(settle, settle)
  })
}

/** What `emitBarrier` did — and, when it wrote nothing, whether the wire it declined to write on
 *  can still carry traffic afterwards. The three outcomes take three different recovery paths, so
 *  collapsing any two of them silently corrupts one of the cases. */
type BarrierEmission =
  /** The barrier bytes may already have reached the server, so no wire can be trusted to hold the
   *  session: abandon both and reconcile afresh, upgrades sticky-disabled. */
  | 'emitted'
  /** Nothing written and the wire is fine: stay on it, upgrades still enabled. */
  | 'not-emitted'
  /** Nothing written, but a POST that may never settle owns the flush gate. Every later flush would
   *  return at that guard, so the wire is de facto dead however healthy it looks — the connection
   *  must recover it rather than keep pretending. Still a PRE-barrier outcome: not sticky. */
  | 'wedged'

type OutboundFrameKind = 'reconcile' | 'control' | 'flow-control' | 'ack' | 'data' | 'heartbeat'

type OutboundFrame = {
  kind: OutboundFrameKind
  frame: Uint8Array<ArrayBuffer>
}

interface MuxChannel {
  readonly id: string
  readonly isClosed: boolean
  _onTransportOpen(batched: boolean): void
  /** Entry point for every per-channel wire frame (data + per-channel ctrl). The
   *  channel splits ctrl vs data internally. Connection-level frames (PING/PONG/
   *  FIN/RECONCILED) and channel-termination ctrls (ABORT/ERROR) stay with the
   *  connection — they involve connection-side cleanup. */
  _dispatchFrame(frame: ChannelFrame): void
  _onTransportClose(err?: Error): void
}

interface MuxConnection {
  send(channel: MuxChannel, data: string): number
  sendPublishAckReq(channel: MuxChannel, data: string, onQueued: (seq: number) => void): void
  sendPublishBinaryAckReq(channel: MuxChannel, data: Uint8Array, onQueued: (seq: number) => void): void
  sendTextAckReq(channel: MuxChannel, data: string, onQueued: (seq: number) => void): void
  sendBinaryAckReq(channel: MuxChannel, data: Uint8Array, onQueued: (seq: number) => void): void
  sendBinary(channel: MuxChannel, data: Uint8Array): void
  sendAckRes(channel: MuxChannel, ackedSeq: number, result: string, status?: AckResultStatus): void
  sendAbort(channel: MuxChannel): void
  sendCloseRequest(channel: MuxChannel, timeoutMs: number): void
  sendCloseAck(channel: MuxChannel): void
  sendByteWindowUpdate(channel: MuxChannel, bytes: number): void
  sendMsgWindowUpdate(channel: MuxChannel, count: number): void
  sendBdpPing(channel: MuxChannel): void
  sendBdpPingAck(channel: MuxChannel): void
  sendBroadcastSubscribe(channel: MuxChannel, binary: boolean): void
  sendBroadcastUnsubscribe(channel: MuxChannel, binary: boolean): void
  unregister(channel: MuxChannel, err?: Error): void
}

type ReconcileOutcome = {
  frames: OutboundFrame[]
  channelsToOpen: MuxChannel[]
  reconcileComplete: boolean
}

type ReconcileBatch = {
  reconcileFrame: OutboundFrame
  movedBufferedFrames: OutboundFrame[]
}

type ReconcileBufferedFramesMode = 'batch-on-reconcile' | 'release-after-reconciled'

type ClientConnectionOptions = {
  transports: ChannelTransports
  fetchImpl: typeof fetch
  /** Server-issued sticky-routing token (e.g. Cloudflare DO pinning). Sent as URL param + header. */
  sessionToken?: string
  /** Client-side cache-key extension — distinct values get distinct `ClientConnection` instances. Never sent on the wire. */
  connectionKey?: string
  /** User headers (config.headers + per-call `withContext({ headers })`) merged into every transport fetch. */
  headers?: Record<string, string>
  /** Override the idle-close delay after all channels close. Default: 60 000 ms. Pass 0 to dispose immediately. */
  idleTimeout?: number
}

type ClientChannelTransport = {
  readonly type: ChannelTransport
  readonly reconnectTimeoutMessage: string
  readonly sendReconcileOnOpen: boolean
  readonly reconcileMode: ReconcileBufferedFramesMode
  /** Stable wire id the server uses to look up this connection from out-of-band POSTs.
   *  Set on SSE (the data POST carries it in metadata); `null` on WS (no out-of-band POSTs). */
  readonly connId: string | null
  /** True iff client→server frames are per-POST batched instead of pushed onto
   *  one streaming body — signals the channel to use a larger initial window. */
  readonly batched: boolean
  probe(): Promise<ProbeWire | null>
  start(): void
  hasWire(): boolean
  isConnecting(): boolean
  /** Send a connection-level ping on this wire. Heartbeat's send callback calls this. */
  sendPing(): void
  sendFrame(frame: OutboundFrame): void
  abandonActiveTransport(): void
  closeAbandonedTransport(): void
  applyReconciledSettings(ctrl: ReconciledPayload): void
  /** Puts the barrier out as this wire's FINAL frame and returns — without waiting for any server
   *  acknowledgment, which is the round-trip the whole design exists to remove. `buildFrame` is a
   *  thunk rather than a frame so the payload's cursors are read at the actual moment of emission,
   *  after any mode-specific quiesce.
   *
   *  Every wait inside observes `signal`, so the attempt deadline can actually end an emission that
   *  cannot complete — in batch mode the quiesce is otherwise unbounded, and a wedged emitter gates
   *  user sends for the connection's lifetime with nothing watching.
   *
   *  The return value is the recovery decision and must be EXACT. `'not-emitted'` means not one
   *  barrier byte reached the wire, so the server's session is untouched and the caller simply
   *  stays on it. `'emitted'` means the barrier may already have committed, so no wire can be
   *  trusted and the caller must abandon both. Guessing either way corrupts a live session. */
  emitBarrier(buildFrame: () => OutboundFrame, signal: AbortSignal): Promise<BarrierEmission>
  /** Connection constructs the Heartbeat (with the funnel-bound onDead) and hands it over.
   *  Transport's frame receive path routes PONG to it directly (`heartbeat?.resetPong()`). */
  attachHeartbeat(hb: Heartbeat): void
  detachHeartbeat(): void
  hasHeartbeat(): boolean
  dispose(): void
}

type OutboxEntry = { frame: Uint8Array<ArrayBuffer>; deadline: number }

type SseInitialBatchStage = {
  initialFrames: OutboundFrame[]
  movedOutbox: OutboxEntry[]
  movedBufferedFrames: OutboundFrame[]
}

type ConnectionState =
  | { tag: 'fresh' }
  | { tag: 'open'; upgrade: UpgradeState }
  | {
      tag: 'reconnecting'
      attempt: number
      startedAt: number
      timer: ReturnType<typeof setTimeout>
    }
  | { tag: 'closed' }

type UpgradeState =
  | { tag: 'none' }
  | { tag: 'probing'; attempt: AbortController }
  /** `PREPARE` sent, awaiting `READY`. Deliberately UNGATED (`inDrain` stays false): the old wire is
   *  still fully live and authoritative for this whole window, so gating here would buy nothing and
   *  cost every user send a round-trip. Gating starts one transition later, at `draining`. */
  | { tag: 'preparing'; attempt: AbortController }
  | { tag: 'draining'; attempt: AbortController }
  | {
      tag: 'handoff'
      from: ClientChannelTransport
      /** The barrier attempt this handoff is waiting to have COMMITTED, or `null` on the legacy
       *  flow. Non-null means exactly one RECONCILED can settle it — the one echoing this id. */
      upgradeId: string | null
      buffer: HandoffBuffer
      /** ONE budget across both buffers — see `UPGRADE_HANDOFF_BUFFER_*`. */
      bufferedBytes: number
      bufferedFrames: number
      /** Channel ixes the settling RECONCILED omitted, held back instead of released — see
       *  `releaseDeferredOmitted`. Lives on the state rather than in `handleReconciled` because
       *  FIN and RECONCILED can arrive in either order, so the reconcile that fills this list is
       *  often not the call that completes the handoff. */
      deferredOmitted: number[]
      finReceived: boolean
      joinTimer: ReturnType<typeof setTimeout> | null
    }

/** Frames that arrive during the handoff, kept PARTITIONED BY SOURCE WIRE rather than in one
 *  arrival-ordered list. The old wire's frames are authoritative and largely non-recoverable
 *  (terminal ctrls are seq-less and never replayed); the new wire's are all replayable. So the
 *  old buffer is drained in full before the new one — merging the two by arrival order lets a
 *  replayed new-wire frame advance `trackSeq` past an in-flight old-wire frame, which is then
 *  dropped as a `'dup'` forever. */
type HandoffBuffer = { old: DecodedFrame[]; new: DecodedFrame[] }

/** Per-channel lifecycle. `releasing` = unregistered before the server confirmed —
 *  entry stays so the upcoming RECONCILE carries the ix and buffered ABORT/CLOSE flow alongside. */
type ChannelState =
  | { tag: 'pending'; initial: boolean }
  | { tag: 'open' }
  | { tag: 'releasing'; initial: boolean; err: Error }

type ChannelEntry = {
  channel: MuxChannel
  state: ChannelState
}

class ClientConnection implements MuxConnection {
  private static cache = new Map<string, ClientConnection>()

  static getOrCreate(telefuncUrl: string, channel: MuxChannel, options: ClientConnectionOptions): ClientConnection {
    // `connectionKey` opts callers out of the shared connection without the server seeing it.
    const key = `${options.transports.join(',')}:${telefuncUrl}|${options.connectionKey ?? ''}`
    let connection = ClientConnection.cache.get(key)
    if (!connection || connection.closed) {
      connection = new ClientConnection(telefuncUrl, options, key)
      ClientConnection.cache.set(key, connection)
    }
    connection.register(channel)
    return connection
  }

  private readonly cacheKey: string
  private readonly telefuncUrl: string
  private readonly connectionOptions: ClientConnectionOptions
  private transport: ClientChannelTransport

  private state: ConnectionState = { tag: 'fresh' }
  /** Sticky after a permanent upgrade abort — survives every state transition until dispose. */
  private upgradeDisabled = false
  /** Server-allowed transports from the last settled RECONCILED. Kept so `maybeStartUpgrade`
   *  can run from both `handleReconciled` and `_onTransportOpen` — transport-open and the
   *  first RECONCILED can arrive in either order on the SSE path. */
  private serverTransports: ReconciledPayload['transports'] | null = null
  /** Set for exactly one synchronous `_onTransportOpen`: the one the barrier commit's transport
   *  flip triggers. See `probeAndUpgrade`'s flip block for why a second RECONCILE there is
   *  destructive rather than merely wasteful. */
  private suppressReconcileOnOpen = false
  /** RECONCILE sent, awaiting RECONCILED. SSE sets it true during connecting since the
   *  initial reconcile is baked into the openStream POST body. Bounded by `reconcileTimer`:
   *  written only via `enterReconciling`/`exitReconciling` so the deadline can never outlive
   *  the state it guards. */
  private reconciling = false
  private reconcileTimer: ReturnType<typeof setTimeout> | null = null
  private ttl: ReturnType<typeof setTimeout> | null = null

  private get closed(): boolean {
    return this.state.tag === 'closed'
  }
  private get connected(): boolean {
    return this.state.tag === 'open'
  }
  private get inDrain(): boolean {
    return this.state.tag === 'open' && this.state.upgrade.tag === 'draining'
  }

  private sessionId: string | null = null
  private nextIndex = 0
  private reconcileIxes = new Set<number>()
  private channels = new Map<number, ChannelEntry>()
  private channelIndex = new Map<MuxChannel, number>()
  private sendBuffer: BufferedFrame[] = []
  private lastSeqByChannel = new Map<number, number>()
  private replayBuffers = new Map<number, ReplayBuffer>()
  private reconnectTimeoutMs = CHANNEL_RECONNECT_TIMEOUT_MS
  private idleTimeoutMs: number
  private pingIntervalMs = CHANNEL_PING_INTERVAL_MS
  private clientReplayBufferBytes = CHANNEL_CLIENT_REPLAY_BUFFER_BYTES
  private clientReplayBufferBinaryBytes = CHANNEL_CLIENT_REPLAY_BUFFER_BINARY_BYTES
  private constructor(telefuncUrl: string, options: ClientConnectionOptions, cacheKey: string) {
    this.cacheKey = cacheKey
    this.telefuncUrl = telefuncUrl
    this.connectionOptions = options
    this.idleTimeoutMs = options.idleTimeout ?? CHANNEL_IDLE_TIMEOUT_MS
    this.transport = TRANSPORT_REGISTRY[options.transports[0]!](telefuncUrl, options, this)
  }

  // ── State transitions: every `this.state =` write goes through these. ──

  private enterOpen(): void {
    if (this.state.tag === 'open') return
    this.state = { tag: 'open', upgrade: { tag: 'none' } }
  }

  /** Owns the reconnect timer's lifecycle so callers can't forget to cancel a prior one. */
  private enterReconnecting(attempt: number, startedAt: number, delay: number): void {
    if (this.state.tag === 'reconnecting') clearTimeout(this.state.timer)
    const timer = setTimeout(() => this.transport.start(), delay)
    this.state = { tag: 'reconnecting', attempt, startedAt, timer }
  }

  private enterClosed(): void {
    this.state = { tag: 'closed' }
  }

  private enterUpgradeProbing(attempt: AbortController): void {
    assert(this.state.tag === 'open' && this.state.upgrade.tag === 'none')
    this.state = { tag: 'open', upgrade: { tag: 'probing', attempt } }
  }

  private enterUpgradePreparing(attempt: AbortController): void {
    assert(this.state.tag === 'open' && this.state.upgrade.tag === 'probing' && this.state.upgrade.attempt === attempt)
    this.state = { tag: 'open', upgrade: { tag: 'preparing', attempt } }
  }

  /** Reached from `probing` on the legacy flow and from `preparing` on the barrier flow — the two
   *  differ in what proves the new wire is usable, not in what gating means once it is. */
  private enterUpgradeDraining(attempt: AbortController): void {
    assert(this.state.tag === 'open')
    const u = this.state.upgrade
    assert((u.tag === 'probing' || u.tag === 'preparing') && u.attempt === attempt)
    this.state = { tag: 'open', upgrade: { tag: 'draining', attempt } }
  }

  private enterUpgradeHandoff(from: ClientChannelTransport, upgradeId: string | null = null): void {
    assert(this.state.tag === 'open' && this.state.upgrade.tag === 'draining')
    this.state = {
      tag: 'open',
      upgrade: {
        tag: 'handoff',
        from,
        upgradeId,
        buffer: { old: [], new: [] },
        bufferedBytes: 0,
        bufferedFrames: 0,
        deferredOmitted: [],
        finReceived: false,
        // Armed HERE, not on FIN arrival: the join deadline has to bound both limbs from the
        // moment two wires are live, or the missing-FIN limb has no watchdog at all.
        joinTimer: setTimeout(() => this.onHandoffJoinTimeout(), UPGRADE_HANDOFF_JOIN_TIMEOUT_MS),
      },
    }
  }

  /** Idempotent: no-op if `attempt` is already cleared, replaced, or committed to handoff. */
  private exitUpgradeAttempt(attempt: AbortController): void {
    if (this.state.tag !== 'open') return
    const u = this.state.upgrade
    if (u.tag !== 'probing' && u.tag !== 'preparing' && u.tag !== 'draining') return
    if (u.attempt !== attempt) return
    this.state = { tag: 'open', upgrade: { tag: 'none' } }
  }

  private exitUpgradeHandoff(): {
    from: ClientChannelTransport
    buffer: HandoffBuffer
    deferredOmitted: number[]
    joinTimer: ReturnType<typeof setTimeout> | null
  } {
    assert(this.state.tag === 'open' && this.state.upgrade.tag === 'handoff')
    const { from, buffer, deferredOmitted, joinTimer } = this.state.upgrade
    this.state = { tag: 'open', upgrade: { tag: 'none' } }
    return { from, buffer, deferredOmitted, joinTimer }
  }

  private canSendImmediately(): boolean {
    return this.connected && !this.reconciling && !this.inDrain && this.registerReconcileTimer === null
  }

  // ── Per-channel state transitions: every `entry.state =` write goes through these. ──

  private enterChannelPending(ix: number, channel: MuxChannel, initial: boolean): void {
    this.channels.set(ix, { channel, state: { tag: 'pending', initial } })
    this.channelIndex.set(channel, ix)
  }

  private enterChannelOpen(ix: number): void {
    const entry = this.channels.get(ix)
    assert(entry && entry.state.tag === 'pending')
    entry.state = { tag: 'open' }
  }

  private enterChannelReleasing(ix: number, err: Error): void {
    const entry = this.channels.get(ix)
    assert(entry && entry.state.tag === 'pending')
    entry.state = { tag: 'releasing', initial: entry.state.initial, err }
  }

  private register(channel: MuxChannel): void {
    if (this.ttl) {
      clearTimeout(this.ttl)
      this.ttl = null
    }
    const ix = this.nextIndex++
    this.enterChannelPending(ix, channel, true)
    this.replayBuffers.set(
      ix,
      new ReplayBuffer(this.clientReplayBufferBytes, this.reconnectTimeoutMs, this.clientReplayBufferBinaryBytes),
    )

    if (!this.transport.hasWire() && !this.transport.isConnecting()) {
      this.transport.start()
      return
    }
    this.scheduleRegisterReconcile()
  }

  private registerReconcileTimer: ReturnType<typeof setTimeout> | null = null
  /** Coalesces sync-burst registrations into one RECONCILE round-trip. */
  private scheduleRegisterReconcile(): void {
    if (this.registerReconcileTimer !== null) return
    this.registerReconcileTimer = setTimeout(() => this.flushPendingRegisterReconcile(), 0)
  }

  /** Send the queued RECONCILE on the live wire. No-op when nothing's queued. While the wire
   *  can't take it (connecting, awaiting RECONCILED, mid-upgrade) the obligation is KEPT, not
   *  consumed — `registerReconcileTimer` stays non-null as the marker (which also keeps
   *  `canSendImmediately` false so data frames buffer behind the RECONCILE), and the
   *  transitions that make the wire sendable re-invoke this: `_onTransportOpen`, a settled
   *  RECONCILED, upgrade-attempt exit, and handoff completion. */
  private flushPendingRegisterReconcile(): void {
    if (this.registerReconcileTimer === null) return
    if (this.state.tag !== 'open' || this.state.upgrade.tag !== 'none' || this.reconciling) return
    this.sendReconcileBatch(this.stageReconcileBatch())
    this.releaseUnconfirmedReleasing()
  }

  /** Cancel without sending — wire is dying. Drops releasing entries so they don't
   *  leak onto the post-reconnect RECONCILE. */
  private cancelPendingRegisterReconcile(): void {
    this.cancelRegisterReconcileTimer()
    this.releaseUnconfirmedReleasing()
  }

  private cancelRegisterReconcileTimer(): void {
    if (this.registerReconcileTimer === null) return
    clearTimeout(this.registerReconcileTimer)
    this.registerReconcileTimer = null
  }

  private releaseUnconfirmedReleasing(): void {
    let droppedAny = false
    for (const [ix, entry] of this.channels) {
      if (entry.state.tag === 'releasing' && entry.state.initial) {
        this.releaseChannel(ix, entry.channel, entry.state.err)
        droppedAny = true
      }
    }
    if (droppedAny) this.startTtlIfIdle()
  }

  unregister(channel: MuxChannel, err = new ChannelClosedError()): void {
    const ix = this.channelIndex.get(channel)
    if (ix === undefined) return
    const entry = this.channels.get(ix)!
    if (entry.state.tag === 'pending') {
      this.enterChannelReleasing(ix, err)
      return
    }
    this.releaseChannel(ix, channel, err)
    this.startTtlIfIdle()
  }

  send(channel: MuxChannel, data: string): number {
    const ix = this.channelIndex.get(channel)
    if (ix === undefined) return 0
    const replay = this.replayBuffers.get(ix)!
    const seq = replay.nextSeq()
    const frame = encode.text(ix, data, seq)
    if (!this.canSendImmediately()) {
      this.sendBuffer.push({ frame, channelIx: ix, seq })
    } else {
      replay.push(seq, frame)
      this.transport.sendFrame({ kind: 'data', frame })
    }
    return payloadBytes(frame)
  }

  sendPublishAckReq(channel: MuxChannel, data: string, onQueued: (seq: number) => void): void {
    this.sendAckReq(channel, (ix, seq) => encode.publishAckReq(ix, data, seq), false, onQueued)
  }

  sendPublishBinaryAckReq(channel: MuxChannel, data: Uint8Array, onQueued: (seq: number) => void): void {
    this.sendAckReq(channel, (ix, seq) => encode.publishBinaryAckReq(ix, data, seq), true, onQueued)
  }

  sendTextAckReq(channel: MuxChannel, data: string, onQueued: (seq: number) => void): void {
    this.sendAckReq(channel, (ix, seq) => encode.textAckReq(ix, data, seq), false, onQueued)
  }

  sendBinaryAckReq(channel: MuxChannel, data: Uint8Array, onQueued: (seq: number) => void): void {
    this.sendAckReq(channel, (ix, seq) => encode.binaryAckReq(ix, data, seq), true, onQueued)
  }

  /** Shared ack-req issuance — encodes via `buildFrame`, invokes `onQueued(seq)` so the
   *  channel registers the pending ack *before* the frame hits the wire (so an
   *  immediate `ACK_RES` can't be lost), then ships or buffers the frame. Mirrors
   *  `IndexedPeer.sendTextAckReq` on the server side. */
  private sendAckReq(
    channel: MuxChannel,
    buildFrame: (ix: number, seq: number) => Uint8Array<ArrayBuffer>,
    binary: boolean,
    onQueued: (seq: number) => void,
  ): void {
    const ix = this.channelIndex.get(channel)
    if (ix === undefined) return
    const replay = this.replayBuffers.get(ix)!
    const seq = replay.nextSeq()
    const frame = buildFrame(ix, seq)
    onQueued(seq)
    if (!this.canSendImmediately()) {
      this.sendBuffer.push({ frame, channelIx: ix, seq })
      return
    }
    replay.push(seq, frame, binary)
    this.transport.sendFrame({ kind: 'ack', frame })
  }

  sendBinary(channel: MuxChannel, data: Uint8Array): void {
    const ix = this.channelIndex.get(channel)
    if (ix === undefined) return
    const replay = this.replayBuffers.get(ix)!
    const seq = replay.nextSeq()
    const frame = encode.binary(ix, data, seq)
    if (!this.canSendImmediately()) {
      this.sendBuffer.push({ frame, channelIx: ix, seq })
      return
    }
    replay.push(seq, frame, true)
    this.transport.sendFrame({ kind: 'data', frame })
  }

  sendAckRes(channel: MuxChannel, ackedSeq: number, result: string, status: AckResultStatus = ACK_STATUS.OK): void {
    const ix = this.channelIndex.get(channel)
    if (ix === undefined) return
    const replay = this.replayBuffers.get(ix)!
    const seq = replay.nextSeq()
    const frame = encode.ackRes(ix, seq, ackedSeq, result, status)
    if (!this.canSendImmediately()) {
      this.sendBuffer.push({ frame, channelIx: ix, seq })
      return
    }
    replay.push(seq, frame)
    this.transport.sendFrame({ kind: 'ack', frame })
  }

  sendAbort(channel: MuxChannel): void {
    const ix = this.channelIndex.get(channel)
    if (ix === undefined) return
    const frame = encode.close(ix, 0)
    if (!this.canSendImmediately()) {
      this.sendBuffer.push({ frame, channelIx: ix, seq: undefined })
      return
    }
    this.transport.sendFrame({ kind: 'control', frame })
  }

  sendCloseRequest(channel: MuxChannel, timeoutMs: number): void {
    const ix = this.channelIndex.get(channel)
    if (ix === undefined) return
    const frame = encode.close(ix, timeoutMs)
    if (!this.canSendImmediately()) {
      this.sendBuffer.push({ frame, channelIx: ix, seq: undefined })
      return
    }
    this.transport.sendFrame({ kind: 'control', frame })
  }

  sendCloseAck(channel: MuxChannel): void {
    const ix = this.channelIndex.get(channel)
    if (ix === undefined) return
    const frame = encode.closeAck(ix)
    if (!this.canSendImmediately()) {
      this.sendBuffer.push({ frame, channelIx: ix, seq: undefined })
      return
    }
    this.transport.sendFrame({ kind: 'control', frame })
  }

  sendByteWindowUpdate(channel: MuxChannel, bytes: number): void {
    const ix = this.channelIndex.get(channel)
    if (ix === undefined) return
    // Window updates are ephemeral — the sender resets `_peerWindow` to the initial
    // value on reconnect and re-adopts the peer's advertised `W` from the next update,
    // so dropping one mid-disconnect is harmless.
    if (!this.canSendImmediately()) return
    this.transport.sendFrame({ kind: 'flow-control', frame: encode.window(ix, bytes) })
  }

  sendMsgWindowUpdate(channel: MuxChannel, count: number): void {
    const ix = this.channelIndex.get(channel)
    if (ix === undefined) return
    // Ephemeral — same rationale as `sendByteWindowUpdate`.
    if (!this.canSendImmediately()) return
    this.transport.sendFrame({ kind: 'flow-control', frame: encode.msgWindow(ix, count) })
  }

  sendBdpPing(channel: MuxChannel): void {
    const ix = this.channelIndex.get(channel)
    if (ix === undefined) return
    const frame = encode.bdpPing(ix)
    if (!this.canSendImmediately()) {
      this.sendBuffer.push({ frame, channelIx: ix, seq: undefined })
      return
    }
    this.transport.sendFrame({ kind: 'flow-control', frame })
  }

  sendBdpPingAck(channel: MuxChannel): void {
    const ix = this.channelIndex.get(channel)
    if (ix === undefined) return
    const frame = encode.bdpPingAck(ix)
    if (!this.canSendImmediately()) {
      this.sendBuffer.push({ frame, channelIx: ix, seq: undefined })
      return
    }
    this.transport.sendFrame({ kind: 'flow-control', frame })
  }

  sendBroadcastSubscribe(channel: MuxChannel, binary: boolean): void {
    const ix = this.channelIndex.get(channel)
    if (ix === undefined) return
    const frame = encode.broadcastSub(ix, binary)
    if (!this.canSendImmediately()) {
      this.sendBuffer.push({ frame, channelIx: ix, seq: undefined })
      return
    }
    this.transport.sendFrame({ kind: 'control', frame })
  }

  sendBroadcastUnsubscribe(channel: MuxChannel, binary: boolean): void {
    const ix = this.channelIndex.get(channel)
    if (ix === undefined) return
    const frame = encode.broadcastUnsub(ix, binary)
    if (!this.canSendImmediately()) {
      this.sendBuffer.push({ frame, channelIx: ix, seq: undefined })
      return
    }
    this.transport.sendFrame({ kind: 'control', frame })
  }

  _onTransportOpen(transport: ClientChannelTransport): void {
    if (this.closed) return
    if (transport !== this.transport) return
    const suppressReconcile = this.suppressReconcileOnOpen
    this.suppressReconcileOnOpen = false
    this.enterOpen()
    if (this.transport.sendReconcileOnOpen && !suppressReconcile) {
      this.sendReconcileBatch(this.stageReconcileBatch())
      return
    }
    if (!this.reconciling) {
      // A register-reconcile queued during the connecting window sends its RECONCILE here and
      // carries the buffered frames after it, emptying the buffer; with none queued, flush
      // no-ops and the drain sends them directly. Buffered frames must land after any RECONCILE
      // or the server drops them as unknown-ix — flush-then-drain preserves that order.
      this.flushPendingRegisterReconcile()
      for (const frame of this.drainBufferedFrames(this.channels, undefined)) this.transport.sendFrame(frame)
    }
    this.maybeStartUpgrade()
  }

  /** `source` is the transport the frame came off. During the handoff two wires are live at once
   *  and which one a frame arrived on is load-bearing, so every caller must identify itself. */
  _onTransportFrame(frame: DecodedFrame, source: ClientChannelTransport, byteLength: number): void {
    if (this.state.tag === 'open' && this.state.upgrade.tag === 'handoff') {
      this.bufferFrameDuringHandoff(frame, source === this.state.upgrade.from ? 'old' : 'new', byteLength)
    } else {
      this.dispatchFrame(frame)
    }
  }

  private dispatchFrame(frame: DecodedFrame): void {
    // Track seq for ALL data frames including ACK_RES; otherwise reconciles under-report lastSeq.
    if (isChannelDataFrame(frame)) {
      if (this.trackSeq(frame.index, frame.seq) === 'dup') return
    }
    // Connection-level + channel-termination ctrls stay here; they involve connection
    // bookkeeping (handoff state, channel release, TTL). Everything else is per-channel
    // and goes through `channel._dispatchFrame`.
    switch (frame.tag) {
      case TAG.FIN:
        this.handleHandoffFin()
        return
      case TAG.RECONCILED:
        this.handleReconciled(frame.payload)
        return
      case TAG.ABORT:
        this.closeRemoteChannel(frame.index, makeAbortError(parse(frame.abortValue)))
        this.startTtlIfIdle()
        return
      case TAG.ERROR:
        this.closeRemoteChannel(frame.index, makeBugError())
        this.startTtlIfIdle()
        return
    }
    // PING/PONG/RECONCILE/STREAM_REQUEST_OPEN_ACK never reach `dispatchFrame` —
    // transports peel them off in their own receive paths. Everything that lands
    // here is per-channel and carries `index`.
    const channelFrame = frame as ChannelFrame
    this.channels.get(channelFrame.index)?.channel._dispatchFrame(channelFrame)
  }

  private bufferFrameDuringHandoff(frame: DecodedFrame, source: 'old' | 'new', byteLength: number): void {
    switch (frame.tag) {
      case TAG.FIN:
        this.handleHandoffFin()
        return
      case TAG.RECONCILED:
        this.handleReconciled(frame.payload)
        return
    }
    assert(this.state.tag === 'open' && this.state.upgrade.tag === 'handoff')
    const upgrade = this.state.upgrade
    upgrade.buffer[source].push(frame)
    upgrade.bufferedFrames += 1
    upgrade.bufferedBytes += byteLength
    // Checked AFTER the push, so the frame that trips the budget is still part of the old prefix
    // the fallback delivers. Overshooting by one frame is the cheap direction to be wrong in;
    // dropping a non-replayable old-wire ctrl is not.
    if (
      upgrade.bufferedFrames > UPGRADE_HANDOFF_BUFFER_FRAMES ||
      upgrade.bufferedBytes > UPGRADE_HANDOFF_BUFFER_BYTES
    ) {
      this.abortUpgradeAndReconnectSse(new NetworkError('Upgrade handoff buffer limit exceeded', true))
    }
  }

  /** The FIN+RECONCILED join did not complete in time. The two limbs get distinguishable messages
   *  so a missing FIN can never be mistaken for a missing RECONCILED — and so neither limb can be
   *  silently satisfied by `RECONCILE_TIMEOUT_MS` firing 8s later instead. */
  private onHandoffJoinTimeout(): void {
    if (this.state.tag !== 'open' || this.state.upgrade.tag !== 'handoff') return
    this.state.upgrade.joinTimer = null
    const waitingFor = this.state.upgrade.finReceived ? 'RECONCILED' : 'FIN'
    this.abortUpgradeAndReconnectSse(new NetworkError(`Upgrade handoff timed out waiting for ${waitingFor}`, true))
  }

  /** Idempotent. Detaches first either way so a fresh install can never leak the prior. */
  private installHeartbeat(transport: ClientChannelTransport, intervalMs: number): void {
    if (transport.hasHeartbeat() && this.pingIntervalMs === intervalMs) return
    transport.detachHeartbeat()
    this.pingIntervalMs = intervalMs
    const hb = new Heartbeat(
      intervalMs,
      intervalMs * 2,
      () => transport.sendPing(),
      () => this.handlePongTimeout(transport),
    )
    transport.attachHeartbeat(hb)
    hb.start()
  }

  /** Funnel for pong-timeouts. Suppress while reconciling — pings are delayed by the round-trip;
   *  `reconcileTimer` (armed by `enterReconciling`) is the liveness bound for that window. */
  private handlePongTimeout(transport: ClientChannelTransport): void {
    if (this.reconciling) return
    transport.detachHeartbeat()
    transport.abandonActiveTransport()
    this._onTransportClosed(transport, false)
  }

  /** Enter the await-RECONCILED window and arm its liveness bound. A follow-up RECONCILE for
   *  late registrations re-enters and restarts the deadline from scratch. */
  private enterReconciling(): void {
    this.reconciling = true
    if (this.reconcileTimer) clearTimeout(this.reconcileTimer)
    this.reconcileTimer = setTimeout(() => this.onReconcileTimeout(), RECONCILE_TIMEOUT_MS)
  }

  /** Leave the await-RECONCILED window: RECONCILED settled, the wire was lost, or disposed. */
  private exitReconciling(): void {
    this.reconciling = false
    if (this.reconcileTimer) {
      clearTimeout(this.reconcileTimer)
      this.reconcileTimer = null
    }
  }

  /** RECONCILED never arrived on a silently-stalled wire — same outcome as a missed pong:
   *  drop the wire and let `handleTransportLoss` reconnect. */
  private onReconcileTimeout(): void {
    if (this.closed || !this.reconciling) return
    const transport = this.transport
    transport.detachHeartbeat()
    transport.abandonActiveTransport()
    this._onTransportClosed(transport, false)
  }

  private handleHandoffFin(): void {
    if (this.state.tag !== 'open' || this.state.upgrade.tag !== 'handoff') return
    this.state.upgrade.finReceived = true
    // The remaining wait for RECONCILED is already bounded by the join deadline armed at
    // `enterUpgradeHandoff` — nothing to arm here.
    this.tryCompleteUpgradeHandoff()
  }

  /** Handoff commits only after BOTH FIN (old wire) and RECONCILED (new wire) — they may reorder. */
  private tryCompleteUpgradeHandoff(): void {
    if (this.state.tag !== 'open' || this.state.upgrade.tag !== 'handoff') return
    if (!this.state.upgrade.finReceived || this.reconciling) return
    const { from, buffer, deferredOmitted, joinTimer } = this.exitUpgradeHandoff()
    if (joinTimer) clearTimeout(joinTimer)
    from.detachHeartbeat()
    from.abandonActiveTransport()
    from.dispose()
    // ENTIRE old buffer first, then the new one — never interleaved by arrival order.
    for (const frame of buffer.old) this.dispatchFrame(frame)
    // Only now is it settled which omitted channels had a real terminal frame in flight.
    this.releaseDeferredOmitted(deferredOmitted)
    for (const frame of buffer.new) this.dispatchFrame(frame)
    // Registrations deferred while the upgrade was in flight can go out now.
    this.flushPendingRegisterReconcile()
    // ── Secondary cleanup that split settlement moved out from under its usual owners ──
    // Ordinary settlement releases an omitted channel INSIDE `applyReconciled`, which puts that
    // release ahead of both `applyReconciled`'s own `drainBufferedFrames` compaction and
    // `handleReconciled`'s `startTtlIfIdle()`. Deferring the release moves it after both, and on
    // the RECONCILED-before-FIN ordering neither ever runs again — so they are re-run here, once
    // both drains have settled which channels actually survived.
    this.pruneSendBufferForReleasedChannels()
    this.startTtlIfIdle()
  }

  /** Drop queued sends whose channel is gone. `drainBufferedFrames` normally does this as a side
   *  effect of its release pass — an entry belonging to neither `serverMap` nor `this.channels` is
   *  simply not copied forward — but a channel released AFTER that pass keeps its entries, and no
   *  later pass would collect them: channel indexes are monotonic and never reused, so the entries
   *  can never be matched, released, or even mis-sent. They would just sit there. */
  private pruneSendBufferForReleasedChannels(): void {
    const sendBuffer = this.sendBuffer
    if (sendBuffer.length === 0) return
    let writeIx = 0
    for (let readIx = 0; readIx < sendBuffer.length; readIx++) {
      const entry = sendBuffer[readIx]!
      if (this.channels.has(entry.channelIx)) sendBuffer[writeIx++] = entry
    }
    sendBuffer.length = writeIx
  }

  _onTransportClosed(transport: ClientChannelTransport, rejectedInitial = false): void {
    if (this.closed) return
    transport.detachHeartbeat()
    if (transport !== this.transport) {
      if (this.state.tag === 'open' && this.state.upgrade.tag === 'handoff' && transport === this.state.upgrade.from) {
        this.abortUpgradeAndReconnectSse(new NetworkError('Connection dropped', true))
      }
      return
    }
    if (this.state.tag === 'open' && this.state.upgrade.tag !== 'none' && this.state.upgrade.tag !== 'handoff') {
      this.state.upgrade.attempt.abort()
    }
    const err = new NetworkError(
      rejectedInitial
        ? `Server rejected ${this.transport.type === CHANNEL_TRANSPORT.SSE ? 'SSE' : 'WebSocket'} connection`
        : 'Connection dropped',
      true,
    )
    this.handleTransportLoss(err, rejectedInitial)
  }

  private handleReconciled(ctrl: ReconciledPayload): void {
    // A BARRIER handoff is settled by exactly one reconciled: the COMMITTED echoing this attempt's
    // `upgradeId`. Any other reconciled arriving in that window — above all a delayed ordinary one
    // still in flight from the OLD wire — would otherwise be consumed as the commit and complete a
    // handoff the server never performed, retiring a live SSE session against a WS that holds no
    // session at all. Checked BEFORE the apply, which is itself unchanged; the legacy flow leaves
    // `upgradeId` null and never enters this branch.
    if (this.state.tag === 'open' && this.state.upgrade.tag === 'handoff' && this.state.upgrade.upgradeId !== null) {
      if (ctrl.upgradeId !== this.state.upgrade.upgradeId) return
      // Consumed. The follow-up reconciled for any channel registered mid-attempt is ordinary.
      this.state.upgrade.upgradeId = null
    }
    this.transport.applyReconciledSettings(ctrl)
    // Split settlement, but ONLY for the reconcile that settles a handoff. The rest of the apply —
    // the C2S ungate, the flow-control reset, `installHeartbeat` — must NOT be deferred with it.
    const deferredOmitted =
      this.state.tag === 'open' && this.state.upgrade.tag === 'handoff' ? this.state.upgrade.deferredOmitted : null
    const outcome = this.applyReconciled(ctrl, deferredOmitted)
    this.installHeartbeat(this.transport, ctrl.pingInterval)
    this.transport.closeAbandonedTransport()
    for (const frame of outcome.frames) this.transport.sendFrame(frame)
    for (const channel of outcome.channelsToOpen) channel._onTransportOpen(this.transport.batched)
    this.tryCompleteUpgradeHandoff()
    if (outcome.reconcileComplete) {
      this.serverTransports = ctrl.transports
      this.startTtlIfIdle()
      this.flushPendingRegisterReconcile()
      this.maybeStartUpgrade()
    }
  }

  // ── SSE→WS upgrade ──

  private maybeStartUpgrade(): void {
    if (this.upgradeDisabled) return
    if (this.state.tag !== 'open' || this.state.upgrade.tag !== 'none') return
    // Settled-reconcile gate; a flush above may have just re-armed `reconciling` — the next
    // RECONCILED retries via `handleReconciled`.
    if (this.reconciling) return
    const nextTransport = UPGRADE_PATH[this.transport.type]
    if (!nextTransport) return
    if (!this.isTransportUpgradeAllowed(nextTransport)) return
    if (!this.serverTransports?.includes(nextTransport)) return
    void this.probeAndUpgrade(nextTransport)
  }

  /** Tear down upgrade state, fall back to a fresh SSE, and disable upgrades for this connection.
   *
   *  When a handoff is in flight this is also the R2 fallback, and its shape is load-bearing:
   *  exiting the handoff state stops further arrivals from buffering, then the OLD wire's received
   *  FIFO prefix is dispatched while channel membership is still intact — those frames include
   *  terminal ctrls (ABORT/ERROR) that carry no seq and that no replay can reproduce. The NEW
   *  wire's buffer is discarded WHOLE: every frame in it is replayable from the server, and
   *  dispatching a prefix of it would advance `trackSeq` past old-wire frames that never arrived,
   *  converting a recoverable abort into permanent loss. Nothing is ever evicted to make room.
   *
   *  Draining the old prefix also lifts `lastSeqByChannel`, so the fresh RECONCILE that follows
   *  reports the HIGHER `lastSeq` and the server replays only past it — no double delivery. */
  private abortUpgradeAndReconnectSse(err: Error): void {
    if (this.closed) return
    if (this.state.tag === 'open') {
      const u = this.state.upgrade
      if (u.tag === 'probing' || u.tag === 'preparing' || u.tag === 'draining') {
        u.attempt.abort()
        this.exitUpgradeAttempt(u.attempt)
      } else if (u.tag === 'handoff') {
        const { from, buffer, deferredOmitted, joinTimer } = this.exitUpgradeHandoff()
        if (joinTimer) clearTimeout(joinTimer)
        from.detachHeartbeat()
        from.abandonActiveTransport()
        from.dispose()
        for (const frame of buffer.old) this.dispatchFrame(frame)
        this.releaseDeferredOmitted(deferredOmitted)
      }
    }
    this.upgradeDisabled = true
    this.transport.abandonActiveTransport()
    this.transport.dispose()
    this.transport = TRANSPORT_REGISTRY[CHANNEL_TRANSPORT.SSE](this.telefuncUrl, this.connectionOptions, this)
    this.handleTransportLoss(err)
  }

  private isTransportUpgradeAllowed(nextTransport: ChannelTransport): boolean {
    return this.connectionOptions.transports.includes(nextTransport)
  }

  /** The barrier upgrade, end to end: PREPARE stages the new wire while the old one stays fully
   *  live, READY licenses the gate, and the barrier is the old wire's FINAL frame — it commits the
   *  swap server-side, so the new wire never has to reconcile for itself.
   *
   *  Version skew degrades to "stay on SSE" in both directions, so neither gets compatibility code.
   *  An old client against a new server sends a RECONCILE naming no stage, which the server treats
   *  as ordinary. A new client against an old server trips that server's unknown-tag assert on the
   *  PREPARE, killing the probe; the pre-barrier rollback leaves the live SSE wire untouched. */
  private async probeAndUpgrade(targetTransport: ChannelTransport): Promise<void> {
    // Flush pending register-reconcile inline before the upgrade so its RECONCILE
    // doesn't fire mid-drain on the dying old wire.
    this.flushPendingRegisterReconcile()
    // `sessionId` is a PREPARE field the server hard-requires: with none there is nothing to stage
    // against, so there is no attempt.
    const sessionId = this.sessionId
    if (sessionId === null) return
    const attempt = new AbortController()
    this.enterUpgradeProbing(attempt)
    // A holder because the watchdog is armed deep inside staging — it must start at the PREPARE,
    // not at the probe — but is disarmed here on every path.
    const watchdog: AttemptWatchdog = { timer: null }
    try {
      const from = this.transport
      const to = TRANSPORT_REGISTRY[targetTransport](this.telefuncUrl, this.connectionOptions, this)
      const staged = await this.stageUpgrade(to, sessionId, attempt, watchdog)
      if (!staged) return
      if ((await this.commitBarrier(from, staged.upgradeId, attempt)) !== 'committed') return
      this.flipToNewWire(from, to, staged)
    } finally {
      // Disarmed before the attempt is released, so a deadline can never fire against a committed
      // handoff — where `probe.close()` would be closing the LIVE transport's socket.
      if (watchdog.timer) clearTimeout(watchdog.timer)
      this.exitUpgradeAttempt(attempt)
      // No-op unless the attempt aborted with a registration deferred behind it (the gate
      // blocks while a handoff is still in flight).
      this.flushPendingRegisterReconcile()
    }
  }

  /** PREPARE → READY on the probed socket, without that socket becoming the transport. Resolves
   *  null when the attempt is over pre-barrier, having already performed whatever recovery that
   *  ending needed — the caller only has to return. */
  private async stageUpgrade(
    to: ClientChannelTransport,
    sessionId: string,
    attempt: AbortController,
    watchdog: AttemptWatchdog,
  ): Promise<StagedProbe | null> {
    const probe = await to.probe()
    if (attempt.signal.aborted || !probe) {
      probe?.close()
      return null
    }
    const heartbeat = new Heartbeat(
      this.pingIntervalMs,
      this.pingIntervalMs * 2,
      () => probe.ping(),
      () => attempt.abort(),
    )
    probe.onPong(() => heartbeat.resetPong())
    probe.onClose(() => attempt.abort())
    heartbeat.start()
    attempt.signal.addEventListener(
      'abort',
      () => {
        heartbeat.stop()
        probe.close()
      },
      { once: true },
    )

    const upgradeId = crypto.randomUUID()
    const heldFrames: HeldProbeFrame[] = []
    let heldCount = 0
    let heldBytes = 0
    let heldOverflow = false
    let onReady: ((payload: ReadyPayload) => void) | null = null
    probe.onFrame((frame, byteLength) => {
      if (frame.tag === TAG.READY) {
        onReady?.(frame.payload)
        return
      }
      // The COMMITTED can beat the transport flip. HELD, not dropped — replayed through the
      // ordinary receive path right after the flip, where the handoff buffer takes it. Charged
      // against the SAME budget that buffer enforces, and charged HERE rather than at replay: this
      // queue exists before any handoff state does, so a check only at replay would leave it
      // unbounded.
      if (heldOverflow) return
      heldFrames.push({ frame, byteLength })
      heldCount += 1
      heldBytes += byteLength
      if (heldCount > UPGRADE_HANDOFF_BUFFER_FRAMES || heldBytes > UPGRADE_HANDOFF_BUFFER_BYTES) {
        // Held frames are NEW-wire frames, so R2's discard-the-new-buffer-WHOLE rule applies to them
        // as a set: nothing is added after this flag, and the abort ends the attempt before any
        // replay, so no prefix can survive.
        heldOverflow = true
        heldFrames.length = 0
        attempt.abort()
      }
    })

    // The ONLY watchdog over PREPARE→handoff-entry. A barrier the server finds stale is refused
    // SILENTLY, so without this a refused attempt would sit in `preparing`/`draining` forever.
    watchdog.timer = setTimeout(() => attempt.abort(), UPGRADE_ATTEMPT_TIMEOUT_MS)
    // Armed BEFORE the PREPARE leaves: a server may answer within the same turn, and a resolver
    // installed afterwards would miss that READY and wedge the attempt until its deadline.
    const readyP = new Promise<ReadyPayload | null>((resolve) => {
      onReady = resolve
      attempt.signal.addEventListener('abort', () => resolve(null), { once: true })
    })
    this.enterUpgradePreparing(attempt)
    probe.send(this.buildPrepareFrame(upgradeId, sessionId))
    const ready = await readyP
    // A READY naming another attempt is worth exactly as much as none: it says nothing about
    // whether OUR stage was installed.
    if (!ready || ready.upgradeId !== upgradeId || attempt.signal.aborted) {
      // Pre-barrier: nothing has left that could rotate the session, so the old wire keeps it.
      // Aborting is also what closes the probe.
      attempt.abort()
      this.rollbackToOldWire()
      return null
    }
    return { upgradeId, heartbeat, heldFrames }
  }

  /** Emits the barrier as the old wire's final frame. Every verdict other than `committed` has
   *  already performed its own recovery, so the caller only has to return. */
  private async commitBarrier(
    from: ClientChannelTransport,
    upgradeId: string,
    attempt: AbortController,
  ): Promise<'committed' | 'recovered'> {
    // Gate down. From here the old wire carries exactly one more frame, and the payload is built
    // inside `emitBarrier` — at emission — so its cursors reflect everything delivered since.
    this.enterUpgradeDraining(attempt)
    const emission = await from.emitBarrier(() => this.buildReconcileFrame({ upgradeId }), attempt.signal)

    if (emission === 'wedged') {
      // Nothing was written, so upgrades stay ENABLED — but the old wire cannot make progress: a
      // POST that may never settle owns its flush gate, so released frames would sit in the outbox
      // behind a guard that never opens. Recovery is the ordinary transport-loss path. Deliberately
      // NOT drained into the new wire's outbox first: the reconnect's initial batch would then carry
      // them AHEAD of the replay of frames the wedged POST swallowed, which the server dup-drops.
      attempt.abort()
      this.recoverWedgedOldWire(new NetworkError('Upgrade aborted with the old wire stalled', true))
      return 'recovered'
    }
    if (emission === 'not-emitted') {
      // Not one barrier byte written and the wire still usable, so the server's session is exactly
      // where it was: stay on it.
      attempt.abort()
      this.rollbackToOldWire()
      return 'recovered'
    }
    if (attempt.signal.aborted) {
      // The barrier MAY already have reached the server, so no wire can be trusted to still hold the
      // session: abandon BOTH and reconcile afresh. This must win over the reconcile deadline
      // `buildReconcileFrame` just armed, which would take the GENERIC reconnect and leave upgrades
      // enabled. It wins by construction — the attempt deadline was armed at PREPARE, strictly
      // earlier, and both are 10 s.
      this.abortUpgradeAndReconnectSse(new NetworkError('Upgrade barrier attempt timed out', true))
      return 'recovered'
    }
    return 'committed'
  }

  /** Adopts the probed socket as the transport and replays what it received before the flip. */
  private flipToNewWire(from: ClientChannelTransport, to: ClientChannelTransport, staged: StagedProbe): void {
    staged.heartbeat.stop()
    this.transport = to
    this.enterUpgradeHandoff(from, staged.upgradeId)
    // The flip must NOT fire `sendReconcileOnOpen`: the barrier already WAS this attempt's
    // reconcile, and a second one would rotate the session again and delete the FIN finalizer the
    // barrier just installed. Suppression spans only the synchronous adoption of the probed socket —
    // a socket that instead has to CONNECT opens later, after the flag is down, and correctly
    // reconciles itself.
    this.suppressReconcileOnOpen = true
    try {
      to.start()
    } finally {
      this.suppressReconcileOnOpen = false
    }
    // No mid-loop cap check is needed: the held set is bounded by the SAME budget the handoff buffer
    // enforces, that buffer starts empty, and this loop is fully synchronous. Introducing an `await`
    // here would break that — the remaining held frames would then have to be DISCARDED rather than
    // dispatched, or `trackSeq` advances past frames the fallback dropped and the recovery replay
    // stays dup-suppressed forever.
    for (const held of staged.heldFrames) this._onTransportFrame(held.frame, to, held.byteLength)
  }

  /** The attempt ended pre-barrier on a wire that can no longer make progress.
   *
   *  A replacement transport rather than a reset of the wedged one: the stalled POST still owns
   *  that instance's flush gate and will only let go if and when it settles, so the instance is
   *  unusable for as long as it exists. Replacing it also makes the stalled POST's eventual
   *  settlement harmless — it reports loss against a transport that is no longer
   *  `this.transport`, which `_onTransportClosed` already ignores, so the successor wire is never
   *  abandoned by a straggler.
   *
   *  Sequenced frames the wedged POST swallowed are not lost: they are still in the per-channel
   *  replay buffers, and `applyReconciled` re-sends everything past the server's reported
   *  `lastSeq`. Unsequenced C2S ctrl on a dying wire remains the pre-existing, documented gap.
   *
   *  NOT sticky: nothing was emitted, so this attempt cost the connection a reconnect, not its
   *  ability to upgrade later. */
  private recoverWedgedOldWire(err: Error): void {
    const wedged = this.transport
    this.transport = TRANSPORT_REGISTRY[CHANNEL_TRANSPORT.SSE](this.telefuncUrl, this.connectionOptions, this)
    wedged.abandonActiveTransport()
    wedged.dispose()
    this.handleTransportLoss(err)
  }

  /** Attempt ended before anything committed: stay on the old wire. The `finally` exits the attempt
   *  and flushes any queued register-reconcile, whose batch carries the frames buffered meanwhile.
   *  With none queued that flush no-ops, so drain those buffered frames onto the old wire here. */
  private rollbackToOldWire(): void {
    if (this.registerReconcileTimer !== null) return
    for (const frame of this.drainBufferedFrames(this.channels, undefined)) this.transport.sendFrame(frame)
  }

  private handleTransportLoss(err: Error, rejected = false): void {
    if (this.closed) return
    if (this.state.tag === 'open' && this.state.upgrade.tag === 'handoff') {
      this.abortUpgradeAndReconnectSse(err)
      return
    }
    // The wire is dying — cancel the queued RECONCILE (no point sending) and release
    // unconfirmed-releasing entries so they don't leak onto the post-reconnect RECONCILE.
    this.cancelPendingRegisterReconcile()
    this.exitReconciling()
    this.reconcileIxes.clear()
    if (this.ttl) {
      clearTimeout(this.ttl)
      this.ttl = null
    }

    const { attempt: prevAttempt, startedAt: prevStartedAt } =
      this.state.tag === 'reconnecting' ? this.state : { attempt: 0, startedAt: 0 }

    if (rejected && prevAttempt === 0) {
      this.closeAll(err instanceof Error ? err : new NetworkError('Connection dropped', true))
      this.dispose()
      return
    }
    if (this.channels.size === 0) {
      this.dispose()
      return
    }
    const startedAt = prevStartedAt || Date.now()
    if (Date.now() - startedAt > this.reconnectTimeoutMs) {
      this.closeAll(err instanceof Error ? err : new NetworkError('Connection dropped', true))
      this.dispose()
      return
    }
    const delay = Math.min(CHANNEL_RECONNECT_INITIAL_DELAY_MS * 2 ** prevAttempt, CHANNEL_RECONNECT_MAX_DELAY_MS)
    this.enterReconnecting(prevAttempt + 1, startedAt, delay)
  }

  private startTtlIfIdle(): void {
    if (this.closed || this.channels.size > 0 || this.ttl) return
    this.ttl = setTimeout(() => {
      if (this.channels.size === 0) this.dispose()
    }, this.idleTimeoutMs)
  }

  private dispose(): void {
    if (this.closed) return
    if (this.ttl) {
      clearTimeout(this.ttl)
      this.ttl = null
    }
    if (this.registerReconcileTimer !== null) {
      clearTimeout(this.registerReconcileTimer)
      this.registerReconcileTimer = null
    }
    // Tear down any in-flight phase before transitioning to `closed`.
    if (this.state.tag === 'reconnecting') clearTimeout(this.state.timer)
    if (this.state.tag === 'open') {
      const u = this.state.upgrade
      if (u.tag === 'probing' || u.tag === 'preparing' || u.tag === 'draining') u.attempt.abort()
      if (u.tag === 'handoff') {
        if (u.joinTimer) clearTimeout(u.joinTimer)
        u.from.detachHeartbeat()
        u.from.abandonActiveTransport()
        u.from.dispose()
      }
    }
    this.enterClosed()
    this.transport.detachHeartbeat()
    this.transport.dispose()
    for (const replayBuffer of this.replayBuffers.values()) replayBuffer.dispose()
    this.channels.clear()
    this.channelIndex.clear()
    this.sendBuffer = []
    this.lastSeqByChannel.clear()
    this.replayBuffers.clear()
    this.reconcileIxes.clear()
    this.exitReconciling()
    ClientConnection.cache.delete(this.cacheKey)
  }

  // ── Protocol internals ──

  /** `barrier` turns this into the OLD wire's final frame: authoritative membership plus cursors
   *  that must be read HERE, at emission, not when the attempt was staged. */
  buildReconcileFrame(barrier?: { upgradeId: string }): OutboundFrame {
    this.enterReconciling()
    this.reconcileIxes = new Set()
    const open: ReconcilePayload['open'] = []
    for (const [ix, entry] of this.channels) {
      const isInitial = entry.state.tag !== 'open' && entry.state.initial
      // A barrier carries ESTABLISHED channels only — the server rejects `initial: true` there,
      // because `attach` would park the commit mid-flight waiting for a channel to appear. Such a
      // channel is left out of `reconcileIxes` as well, so the settling `applyReconciled` reads it
      // as registered-after-the-fact and stages the ordinary follow-up RECONCILE for it. Leaving it
      // IN `reconcileIxes` instead would classify it as server-omitted and release it — killing a
      // channel the user just opened.
      if (barrier && isInitial) continue
      this.reconcileIxes.add(ix)
      const payloadEntry: ReconcilePayload['open'][number] = {
        id: entry.channel.id,
        ix,
        lastSeq: this.lastSeqByChannel.get(ix) ?? 0,
      }
      if (isInitial) payloadEntry.initial = true
      open.push(payloadEntry)
    }
    const reconcile: ReconcilePayload = { open }
    if (this.sessionId) reconcile.sessionId = this.sessionId
    if (barrier) {
      reconcile.barrier = true
      reconcile.upgradeId = barrier.upgradeId
    }
    return { kind: 'reconcile', frame: encode.reconcile(reconcile) }
  }

  /** Identity only — no cursors. Anything captured here is stale by the time the barrier commits,
   *  and replaying from a stale watermark would burst past the peer's flow-control credit. Must NOT
   *  go through `buildReconcileFrame`: `enterReconciling` would gate every user send for the whole
   *  `PREPARE`→`READY` window, which is precisely what leaving `preparing` ungated buys back. */
  private buildPrepareFrame(upgradeId: string, sessionId: string): Uint8Array<ArrayBuffer> {
    const open: PreparePayload['open'] = []
    for (const [ix, entry] of this.channels) {
      if (entry.state.tag !== 'open' && entry.state.initial) continue
      open.push({ id: entry.channel.id, ix })
    }
    return encode.prepare({ upgradeId, sessionId, open })
  }

  drainBufferedFramesForReconcile(isInitialBatch: boolean): OutboundFrame[] {
    if (this.transport.reconcileMode !== 'batch-on-reconcile') return []
    // The hazard is confined to a *reconnect's* initial batch: the previous wire may have died
    // with an unacked frame still only in the replay buffer, which is re-sent after RECONCILED
    // (`applyReconciled`'s `getAfter`). Eager-batching a newer frame into that initial batch
    // would reach the server first, advance its `lastClientSeq` past the older one, and get the
    // older one dup-dropped on replay — silent message loss. Defer to the post-RECONCILED
    // release there (same ordering discipline as the WS 'release-after-reconciled' mode).
    // Every other reconcile is on a live wire: a fresh connect has no prior server state, and a
    // reconcile on an established wire (new-channel registration, chained reconcile) has no
    // in-transit replay frame to jump ahead of — so eager-batch, it saves a round-trip.
    if (isInitialBatch && this.sessionId !== null) return []
    return this.drainBufferedFrames(this.channels, undefined)
  }

  stageReconcileBatch(isInitialBatch = false): ReconcileBatch {
    // This batch includes every channel, so it already covers any pending registration.
    this.cancelRegisterReconcileTimer()
    const reconcileFrame = this.buildReconcileFrame()
    const movedBufferedFrames = this.drainBufferedFramesForReconcile(isInitialBatch)
    return { reconcileFrame, movedBufferedFrames }
  }

  private sendReconcileBatch(reconcileBatch: ReconcileBatch): void {
    this.transport.sendFrame(reconcileBatch.reconcileFrame)
    for (const frame of reconcileBatch.movedBufferedFrames) this.transport.sendFrame(frame)
  }

  private appendReconcileBatch(target: OutboundFrame[], reconcileBatch: ReconcileBatch): void {
    target.push(reconcileBatch.reconcileFrame)
    for (const frame of reconcileBatch.movedBufferedFrames) target.push(frame)
  }

  /** `deferredOmitted` is non-null ONLY while an upgrade handoff is settling. On every ordinary
   *  reconcile it is null and this function behaves exactly as it did before R4 — the one added
   *  branch is guarded on it. */
  private applyReconciled(ctrl: ReconciledPayload, deferredOmitted: number[] | null): ReconcileOutcome {
    this.sessionId = ctrl.sessionId
    if (ctrl.reconnectTimeout) this.reconnectTimeoutMs = ctrl.reconnectTimeout
    if (ctrl.idleTimeout) this.idleTimeoutMs = ctrl.idleTimeout
    if (ctrl.clientReplayBuffer) this.clientReplayBufferBytes = ctrl.clientReplayBuffer
    if (ctrl.clientReplayBufferBinary) this.clientReplayBufferBinaryBytes = ctrl.clientReplayBufferBinary

    const serverMap = new Map<number, number>()
    for (const channel of ctrl.open) serverMap.set(channel.ix, channel.lastSeq)
    const reconcileIxes = this.reconcileIxes
    this.reconcileIxes = new Set()
    const releaseFrames: OutboundFrame[] = []
    const channelsToOpen: MuxChannel[] = []
    let hasNewChannels = false

    for (const [ix, entry] of this.channels) {
      if (!reconcileIxes.has(ix)) {
        // Registered after the RECONCILE we just got back was built — wait for next round.
        if (!serverMap.has(ix)) hasNewChannels = true
        continue
      }
      if (entry.state.tag === 'releasing') {
        this.releaseChannel(ix, entry.channel, entry.state.err)
        continue
      }
      if (!serverMap.has(ix)) {
        if (deferredOmitted) {
          // A handoff is settling and the old wire's buffer may still hold a terminal ABORT/ERROR
          // for this channel. Releasing now would make `closeRemoteChannel` a no-op when that
          // frame finally drains, and the server's real abort value — terminal, seq-less, and
          // unreproducible by any later reconnect — would die there. Hold it back instead.
          deferredOmitted.push(ix)
          continue
        }
        const err = new NetworkError('Channel not acknowledged by server after reconnect', true)
        this.releaseChannel(ix, entry.channel, err)
        entry.channel._onTransportClose(err)
        continue
      }
      if (entry.state.tag === 'pending') this.enterChannelOpen(ix)
      const replay = this.replayBuffers.get(ix)
      if (replay)
        for (const frame of replay.getAfter(serverMap.get(ix)!)) releaseFrames.push({ kind: 'reconcile', frame })
      if (!entry.channel.isClosed) channelsToOpen.push(entry.channel)
    }

    for (const frame of this.drainBufferedFrames(serverMap, this.channels)) releaseFrames.push(frame)

    if (hasNewChannels) {
      const reconcileBatch = this.stageReconcileBatch()
      this.appendReconcileBatch(releaseFrames, reconcileBatch)
    } else {
      this.exitReconciling()
    }

    return { frames: releaseFrames, channelsToOpen, reconcileComplete: !hasNewChannels }
  }

  /** Second half of the split settlement. Channels the settling RECONCILED omitted were held back
   *  until the old wire's buffer drained; any that a real ABORT/ERROR closed during that drain are
   *  already gone from `this.channels`, and whatever is still present genuinely vanished
   *  server-side and gets exactly today's generic release. */
  private releaseDeferredOmitted(ixes: number[]): void {
    for (const ix of ixes) {
      const entry = this.channels.get(ix)
      if (!entry) continue
      const err = new NetworkError('Channel not acknowledged by server after reconnect', true)
      this.releaseChannel(ix, entry.channel, err)
      entry.channel._onTransportClose(err)
    }
  }

  private closeRemoteChannel(ix: number, err?: Error): void {
    const entry = this.channels.get(ix)
    if (!entry) return
    this.releaseChannel(ix, entry.channel, err ?? new ChannelClosedError())
    entry.channel._onTransportClose(err)
  }

  private closeAll(err: Error): void {
    for (const [, entry] of this.channels) {
      entry.channel._onTransportClose(err)
    }
    this.dispose()
  }

  /** Dedup against double-delivery. Transports are TCP-ordered and replay sends a
   *  contiguous slice starting at our reported `lastSeq + 1`, so duplicates shouldn't
   *  occur in normal operation — kept as a cheap safety net. */
  private trackSeq(ix: number, seq: number): 'accept' | 'dup' {
    const prev = this.lastSeqByChannel.get(ix) ?? 0
    if (seq <= prev) return 'dup'
    this.lastSeqByChannel.set(ix, seq)
    return 'accept'
  }

  private drainBufferedFrames(
    releasableChannels: Set<number> | Map<number, unknown>,
    retainedChannels: Set<number> | Map<number, unknown> | undefined,
  ): OutboundFrame[] {
    const frames: OutboundFrame[] = []
    const sendBuffer = this.sendBuffer
    let writeIx = 0
    for (let readIx = 0; readIx < sendBuffer.length; readIx++) {
      const entry = sendBuffer[readIx]!
      const frame = entry.frame
      const channelIx = entry.channelIx
      const seq = entry.seq
      if (!releasableChannels.has(channelIx)) {
        if (retainedChannels?.has(channelIx)) sendBuffer[writeIx++] = entry
        continue
      }
      if (seq !== undefined) {
        const tag = frame[0]
        const isBinary = tag === TAG.BINARY || tag === TAG.PUBLISH_BINARY || tag === TAG.PUBLISH_BINARY_ACK_REQ
        this.replayBuffers.get(channelIx)?.push(seq, frame, isBinary)
      }
      frames.push({ kind: 'reconcile', frame })
    }
    sendBuffer.length = writeIx
    return frames
  }

  private releaseChannel(ix: number, channel: MuxChannel, err: Error): void {
    this.channels.delete(ix)
    this.channelIndex.delete(channel)
    this.lastSeqByChannel.delete(ix)
    const replayBuffer = this.replayBuffers.get(ix)
    replayBuffer?.dispose()
    this.replayBuffers.delete(ix)
    // Pending acks on this channel are rejected by the channel itself via
    // `_onTransportClose(err)` — connection no longer owns them.
    void err
  }
}

class WsTransport implements ClientChannelTransport {
  readonly type = CHANNEL_TRANSPORT.WS
  readonly reconnectTimeoutMessage = 'WebSocket reconnect timed out'
  readonly sendReconcileOnOpen = true
  readonly reconcileMode = 'release-after-reconciled' as const
  readonly connId = null
  readonly batched = false
  private heartbeat: Heartbeat | null = null
  private probedWs: WebSocket | null = null
  private ws: WebSocket | null = null
  private abandonedWs: WebSocket | null = null
  private connecting = false
  private everOpened = false

  private readonly wsUrl: string

  constructor(
    telefuncUrl: string,
    private readonly owner: ClientConnection,
  ) {
    const base = typeof window === 'undefined' ? undefined : window.location.href
    const url = new URL(telefuncUrl, base)
    url.protocol = url.protocol === 'https:' ? 'wss:' : 'ws:'
    this.wsUrl = url.href
  }

  async probe(): Promise<ProbeWire | null> {
    let ws: WebSocket
    try {
      ws = new WebSocket(this.wsUrl)
    } catch {
      return null
    }
    ws.binaryType = 'arraybuffer'

    let onPong: (() => void) | null = null
    let onClose: (() => void) | null = null
    let onFrame: ((frame: DecodedFrame, byteLength: number) => void) | null = null
    ws.onmessage = ({ data }: MessageEvent) => {
      const raw = new Uint8Array(data as ArrayBuffer)
      const frame = decode(raw)
      if (frame.tag === TAG.PONG) {
        onPong?.()
        return
      }
      // Forwarded rather than discarded: the barrier flow's READY arrives here, and so can the
      // COMMITTED, when the server answers the barrier before the transport flip has run.
      onFrame?.(frame, raw.byteLength)
    }
    ws.onclose = () => {
      if (this.probedWs === ws) this.probedWs = null
      onClose?.()
    }
    ws.onerror = () => {}
    ws.onopen = () => ws.send(encode.ping())

    // First pong proves the wire is alive — consumer reassigns onPong/onClose after the await.
    const ready = await new Promise<boolean>((resolve) => {
      const timer = setTimeout(() => resolve(false), WS_PROBE_TIMEOUT_MS)
      onPong = () => {
        clearTimeout(timer)
        resolve(true)
      }
      onClose = () => {
        clearTimeout(timer)
        resolve(false)
      }
    })
    if (!ready) {
      try {
        ws.close()
      } catch {}
      return null
    }

    this.probedWs = ws
    return {
      ping: () => {
        try {
          ws.send(encode.ping())
        } catch {}
      },
      onPong: (cb) => {
        onPong = cb
      },
      onClose: (cb) => {
        onClose = cb
      },
      send: (frame) => {
        try {
          ws.send(frame)
        } catch {}
      },
      onFrame: (cb) => {
        onFrame = cb
      },
      close: () => {
        if (this.probedWs === ws) this.probedWs = null
        try {
          ws.close()
        } catch {}
      },
    }
  }

  /** Unreachable by construction — `UPGRADE_PATH` only ever upgrades SSE→WS, so a WS wire is never
   *  the one a barrier is emitted on. Mirrors `SseTransport.probe`'s stance on the same asymmetry. */
  async emitBarrier(): Promise<BarrierEmission> {
    throw new Error('WS transport does not emit an upgrade barrier')
  }

  start(): void {
    if (this.connecting || this.hasWire()) return

    const wsProbed = this.probedWs
    if (wsProbed) {
      this.probedWs = null
      this.ws = wsProbed
      this.setupHandlers(wsProbed)
      this.handleOpen(wsProbed)
      return
    }

    this.connecting = true

    let ws: WebSocket
    try {
      ws = new WebSocket(this.wsUrl)
    } catch {
      this.connecting = false
      this.owner._onTransportClosed(this, false)
      return
    }

    this.ws = ws
    ws.binaryType = 'arraybuffer'

    ws.onopen = () => {
      if (this.ws !== ws) return
      this.handleOpen(ws)
    }

    this.setupHandlers(ws)
  }

  private handleOpen(ws: WebSocket): void {
    if (this.ws !== ws) return
    this.everOpened = true
    this.connecting = false
    this.owner._onTransportOpen(this)
  }

  attachHeartbeat(hb: Heartbeat): void {
    this.heartbeat = hb
  }

  detachHeartbeat(): void {
    this.heartbeat?.stop()
    this.heartbeat = null
  }

  hasHeartbeat(): boolean {
    return this.heartbeat !== null
  }

  private setupHandlers(ws: WebSocket): void {
    ws.onmessage = ({ data }: MessageEvent) => {
      const raw = new Uint8Array(data as ArrayBuffer)
      const frame = decode(raw)
      if (frame.tag === TAG.PONG) {
        this.heartbeat?.resetPong()
        return
      }
      this.owner._onTransportFrame(frame, this, raw.byteLength)
    }
    ws.onclose = () => {
      if (this.ws === ws) this.ws = null
      this.connecting = false
      this.owner._onTransportClosed(this, !this.everOpened)
    }
    ws.onerror = () => {}
  }

  hasWire(): boolean {
    return this.ws !== null
  }

  isConnecting(): boolean {
    return this.connecting
  }

  sendFrame(frame: OutboundFrame): void {
    const ws = this.ws
    assert(ws)
    ws.send(frame.frame)
  }

  abandonActiveTransport(): void {
    const ws = this.ws
    if (!ws) return
    this.ws = null
    this.closeAbandonedTransport()
    this.abandonedWs = ws
    ws.onopen = ws.onerror = ws.onclose = null
  }

  closeAbandonedTransport(): void {
    const ws = this.abandonedWs
    if (!ws) return
    this.abandonedWs = null
    ws.onmessage = ws.onclose = null
    try {
      ws.close()
    } catch {}
  }

  applyReconciledSettings(): void {}

  sendPing(): void {
    if (this.ws?.readyState !== WebSocket.OPEN) return
    this.ws.send(encode.ping())
  }

  dispose(): void {
    this.connecting = false
    const wsProbed = this.probedWs
    this.probedWs = null
    if (wsProbed) {
      wsProbed.onopen = wsProbed.onmessage = wsProbed.onerror = wsProbed.onclose = null
      try {
        wsProbed.close(1000)
      } catch {}
    }
    const ws = this.ws
    this.ws = null
    if (ws) {
      ws.onopen = ws.onmessage = ws.onerror = ws.onclose = null
      try {
        ws.close(1000)
      } catch {}
    }
    this.closeAbandonedTransport()
  }
}

class SseTransport implements ClientChannelTransport {
  readonly type = CHANNEL_TRANSPORT.SSE
  readonly reconnectTimeoutMessage = 'SSE reconnect timed out'
  readonly sendReconcileOnOpen = false
  readonly reconcileMode = 'batch-on-reconcile' as const
  async probe(): Promise<ProbeWire | null> {
    throw new Error('SSE transport does not implement probe()')
  }

  readonly connId = crypto.randomUUID()
  get batched(): boolean {
    return this.streamRequest.tag !== 'active'
  }
  private heartbeat: Heartbeat | null = null
  private connecting = false
  private startTimer: ReturnType<typeof setTimeout> | null = null
  /** Abort handle for the active transport's fetches. `null` means no active transport. */
  private transportAbort: AbortController | null = null
  private abandonedStream: AbortController | null = null
  private readonly abandonedControllers = new WeakSet<AbortController>()
  private outbox: OutboxEntry[] = []
  private readonly flushScheduler = new DeadlineScheduler(() => {
    void this.flushOutbox()
  })
  private flushing = false
  private lastPostStartedAt = 0
  private flushThrottleMs = SSE_FLUSH_THROTTLE_MS
  private postIdleFlushDelayMs = SSE_POST_IDLE_FLUSH_DELAY_MS
  private heartbeatFlushDelayMs = Math.floor(CHANNEL_PING_INTERVAL_MS / 2)
  private drainCallbacks: Array<() => void> = []
  /** Client→server upstream POST. `failed` is sticky → fall back to outbox+batch POSTs forever. */
  private streamRequest:
    | { tag: 'idle' }
    | {
        tag: 'active'
        body: PushReadableStream<Uint8Array<ArrayBuffer>>
        fetch: Promise<unknown>
      }
    | { tag: 'failed' } = { tag: 'idle' }

  constructor(
    private readonly telefuncUrl: string,
    private readonly fetchImpl: typeof fetch,
    private readonly sessionToken: string | undefined,
    private readonly userHeaders: Record<string, string> | undefined,
    private readonly owner: ClientConnection,
  ) {}

  /** Batch-mode quiesce: wait for the wire to go naturally empty, or `timeoutMs`, whichever comes
   *  first. Only `emitBarrier` needs it — a barrier concurrent with an earlier POST has no defined
   *  server-side dispatch order. */
  private async gracefulDrain(timeoutMs: number): Promise<void> {
    if (this.streamRequest.tag === 'active') return
    if (!this.flushing && this.outbox.length === 0) return
    const drained = new Promise<void>((resolve) => this.drainCallbacks.push(resolve))
    await Promise.race([drained, new Promise<void>((resolve) => setTimeout(resolve, timeoutMs))])
  }

  /** The barrier is this wire's last frame. It is never concurrent with another upstream POST,
   *  never retried, and never an ordinary outbox item — all three would let it commit out of order
   *  with frames the server then drops as post-rotation stragglers. */
  async emitBarrier(buildFrame: () => OutboundFrame, signal: AbortSignal): Promise<BarrierEmission> {
    if (this.streamRequest.tag === 'active') {
      assert(!this.flushing && this.outbox.length === 0)
      const frame = buildFrame()
      this.streamRequest.body.push(encodeU32(frame.frame.byteLength))
      this.streamRequest.body.push(frame.frame)
      // Body closed, fetch NOT awaited. That await is the round-trip this design removes, and it
      // never meant "server acknowledged" anyway — `sse.ts` void-dispatches the response. Nothing
      // here awaits, so the attempt deadline has no window to interrupt and needs none.
      this.closeStreamRequest()
      return 'emitted'
    }

    // Batch mode. Two POSTs in flight at once have no defined server-side dispatch order — entry
    // into the shared recv chain is the order each body is READ — so a barrier racing an earlier
    // POST can commit ahead of its frames, which then land post-rotation and are dropped silently.
    // `gracefulDrain` is the natural quiesce; if it times out with a POST still in flight we keep
    // waiting rather than racing it, because a concurrent barrier is the wrong direction to be
    // impatient in. What bounds the wait is the ATTEMPT SIGNAL, not a shorter timeout.
    await this.gracefulDrain(UPGRADE_DRAIN_TIMEOUT_MS)
    while (this.flushing) {
      // Checked BEFORE the abort: a wire that already died has its own recovery in flight, and
      // reporting it wedged here would reconnect a second time on top of that one.
      if (!this.hasWire()) return 'not-emitted'
      // Aborted with a flush STILL in flight. Nothing was written, so this stays a pre-barrier
      // outcome — but the POST that owns the flush gate may never settle, and until it does every
      // `flushOutbox` returns at that guard. Merely un-gating the connection here would leave a
      // wire that reports itself healthy and silently carries nothing.
      if (signal.aborted) return 'wedged'
      await raceAbort(new Promise<void>((resolve) => this.drainCallbacks.push(resolve)), signal)
    }
    if (!this.hasWire() || signal.aborted) return 'not-emitted'
    this.flushScheduler.cancel()
    // Whatever the quiesce left behind rides along in this same final POST, barrier LAST.
    const queued = this.outbox.splice(0, this.outbox.length).map((entry) => entry.frame)
    queued.push(buildFrame().frame)
    // Past this line the barrier is on the wire and the server may already have read it, so no
    // later abort can claim otherwise. The await is raced only so a hanging POST cannot outlive the
    // attempt deadline — the verdict stays `'emitted'` either way, which is what keeps the caller
    // on the sticky both-wires fallback instead of a generic reconnect.
    const post = this.sendStandalonePost(queued)
    await raceAbort(post, signal)
    return 'emitted'
  }

  start(): void {
    if (this.connecting || this.hasWire()) return
    this.connecting = true
    // Defer one reconcile window so startup code can register channels before the initial batch.
    this.startTimer = setTimeout(() => {
      this.startTimer = null
      if (!this.connecting || this.hasWire()) return
      void this.openStream()
    }, SSE_RECONCILE_DEADLINE_MS)
  }

  hasWire(): boolean {
    return this.transportAbort !== null
  }

  isConnecting(): boolean {
    return this.connecting
  }

  sendFrame(frame: OutboundFrame): void {
    if (this.flushing && frame.kind === 'heartbeat') {
      this.schedulePingDuringFlush(frame)
      return
    }
    if (this.streamRequest.tag === 'active') {
      this.streamRequest.body.push(encodeU32(frame.frame.byteLength))
      this.streamRequest.body.push(frame.frame)
      return
    }
    const now = Date.now()
    const deadline = this.getFrameDeadline(frame.kind, now)
    this.outbox.push({ frame: frame.frame, deadline })
    this.scheduleFlush()
    if (deadline <= now) void this.flushOutbox()
  }

  private async openStream(): Promise<void> {
    const abortController = new AbortController()
    this.transportAbort = abortController
    const stage = this.stageInitialBatch()

    // SSE downstream + upstream POST fire in parallel. If upstream fails, we fall back to outbox+batch.
    const ssePromise = this.fetchImpl(getMarkedRequestUrl(this.telefuncUrl, REQUEST_KIND.SSE), {
      method: 'POST',
      headers: {
        ...this.userHeaders,
        Accept: 'text/event-stream',
        'Content-Type': 'application/octet-stream',
        [REQUEST_KIND_HEADER]: REQUEST_KIND.SSE,
        ...(this.sessionToken ? { [TELEFUNC_SESSION_HEADER]: this.sessionToken } : undefined),
      },
      body: encodeSseRequest(
        { connId: this.connId, streamResponse: true },
        encodeLengthPrefixedFrames(stage.initialFrames, (entry) => entry.frame),
      ),
      signal: abortController.signal,
    })
    // The duplex:'half' POST never resolves while the body stays open. `fetchEndedP`
    // catches its rejection eagerly so it's always handled even if openStream exits early.
    let fetchEndedP: Promise<'fetch-ended'> | undefined
    if (this.streamRequest.tag !== 'failed') {
      const body = createPushReadableStream<Uint8Array<ArrayBuffer>>()
      // Metadata header first — the server classifies the POST by it; `streamRequest: true`
      // makes it emit `reconciled` inline (the body never ends, can't defer to body-end).
      body.push(encodeSseRequestMetadata({ connId: this.connId, streamRequest: true }))
      const fetch = this.openStreamRequest(body, abortController.signal)
      this.streamRequest = { tag: 'active', body, fetch }
      fetchEndedP = (async (): Promise<'fetch-ended'> => {
        try {
          await fetch
        } catch {}
        return 'fetch-ended'
      })()
    }

    const failOpen = (permanent: boolean): void => {
      this.rollbackInitialBatch(stage)
      this.closeStreamRequest()
      abortController.abort()
      this.transportAbort = null
      this.connecting = false
      this.owner._onTransportClosed(this, permanent)
    }

    let response: Response
    try {
      response = await ssePromise
    } catch {
      failOpen(false)
      return
    }

    if (!response.ok || !response.body) {
      failOpen(true)
      return
    }

    const reader = createSseEventStreamReader(
      response.body.getReader() as ReadableStreamDefaultReader<Uint8Array<ArrayBuffer>>,
      abortController,
    )

    // Run the SSE loop concurrently with the handshake wait — frames (including the first
    // RECONCILED) can arrive while the open-ack is still in flight and must be dispatched.
    let resolveHandshakeOk!: () => void
    const handshakeOkP = new Promise<'ok'>((resolve) => {
      resolveHandshakeOk = () => resolve('ok')
    })
    ;(async () => {
      try {
        while (true) {
          const entry = await reader.readNextEntry()
          if (!entry) break
          if (!entry.frame) continue
          if (entry.frame[0] === TAG.STREAM_REQUEST_OPEN_ACK) {
            resolveHandshakeOk()
            continue
          }
          const frame = decode(entry.frame)
          if (frame.tag === TAG.PONG) {
            this.heartbeat?.resetPong()
            continue
          }
          this.owner._onTransportFrame(frame, this, entry.frame.byteLength)
        }
      } catch {
        if (abortController.signal.aborted) return
      } finally {
        reader.cancel()
        // The old SSE reader's death must NOT trample a successor openStream's streamRequest /
        // transportAbort. Only mutate transport state if this controller is still the active one.
        if (this.transportAbort === abortController) {
          this.closeStreamRequest()
          this.transportAbort = null
        }
        // Abandoned controllers are owned by a successor transport — don't notify closed.
        if (!this.abandonedControllers.has(abortController)) this.owner._onTransportClosed(this, false)
      }
    })()

    // Race upstream readiness: ack (ok), fetch ended (dead), or timeout. Non-ok → outbox+batch.
    if (fetchEndedP) {
      const timeoutP = new Promise<'timeout'>((resolve) =>
        setTimeout(() => resolve('timeout'), STREAM_REQUEST_HANDSHAKE_TIMEOUT_MS),
      )
      const result = await Promise.race([handshakeOkP, timeoutP, fetchEndedP])
      if (result !== 'ok') {
        this.closeStreamRequest()
        this.streamRequest = { tag: 'failed' }
      }
    }

    this.connecting = false
    this.owner._onTransportOpen(this)
    if (this.outbox.length > 0) void this.flushOutbox()
  }

  private stageInitialBatch(): SseInitialBatchStage {
    const reconcileBatch = this.owner.stageReconcileBatch(true)
    const initialFrames: OutboundFrame[] = []
    initialFrames.push(reconcileBatch.reconcileFrame)
    const movedBufferedFrames = reconcileBatch.movedBufferedFrames
    const movedOutbox = this.outbox
    this.outbox = []
    // Outbox contains frames carried over from earlier (failed) connect attempts —
    // they have OLDER seqs than whatever was just drained from `sendBuffer`. Sending
    // them first preserves monotonic seq order on the wire; otherwise the server
    // accepts the newer batch first, advances `lastClientSeq`, then dup-drops the
    // older batch (losing user messages sent while offline).
    for (const entry of movedOutbox) initialFrames.push({ kind: 'data', frame: entry.frame })
    for (const frame of movedBufferedFrames) initialFrames.push(frame)
    return { initialFrames, movedOutbox, movedBufferedFrames }
  }

  private rollbackInitialBatch(stage: SseInitialBatchStage): void {
    if (stage.movedOutbox.length === 0 && stage.movedBufferedFrames.length === 0) return
    const now = Date.now()
    const movedBuffered: OutboxEntry[] = stage.movedBufferedFrames.map((entry) => ({
      frame: entry.frame,
      deadline: this.getFrameDeadline(entry.kind, now),
    }))
    this.outbox = stage.movedOutbox.concat(movedBuffered, this.outbox)
  }

  private async flushOutbox(): Promise<void> {
    if (!this.hasWire() || this.flushing || this.outbox.length === 0) return
    assert(this.transportAbort)
    this.flushScheduler.cancel()
    this.flushing = true
    try {
      const now = Date.now()
      const queued = this.outbox.splice(0, this.outbox.length)
      this.lastPostStartedAt = now

      try {
        const response = await this.fetchImpl(getMarkedRequestUrl(this.telefuncUrl, REQUEST_KIND.SSE), {
          method: 'POST',
          headers: {
            ...this.userHeaders,
            'Content-Type': 'application/octet-stream',
            [REQUEST_KIND_HEADER]: REQUEST_KIND.SSE,
            ...(this.sessionToken ? { [TELEFUNC_SESSION_HEADER]: this.sessionToken } : undefined),
          },
          body: encodeSseRequest(
            { connId: this.connId },
            encodeLengthPrefixedFrames(queued, (entry) => entry.frame),
          ),
          signal: this.transportAbort.signal,
        })
        if (!response.ok) throw new Error('POST failed')
      } catch {
        this.outbox = queued.concat(this.outbox)
        this.abandonActiveTransport()
        this.owner._onTransportClosed(this, false)
        return
      }
    } finally {
      this.flushing = false
      if (this.outbox.length > 0) {
        this.scheduleFlush()
      } else {
        const cbs = this.drainCallbacks.splice(0)
        for (const cb of cbs) cb()
      }
    }
  }

  /** Concurrent ping POST while a flush POST is in flight. */
  private schedulePingDuringFlush(frame: OutboundFrame): void {
    const delay = Math.max(0, this.getFrameDeadline(frame.kind) - Date.now())
    setTimeout(() => {
      if (this.flushing) {
        void this.sendStandalonePost([frame.frame])
      } else {
        this.sendFrame(frame)
      }
    }, delay)
  }

  /** One POST that stands outside the outbox/flush cycle: no retry, no re-queue on failure, and no
   *  effect on `flushing`. The heartbeat uses it to slip a ping past an in-flight flush; the barrier
   *  uses it AFTER quiescing, precisely because it is the one send path that cannot be retried into
   *  a post-commit straggler. */
  private async sendStandalonePost(frames: Uint8Array<ArrayBuffer>[]): Promise<void> {
    if (!this.hasWire()) return
    assert(this.transportAbort)
    try {
      await this.fetchImpl(getMarkedRequestUrl(this.telefuncUrl, REQUEST_KIND.SSE), {
        method: 'POST',
        headers: {
          ...this.userHeaders,
          'Content-Type': 'application/octet-stream',
          [REQUEST_KIND_HEADER]: REQUEST_KIND.SSE,
          ...(this.sessionToken ? { [TELEFUNC_SESSION_HEADER]: this.sessionToken } : undefined),
        },
        body: encodeSseRequest({ connId: this.connId }, encodeLengthPrefixedFrames(frames)),
        signal: this.transportAbort.signal,
      })
    } catch {} // best-effort — connection will timeout and reconnect on real failure
  }

  private scheduleFlush(): void {
    if (this.outbox.length === 0 || !this.hasWire()) return
    let earliest = Infinity
    for (const entry of this.outbox) if (entry.deadline < earliest) earliest = entry.deadline
    this.flushScheduler.schedule(earliest)
  }

  private getFrameDeadline(kind: OutboundFrameKind, now = Date.now()): number {
    switch (kind) {
      case 'reconcile':
        return now + SSE_RECONCILE_DEADLINE_MS
      case 'control':
        return now
      case 'heartbeat':
        return now + this.heartbeatFlushDelayMs
      case 'flow-control':
      case 'ack':
      case 'data':
        return (
          now +
          (now - this.lastPostStartedAt >= this.flushThrottleMs ? this.postIdleFlushDelayMs : this.flushThrottleMs)
        )
    }
  }

  abandonActiveTransport(): void {
    const abortController = this.transportAbort
    if (!abortController) return
    this.transportAbort = null
    if (this.streamRequest.tag === 'active') this.streamRequest = { tag: 'idle' }
    this.closeAbandonedTransport()
    this.abandonedStream = abortController
    this.abandonedControllers.add(abortController)
    const cbs = this.drainCallbacks.splice(0)
    for (const cb of cbs) cb()
  }

  closeAbandonedTransport(): void {
    const abortController = this.abandonedStream
    if (!abortController) return
    this.abandonedStream = null
    abortController.abort()
  }

  applyReconciledSettings(ctrl: ReconciledPayload): void {
    if (ctrl.sseFlushThrottle) this.flushThrottleMs = ctrl.sseFlushThrottle
    if (ctrl.ssePostIdleFlushDelay) this.postIdleFlushDelayMs = ctrl.ssePostIdleFlushDelay
    this.heartbeatFlushDelayMs = Math.floor(ctrl.pingInterval / 2)
  }

  sendPing(): void {
    if (!this.hasWire()) return
    this.sendFrame({ kind: 'heartbeat', frame: encode.ping() })
  }

  attachHeartbeat(hb: Heartbeat): void {
    this.heartbeat = hb
  }

  detachHeartbeat(): void {
    this.heartbeat?.stop()
    this.heartbeat = null
  }

  hasHeartbeat(): boolean {
    return this.heartbeat !== null
  }

  dispose(): void {
    this.connecting = false
    if (this.startTimer) {
      clearTimeout(this.startTimer)
      this.startTimer = null
    }
    this.flushScheduler.cancel()
    this.outbox = []
    this.closeStreamRequest()
    this.transportAbort?.abort()
    this.transportAbort = null
    this.closeAbandonedTransport()
    const cbs = this.drainCallbacks.splice(0)
    for (const cb of cbs) cb()
  }

  // ── Persistent client→server stream-request POST (half-duplex streaming body) ──

  /** Half-duplex POST. Resolves on body-end, rejects on fetch error. */
  private openStreamRequest(body: PushReadableStream<Uint8Array<ArrayBuffer>>, signal: AbortSignal): Promise<unknown> {
    return this.fetchImpl(getMarkedRequestUrl(this.telefuncUrl, REQUEST_KIND.SSE), {
      method: 'POST',
      headers: {
        ...this.userHeaders,
        'Content-Type': 'application/octet-stream',
        [REQUEST_KIND_HEADER]: REQUEST_KIND.SSE,
        ...(this.sessionToken ? { [TELEFUNC_SESSION_HEADER]: this.sessionToken } : undefined),
      },
      // `PushReadableStream` IS-A `ReadableStream` — fetch reads it directly,
      // producer's `push(chunk)` lands in the same stream's internal queue via
      // `controller.enqueue`, no async-iterator adapter in between.
      body,
      signal,
      // @ts-ignore duplex is not yet in TypeScript's RequestInit
      duplex: 'half',
    })
  }

  private closeStreamRequest(): void {
    // Only 'active' has a body to close; 'failed' is sticky-terminal so don't regress to 'idle'.
    if (this.streamRequest.tag !== 'active') return
    this.streamRequest.body.close()
    this.streamRequest = { tag: 'idle' }
  }
}

// ── Transport registry ──

/** Maps each ChannelTransport to a factory that creates the corresponding ClientChannelTransport. */
const TRANSPORT_REGISTRY: Record<
  ChannelTransport,
  (telefuncUrl: string, options: ClientConnectionOptions, owner: ClientConnection) => ClientChannelTransport
> = {
  [CHANNEL_TRANSPORT.WS]: (telefuncUrl, _options, owner) => new WsTransport(telefuncUrl, owner),
  [CHANNEL_TRANSPORT.SSE]: (telefuncUrl, options, owner) =>
    new SseTransport(telefuncUrl, options.fetchImpl, options.sessionToken, options.headers, owner),
}

/** Defines which transport can upgrade to which. */
const UPGRADE_PATH: Partial<Record<ChannelTransport, ChannelTransport>> = {
  [CHANNEL_TRANSPORT.SSE]: CHANNEL_TRANSPORT.WS,
}

function createSseEventStreamReader(
  reader: ReadableStreamDefaultReader<Uint8Array<ArrayBuffer>>,
  abortController: AbortController,
): {
  cancel: () => void
  readNextEntry: () => Promise<{ comment?: string; frame?: Uint8Array<ArrayBuffer> } | null>
} {
  const decoder = new TextDecoder()
  // Cursor-based incremental parser. `lineBuf` accumulates decoded text; `cursor` is
  // the offset of the first unparsed byte. We walk it line-by-line via `indexOf('\n')`
  // and queue completed events as we go — no full-buffer splits, no re-joins. The
  // prefix gets trimmed amortised once the consumed region exceeds half the buffer.
  let lineBuf = ''
  let cursor = 0
  let pendingComment: string | null = null
  let pendingData = ''
  const ready: Array<{ comment?: string; frame?: Uint8Array<ArrayBuffer> }> = []
  let cancelled = false

  const cancel = () => {
    if (cancelled) return
    cancelled = true
    reader.cancel().catch(() => {})
  }

  abortController.signal.addEventListener('abort', cancel, { once: true })

  const flushEvent = () => {
    if (pendingComment !== null) {
      ready.push({ comment: pendingComment })
      pendingComment = null
    }
    if (pendingData !== '') {
      ready.push({ frame: base64urlToUint8Array(pendingData) })
      pendingData = ''
    }
  }

  const processBufferedLines = () => {
    while (cursor < lineBuf.length) {
      const nl = lineBuf.indexOf('\n', cursor)
      if (nl === -1) break // incomplete tail line — wait for more bytes
      const line = lineBuf.slice(cursor, nl)
      cursor = nl + 1
      if (line.length === 0) {
        flushEvent()
        continue
      }
      if (line.charCodeAt(0) === 58 /* ':' */) {
        pendingComment = line
        continue
      }
      if (line.startsWith('data: ')) {
        pendingData = line.slice(6)
      }
    }
    // Amortised compaction — discard the consumed prefix once it dominates the buffer.
    if (cursor > 16384 && cursor * 2 >= lineBuf.length) {
      lineBuf = lineBuf.slice(cursor)
      cursor = 0
    }
  }

  const readNextEntry = async (): Promise<{ comment?: string; frame?: Uint8Array<ArrayBuffer> } | null> => {
    while (true) {
      if (ready.length > 0) return ready.shift()!

      let done: boolean
      let value: Uint8Array<ArrayBuffer> | undefined
      let readError: unknown
      try {
        ;({ done, value } = await reader.read())
      } catch (err) {
        readError = err
        done = true
      }
      if (done) {
        if (abortController.signal.aborted || cancelled) return null
        throw readError ?? new Error('Connection lost before all SSE frames were received.')
      }
      lineBuf += decoder.decode(value!, { stream: true })
      processBufferedLines()
    }
  }

  return { cancel, readNextEntry }
}
