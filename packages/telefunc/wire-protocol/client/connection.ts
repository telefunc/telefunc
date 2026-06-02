export { ClientConnection }
export type { MuxChannel, MuxConnection }

import { parse } from '@brillout/json-serializer/parse'
import { makeAbortError, makeBugError } from '../../client/remoteTelefunctionCall/errors.js'
import { assert } from '../../utils/assert.js'
import { ChannelClosedError, ChannelNetworkError } from '../channel-errors.js'
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
  SSE_FLUSH_THROTTLE_MS,
  SSE_POST_IDLE_FLUSH_DELAY_MS,
  SSE_RECONCILE_DEADLINE_MS,
  STREAM_REQUEST_HANDSHAKE_TIMEOUT_MS,
  TELEFUNC_SESSION_HEADER,
  UPGRADE_FIN_RECONCILED_TIMEOUT_MS,
  UPGRADE_DRAIN_TIMEOUT_MS,
  WS_PROBE_TIMEOUT_MS,
  type ChannelTransport,
  type ChannelTransports,
} from '../constants.js'
import { encodeU32, encodeLengthPrefixedFrames, textEncoder } from '../frame.js'
import { createPushReadableStream, type PushReadableStream } from '../push-readable-stream.js'
import { ReplayBuffer } from '../replay-buffer.js'
import { REQUEST_KIND, REQUEST_KIND_HEADER, getMarkedRequestUrl } from '../request-kind.js'
import { ACK_STATUS, TAG, decode, encode, isChannelDataFrame, payloadBytes } from '../shared-ws.js'
import type { AckResultStatus, ChannelFrame, DecodedFrame, ReconcilePayload, ReconciledPayload } from '../shared-ws.js'
import { encodeSseRequest, METADATA_REFRESH_ALIAS, type SseRouteChannel } from '../sse-request.js'
import { DeadlineScheduler } from './deadlineScheduler.js'

type BufferedFrame = {
  frame: Uint8Array<ArrayBuffer>
  channelIx: number
  seq?: number
}

/** Probe wire returned by `WsTransport.probe`. Liveness is the consumer's responsibility
 *  until the swap commits — typically driven via a transient Heartbeat. */
type ProbeWire = {
  ping: () => void
  onPong: (cb: () => void) => void
  onClose: (cb: () => void) => void
  close: () => void
}

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
  /** Cluster-stable wire id used by the server to route cross-instance — set on SSE, `null` on WS. */
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
  /** Emit post-reconcile routing on the persistent upstream stream. No-op on WS. */
  pushReconciledRouting(): void
  /** Phase 1 of upgrade drain — gate still down, user sends keep flowing. Returns when the
   *  wire is naturally empty or `timeoutMs` elapses, whichever comes first. */
  gracefulDrain(timeoutMs: number): Promise<void>
  /** Phase 2 of upgrade drain — caller has gated user sends. After this resolves, every
   *  frame the client pushed on this transport has been server-acknowledged. */
  forceDrain(): Promise<void>
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
  | { tag: 'draining'; attempt: AbortController }
  | {
      tag: 'handoff'
      from: ClientChannelTransport
      buffer: DecodedFrame[]
      finReceived: boolean
      finTimer: ReturnType<typeof setTimeout> | null
    }

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
  /** RECONCILE sent, awaiting RECONCILED. SSE sets it true during connecting since the
   *  initial reconcile is baked into the openStream POST body. */
  private reconciling = false
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

  private enterUpgradeDraining(attempt: AbortController): void {
    assert(this.state.tag === 'open' && this.state.upgrade.tag === 'probing' && this.state.upgrade.attempt === attempt)
    this.state = { tag: 'open', upgrade: { tag: 'draining', attempt } }
  }

  private enterUpgradeHandoff(from: ClientChannelTransport): void {
    assert(this.state.tag === 'open' && this.state.upgrade.tag === 'draining')
    this.state = {
      tag: 'open',
      upgrade: { tag: 'handoff', from, buffer: [], finReceived: false, finTimer: null },
    }
  }

  /** Idempotent: no-op if `attempt` is already cleared, replaced, or committed to handoff. */
  private exitUpgradeAttempt(attempt: AbortController): void {
    if (this.state.tag !== 'open') return
    const u = this.state.upgrade
    if (u.tag !== 'probing' && u.tag !== 'draining') return
    if (u.attempt !== attempt) return
    this.state = { tag: 'open', upgrade: { tag: 'none' } }
  }

  private exitUpgradeHandoff(): {
    from: ClientChannelTransport
    buffer: DecodedFrame[]
    finTimer: ReturnType<typeof setTimeout> | null
  } {
    assert(this.state.tag === 'open' && this.state.upgrade.tag === 'handoff')
    const { from, buffer, finTimer } = this.state.upgrade
    this.state = { tag: 'open', upgrade: { tag: 'none' } }
    return { from, buffer, finTimer }
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

  /** Send the queued RECONCILE on the live wire. No-op when nothing's queued. */
  private flushPendingRegisterReconcile(): void {
    if (this.registerReconcileTimer === null) return
    this.cancelRegisterReconcileTimer()
    if (this.connected && !this.reconciling) {
      this.sendReconcileBatch(this.stageReconcileBatch())
    }
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
    this.enterOpen()
    if (this.transport.sendReconcileOnOpen) {
      this.sendReconcileBatch(this.stageReconcileBatch())
      return
    }
    if (!this.reconciling) {
      const drained = this.drainBufferedFrames(this.channels, undefined)
      for (const frame of drained) this.transport.sendFrame(frame)
    }
  }

  _onTransportFrame(frame: DecodedFrame): void {
    if (this.state.tag === 'open' && this.state.upgrade.tag === 'handoff') {
      this.bufferFrameDuringHandoff(frame)
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

  private bufferFrameDuringHandoff(frame: DecodedFrame): void {
    switch (frame.tag) {
      case TAG.FIN:
        this.handleHandoffFin()
        return
      case TAG.RECONCILED:
        this.handleReconciled(frame.payload)
        return
    }
    assert(this.state.tag === 'open' && this.state.upgrade.tag === 'handoff')
    this.state.upgrade.buffer.push(frame)
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

  /** Funnel for pong-timeouts. Suppress while reconciling — pings are delayed by the round-trip. */
  private handlePongTimeout(transport: ClientChannelTransport): void {
    if (this.reconciling) return
    transport.detachHeartbeat()
    transport.abandonActiveTransport()
    this._onTransportClosed(transport, false)
  }

  private handleHandoffFin(): void {
    if (this.state.tag !== 'open' || this.state.upgrade.tag !== 'handoff') return
    this.state.upgrade.finReceived = true
    // Bound the wait for RECONCILED — abort and reconnect if it never shows.
    if (this.reconciling && !this.state.upgrade.finTimer) {
      this.state.upgrade.finTimer = setTimeout(() => {
        if (this.state.tag === 'open' && this.state.upgrade.tag === 'handoff') {
          this.state.upgrade.finTimer = null
        }
        this.abortUpgradeAndReconnectSse(new ChannelNetworkError('Upgrade FIN without RECONCILED'))
      }, UPGRADE_FIN_RECONCILED_TIMEOUT_MS)
    }
    this.tryCompleteUpgradeHandoff()
  }

  /** Handoff commits only after BOTH FIN (old wire) and RECONCILED (new wire) — they may reorder. */
  private tryCompleteUpgradeHandoff(): void {
    if (this.state.tag !== 'open' || this.state.upgrade.tag !== 'handoff') return
    if (!this.state.upgrade.finReceived || this.reconciling) return
    const { from, buffer, finTimer } = this.exitUpgradeHandoff()
    if (finTimer) clearTimeout(finTimer)
    from.detachHeartbeat()
    from.abandonActiveTransport()
    from.dispose()
    for (const frame of buffer) this.dispatchFrame(frame)
  }

  _onTransportClosed(transport: ClientChannelTransport, rejectedInitial = false): void {
    if (this.closed) return
    transport.detachHeartbeat()
    if (transport !== this.transport) {
      if (this.state.tag === 'open' && this.state.upgrade.tag === 'handoff' && transport === this.state.upgrade.from) {
        this.abortUpgradeAndReconnectSse(new ChannelNetworkError('Connection dropped'))
      }
      return
    }
    if (this.state.tag === 'open' && this.state.upgrade.tag !== 'none' && this.state.upgrade.tag !== 'handoff') {
      this.state.upgrade.attempt.abort()
    }
    const err = new ChannelNetworkError(
      rejectedInitial
        ? `Server rejected ${this.transport.type === CHANNEL_TRANSPORT.SSE ? 'SSE' : 'WebSocket'} connection`
        : 'Connection dropped',
    )
    this.handleTransportLoss(err, rejectedInitial)
  }

  private handleReconciled(ctrl: ReconciledPayload): void {
    this.transport.applyReconciledSettings(ctrl)
    this.transport.pushReconciledRouting()
    const outcome = this.applyReconciled(ctrl)
    this.installHeartbeat(this.transport, ctrl.pingInterval)
    this.transport.closeAbandonedTransport()
    for (const frame of outcome.frames) this.transport.sendFrame(frame)
    for (const channel of outcome.channelsToOpen) channel._onTransportOpen(this.transport.batched)
    if (outcome.reconcileComplete) {
      this.startTtlIfIdle()
      this.maybeStartUpgrade(ctrl)
    }
    this.tryCompleteUpgradeHandoff()
  }

  // ── SSE→WS upgrade ──

  private maybeStartUpgrade(ctrl: ReconciledPayload): void {
    if (this.upgradeDisabled) return
    if (this.state.tag !== 'open' || this.state.upgrade.tag !== 'none') return
    const nextTransport = UPGRADE_PATH[this.transport.type]
    if (!nextTransport) return
    if (!this.isTransportUpgradeAllowed(nextTransport)) return
    if (!ctrl.transports.includes(nextTransport)) return
    void this.probeAndUpgrade(nextTransport)
  }

  /** Tear down upgrade state, fall back to a fresh SSE, and disable upgrades for this connection. */
  private abortUpgradeAndReconnectSse(err: Error): void {
    if (this.closed) return
    if (this.state.tag === 'open') {
      const u = this.state.upgrade
      if (u.tag === 'probing' || u.tag === 'draining') {
        u.attempt.abort()
        this.exitUpgradeAttempt(u.attempt)
      } else if (u.tag === 'handoff') {
        const { from, finTimer } = this.exitUpgradeHandoff()
        if (finTimer) clearTimeout(finTimer)
        from.detachHeartbeat()
        from.abandonActiveTransport()
        from.dispose()
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

  private async probeAndUpgrade(targetTransport: ChannelTransport): Promise<void> {
    // Flush pending register-reconcile inline before the upgrade so its RECONCILE
    // doesn't fire mid-drain on the dying old wire. The freshly-registered channels
    // either go on the old wire now (entering the upgrade drain naturally) or have
    // their deferred releases settled before the handoff RECONCILE is built.
    this.flushPendingRegisterReconcile()
    const attempt = new AbortController()
    this.enterUpgradeProbing(attempt)
    try {
      const from = this.transport
      const to = TRANSPORT_REGISTRY[targetTransport](this.telefuncUrl, this.connectionOptions, this)

      const probe = await to.probe()
      if (attempt.signal.aborted || !probe) {
        probe?.close()
        return
      }
      const probeHeartbeat = new Heartbeat(
        this.pingIntervalMs,
        this.pingIntervalMs * 2,
        () => probe.ping(),
        () => attempt.abort(),
      )
      probe.onPong(() => probeHeartbeat.resetPong())
      probe.onClose(() => attempt.abort())
      probeHeartbeat.start()
      attempt.signal.addEventListener(
        'abort',
        () => {
          probeHeartbeat.stop()
          probe.close()
        },
        { once: true },
      )

      if (!(await this.drainOldWire(from, attempt))) {
        this.exitUpgradeAttempt(attempt)
        const drained = this.drainBufferedFrames(this.channels, undefined)
        for (const frame of drained) this.transport.sendFrame(frame)
        return
      }

      probeHeartbeat.stop()
      this.transport = to
      this.enterUpgradeHandoff(from)
      to.start()
    } finally {
      this.exitUpgradeAttempt(attempt)
    }
  }

  /** Two-phase drain. Returns false if the probe aborted; caller rolls back state. */
  private async drainOldWire(from: ClientChannelTransport, attempt: AbortController): Promise<boolean> {
    await from.gracefulDrain(UPGRADE_DRAIN_TIMEOUT_MS)
    if (attempt.signal.aborted) return false
    this.enterUpgradeDraining(attempt)
    await from.forceDrain()
    return !attempt.signal.aborted
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
    this.reconciling = false
    this.reconcileIxes.clear()
    if (this.ttl) {
      clearTimeout(this.ttl)
      this.ttl = null
    }

    const { attempt: prevAttempt, startedAt: prevStartedAt } =
      this.state.tag === 'reconnecting' ? this.state : { attempt: 0, startedAt: 0 }

    if (rejected && prevAttempt === 0) {
      this.closeAll(err instanceof Error ? err : new ChannelNetworkError('Connection dropped'))
      this.dispose()
      return
    }
    if (this.channels.size === 0) {
      this.dispose()
      return
    }
    const startedAt = prevStartedAt || Date.now()
    if (Date.now() - startedAt > this.reconnectTimeoutMs) {
      this.closeAll(err instanceof Error ? err : new ChannelNetworkError('Connection dropped'))
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
      if (u.tag === 'probing' || u.tag === 'draining') u.attempt.abort()
      if (u.tag === 'handoff') {
        if (u.finTimer) clearTimeout(u.finTimer)
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
    this.reconciling = false
    ClientConnection.cache.delete(this.cacheKey)
  }

  // ── Protocol internals ──

  buildReconcileFrame(): OutboundFrame {
    this.reconciling = true
    this.reconcileIxes = new Set()
    const open: ReconcilePayload['open'] = []
    for (const [ix, entry] of this.channels) {
      this.reconcileIxes.add(ix)
      const payloadEntry: ReconcilePayload['open'][number] = {
        id: entry.channel.id,
        ix,
        lastSeq: this.lastSeqByChannel.get(ix) ?? 0,
      }
      if (entry.state.tag !== 'open' && entry.state.initial) payloadEntry.initial = true
      open.push(payloadEntry)
    }
    const reconcile: ReconcilePayload = { open }
    if (this.sessionId) reconcile.sessionId = this.sessionId
    if (this.state.tag === 'open' && this.state.upgrade.tag === 'handoff') {
      reconcile.upgrade = true
      // Lets a non-owner cluster instance route the reconcile back. Null on WS (no stable id).
      const handoffConnId = this.state.upgrade.from.connId
      if (handoffConnId !== null) reconcile.prevConnId = handoffConnId
    }
    return { kind: 'reconcile', frame: encode.reconcile(reconcile) }
  }

  drainBufferedFramesForReconcile(): OutboundFrame[] {
    if (this.transport.reconcileMode !== 'batch-on-reconcile') return []
    return this.drainBufferedFrames(this.channels, undefined)
  }

  stageReconcileBatch(): ReconcileBatch {
    const reconcileFrame = this.buildReconcileFrame()
    const movedBufferedFrames = this.drainBufferedFramesForReconcile()
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

  private applyReconciled(ctrl: ReconciledPayload): ReconcileOutcome {
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
        const err = new ChannelNetworkError('Channel not acknowledged by server after reconnect')
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
      this.reconciling = false
    }

    return { frames: releaseFrames, channelsToOpen, reconcileComplete: !hasNewChannels }
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
    ws.onmessage = ({ data }: MessageEvent) => {
      const frame = decode(new Uint8Array(data as ArrayBuffer))
      if (frame.tag === TAG.PONG) onPong?.()
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
      close: () => {
        if (this.probedWs === ws) this.probedWs = null
        try {
          ws.close()
        } catch {}
      },
    }
  }

  gracefulDrain(): Promise<void> {
    return Promise.resolve()
  }

  forceDrain(): Promise<void> {
    return Promise.resolve()
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
      this.owner._onTransportFrame(frame)
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
  pushReconciledRouting(): void {}

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
        metadataPushed: boolean
      }
    | { tag: 'failed' } = { tag: 'idle' }
  // Routing table from `applyReconciledSettings`: alias N = `channels[N − 1]`, alias 0 = ownerInstance.
  private ownerInstance = ''
  private channels: SseRouteChannel[] = []
  private ixToAlias = new Map<number, number>()

  constructor(
    private readonly telefuncUrl: string,
    private readonly fetchImpl: typeof fetch,
    private readonly sessionToken: string | undefined,
    private readonly userHeaders: Record<string, string> | undefined,
    private readonly owner: ClientConnection,
  ) {}

  /** Phase 1: gate down. Wait for natural drain or `timeoutMs`, whichever first. */
  async gracefulDrain(timeoutMs: number): Promise<void> {
    if (this.streamRequest.tag === 'active') return
    if (!this.flushing && this.outbox.length === 0) return
    const drained = new Promise<void>((resolve) => this.drainCallbacks.push(resolve))
    await Promise.race([drained, new Promise<void>((resolve) => setTimeout(resolve, timeoutMs))])
  }

  /** Phase 2: gate up. Closes streamRequest body or awaits outbox drain. */
  async forceDrain(): Promise<void> {
    if (this.streamRequest.tag === 'active') {
      assert(!this.flushing && this.outbox.length === 0)
      const fetch = this.streamRequest.fetch
      this.closeStreamRequest()
      try {
        await fetch
      } catch {}
      return
    }
    if (!this.flushing && this.outbox.length === 0) return
    await new Promise<void>((resolve) => this.drainCallbacks.push(resolve))
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
      const aliased = this.aliasPrepend(frame.frame)
      this.streamRequest.body.push(encodeU32(aliased.byteLength))
      this.streamRequest.body.push(aliased)
      return
    }
    const now = Date.now()
    const deadline = this.getFrameDeadline(frame.kind, now)
    this.outbox.push({ frame: frame.frame, deadline })
    this.scheduleFlush()
    if (deadline <= now) void this.flushOutbox()
  }

  /** Routing alias for a frame: 0 = owner; N ≥ 1 = `channels[N − 1]`. Unknown ix → 0. */
  private aliasFor(frame: Uint8Array): number {
    const decoded = decode(frame)
    return 'index' in decoded ? (this.ixToAlias.get(decoded.index) ?? 0) : 0
  }

  private aliasPrepend(frame: Uint8Array<ArrayBuffer>): Uint8Array<ArrayBuffer> {
    const out = new Uint8Array(1 + frame.byteLength)
    out[0] = this.aliasFor(frame)
    out.set(frame, 1)
    return out
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
      body: encodeSseRequest({
        connId: this.connId,
        streamResponse: true,
        batch: encodeLengthPrefixedFrames(stage.initialFrames, (entry) => entry.frame),
      }),
      signal: abortController.signal,
    })
    // The duplex:'half' POST never resolves while the body stays open. `fetchEndedP`
    // catches its rejection eagerly so it's always handled even if openStream exits early.
    let fetchEndedP: Promise<'fetch-ended'> | undefined
    if (this.streamRequest.tag !== 'failed') {
      const body = createPushReadableStream<Uint8Array<ArrayBuffer>>()
      const fetch = this.openStreamRequest(body, abortController.signal)
      this.streamRequest = { tag: 'active', body, fetch, metadataPushed: false }
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

    const reader = createSseEventStreamReader(response.body.getReader(), abortController)

    // Run the SSE loop concurrently with the handshake wait — RECONCILED arriving during
    // the wait must still be dispatched (its `pushReconciledRouting` is what unblocks the ack).
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
          this.owner._onTransportFrame(frame)
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
    const reconcileBatch = this.owner.stageReconcileBatch()
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
          body: encodeSseRequest({
            connId: this.connId,
            ownerInstance: this.ownerInstance,
            channels: this.channels,
            batch: encodeLengthPrefixedFrames(queued.map((entry) => this.aliasPrepend(entry.frame))),
          }),
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
        void this.sendConcurrentPost([frame.frame])
      } else {
        this.sendFrame(frame)
      }
    }, delay)
  }

  private async sendConcurrentPost(frames: Uint8Array<ArrayBuffer>[]): Promise<void> {
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
        body: encodeSseRequest({
          connId: this.connId,
          ownerInstance: this.ownerInstance,
          channels: this.channels,
          batch: encodeLengthPrefixedFrames(frames.map((f) => this.aliasPrepend(f))),
        }),
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
    this.ownerInstance = ctrl.ownerInstance
    this.channels = ctrl.open.map((entry) => ({ id: entry.id, home: entry.home }))
    this.ixToAlias = new Map(ctrl.open.map((entry, i) => [entry.ix, i + 1]))
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

  /** Push the post-reconcile routing table onto the live stream-request body. */
  pushReconciledRouting(): void {
    const sr = this.streamRequest
    if (sr.tag !== 'active') return
    if (sr.metadataPushed) {
      const refresh = textEncoder.encode(JSON.stringify(this.channels))
      const entry = new Uint8Array(1 + refresh.byteLength)
      entry[0] = METADATA_REFRESH_ALIAS
      entry.set(refresh, 1)
      sr.body.push(encodeU32(entry.byteLength))
      sr.body.push(entry)
      return
    }
    // `streamRequest: true` → server emits `reconciled` inline (the body never ends).
    const meta = textEncoder.encode(
      JSON.stringify({
        connId: this.connId,
        ownerInstance: this.ownerInstance,
        channels: this.channels,
        streamRequest: true,
      }),
    )
    sr.body.push(encodeU32(meta.byteLength))
    sr.body.push(meta)
    sr.metadataPushed = true
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
