export { ClientConnection }
export type { ClientChannelTransport, MuxChannel, MuxConnection, OutboundFrame, OutboundFrameKind, TransportOwner }

import { parse } from '@brillout/json-serializer/parse'
import { makeAbortError, makeBugError } from '../../../client/remoteTelefunctionCall/errors.js'
import { assert, assertUsage } from '../../../utils/assert.js'
import { ChannelOverflowError, replayLossError } from '../channelErrors.js'
import { NetworkError } from '../../../shared/NetworkError.js'
import {
  CHANNEL_CLIENT_REPLAY_BUFFER_BYTES,
  CHANNEL_CLIENT_REPLAY_BUFFER_BINARY_BYTES,
  CHANNEL_IDLE_TIMEOUT_MS,
  CHANNEL_RECONNECT_INITIAL_DELAY_MS,
  CHANNEL_RECONNECT_MAX_DELAY_MS,
  CHANNEL_RECONNECT_TIMEOUT_MS,
  CHANNEL_TRANSPORT,
  MAX_CHANNELS_PER_CONNECTION,
  type ChannelTransport,
  type ChannelTransports,
  TIMER_DELAY_MAX_MS,
  WIRE_MAX_RAW_FRAME_BYTES,
} from '../../constants.js'
import { replayWindow, type Urgency } from '../../flowControl/credit.js'
import { ReplayBuffer } from '../ReplayBuffer.js'
import { ACK_STATUS, ERROR_REASON, encode, isSequencedFrame } from '../../wire/frame.js'
import { TAG } from '../../wire/tags.js'
import { seqNear } from '../../wire/seq.js'
import type {
  AckResultStatus,
  BdpPingAck,
  ChannelFrame,
  DecodedFrame,
  PongEntry,
  SequencedEncoder,
} from '../../wire/frame.js'
import type { ReattachState, ReconciledPayload } from '../../wire/payloads.js'
import type { Heartbeat } from './Heartbeat.js'
import { ChannelTable, type ChannelRecord } from './ChannelTable.js'
import { SendGate, type SendGateOwner } from './SendGate.js'
import { Liveness, type LivenessOwner } from './Liveness.js'
import { Upgrade, type UpgradeOwner, type UpgradeTarget } from './Upgrade.js'
import { AttachExchange, type AttachExchangeOwner, type ReconcileBatch } from './AttachExchange.js'
import { pingDeadline } from '../../wire/pingDeadline.js'
import type { Uplink } from '../../flowControl/client/Uplink.js'
import type { Downlink } from '../../flowControl/client/Downlink.js'
import { WsTransport } from './ws.js'
import { SseTransport } from './sse.js'

type OutboundFrameKind = 'reconcile' | 'released' | 'control' | Urgency | 'ack' | 'data' | 'heartbeat'

type OutboundFrame = {
  kind: OutboundFrameKind
  frame: Uint8Array<ArrayBuffer>
}

interface MuxChannel {
  readonly id: string
  readonly isClosed: boolean
  /** The largest frame its server takes on a WebSocket. */
  readonly _maxFrameBytes: number
  /** It opened on `uplink`'s wire: the one of the channel's last attach means that same wire, which lost nothing. */
  _onTransportOpen(batched: boolean, uplink: Uplink, downlink: Downlink): void
  /** Its frames go in batch POSTs from now on: the wire's streaming upload failed after the channel opened. */
  _onTransportBatched(): void
  /** The upgrade joined: it is on the WebSocket, and was handed at once the frames the join held. */
  _onUpgradeJoined(): void
  /** Entry point for every per-channel wire frame (data + per-channel ctrl). The
   *  channel splits ctrl vs data internally. Connection-level frames (PING/PONG/
   *  FIN/RECONCILED), channel-termination ctrls (ABORT/ERROR) and ATTACH_RESULT stay with the
   *  connection — they involve connection-side cleanup. */
  _dispatchFrame(frame: ChannelFrame): void
  _onEnded(err?: Error): void
  /** Its RECONCILE entry on each (re)attach to `uplink`'s wire. */
  _reattachState(uplink: Uplink): ReattachState
  /** Its RECONCILE entry's `probe` was answered by the RECONCILED. */
  _onProbeAnswered(probe: number): void
  /** The largest windows the replay buffers allow: the one the page grants, and the one the server grants it. */
  _fitReplays(window: number, peerWindow: number): void
  /** At each heartbeat, a WINDOW for what arrived since the last: the server's replay lets it go while quiet. */
  _acknowledge(): void
  /** It answered the newest frame the server sent it, which acknowledges every frame before it too. */
  _answered(): void
}

interface MuxConnection {
  sendSequenced<T>(
    channel: MuxChannel,
    kind: 'data' | 'ack',
    encodeFrame: SequencedEncoder<T>,
    data: T,
    onQueued?: (seq: number) => void,
  ): number
  sendAckRes(channel: MuxChannel, ackedSeq: number, result: string, status?: AckResultStatus): void
  sendAbort(channel: MuxChannel, abortValue: string): void
  sendCloseRequest(channel: MuxChannel, timeoutMs: number): void
  sendCloseAck(channel: MuxChannel): void
  sendByteWindowUpdate(channel: MuxChannel, limit: number, urgency: Urgency): void
  sendMsgWindowUpdate(channel: MuxChannel, limit: number, urgency: Urgency): void
  sendBdpPing(channel: MuxChannel, probe: number): void
  sendBdpPingAck(channel: MuxChannel, ack: BdpPingAck): void
  /** Bytes the wire holds that haven't gone out. */
  bufferedAmount(): number
  /** The bytes the wire delivered in the `ms` before it last delivered: before a frame handled as it arrives. */
  deliveredBeforeLast(ms: number): number
  sendBroadcastSubscribe(channel: MuxChannel, binary: boolean): void
  sendBroadcastUnsubscribe(channel: MuxChannel, binary: boolean): void
  unregister(channel: MuxChannel): void
  reconnectWindow(): number
}

type ClientConnectionOptions = {
  transports: ChannelTransports
  fetchImpl: typeof fetch
  /** Client-side cache-key extension — distinct values get distinct `ClientConnection` instances. Never sent on the wire. */
  connectionKey?: string
  /** User headers (config.headers + per-call `withContext({ headers })`) merged into every transport fetch: the first
   *  channel's, for every channel the connection carries. */
  headers?: Record<string, string>
  /** Override the idle-close delay after all channels close. Default: the server's `idleTimeout` once a RECONCILED says
   *  it, 60 000 ms before. Pass 0 to dispose immediately. */
  idleTimeout?: number
}

type ClientChannelTransport = {
  readonly type: ChannelTransport
  /** The request that opens its wire carries the RECONCILE and the frames buffered behind it; else the RECONCILE goes
   *  once the wire is open, and the buffered frames after its RECONCILED. */
  readonly carriesReconcileInOpen: boolean
  /** True iff client→server frames are per-POST batched instead of pushed onto
   *  one streaming body — signals the channel to use a larger initial window. */
  readonly batched: boolean
  /** The uplink and the downlink of the wire it carries, or opens next. */
  readonly uplink: Uplink
  readonly downlink: Downlink
  start(): void
  hasWire(): boolean
  isConnecting(): boolean
  /** Send a connection-level ping on this wire. Heartbeat's send callback calls this. */
  sendPing(frame: Uint8Array<ArrayBuffer>): void
  sendFrame(frame: OutboundFrame): void
  /** The bytes of data frames it takes now, `Infinity` where nothing bounds them: the others go at once. */
  headroom(): number
  /** Bytes of the frames it was handed that haven't gone out to the network. */
  bufferedAmount(): number
  abandonActiveTransport(): void
  closeAbandonedTransport(): void
  applyReconciledSettings(ctrl: ReconciledPayload): void
  /** Its wire's, which tracks what the wire delivers. */
  readonly heartbeat: Heartbeat | null
  /** Liveness constructs the Heartbeat and hands it over; the transport's receive path tells it of each PONG. */
  attachHeartbeat(hb: Heartbeat): void
  detachHeartbeat(): void
  /** Settles once the frames it was handed have left it, or its wire is gone. */
  drained(): Promise<void>
  dispose(): void
}

/** What a transport tells its connection, and the reconcile batch it asks for when its wire opens. */
interface TransportOwner {
  onTransportOpen(transport: ClientChannelTransport): void
  onTransportClosed(transport: ClientChannelTransport, options?: { rejectedByServer?: boolean }): void
  onTransportFrame(frame: DecodedFrame, source: ClientChannelTransport, byteLength: number, abandoned: boolean): void
  onTransportPong(ended: PongEntry[]): void
  /** The transport may have headroom again: what waited for it goes. */
  onTransportHeadroom(transport: ClientChannelTransport): void
  /** The wire's upload fell back to batch POSTs, which a channel may have opened before (see `onTransportOpen`). */
  onTransportBatched(transport: ClientChannelTransport): void
  /** An SSE wire's RECONCILE went out with the request that opens it, before the wire opens (see `beatFromReconcile`). */
  onTransportReconcileSent(transport: ClientChannelTransport): void
  /** A batch POST has been out longer than its upload takes at the slowest rate a wire is held to. */
  onTransportPostLate(transport: ClientChannelTransport): void
  /** What a transport's wire carries as it opens: the RECONCILE, and the frames buffered behind it. */
  stageReconcileBatch(isInitialBatch: boolean): ReconcileBatch
}

type ConnectionState =
  | { tag: 'fresh' }
  | { tag: 'open' }
  | {
      tag: 'reconnecting'
      attempt: number
      startedAt: number
      timer: ReturnType<typeof setTimeout>
    }
  | { tag: 'closed' }

class ClientConnection
  implements MuxConnection, TransportOwner, SendGateOwner, LivenessOwner, UpgradeOwner, AttachExchangeOwner
{
  private static cache = new Map<string, ClientConnection>()

  static getOrCreate(telefuncUrl: string, channel: MuxChannel, options: ClientConnectionOptions): ClientConnection {
    // `connectionKey` opts callers out of the shared connection without the server seeing it.
    const key = `${options.transports.join(',')}:${telefuncUrl}|${options.connectionKey ?? ''}`
    let connection = ClientConnection.cache.get(key)
    // Wire indexes are u16 and never reused, so a new channel starts a fresh connection once they run out.
    if (!connection || connection.closed || connection.nextIndex > 0xffff) {
      connection = new ClientConnection(telefuncUrl, options, key)
      ClientConnection.cache.set(key, connection)
    }
    connection.register(channel)
    return connection
  }

  private readonly cacheKey: string
  private readonly telefuncUrl: string
  private readonly connectionOptions: ClientConnectionOptions
  /** Its transports include a WebSocket, whose runtime may take smaller frames than the wire's limit. */
  private readonly usesWebSocket: boolean
  transport: ClientChannelTransport

  private state: ConnectionState = { tag: 'fresh' }
  private ttl: ReturnType<typeof setTimeout> | null = null
  get closed(): boolean {
    return this.state.tag === 'closed'
  }
  get connected(): boolean {
    return this.state.tag === 'open'
  }

  /** Advances each time the connection moves to another wire: what went out on the one before may not have arrived. */
  wire = 0
  private nextIndex = 0
  private readonly channels = new ChannelTable()
  private readonly gate = new SendGate({ owner: this, channels: this.channels })
  private readonly liveness = new Liveness({ owner: this, channels: this.channels, gate: this.gate })
  private readonly exchange = new AttachExchange({ owner: this, channels: this.channels, gate: this.gate })
  private readonly upgrade: Upgrade
  private reconnectTimeoutMs = CHANNEL_RECONNECT_TIMEOUT_MS
  private idleTimeoutMs: number
  private clientReplayBufferBytes = CHANNEL_CLIENT_REPLAY_BUFFER_BYTES
  private clientReplayBufferBinaryBytes = CHANNEL_CLIENT_REPLAY_BUFFER_BINARY_BYTES
  /** The windows the replay buffers allow, the page's and the server's, as the last RECONCILED says them. */
  private replayWindows: { window: number; peerWindow: number } | null = null
  /** The largest frame the server takes on a WebSocket, as the last RECONCILED says it. */
  private maxFrameBytes = WIRE_MAX_RAW_FRAME_BYTES
  private constructor(telefuncUrl: string, options: ClientConnectionOptions, cacheKey: string) {
    this.cacheKey = cacheKey
    this.telefuncUrl = telefuncUrl
    this.connectionOptions = options
    this.usesWebSocket = options.transports.includes(CHANNEL_TRANSPORT.WS)
    this.upgrade = new Upgrade({
      owner: this,
      channels: this.channels,
      liveness: this.liveness,
      exchange: this.exchange,
      allowsWebSocket: this.usesWebSocket,
    })
    this.idleTimeoutMs = options.idleTimeout ?? CHANNEL_IDLE_TIMEOUT_MS
    this.transport = TRANSPORT_REGISTRY[options.transports[0]!](telefuncUrl, options, this)
  }

  // ── State transitions: every `this.state =` write goes through these. ──

  private enterOpen(): void {
    if (this.state.tag === 'open') return
    this.state = { tag: 'open' }
  }

  /** Owns the reconnect timer's lifecycle so callers can't forget to cancel a prior one. */
  private enterReconnecting(attempt: number, startedAt: number, delay: number): void {
    if (this.state.tag === 'reconnecting') clearTimeout(this.state.timer)
    const timer = setTimeout(() => this.transport.start(), delay)
    this.state = { tag: 'reconnecting', attempt, startedAt, timer }
    this.upgrade.forget()
  }

  private enterClosed(): void {
    this.state = { tag: 'closed' }
    this.upgrade.forget()
  }

  get sendsOpen(): boolean {
    return this.connected && !this.exchange.reconciling && !this.upgrade.gatesSends && !this.exchange.reconcileOwed
  }

  get sessionId(): string | null {
    return this.exchange.sessionId
  }

  get reconcileHeld(): boolean {
    return !this.connected || this.upgrade.committing !== null || this.upgrade.ready
  }

  get upgradeReady(): boolean {
    return this.upgrade.ready
  }

  private register(channel: MuxChannel): void {
    // A new wire's RECONCILE lists every channel, the closed ones too, so the oldest the server has all of makes way.
    if (this.channels.size === MAX_CHANNELS_PER_CONNECTION) {
      for (const record of this.channels.values()) {
        if (record.state.tag !== 'closed' || !record.state.delivered) continue
        this.releaseChannel(record)
        break
      }
    }
    assertUsage(
      this.channels.size < MAX_CHANNELS_PER_CONNECTION,
      `Too many channels on one connection (${MAX_CHANNELS_PER_CONNECTION} max) — open another with \`connectionKey\``,
    )
    if (this.ttl) {
      clearTimeout(this.ttl)
      this.ttl = null
    }
    const replay = new ReplayBuffer({
      maxBytes: this.clientReplayBufferBytes,
      binaryMaxBytes: this.clientReplayBufferBinaryBytes,
    })
    this.channels.add(this.nextIndex++, channel, replay)
    this.fitReplays(channel)

    if (!this.transport.hasWire() && !this.transport.isConnecting()) {
      this.transport.start()
      return
    }
    this.exchange.scheduleRegisterReconcile()
  }

  /** Fits the replays' windows once a RECONCILED says them; till then the defaults' initial window fits any budget. */
  private fitReplays(channel: MuxChannel): void {
    if (this.replayWindows === null) return
    channel._fitReplays(this.replayWindows.window, this.replayWindows.peerWindow)
  }

  onTransportHeadroom(transport: ClientChannelTransport): void {
    if (transport !== this.transport || !this.sendsOpen) return
    this.gate.sendWaiting()
    this.startTtlIfIdle()
  }

  /** How long a gone server is still held: until its loss is noticed at the pong deadline, then for `reconnectTimeout`. */
  reconnectWindow(): number {
    return Math.min(TIMER_DELAY_MAX_MS, pingDeadline(this.liveness.pingInterval) + this.reconnectTimeoutMs)
  }

  /** The channel sends nothing more. */
  unregister(channel: MuxChannel): void {
    const record = this.channels.of(channel)
    if (record === undefined) return
    this.channels.close(record)
    this.startTtlIfIdle()
  }

  /** Encodes `data` by `encodeFrame` at the next seq, then takes it, so a refused frame takes none; `onQueued` runs
   *  before the wire has it. Returns the frame's bytes, 0 once the channel sends nothing more. */
  sendSequenced<T>(
    channel: MuxChannel,
    kind: 'data' | 'ack',
    encodeFrame: SequencedEncoder<T>,
    data: T,
    onQueued?: (seq: number) => void,
  ): number {
    const record = this.channels.of(channel)
    if (record === undefined) return 0
    const { replay } = record
    const frame = encodeFrame(record.ix, data, replay.seq + 1)
    const maxFrameBytes = this.maxFrameBytesOf(channel)
    // The server ends the wire a larger frame arrives on, and the one its replay arrives on next.
    assertUsage(
      frame.byteLength <= maxFrameBytes,
      `Channel message too large: ${frame.byteLength} bytes encoded, the server accepts ${maxFrameBytes} at most`,
    )
    const seq = replay.nextSeq()
    onQueued?.(seq)
    this.gate.send(record, seq, frame, kind)
    return frame.byteLength
  }

  sendAckRes(channel: MuxChannel, ackedSeq: number, result: string, status: AckResultStatus = ACK_STATUS.OK): void {
    const record = this.channels.of(channel)
    if (record === undefined) return
    const { ix } = record
    const seq = record.replay.nextSeq()
    let frame = encode.ackRes(ix, seq, ackedSeq, result, status)
    const maxFrameBytes = this.maxFrameBytesOf(channel)
    // The server ends the wire a larger frame arrives on, so the send it answers gets an error instead.
    if (frame.byteLength > maxFrameBytes) {
      const error = `Channel ack answer too large: ${frame.byteLength} bytes encoded, the server accepts ${maxFrameBytes} at most`
      frame = encode.ackRes(ix, seq, ackedSeq, error, ACK_STATUS.ERROR)
    }
    this.gate.send(record, seq, frame, 'ack')
    if (ackedSeq === record.lastSeq) channel._answered()
  }

  /** The largest frame of `channel`'s the server takes: a runtime's lower limit is its WebSocket's. */
  private maxFrameBytesOf(channel: MuxChannel): number {
    if (!this.usesWebSocket) return WIRE_MAX_RAW_FRAME_BYTES
    return Math.min(channel._maxFrameBytes, this.maxFrameBytes)
  }

  /** Sequenced as data is, as CLOSE and CLOSE_ACK are, so a closing frame a dead wire lost replays. */
  sendAbort(channel: MuxChannel, abortValue: string): void {
    const record = this.channels.of(channel)
    if (record === undefined) return
    const seq = record.replay.nextSeq()
    this.gate.send(record, seq, encode.abort(record.ix, abortValue, seq), 'control')
  }

  sendCloseRequest(channel: MuxChannel, timeoutMs: number): void {
    const record = this.channels.of(channel)
    if (record === undefined) return
    const seq = record.replay.nextSeq()
    this.gate.send(record, seq, encode.close(record.ix, timeoutMs, seq), 'control')
  }

  sendCloseAck(channel: MuxChannel): void {
    const record = this.channels.of(channel)
    if (record === undefined) return
    const seq = record.replay.nextSeq()
    this.gate.send(record, seq, encode.closeAck(record.ix, seq), 'control')
  }

  sendByteWindowUpdate(channel: MuxChannel, limit: number, urgency: Urgency): void {
    const record = this.channels.of(channel)
    if (record === undefined) return
    this.gate.sendUnsequenced(record, encode.window(record.ix, limit, record.lastSeq), urgency)
  }

  sendMsgWindowUpdate(channel: MuxChannel, limit: number, urgency: Urgency): void {
    const record = this.channels.of(channel)
    if (record === undefined) return
    this.gate.sendUnsequenced(record, encode.msgWindow(record.ix, limit), urgency)
  }

  sendBdpPing(channel: MuxChannel, probe: number): void {
    const record = this.channels.of(channel)
    if (record === undefined) return
    this.gate.sendUnsequenced(record, encode.bdpPing(record.ix, probe), 'flow-control')
  }

  sendBdpPingAck(channel: MuxChannel, { probe, backlog, pathRtt }: BdpPingAck): void {
    const record = this.channels.of(channel)
    if (record === undefined) return
    // The round trip a page measures on batch POSTs leaves out the wait for its POST, so it vouches for no path.
    const frame = encode.bdpPingAck(record.ix, probe, backlog, this.transport.batched ? Infinity : pathRtt)
    this.gate.sendUnsequenced(record, frame, 'flow-control')
  }

  bufferedAmount(): number {
    return this.transport.bufferedAmount() + this.gate.waitingBytes
  }

  deliveredBeforeLast(ms: number): number {
    return this.transport.heartbeat?.deliveredBefore(ms) ?? 0
  }

  sendBroadcastSubscribe(channel: MuxChannel, binary: boolean): void {
    const record = this.channels.of(channel)
    if (record === undefined) return
    this.gate.sendUnsequenced(record, encode.broadcastSub(record.ix, binary), 'control')
  }

  sendBroadcastUnsubscribe(channel: MuxChannel, binary: boolean): void {
    const record = this.channels.of(channel)
    if (record === undefined) return
    this.gate.sendUnsequenced(record, encode.broadcastUnsub(record.ix, binary), 'control')
  }

  onTransportOpen(transport: ClientChannelTransport): void {
    if (this.closed) return
    if (transport !== this.transport) return
    this.enterOpen()
    if (!this.transport.carriesReconcileInOpen) {
      this.exchange.sendReconcileBatch(this.exchange.stageReconcileBatch(false))
      this.liveness.beatFromReconcile(transport)
      return
    }
    if (!this.exchange.reconciling) {
      // A register-reconcile queued during the connecting window sends its RECONCILE here and
      // carries the buffered frames after it, emptying the buffer; with none queued, flush
      this.exchange.flushPendingRegisterReconcile()
      this.drainBufferedFramesToWire()
    }
    this.upgrade.maybeStart()
  }

  onTransportBatched(transport: ClientChannelTransport): void {
    if (transport !== this.transport) return
    // Open ones only, as the join's undo: one still opening takes the batched window from its own open on this wire.
    for (const { channel, state } of this.channels.values()) if (state.tag === 'open') channel._onTransportBatched()
  }

  onTransportFrame(frame: DecodedFrame, source: ClientChannelTransport, byteLength: number, abandoned: boolean): void {
    // A wire the page gave up still delivers, but its RECONCILED answers a RECONCILE the next wire's replaces.
    if (abandoned && frame.tag === TAG.RECONCILED) return
    const u = this.upgrade.committing
    if (u !== null && this.transport === u.to) {
      this.upgrade.ingestDuringHandoff(frame, source === u.from ? 'old' : 'new', byteLength)
    } else {
      this.dispatchFrame(frame)
    }
  }

  dispatchFrame(frame: DecodedFrame): void {
    // Track seq for ALL sequenced frames, ACK_RES and the closing ones too; otherwise reconciles under-report lastSeq.
    if (isSequencedFrame(frame)) {
      const seq = this.trackSeq(frame.index, frame.seq)
      if (seq === null) return
      frame.seq = seq
    }
    if (frame.tag === TAG.WINDOW) {
      this.channels.serverHasThrough(frame.index, this.channels.sentThrough(frame.index, frame.lastSeq))
    }
    if (frame.tag === TAG.ACK_RES) this.channels.serverHasThrough(frame.index, frame.ackedSeq)
    // ATTACH_RESULT and the connection-level and closing ctrls need connection bookkeeping; the rest is per-channel.
    switch (frame.tag) {
      case TAG.FIN:
        this.upgrade.handleFin()
        return
      case TAG.RECONCILED:
        this.handleReconciled(frame.payload)
        return
      case TAG.ATTACH_RESULT: {
        const lastSeq = frame.lastSeq === null ? null : this.channels.sentThrough(frame.index, frame.lastSeq)
        if (!this.exchange.handleAttachResult(frame.index, lastSeq)) return
        this.startTtlIfIdle()
        this.upgrade.maybeStart()
        return
      }
      case TAG.ABORT:
        this.closeRemoteChannel(frame.index, makeAbortError(parse(frame.abortValue)))
        this.startTtlIfIdle()
        return
      case TAG.ERROR:
        this.closeRemoteChannel(frame.index, channelErrorFor(frame.reason))
        this.startTtlIfIdle()
        return
    }
    // PING/PONG/PIECE/PIECES_ACK/ANNOUNCE/RECONCILE/STREAM_REQUEST_OPEN_ACK never reach `dispatchFrame` —
    // transports peel them off in their own receive paths. Everything that lands
    // here is per-channel and carries `index`.
    const channelFrame = frame as ChannelFrame
    const record = this.channels.get(channelFrame.index)
    // A closed channel stays listed only for what it still has to tell the server.
    if (record && record.state.tag !== 'closed') record.channel._dispatchFrame(channelFrame)
  }

  onTransportReconcileSent(transport: ClientChannelTransport): void {
    if (this.closed || transport !== this.transport) return
    this.liveness.beatFromReconcile(transport)
  }

  onTransportPong(ended: PongEntry[]): void {
    this.liveness.onPong(ended)
  }

  dropWire(transport: ClientChannelTransport): void {
    transport.detachHeartbeat()
    transport.abandonActiveTransport()
    this.onTransportClosed(transport)
  }

  onTransportPostLate(transport: ClientChannelTransport): void {
    this.onPongTimeout(transport)
  }

  /** Till its first RECONCILED a wire's RECONCILE deadline bounds it: what precedes may outlast the pong deadline. */
  onPongTimeout(transport: ClientChannelTransport): void {
    if (this.exchange.boundsWire) return
    this.dropWire(transport)
  }

  onTransportClosed(transport: ClientChannelTransport, { rejectedByServer = false } = {}): void {
    if (this.closed) return
    transport.detachHeartbeat()
    if (transport !== this.transport) {
      if (this.upgrade.flipped && transport === this.upgrade.committing!.from) {
        this.fallbackToSse(new NetworkError('Connection dropped', true))
      }
      return
    }
    this.upgrade.abortAttempt()
    const err = new NetworkError(rejectedByServer ? 'Server rejected SSE connection' : 'Connection dropped', true)
    this.handleTransportLoss(err, rejectedByServer)
  }

  handleReconciled(ctrl: ReconciledPayload): void {
    if (!this.upgrade.onReconciled(ctrl)) return
    this.transport.applyReconciledSettings(ctrl)
    const deferredOmitted = this.upgrade.committing?.deferredOmitted ?? null
    const outcome = this.exchange.applyReconciled(ctrl, deferredOmitted)
    this.liveness.install(this.transport, ctrl.pingInterval, false)
    this.transport.closeAbandonedTransport()
    for (const frame of outcome.frames) this.transport.sendFrame(frame)
    for (const channel of outcome.channelsToOpen) {
      channel._onTransportOpen(this.transport.batched, this.transport.uplink, this.transport.downlink)
    }
    this.upgrade.tryComplete()
    if (outcome.reconcileComplete) {
      this.upgrade.serverTransports = ctrl.transports
      this.startTtlIfIdle()
      this.upgrade.maybeStart()
    }
  }

  // ── SSE→WS upgrade ──

  createWebSocket(): UpgradeTarget {
    return new WsTransport({ telefuncUrl: this.telefuncUrl, owner: this })
  }

  adoptTransport(transport: UpgradeTarget): void {
    this.transport = transport
    this.wire++
  }

  fallbackToSse(err: Error): void {
    if (this.closed) return
    this.upgrade.abandon()
    this.transport.abandonActiveTransport()
    this.transport.dispose()
    this.transport = TRANSPORT_REGISTRY[CHANNEL_TRANSPORT.SSE](this.telefuncUrl, this.connectionOptions, this)
    this.handleTransportLoss(err, false)
  }

  drainBufferedFramesToWire(): void {
    for (const frame of this.gate.drainSendable()) this.transport.sendFrame(frame)
    this.startTtlIfIdle()
  }

  private handleTransportLoss(err: Error, rejected: boolean): void {
    if (this.closed) return
    if (this.upgrade.flipped) {
      this.fallbackToSse(err)
      return
    }
    this.wire++
    // The wire is dying: the queued RECONCILE isn't sent, the reconnect's lists every channel.
    this.exchange.cancelRegisterReconcile()
    this.exchange.forgetWire()
    if (this.ttl) {
      clearTimeout(this.ttl)
      this.ttl = null
    }

    const { attempt: prevAttempt, startedAt: prevStartedAt } =
      this.state.tag === 'reconnecting' ? this.state : { attempt: 0, startedAt: 0 }

    if (rejected && prevAttempt === 0) {
      this.closeAll(err)
      this.dispose()
      return
    }
    // A closed channel the server has all of needs nothing from another wire.
    for (const record of this.channels.values()) {
      if (record.state.tag === 'closed' && record.state.delivered) this.releaseChannel(record)
    }
    if (this.channels.size === 0) {
      this.dispose()
      return
    }
    const startedAt = prevStartedAt || performance.now()
    if (performance.now() - startedAt > this.reconnectTimeoutMs) {
      this.closeAll(err)
      this.dispose()
      return
    }
    const delay = Math.min(CHANNEL_RECONNECT_INITIAL_DELAY_MS * 2 ** prevAttempt, CHANNEL_RECONNECT_MAX_DELAY_MS)
    this.enterReconnecting(prevAttempt + 1, startedAt, delay)
  }

  /** On a live wire with no channel open and nothing queued: what the transport holds leaves before the wire does. */
  startTtlIfIdle(): void {
    if (!this.connected || this.ttl || !this.gate.idle || !this.channels.idle) {
      return
    }
    const ttl = setTimeout(() => {
      void this.transport.drained().then(() => {
        if (this.ttl === ttl && this.channels.idle) this.dispose()
      })
    }, this.idleTimeoutMs)
    this.ttl = ttl
  }

  private dispose(): void {
    if (this.closed) return
    if (this.ttl) {
      clearTimeout(this.ttl)
      this.ttl = null
    }
    this.exchange.cancelRegisterReconcile()
    // Tear down any in-flight phase before transitioning to `closed`.
    if (this.state.tag === 'reconnecting') clearTimeout(this.state.timer)
    this.upgrade.teardown()
    this.enterClosed()
    this.transport.detachHeartbeat()
    this.transport.dispose()
    this.channels.clear()
    this.gate.clear()
    this.exchange.forgetWire()
    // A fresh connection replaces this one once its indexes run out.
    if (ClientConnection.cache.get(this.cacheKey) === this) ClientConnection.cache.delete(this.cacheKey)
  }

  // ── Protocol internals ──

  stageReconcileBatch(isInitialBatch: boolean): ReconcileBatch {
    return this.exchange.stageReconcileBatch(isInitialBatch)
  }

  /** The timeouts, replay sizes and frame limit the server sets, each replay fitted to them. */
  takeServerSettings(ctrl: ReconciledPayload): void {
    this.reconnectTimeoutMs = ctrl.reconnectTimeout
    this.maxFrameBytes = ctrl.maxFrameBytes
    // A per-call `idleTimeout` (withContext) is kept over the server's.
    if (this.connectionOptions.idleTimeout === undefined) this.idleTimeoutMs = ctrl.idleTimeout
    const window = replayWindow(ctrl.serverReplayBuffer, ctrl.serverReplayBufferBinary)
    // Each replay already fits the sizes a RECONCILED said before: `register` sizes and fits a channel with them.
    if (
      this.replayWindows?.window === window &&
      this.clientReplayBufferBytes === ctrl.clientReplayBuffer &&
      this.clientReplayBufferBinaryBytes === ctrl.clientReplayBufferBinary
    ) {
      return
    }
    this.clientReplayBufferBytes = ctrl.clientReplayBuffer
    this.clientReplayBufferBinaryBytes = ctrl.clientReplayBufferBinary
    this.replayWindows = { window, peerWindow: replayWindow(ctrl.clientReplayBuffer, ctrl.clientReplayBufferBinary) }
    // Before this reconcile stores anything: a channel registered before the first RECONCILED was sized with the defaults.
    for (const { replay } of this.channels.values()) {
      replay.setLimits(this.clientReplayBufferBytes, this.clientReplayBufferBinaryBytes)
    }
    for (const { channel } of this.channels.values()) this.fitReplays(channel)
  }

  /** The server ended it, so it needs nothing more the page sent on it. */
  private closeRemoteChannel(ix: number, err?: Error): void {
    const record = this.channels.get(ix)
    if (!record) return
    this.unregister(record.channel)
    assert(record.state.tag === 'closed')
    this.channels.serverEnded(record.state)
    record.channel._onEnded(err)
  }

  private closeAll(err: Error): void {
    for (const { channel } of this.channels.values()) {
      channel._onEnded(err)
    }
    this.dispose()
  }

  /** The seq of a frame of channel `ix` with low bits `bits`, or null to drop a duplicate or one of a let-go one. */
  private trackSeq(ix: number, bits: number): number | null {
    const record = this.channels.get(ix)
    if (record === undefined) return null
    const prev = record.lastSeq
    const seq = seqNear(bits, prev + 1)
    if (seq <= prev) return null
    record.lastSeq = seq
    return seq
  }

  /** Drops all the channel holds, its ATTACH_RESULT wait too, as the RECONCILE releasing it, or a new wire's leaving it
   *  out, ends that on the server. */
  releaseChannel(record: ChannelRecord): void {
    this.channels.release(record)
    this.gate.dropQueue(record.ix)
  }
}

// ── Transport registry ──

/** Maps each ChannelTransport to a factory that creates the corresponding ClientChannelTransport. */
const TRANSPORT_REGISTRY: Record<
  ChannelTransport,
  (telefuncUrl: string, options: ClientConnectionOptions, owner: ClientConnection) => ClientChannelTransport
> = {
  [CHANNEL_TRANSPORT.WS]: (telefuncUrl, _options, owner) => new WsTransport({ telefuncUrl, owner }),
  [CHANNEL_TRANSPORT.SSE]: (telefuncUrl, options, owner) =>
    new SseTransport({ telefuncUrl, fetchImpl: options.fetchImpl, userHeaders: options.headers, owner }),
}

/** What a channel the server ended with an ERROR of `reason` closes with. */
function channelErrorFor(reason: number): Error {
  switch (reason) {
    case ERROR_REASON.OVERFLOW:
      return new ChannelOverflowError(
        'Broadcast closed: this client fell further behind than the server holds for a client',
      )
    case ERROR_REASON.LOST:
      return replayLossError('server')
    default:
      return makeBugError()
  }
}
