export { WireLifecycle }
export type { TransportOwner, WireLifecycleOwner }

import { parse } from '@brillout/json-serializer/parse'
import { makeAbortError, makeBugError } from '../../../client/remoteTelefunctionCall/errors.js'
import { assert, assertUsage } from '../../../utils/assert.js'
import { ChannelOverflowError } from '../channelErrors.js'
import { NetworkError } from '../../../shared/NetworkError.js'
import {
  CHANNEL_CLIENT_REPLAY_BUFFER_BYTES,
  CHANNEL_IDLE_TIMEOUT_MS,
  CHANNEL_RECONNECT_INITIAL_DELAY_MS,
  CHANNEL_RECONNECT_MAX_DELAY_MS,
  CHANNEL_RECONNECT_TIMEOUT_MS,
  CHANNEL_TRANSPORT,
  MAX_CHANNELS_PER_CONNECTION,
  type ChannelTransport,
  TIMER_DELAY_MAX_MS,
  WIRE_MAX_RAW_FRAME_BYTES,
} from '../../constants.js'
import { ReplayBuffer } from './ReplayBuffer.js'
import { ERROR_REASON, isSequencedFrame } from '../../wire/frame.js'
import { TAG, isClosingTag } from '../../wire/tags.js'
import { seqNear } from '../../wire/seq.js'
import type { ChannelFrame, DecodedFrame, PongEntry } from '../../wire/frame.js'
import type { ReconciledPayload } from '../../wire/payloads.js'

import { ChannelTable } from './ChannelTable.js'
import { SendGate, type SendGateOwner } from './SendGate.js'
import { Liveness, type LivenessOwner } from './Liveness.js'
import { Upgrade, type JoinRelease, type UpgradeOwner, type UpgradeSource, type UpgradeTarget } from './Upgrade.js'
import { AttachExchange, type AttachExchangeOwner, type ReconcileBatch } from './AttachExchange.js'

import { pingDeadline } from '../../wire/pingDeadline.js'

import { WsTransport } from './WsTransport.js'
import { SseTransport } from './SseTransport.js'

import type { ClientChannelTransport, ClientConnectionOptions, MuxChannel } from './ClientConnection.js'

/** What a transport tells its connection, and the reconcile batch it asks for when its wire opens. */
interface TransportOwner {
  onTransportOpen(transport: ClientChannelTransport): void
  /** `rejectedByServer`: it refused the wire's opening; `otherWireVersion`: it runs another `WIRE_VERSION`. */
  onTransportClosed(
    transport: ClientChannelTransport,
    options?: { rejectedByServer?: boolean; otherWireVersion?: boolean },
  ): void
  onTransportFrame(frame: DecodedFrame, source: ClientChannelTransport, byteLength: number, abandoned: boolean): void
  onTransportPong(ended: PongEntry[]): void
  /** The transport may have headroom again, so the frames waiting for it are sent. */
  onTransportHeadroom(transport: ClientChannelTransport): void
  /** The wire's upload fell back to batch POSTs; a channel may have opened before that (see `onTransportOpen`). */
  onTransportBatched(transport: ClientChannelTransport): void
  /** An SSE wire's RECONCILE went out with the request that opens it, before the wire opens (see `beatFromReconcile`). */
  onTransportReconcileSent(transport: ClientChannelTransport): void
  /** A batch POST has been out longer than its upload takes at the slowest rate a wire is held to. */
  onWireLate(transport: ClientChannelTransport): void
  /** What a transport's wire carries as it opens: the RECONCILE, and the frames buffered behind it. */
  stageInitialReconcileBatch(): ReconcileBatch
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

/** The connection, which a disposed wire lifecycle's next `getOrCreate` replaces. */
interface WireLifecycleOwner {
  onDisposed(): void
}

type WireLifecycleOptions = {
  owner: WireLifecycleOwner
  telefuncUrl: string
  options: ClientConnectionOptions
}

/** The wire a connection's channels ride, from its first open to its idle close: the transports, the reconnects and the
 *  upgrade, the liveness and the attach exchange, and the frames the wire delivers to the channels. */
class WireLifecycle implements TransportOwner, SendGateOwner, LivenessOwner, UpgradeOwner, AttachExchangeOwner {
  private readonly owner: WireLifecycleOwner
  private readonly telefuncUrl: string
  private readonly connectionOptions: ClientConnectionOptions
  transport: ClientChannelTransport

  private state: ConnectionState = { tag: 'fresh' }
  private ttl: ReturnType<typeof setTimeout> | null = null
  get closed(): boolean {
    return this.state.tag === 'closed'
  }
  get connected(): boolean {
    return this.state.tag === 'open'
  }

  /** Increments each time the connection moves to another wire: what went out on the previous one may not have
   *  arrived. */
  wire = 0
  private nextIndex = 0
  readonly channels = new ChannelTable()
  readonly gate = new SendGate({ owner: this, channels: this.channels })
  private readonly liveness = new Liveness({ owner: this, channels: this.channels, gate: this.gate })
  private readonly exchange = new AttachExchange({ owner: this, channels: this.channels, gate: this.gate })
  private readonly upgrade: Upgrade
  /** The page's transports include a WebSocket. */
  private readonly allowsWebSocket: boolean
  /** The transports the server allows, from the last settled RECONCILED: an upgrade may start once it came, whether
   *  before or after the SSE wire opened. */
  private serverTransports: ReconciledPayload['transports'] | null = null
  private reconnectTimeoutMs = CHANNEL_RECONNECT_TIMEOUT_MS
  private idleTimeoutMs: number
  /** The largest credit window, each way, from the last RECONCILED. */
  private creditWindow: number | null = null
  /** The largest frame the server accepts on a WebSocket, from the last RECONCILED. */
  maxFrameBytes = WIRE_MAX_RAW_FRAME_BYTES
  constructor({ owner, telefuncUrl, options }: WireLifecycleOptions) {
    this.owner = owner
    this.telefuncUrl = telefuncUrl
    this.connectionOptions = options
    this.upgrade = new Upgrade({ owner: this })
    this.allowsWebSocket = options.transports.includes(CHANNEL_TRANSPORT.WS)
    this.idleTimeoutMs = options.idleTimeout ?? CHANNEL_IDLE_TIMEOUT_MS
    this.transport = TRANSPORT_REGISTRY[options.transports[0]!](telefuncUrl, options, this)
  }

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

  /** The wire can't take a RECONCILE now: it isn't open, or an upgrade is between its READY and its handoff. */
  private get reconcileHeld(): boolean {
    return !this.connected || this.upgrade.holdsReconcile
  }

  /** Sends the RECONCILE a registration owes, where the wire takes one; no-op when none is owed. While the wire can't
   *  (connecting, awaiting a RECONCILED, from an upgrade's READY until its handoff) it stays owed: `reconcileOwed` keeps
   *  `sendsOpen` false, so data frames buffer behind it, and the wire's open and an upgrade attempt's end call this
   *  again. A RECONCILED, the handoff's too, names what registered meanwhile in its follow-up. */
  onReconcileDue(): void {
    if (!this.exchange.reconcileOwed || this.reconcileHeld || this.exchange.reconciling) return
    this.exchange.sendReconcileBatch(this.exchange.stageReconcileBatch())
  }

  /** Wire indexes are u16 and never reused, so once they run out a new channel needs a fresh connection. */
  get takesChannels(): boolean {
    return !this.closed && this.nextIndex <= 0xffff
  }

  register(channel: MuxChannel): void {
    // A new wire's RECONCILE lists closed channels too, so at the limit the oldest closed one the server has in full
    // makes way.
    if (this.channels.size === MAX_CHANNELS_PER_CONNECTION) {
      for (const record of this.channels.values()) {
        if (record.state.tag !== 'closed' || !record.state.delivered) continue
        this.gate.release(record)
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
    this.channels.add(this.nextIndex++, channel, new ReplayBuffer(CHANNEL_CLIENT_REPLAY_BUFFER_BYTES))
    if (this.creditWindow !== null) channel._attach.capWindows(this.creditWindow)

    if (!this.transport.hasWire() && !this.transport.isConnecting()) {
      this.transport.start()
      return
    }
    this.exchange.scheduleRegisterReconcile()
  }

  onTransportHeadroom(transport: ClientChannelTransport): void {
    if (transport !== this.transport || !this.sendsOpen) return
    this.gate.sendWaiting()
    this.startTtlIfIdle()
  }

  /** How long the page waits for a server that went away: until the pong deadline, then `reconnectTimeout` more. */
  reconnectWindow(): number {
    return Math.min(TIMER_DELAY_MAX_MS, pingDeadline(this.liveness.pingInterval) + this.reconnectTimeoutMs)
  }

  /** Called once the channel sends nothing more. */
  unregister(channel: MuxChannel): void {
    const record = this.channels.of(channel)
    if (record === undefined) return
    this.channels.close(record)
    this.startTtlIfIdle()
  }

  onTransportOpen(transport: ClientChannelTransport): void {
    if (this.closed) return
    if (transport !== this.transport) return
    this.enterOpen()
    if (!this.transport.carriesReconcileInOpen) {
      this.exchange.sendReconcileBatch(this.exchange.stageReconcileBatch())
      this.liveness.beatFromReconcile(transport, this.exchange.sessionId)
      return
    }
    if (!this.exchange.reconciling) {
      // A register-reconcile queued during the connecting window sends its RECONCILE here and
      // carries the buffered frames after it, emptying the buffer; with none queued, the buffered frames go out alone.
      this.onReconcileDue()
      this.drainBufferedFramesToWire()
    }
    this.maybeUpgrade()
  }

  onTransportBatched(transport: ClientChannelTransport): void {
    if (transport !== this.transport) return
    // Open channels only, as the join's undo does: one still opening gets the batched window as it opens on this wire.
    for (const { channel, state } of this.channels.values()) if (state.tag === 'open') channel._attach.onBatched()
  }

  onTransportFrame(frame: DecodedFrame, source: ClientChannelTransport, byteLength: number, abandoned: boolean): void {
    // An abandoned wire still delivers, but its RECONCILED answers a RECONCILE the next wire's has replaced.
    if (abandoned && frame.tag === TAG.RECONCILED) return
    switch (this.upgrade.takeHandoffFrame(frame, source, byteLength)) {
      case 'dispatch':
        this.dispatchFrame(frame)
        return
      case 'complete':
        this.applyCompletionFrame(frame)
        return
      case 'fallback':
        this.fallbackToSse(new NetworkError('Upgrade handoff buffer limit exceeded', true))
        return
    }
  }

  private dispatchFrame(frame: DecodedFrame): void {
    // Track seq for ALL sequenced frames, ACK_RES and the closing ones too; otherwise reconciles under-report lastSeq.
    if (isSequencedFrame(frame)) {
      const seq = this.trackSeq(frame)
      if (seq === null) return
      frame.seq = seq
    }
    if (frame.tag === TAG.WINDOW) {
      this.channels.serverHasThrough(frame.index, this.channels.sentThrough(frame.index, frame.lastSeq))
    }
    if (frame.tag === TAG.ACK_RES) this.channels.serverHasThrough(frame.index, frame.ackedSeq)
    // ATTACH_RESULT and the wire-level and closing ctrls need connection bookkeeping; the rest is per-channel.
    switch (frame.tag) {
      case TAG.FIN:
        this.releaseJoin(this.upgrade.handleFin())
        return
      case TAG.RECONCILED:
        this.handleReconciled(frame.payload)
        return
      case TAG.ATTACH_RESULT: {
        const lastSeq = frame.lastSeq === null ? null : this.channels.sentThrough(frame.index, frame.lastSeq)
        if (!this.exchange.handleAttachResult(frame.index, lastSeq)) return
        this.startTtlIfIdle()
        this.maybeUpgrade()
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
    this.liveness.beatFromReconcile(transport, this.exchange.sessionId)
  }

  onTransportPong(ended: PongEntry[]): void {
    this.liveness.onPong(ended)
  }

  onReconcileLate(transport: ClientChannelTransport): void {
    this.dropWire(transport)
  }

  private dropWire(transport: ClientChannelTransport): void {
    transport.detachHeartbeat()
    transport.abandonActiveTransport()
    this.onTransportClosed(transport)
  }

  /** Its PONG or a batch POST came late. Until a wire's first RECONCILED, the RECONCILE deadline bounds it: the wait
   *  may outlast the pong deadline. */
  onWireLate(transport: ClientChannelTransport): void {
    if (this.exchange.boundsWire) return
    this.dropWire(transport)
  }

  onTransportClosed(
    transport: ClientChannelTransport,
    { rejectedByServer = false, otherWireVersion = false } = {},
  ): void {
    if (this.closed) return
    transport.detachHeartbeat()
    // No reconnect reaches a server that speaks this page's wire.
    if (otherWireVersion) {
      this.closeAll(new NetworkError('The server was updated since this page loaded: reload the page.', true))
      return
    }
    if (transport !== this.transport) {
      if (this.upgrade.isLeftWire(transport)) {
        this.fallbackToSse(new NetworkError('Connection dropped', true))
      }
      return
    }
    this.upgrade.abortAttempt()
    const err = new NetworkError(rejectedByServer ? 'Server rejected SSE connection' : 'Connection dropped', true)
    this.handleTransportLoss(err, rejectedByServer)
  }

  private handleReconciled(ctrl: ReconciledPayload): void {
    if (!this.upgrade.onReconciled(ctrl)) return
    this.transport.applyReconciledSettings?.(ctrl)
    this.takeServerSettings(ctrl)
    const outcome = this.exchange.applyReconciled(ctrl, {
      deferOmitted: this.upgrade.handingOff,
      upgradeReady: this.upgrade.ready,
    })
    this.upgrade.deferOmitted(outcome.omitted)
    this.liveness.install(this.transport, ctrl.pingInterval)
    this.transport.closeAbandonedTransport()
    for (const frame of outcome.frames) this.transport.sendFrame(frame)
    for (const channel of outcome.channelsToOpen) {
      channel._onTransportOpen(this.transport.batched, this.transport.uplink, this.transport.downlink)
    }
    this.releaseJoin(this.upgrade.tryComplete())
    if (outcome.reconcileComplete) {
      this.serverTransports = ctrl.transports
      this.startTtlIfIdle()
      this.maybeUpgrade()
    }
  }

  private adoptTransport(transport: UpgradeTarget): void {
    this.transport = transport
    this.wire++
  }

  // ── The SSE→WS upgrade: when one starts, and an attempt's steps ──

  /** Starts an upgrade where one may: on an open SSE wire, the only kind that can end with a barrier, with no
   *  RECONCILE in flight, when both the page and the server take WebSockets and no attempt is under way. */
  private maybeUpgrade(): void {
    if (!this.connected || !this.upgrade.canStart) return
    // A RECONCILE in flight retries it once its RECONCILED settles (`handleReconciled`).
    if (this.exchange.reconciling) return
    if (this.transport.type !== CHANNEL_TRANSPORT.SSE) return
    if (!this.allowsWebSocket || !this.serverTransports?.includes(CHANNEL_TRANSPORT.WS)) return
    const { sessionId } = this.exchange
    if (sessionId === null) return
    void this.runUpgrade(this.transport as UpgradeSource, sessionId)
  }

  /** One attempt, then what its end leaves to the connection. However it ends, the RECONCILE a registration owes goes
   *  after it. */
  private async runUpgrade(from: UpgradeSource, sessionId: string): Promise<void> {
    const to = new WsTransport({ telefuncUrl: this.telefuncUrl, owner: this })
    const end = await this.upgrade.run(
      from,
      to,
      { sessionId, pingInterval: this.liveness.pingInterval },
      {
        reconcileSettled: (signal) => this.exchange.reconcileSettled(signal),
        barrier: (session) => this.exchange.buildBarrierFrame(session),
        adopt: (target) => this.adoptTransport(target),
      },
    )
    switch (end.tag) {
      case 'not-ready':
        if (!this.exchange.reconcileOwed) this.drainBufferedFramesToWire()
        break
      case 'not-emitted':
        if (!this.exchange.reconcileOwed) this.drainBufferedFramesToWire()
        // An attempt that ended without its barrier goes again once the outbox drained.
        void from.drained().then(() => this.maybeUpgrade())
        break
      case 'stranded':
        this.fallbackToSse(new NetworkError('Upgrade barrier attempt timed out', true))
        break
      case 'flipped':
        for (const frame of end.completion) this.applyCompletionFrame(frame)
        this.releaseJoin(this.upgrade.tryComplete())
        break
    }
    this.onReconcileDue()
  }

  /** A completion frame of the join, applied once the connection flipped: FIN from the old wire, RECONCILED from the
   *  new. */
  private applyCompletionFrame(frame: DecodedFrame): void {
    if (frame.tag === TAG.FIN) this.releaseJoin(this.upgrade.handleFin())
    else if (frame.tag === TAG.RECONCILED) this.handleReconciled(frame.payload)
  }

  /** The join completed: the old wire's frames, then the channels its RECONCILED left out closed, then the new wire's,
   *  as the server wrote them; then the channels learn they are on the WebSocket. */
  private releaseJoin(release: JoinRelease | null): void {
    if (release === null) return
    for (const frame of release.old) this.dispatchFrame(frame)
    this.exchange.closeUnacknowledged(release.deferredOmitted)
    for (const frame of release.new) this.dispatchFrame(frame)
    for (const { channel, state } of this.channels.values()) if (state.tag === 'open') channel._attach.onUpgradeJoined()
    this.startTtlIfIdle()
  }

  onHandoffFailed(err: Error): void {
    this.fallbackToSse(err)
  }

  private fallbackToSse(err: Error): void {
    if (this.closed) return
    const abandoned = this.upgrade.abandon()
    if (abandoned) {
      for (const frame of abandoned.old) this.dispatchFrame(frame)
      this.exchange.closeUnacknowledged(abandoned.deferredOmitted)
    }
    this.transport.abandonActiveTransport()
    this.transport.dispose()
    this.transport = TRANSPORT_REGISTRY[CHANNEL_TRANSPORT.SSE](this.telefuncUrl, this.connectionOptions, this)
    this.handleTransportLoss(err, false)
  }

  private drainBufferedFramesToWire(): void {
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
    // The wire is dying: the queued RECONCILE is not sent, since the reconnect's RECONCILE lists every channel.
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
    // A closed channel whose frames the server all has needs nothing from another wire.
    for (const record of this.channels.values()) {
      if (record.state.tag === 'closed' && record.state.delivered) this.gate.release(record)
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

  /** Starts the idle timer on a live wire with no channel open and nothing queued; the transport drains first. */
  private startTtlIfIdle(): void {
    if (!this.connected || this.ttl || !this.gate.idle || !this.channels.idle) {
      return
    }
    const ttl = setTimeout(() => void this.disposeOnceDrained(ttl), this.idleTimeoutMs)
    this.ttl = ttl
  }

  private async disposeOnceDrained(ttl: ReturnType<typeof setTimeout>): Promise<void> {
    await this.transport.drained()
    if (this.ttl === ttl && this.channels.idle) this.dispose()
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
    this.upgrade.abandon()
    this.enterClosed()
    this.transport.detachHeartbeat()
    this.transport.dispose()
    this.channels.clear()
    this.gate.clear()
    this.exchange.forgetWire()
    this.owner.onDisposed()
  }

  stageInitialReconcileBatch(): ReconcileBatch {
    return this.exchange.stageInitialReconcileBatch()
  }

  /** Takes the timeouts, credit window and frame limit the server sets. Until the first RECONCILED, a channel's
   *  initial window fits any server's. */
  private takeServerSettings(ctrl: ReconciledPayload): void {
    this.reconnectTimeoutMs = ctrl.reconnectTimeout
    this.maxFrameBytes = ctrl.maxFrameBytes
    // A per-call `idleTimeout` (withContext) is kept over the server's.
    if (this.connectionOptions.idleTimeout === undefined) this.idleTimeoutMs = ctrl.idleTimeout
    if (this.creditWindow === ctrl.creditWindow) return
    this.creditWindow = ctrl.creditWindow
    for (const { channel } of this.channels.values()) channel._attach.capWindows(ctrl.creditWindow)
  }

  /** The server ended channel `ix`, so it needs nothing more of what the page sent on it. */
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

  /** A frame's 7-byte header holds only the low 32 bits of its seq, while a channel's seqs count past that; this widens
   *  them to the full seq nearest the channel's last one plus one, as no wire or replay holds 2^31 frames. Returns that
   *  seq, or null for a frame to drop: a seq already seen, such as a replay resending what arrived, or a channel this
   *  page let go. */
  private trackSeq({ tag, index, seq: bits }: { tag: number; index: number; seq: number }): number | null {
    const record = this.channels.get(index)
    if (record === undefined) return null
    const prev = record.lastSeq
    const seq = seqNear(bits, prev + 1)
    if (seq <= prev) return null
    record.lastSeq = seq
    // A channel's frames take consecutive seqs, so a gap is publishes the server let go as this page fell behind. A
    // closing frame goes ahead of its channel's waiting publishes on purpose, so its gap is no loss.
    if (seq > prev + 1 && !isClosingTag(tag)) record.channel._onFramesMissed?.()
    return seq
  }
}

// ── Transport registry ──

/** Maps each ChannelTransport to a factory that creates the corresponding ClientChannelTransport. */
const TRANSPORT_REGISTRY: Record<
  ChannelTransport,
  (telefuncUrl: string, options: ClientConnectionOptions, owner: WireLifecycle) => ClientChannelTransport
> = {
  [CHANNEL_TRANSPORT.WS]: (telefuncUrl, _options, owner) => new WsTransport({ telefuncUrl, owner }),
  [CHANNEL_TRANSPORT.SSE]: (telefuncUrl, options, owner) =>
    new SseTransport({ telefuncUrl, fetchImpl: options.fetchImpl, userHeaders: options.headers, owner }),
}

/** What a channel the server ended with an ERROR of `reason` closes with. */
function channelErrorFor(reason: number): Error {
  if (reason === ERROR_REASON.OVERFLOW) {
    return new ChannelOverflowError('Closed: the server could not hold all this client had yet to receive')
  }
  return makeBugError()
}
