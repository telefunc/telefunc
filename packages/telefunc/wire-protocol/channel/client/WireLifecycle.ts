export { WireLifecycle }
export type { TransportOwner, WireLifecycleOwner }

import { parse } from '@brillout/json-serializer/parse'
import { makeAbortError, makeBugError } from '../../../client/remoteTelefunctionCall/errors.js'
import { assert, assertUsage } from '../../../utils/assert.js'
import { ChannelClosedError, ChannelOverflowError, rejectedFrameError } from '../channelErrors.js'
import { closeTimedOutError } from '../ChannelEnding.js'
import { NetworkError } from '../../../shared/NetworkError.js'
import {
  ATTEMPT_ALLOWANCE_GROWTH,
  ATTEMPT_ALLOWANCE_MIN_MS,
  ATTEMPT_ALLOWANCE_OPENS,
  CHANNEL_CLIENT_REPLAY_BUFFER_BYTES,
  CHANNEL_HOLD_MAX_BYTES,
  CHANNEL_IDLE_TIMEOUT_MS,
  CHANNEL_RECONNECT_INITIAL_DELAY_MS,
  CHANNEL_RECONNECT_MAX_DELAY_MS,
  CHANNEL_RECONNECT_TIMEOUT_MS,
  CHANNEL_TRANSPORT,
  MAX_CHANNELS_PER_CONNECTION,
  type ChannelTransport,
  WIRE_MAX_RAW_FRAME_BYTES,
} from '../../constants.js'
import { ReplayBuffer } from './ReplayBuffer.js'
import { ERROR_REASON, isSequencedFrame, tellsLastRead } from '../../wire/frame.js'
import { TAG } from '../../wire/tags.js'
import type { ChannelFrame, DecodedFrame, PongEntry } from '../../wire/frame.js'
import type { ReconciledPayload } from '../../wire/payloads.js'

import { ChannelTable, type ChannelRecord } from './ChannelTable.js'
import { SendGate, type SendGateOwner } from './SendGate.js'
import { Liveness, type LivenessOwner } from './Liveness.js'
import { Upgrade, type JoinRelease, type UpgradeOwner, type UpgradeSource, type UpgradeTarget } from './Upgrade.js'
import { AttachExchange, type AttachExchangeOwner, type ReconcileBatch } from './AttachExchange.js'

import { reconnectWindowOf } from '../../wire/pingDeadline.js'

import { WsTransport } from './WsTransport.js'
import { SseTransport } from './SseTransport.js'

import type { ClientChannelTransport, ClientConnectionOptions, MuxChannel } from './ClientConnection.js'

/** What a transport tells its connection, and the reconcile batch it asks for when its wire opens. */
interface TransportOwner {
  /** `startedAt`: when the socket or request that opened started. */
  onTransportOpen(transport: ClientChannelTransport, startedAt: number): void
  /** The server answered the attempt: a WebSocket's open, or the event stream's head before the wire opens. */
  onTransportAnswered(transport: ClientChannelTransport): void
  /** `refusal`: what the server answered the wire's opening with, which wasn't a wire; `otherWireVersion`: it runs
   *  another `WIRE_VERSION`; `rejectedFrame`: it refused the wire for a frame it couldn't read. */
  onTransportClosed(
    transport: ClientChannelTransport,
    options?: { refusal?: string; otherWireVersion?: boolean; rejectedFrame?: boolean },
  ): void
  onTransportFrame(frame: DecodedFrame, source: ClientChannelTransport, byteLength: number, abandoned: boolean): void
  onTransportPong(ended: PongEntry[]): void
  /** A piece of a RECONCILED came on a WebSocket, the RECONCILED not yet whole. */
  onTransportReconciledPiece(transport: ClientChannelTransport): void
  /** The transport may have headroom again, so the frames waiting for it are sent. */
  onTransportHeadroom(transport: ClientChannelTransport): void
  /** The wire's upload fell back to batch POSTs; a channel may have opened before that (see `onTransportOpen`). */
  onTransportBatched(transport: ClientChannelTransport): void
  /** An SSE wire's RECONCILE went out with the request that opens it, before the wire opens (see `beatFromReconcile`). */
  onTransportReconcileSent(transport: ClientChannelTransport): void
  /** What a transport's wire carries as it opens: the RECONCILE, and the frames buffered behind it. */
  stageInitialReconcileBatch(): ReconcileBatch
}

/** The frames that answer a RECONCILE: from an abandoned wire, they answer one the next wire's replaced. */
const ANSWERS_RECONCILE: ReadonlySet<number> = new Set([TAG.RECONCILED, TAG.RECONCILE_TAKEN, TAG.ATTACH_RESULT])

type ConnectionState =
  | { tag: 'fresh' }
  | { tag: 'open' }
  /** `timer` starts the next attempt; null where none could reconcile before the recovery's deadline. */
  | { tag: 'reconnecting'; timer: ReturnType<typeof setTimeout> | null }
  | { tag: 'closed' }

type Recovery = { attempt: number; deadline: number; refusal: string | null; timer: ReturnType<typeof setTimeout> }

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
  /** From a first connect's start, or the first wire lost since the last RECONCILED: the attempts made (a wire that
   *  opens and never reconciles counts), the last refusal an attempt was answered with, and the deadline whose timer
   *  ends the channels if no wire reconciled by then. */
  private recovery: Recovery | null = null
  /** When the wire being opened started, until its RECONCILED. */
  private attemptStartedAt: number | null = null
  /** Cuts the attempt under way that the server hasn't answered by its allowance (`attemptAllowance`). */
  private allowanceTimer: ReturnType<typeof setTimeout> | null = null
  /** The attempt under way: the backoff its end waits, drawn as it starts, and when it is cut unanswered (null: it runs
   *  to the deadline, or was answered). */
  private attempt: { nextDelay: number; cutAt: number | null } | null = null
  /** When the attempt under way started, till the server answered it. */
  private allowanceFrom = 0
  /** How long the last attempt the server answered took to be answered, and the attempts cut since. */
  private answeredMs: number | null = null
  private cuts = 0
  /** How long the last wire took from its socket's or request's start to its RECONCILED: the room an attempt needs
   *  before the deadline. */
  private handshakeMs: number | null = null
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

  /** Owns the reconnect timer's lifecycle so callers can't forget to cancel a prior one. A null `delay`: no attempt. */
  private enterReconnecting(delay: number | null): void {
    this.clearReconnectTimer()
    const timer = delay === null ? null : setTimeout(() => this.startWire(), delay)
    this.state = { tag: 'reconnecting', timer }
    this.upgrade.forget()
  }

  private clearReconnectTimer(): void {
    if (this.state.tag === 'reconnecting' && this.state.timer) clearTimeout(this.state.timer)
  }

  /** Starts an attempt, and the recovery it belongs to where none is under way: a first connect reports no loss. Past
   *  the recovery's deadline by the clock, the deadline's timer not yet run (a page that slept or blocked its loop),
   *  the channels end instead. */
  private startWire(): void {
    const now = performance.now()
    const recovery = this.recoveryFrom()
    if (now >= recovery.deadline) return this.onRecoveryDeadline()
    this.attemptStartedAt = now
    this.allowanceFrom = now
    this.clearAllowance()
    const nextDelay = reconnectDelay(recovery.attempt)
    // A cut leaves the next attempt its room; where none is left, this attempt runs to the deadline.
    const room = this.latestEnd(nextDelay, recovery.deadline, this.handshakeMs ?? ATTEMPT_ALLOWANCE_MIN_MS) - now
    const allowance = Math.min(attemptAllowance(this.answeredMs, this.cuts), room)
    this.attempt = { nextDelay, cutAt: room > 0 ? now + allowance : null }
    if (room > 0) this.allowanceTimer = setTimeout(() => this.cutAttempt(), allowance)
    this.transport.start()
  }

  /** The attempt under way wasn't answered in time: it goes, and counts as failed. A page that left let it go. */
  private cutAttempt(): void {
    this.allowanceTimer = null
    if (this.closed || !this.transport.isConnecting()) return
    this.cuts++
    this.transport.cancelAttempt()
    this.onTransportClosed(this.transport)
  }

  /** The server answered the attempt under way; from here its RECONCILE's deadline bounds it. */
  private attemptAnswered(): void {
    if (this.attempt === null) return
    this.attempt.cutAt = null
    this.clearAllowance()
    this.answeredMs = performance.now() - this.allowanceFrom
    this.cuts = 0
    this.exchange.restartReconcileDeadline()
  }

  private clearAllowance(): void {
    if (this.allowanceTimer) clearTimeout(this.allowanceTimer)
    this.allowanceTimer = null
  }

  private enterClosed(): void {
    this.state = { tag: 'closed' }
    this.upgrade.forget()
  }

  get reconnecting(): boolean {
    return this.state.tag === 'reconnecting'
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
      // While a reconnect waits, its attempt's RECONCILE lists this channel: it starts none sooner, nor one past the
      // recovery's deadline.
      if (!this.reconnecting) this.startWire()
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
    return reconnectWindowOf(this.liveness.pingInterval, this.reconnectTimeoutMs, this.transport.type)
  }

  /** Called once the channel sends nothing more. */
  unregister(channel: MuxChannel): void {
    const record = this.channels.of(channel)
    if (record === undefined) return
    this.channels.close(record)
    this.startTtlIfIdle()
  }

  onTransportAnswered(transport: ClientChannelTransport): void {
    if (transport === this.transport) this.attemptAnswered()
  }

  onTransportOpen(transport: ClientChannelTransport, startedAt: number): void {
    if (transport !== this.transport) return
    // The handshake's sample is the opened socket's own: one that hung beside it adds nothing. SSE's RECONCILED may
    // have come first, and taken it.
    if (this.attemptStartedAt !== null) this.attemptStartedAt = startedAt
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
    // An abandoned wire still delivers, but what answers its RECONCILE answers one the next wire's has replaced.
    if (abandoned && ANSWERS_RECONCILE.has(frame.tag)) return
    switch (this.upgrade.takeHandoffFrame(frame, source, byteLength)) {
      case 'dispatch':
        this.dispatchFrame(frame)
        return
      case 'complete':
        this.applyCompletionFrame(frame)
        return
      case 'fallback':
        this.fallbackToSse()
        return
    }
  }

  /** What handling `frame` throws, a bug or a malformed frame of the server's, is thrown on its own: the page reports it
   *  as uncaught, and the frames after it still go, on either transport and after a join as well. */
  private dispatchFrame(frame: DecodedFrame): void {
    try {
      this.handleFrame(frame)
    } catch (err) {
      queueMicrotask(() => {
        throw err
      })
    }
  }

  private handleFrame(frame: DecodedFrame): void {
    // Track seq for ALL sequenced frames, ACK_RES and the closing ones too; otherwise reconciles under-report lastSeq.
    if (isSequencedFrame(frame)) {
      const seq = this.channels.receivedSeq(frame)
      if (seq === null) return
      frame.seq = seq
    }
    if (tellsLastRead(frame)) {
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
      case TAG.RECONCILE_TAKEN:
        this.exchange.takeReceipt()
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

  onTransportReconciledPiece(transport: ClientChannelTransport): void {
    if (transport === this.transport) this.exchange.reconciledPieceArrived()
  }

  onReconcileLate(transport: ClientChannelTransport): void {
    // Till the server answered the request that carries it, the attempt's allowance bounds it.
    if (this.allowanceTimer !== null) return
    this.dropWire(transport)
  }

  private dropWire(transport: ClientChannelTransport): void {
    transport.detachHeartbeat()
    // A wire that never reconciled has delivered nothing the next one's RECONCILED won't replay.
    if (this.exchange.boundsWire) transport.cancelAttempt()
    else transport.abandonActiveTransport()
    this.onTransportClosed(transport)
  }

  /** Its PONG came late. Until a wire's first RECONCILED, a RECONCILE deadline that runs from the start bounds it: the
   *  wait may outlast the pong deadline. */
  onWireLate(transport: ClientChannelTransport): void {
    if (this.exchange.timesWire) return
    this.dropWire(transport)
  }

  onTransportClosed(
    transport: ClientChannelTransport,
    {
      refusal,
      otherWireVersion = false,
      rejectedFrame = false,
    }: { refusal?: string; otherWireVersion?: boolean; rejectedFrame?: boolean } = {},
  ): void {
    if (this.closed) return
    transport.detachHeartbeat()
    // A wire the connection left says nothing of the one it is on.
    if (transport !== this.transport) {
      if (this.upgrade.isLeftWire(transport)) this.fallbackToSse()
      return
    }
    // No reconnect reaches a server that speaks this page's wire.
    if (otherWireVersion) {
      this.loseConnection(new NetworkError('The server was updated since this page loaded: reload the page.', true))
      return
    }
    if (rejectedFrame) {
      this.closeAll(rejectedFrameError())
      return
    }
    this.clearAllowance()
    this.upgrade.abortAttempt()
    this.handleTransportLoss(refusal ?? null)
  }

  private handleReconciled(ctrl: ReconciledPayload): void {
    if (!this.upgrade.onReconciled(ctrl)) return
    this.attempt = null
    this.endRecovery()
    if (this.attemptStartedAt !== null) {
      this.handshakeMs = performance.now() - this.attemptStartedAt
      this.attemptStartedAt = null
    }
    this.transport.applyReconciledSettings(ctrl)
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
        void this.upgradeOnceDrained(from)
        break
      case 'stranded':
        this.fallbackToSse()
        break
      case 'flipped':
        // The barrier is the WebSocket's RECONCILE: it beats from here, as any wire does from its RECONCILE, and the
        // server, which judges it from the barrier on, hears from the page while its RECONCILED is on the way.
        this.liveness.beatFromReconcile(this.transport, this.exchange.sessionId)
        for (const frame of end.completion) this.applyCompletionFrame(frame)
        this.releaseJoin(this.upgrade.tryComplete())
        break
    }
    this.onReconcileDue()
  }

  /** An attempt that ended without its barrier goes again once the outbox drained. */
  private async upgradeOnceDrained(from: UpgradeSource): Promise<void> {
    await from.drained()
    this.maybeUpgrade()
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

  onHandoffFailed(): void {
    this.fallbackToSse()
  }

  private fallbackToSse(): void {
    if (this.closed) return
    const abandoned = this.upgrade.abandon()
    this.transport.detachHeartbeat()
    this.transport.abandonActiveTransport()
    this.transport.dispose()
    this.transport = TRANSPORT_REGISTRY[CHANNEL_TRANSPORT.SSE](this.telefuncUrl, this.connectionOptions, this)
    this.handleTransportLoss(null)
    if (abandoned === null) return
    // The old wire's held frames reach their channels once the connection left the wire it gave up, which may have
    // closed: what handling them sends, a flow-control frame or an answer, waits for the reconnect.
    for (const frame of abandoned.old) this.dispatchFrame(frame)
    this.exchange.closeUnacknowledged(abandoned.deferredOmitted)
  }

  private drainBufferedFramesToWire(): void {
    for (const frame of this.gate.drainSendable()) this.transport.sendFrame(frame)
    this.startTtlIfIdle()
  }

  /** A wire lost, or an attempt failed; `refusal`: the words of the answer that refused it, where one came. */
  private handleTransportLoss(refusal: string | null): void {
    if (this.closed) return
    if (this.upgrade.flipped) {
      this.fallbackToSse()
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

    const recovery = this.recoveryFrom()
    const attempt = recovery.attempt
    if (refusal !== null) recovery.refusal = refusal
    if (refusal !== null && attempt === 0) return this.giveUp()
    // A closed channel whose frames the server all has needs nothing from another wire.
    for (const record of this.channels.values()) {
      if (record.state.tag === 'closed' && record.state.delivered) this.gate.release(record)
    }
    if (this.channels.size === 0) {
      this.dispose()
      return
    }
    recovery.attempt = attempt + 1
    // An attempt that couldn't reconcile before the deadline doesn't start, and the deadline ends the channels. A loss
    // counts no later than its cut was due, which a late timer passed.
    const delay = this.attempt?.nextDelay ?? reconnectDelay(attempt)
    const at = Math.min(performance.now(), this.attempt?.cutAt ?? Infinity)
    this.attempt = null
    this.enterReconnecting(at <= this.latestEnd(delay, recovery.deadline, this.handshakeMs ?? 0) ? delay : null)
  }

  /** The latest moment an attempt can end and the next, `delay` later, still reconcile by `deadline` if it takes
   *  `needMs`: where a loss still starts the next attempt, and where an attempt is cut. Both pass the last handshake:
   *  this boundary decides which successor is admitted, and one exactly as slow as the last open arrives at the
   *  deadline's own instant, too late; only a faster one is in time. With none measured, starting an attempt costs
   *  nothing alive and needs nothing, while a cut costs the attempt it cuts and leaves the next at least what any
   *  attempt is given. */
  private latestEnd(delay: number, deadline: number, needMs: number): number {
    return deadline - delay - needMs
  }

  /** The recovery under way, or one from now: the server holds the page's channels until its ping deadline noticed
   *  the loss, then `reconnectTimeout`, so the page tries until then and not past it. */
  private recoveryFrom(): Recovery {
    if (this.recovery) return this.recovery
    const windowMs = this.reconnectWindow()
    const timer = setTimeout(() => this.onRecoveryDeadline(), windowMs)
    this.recovery = { attempt: 0, deadline: performance.now() + windowMs, refusal: null, timer }
    return this.recovery
  }

  /** No wire reconciled in time: the channels end, and the attempt under way closes with them. */
  private onRecoveryDeadline(): void {
    if (this.closed || !this.recovery) return
    this.giveUp()
  }

  /** The connection gives up: each channel ends with the refusal this recovery heard last, else with whether the
   *  server ever attached it. */
  private giveUp(): void {
    const refusal = this.recovery?.refusal ?? null
    for (const record of this.channels.values()) {
      const words = refusal ?? (this.channels.isInitial(record) ? 'Connection not established' : 'Connection dropped')
      record.channel._onConnectionLost(new NetworkError(words, true))
    }
    this.dispose()
  }

  private endRecovery(): void {
    if (!this.recovery) return
    clearTimeout(this.recovery.timer)
    this.recovery = null
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
    this.clearReconnectTimer()
    this.clearAllowance()
    this.endRecovery()
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
    this.endedByServer(record)
    record.channel._onEnded(err)
  }

  /** The server ended `channel`, as its verdict does, which the page then sends nothing more on. */
  serverEnded(channel: MuxChannel): void {
    const record = this.channels.of(channel)
    // Its verdict reaches it on this connection, which it is still registered on.
    assert(record !== undefined)
    this.endedByServer(record)
  }

  /** The page owes nothing more on a channel the server ended: nothing of it is replayed or sent. */
  private endedByServer(record: ChannelRecord): void {
    this.unregister(record.channel)
    assert(record.state.tag === 'closed')
    this.channels.serverEnded(record, record.state)
    this.gate.dropQueue(record.ix)
  }

  /** No wire can reach the server again: its channels' connection is lost for good. */
  private loseConnection(err: Error): void {
    for (const { channel } of this.channels.values()) channel._onConnectionLost(err)
    this.dispose()
  }

  private closeAll(err: Error): void {
    for (const { channel } of this.channels.values()) {
      channel._onEnded(err)
    }
    this.dispose()
  }
}

/** How long the page waits before reconnect `attempt` (0 after a loss): a random part of a delay that doubles from
 *  `CHANNEL_RECONNECT_INITIAL_DELAY_MS` to `CHANNEL_RECONNECT_MAX_DELAY_MS`, so pages that lost their wires together,
 *  to a proxy or a server that restarted, don't come back together, on the first attempt or at the cap. */
function reconnectDelay(attempt: number): number {
  return Math.random() * Math.min(CHANNEL_RECONNECT_INITIAL_DELAY_MS * 2 ** attempt, CHANNEL_RECONNECT_MAX_DELAY_MS)
}

/** How long an attempt may go unanswered before it is cut: three times as long as the last attempt the server answered
 *  took, and never under the minimum, a first connect too; growing with each attempt cut since. */
function attemptAllowance(answeredMs: number | null, cuts: number): number {
  return (
    Math.max(ATTEMPT_ALLOWANCE_MIN_MS, ATTEMPT_ALLOWANCE_OPENS * (answeredMs ?? 0)) * ATTEMPT_ALLOWANCE_GROWTH ** cuts
  )
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
    return new ChannelOverflowError(
      `Channel closed: the server couldn't hold more for this client. What it had yet to receive, or what it sent that the server was still working on, passed what the server holds for one channel (${CHANNEL_HOLD_MAX_BYTES / 2 ** 20} MiB by default) or config.channel.maxMemory. Read messages faster, send less or await each send and publish(), pass \`lossy: true\` to a BroadcastChannel that may skip to the newest, or raise config.channel.maxMemory`,
    )
  }
  if (reason === ERROR_REASON.CLOSE_TIMED_OUT) return closeTimedOutError()
  if (reason === ERROR_REASON.NOT_IN_EFFECT) {
    return new ChannelClosedError(
      'Broadcast closed: its subscription did not take effect on the server. Get the broadcast from a telefunction again to subscribe again.',
    )
  }
  return makeBugError()
}
