export { WireLifecycle }
export type { TransportOwner, TransportEnd, WireLifecycleOwner }

import type { ReceivedFrame } from './pairPublishes.js'
import { parse } from '@brillout/json-serializer/parse'
import { makeAbortError, makeBugError } from '../../../client/remoteTelefunctionCall/errors.js'
import { assert, assertUsage } from '../../../utils/assert.js'
import { endCauseOf } from '../endCause.js'
import { NetworkError } from '../../../shared/NetworkError.js'
import {
  ATTEMPT_ALLOWANCE_GROWTH,
  ATTEMPT_ALLOWANCE_MIN_MS,
  ATTEMPT_ALLOWANCE_OPENS,
  CHANNEL_CLIENT_REPLAY_BUFFER_BYTES,
  CHANNEL_IDLE_TIMEOUT_MS,
  CHANNEL_RECONNECT_INITIAL_DELAY_MS,
  CHANNEL_RECONNECT_MAX_DELAY_MS,
  CHANNEL_RECONNECT_TIMEOUT_MS,
  CHANNEL_TRANSPORT,
  MAX_CHANNELS_PER_CONNECTION,
  RECONCILE_FLOOR_BYTES_PER_MS,
  type ChannelTransport,
  WIRE_MAX_RAW_FRAME_BYTES,
} from '../../constants.js'
import { ReplayBuffer } from './ReplayBuffer.js'
import { isSequencedFrame, tellsLastRead } from '../../wire/frame.js'
import { SEQ_OFFSET, readU16, readU32 } from '../../wire/header.js'
import { TAG, isSequencedTag } from '../../wire/tags.js'
import type { ChannelFrame, PongEntry } from '../../wire/frame.js'
import type { ReconciledPayload } from '../../wire/payloads.js'

import { ChannelTable, type ChannelRecord } from './ChannelTable.js'
import { SendQueue, type SendQueueOwner } from './SendQueue.js'
import { Liveness, type LivenessOwner } from './Liveness.js'
import { Upgrade, type JoinRelease, type UpgradeOwner, type UpgradeSource, type UpgradeTarget } from './Upgrade.js'
import { AttachExchange, type AttachExchangeOwner, type ReconcileBatch } from './AttachExchange.js'

import { reconnectWindowOf } from '../../wire/pingDeadline.js'

import { WsTransport } from './WsTransport.js'
import { SseTransport } from './SseTransport.js'

import type { ClientChannelTransport, ClientConnectionOptions, MuxChannel } from './ClientConnection.js'

/** Why a transport's wire closed, where it knows more than that it was lost. */
type TransportEnd = {
  /** What the server answered the wire's opening with, which wasn't a wire. */
  refusal?: string
  /** The server runs another `WIRE_VERSION`. */
  otherWireVersion?: boolean
  /** A request carrying what the page sent was refused with this answer (see `onTransportRefused`). */
  refused?: string
  /** The wait the answer that ended the wire asked for before the next try (its `Retry-After`). */
  retryAfterMs?: number
  /** Telefunc said it no longer had the wire (`retry`): a loss, with that cause. */
  lost?: string
  /** Telefunc doesn't serve the page's SSE requests so (`route`): the page leaves SSE for this connection. */
  route?: string
  /** A WebSocket that ended before it opened, which may take a page whose transports list SSE to SSE. */
  unopened?: boolean
  /** Telefunc ended `channel` for the cause `cause` (`frames` naming it): that channel ends with the cause's error, and
   *  the wire goes as a loss. */
  framesRefused?: { channel: number; cause: number }
  /** Telefunc ended the page's session for the cause with this code: every channel ends with the cause's error. */
  sessionEnded?: number
}

/** What a transport tells its connection, and the reconcile batch it asks for when its wire opens. */
interface TransportOwner {
  /** `startedAt`: when the socket or request that opened started. */
  onTransportOpen(transport: ClientChannelTransport, startedAt: number): void
  /** The server answered the attempt (a WebSocket's open, or the event stream's head before the wire opens), or the
   *  batch POST that carried a later RECONCILE. */
  onTransportAnswered(transport: ClientChannelTransport): void
  onTransportClosed(transport: ClientChannelTransport, end?: TransportEnd): void
  /** A request carrying `frames` was refused with `answer`. Whether the connection is to be established again, so that
   *  its RECONCILED says what the server has: false only where every channel whose data it refused ended. */
  onTransportRefused(frames: Uint8Array<ArrayBuffer>[], answer: string): boolean
  onTransportFrame(frame: ReceivedFrame, source: ClientChannelTransport, byteLength: number, abandoned: boolean): void
  onTransportPong(ended: PongEntry[]): void
  /** A piece of a RECONCILED came on a WebSocket, the RECONCILED not yet whole. */
  onTransportReconciledPiece(transport: ClientChannelTransport): void
  /** The transport may have headroom again, so the frames waiting for it are sent. */
  onTransportHeadroom(transport: ClientChannelTransport): void
  /** The wire's upload fell back to batch POSTs; a channel may have opened before that (see `onTransportOpen`). */
  onTransportBatched(transport: ClientChannelTransport): void
  /** An SSE wire's RECONCILE went out with the request that opens it, of `bytes`, before the wire opens (see
   *  `beatFromReconcile`), or a later one in a batch POST of `bytes`. */
  onTransportReconcileSent(transport: ClientChannelTransport, bytes: number): void
  /** The most a batch POST carries, which a 413 taught the connection: `Infinity` before one. */
  readonly ssePostLimit: number
  /** The channels whose data the server hasn't confirmed since a loss (`ReplayBuffer.stall`). */
  readonly stalledChannels: ReadonlySet<number>
  /** A POST of stalled channels' data alone got no answer, or one that asks for later, while its wire's event stream is
   *  up. Returns how long its data waits before it goes again. */
  onStalledPostLost(cause: string): number
  /** Whether the server lacks sequenced `frame`, of a channel still carried. */
  stillOwed(frame: Uint8Array<ArrayBuffer>): boolean
  /** A batch POST of `bodyBytes` was refused with a 413: none carries more than half of it from now on. */
  onTransportTooLarge(bodyBytes: number): void
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

type Recovery = { tries: number; deadline: number; lost: Error | null; timer: ReturnType<typeof setTimeout> }

/** The idle timer, from when no channel is open and nothing is queued. `confirming`: it expired, and its PING asked the
 *  server for what the closed channels sent (`disposeOnceDrained`). */
type IdleTimer = { timer: ReturnType<typeof setTimeout>; confirming: boolean }

/** The attempt under way, from its start till its RECONCILED or its loss. */
type Attempt = {
  /** When it started: its allowance runs from here. */
  readonly startedAt: number
  /** When the socket or request that opened started: the handshake's sample. */
  openStartedAt: number
  /** The backoff its end waits, drawn as it starts. */
  readonly nextDelay: number
  /** When it is cut unanswered: null where it runs to the deadline, or once the server answered it. */
  cutAt: number | null
  /** The latest cut that leaves the next attempt its room (`startWire`). */
  readonly latestCut: number
  /** Cuts it at `cutAt`, till it ran, the server answered or the wire closed. */
  cutTimer: ReturnType<typeof setTimeout> | null
}

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
class WireLifecycle implements TransportOwner, SendQueueOwner, LivenessOwner, UpgradeOwner, AttachExchangeOwner {
  private readonly owner: WireLifecycleOwner
  private readonly telefuncUrl: string
  private readonly connectionOptions: ClientConnectionOptions
  transport: ClientChannelTransport

  private state: ConnectionState = { tag: 'fresh' }
  /** From a first connect's start, or the first wire lost since the last RECONCILED: the attempts made (a wire that
   *  opens and never reconciles counts), why the last wire or attempt went where its end said, and the deadline whose
   *  timer ends the channels if no wire reconciled by then. */
  private recovery: Recovery | null = null
  /** The attempts made since a recovery started from none (a wire that opens and never reconciles counts). A RECONCILED
   *  doesn't reset them while a channel's data waits since an earlier loss: the delays keep doubling to the cap. */
  private attempt = 0
  /** When the page last read a frame from the server, a PONG or a RECONCILED included: a channel's date is judged only
   *  once one came after it, whatever the transport. */
  private readAt = Number.NEGATIVE_INFINITY
  /** The attempt under way, cut where the server hasn't answered it by its allowance (`attemptAllowance`). */
  private underWay: Attempt | null = null
  /** How long the next attempt may go unanswered before it is cut (`attemptAllowance`): it doubles with each attempt cut
   *  since the last one the server answered. */
  private allowance = ATTEMPT_ALLOWANCE_MIN_MS
  /** How long the last wire took from its socket's or request's start to its RECONCILED: the room an attempt needs
   *  before the deadline. */
  private handshakeMs: number | null = null
  /** The wire on which a request carrying only the connection's own frames was refused, until a PONG came. */
  private ownFramesRefusedOn: number | null = null
  /** Half of the least batch POST a 413 refused. The connection's, not a transport's: an upgrade to WebSocket and the
   *  fallback after it make new transports, behind the same proxy. */
  private postLimit = Infinity
  private idle: IdleTimer | null = null
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
  readonly sendQueue = new SendQueue({ owner: this, channels: this.channels })
  private readonly liveness = new Liveness({ owner: this, channels: this.channels, sendQueue: this.sendQueue })
  private readonly exchange = new AttachExchange({ owner: this, channels: this.channels, sendQueue: this.sendQueue })
  private readonly upgrade: Upgrade
  /** The page's transports include a WebSocket. */
  private readonly allowsWebSocket: boolean
  /** How many WebSockets ended unopened in a row, a wire that opens breaking the row: null till a WebSocket opened on
   *  this connection. */
  private unopenedInARow: number | null = null
  /** The transports the server allows, from the last settled RECONCILED: an upgrade may start once it came, whether
   *  before or after the SSE wire opened. */
  private serverTransports: ReconciledPayload['transports'] | null = null
  private reconnectTimeoutMs = CHANNEL_RECONNECT_TIMEOUT_MS
  private idleTimeoutMs: number
  /** The largest credit window, each way, from the last RECONCILED. */
  private creditWindow: number | null = null
  /** The largest frame the server accepts on a WebSocket, from the last RECONCILED. */
  private maxFrameBytes = WIRE_MAX_RAW_FRAME_BYTES
  constructor({ owner, telefuncUrl, options }: WireLifecycleOptions) {
    this.owner = owner
    this.telefuncUrl = telefuncUrl
    this.connectionOptions = options
    this.upgrade = new Upgrade({ owner: this })
    this.allowsWebSocket = options.transports.includes(CHANNEL_TRANSPORT.WS)
    this.idleTimeoutMs = options.idleTimeout ?? CHANNEL_IDLE_TIMEOUT_MS
    this.transport = TRANSPORT_REGISTRY[options.transports[0]!](telefuncUrl, options, this)
  }

  /** The largest frame the server accepts from `channel`; on a WebSocket, a runtime's lower limit wins. */
  maxFrameBytesOf(channel: MuxChannel): number {
    if (!this.allowsWebSocket) return WIRE_MAX_RAW_FRAME_BYTES
    return Math.min(channel._maxFrameBytes, this.maxFrameBytes)
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
    this.stopCut()
    const nextDelay = reconnectDelay(this.attempt, this.anyStalled())
    // A cut leaves the next attempt its room; where none is left, this attempt runs to the deadline.
    const room = this.latestEnd(nextDelay, recovery.deadline, this.handshakeMs ?? ATTEMPT_ALLOWANCE_MIN_MS) - now
    const allowance = Math.min(this.allowance, room)
    const cutAt = room > 0 ? now + allowance : null
    const attempt: Attempt = {
      startedAt: now,
      openStartedAt: now,
      nextDelay,
      cutAt,
      latestCut: now + room,
      cutTimer: null,
    }
    if (cutAt !== null) attempt.cutTimer = setTimeout(() => this.cutAttempt(attempt), allowance)
    this.underWay = attempt
    this.transport.start()
  }

  /** The attempt under way wasn't answered in time: it goes, and counts as failed. A page that left let it go. */
  private cutAttempt(attempt: Attempt): void {
    attempt.cutTimer = null
    if (this.closed || !this.transport.isConnecting()) return
    this.allowance *= ATTEMPT_ALLOWANCE_GROWTH
    this.transport.cancelAttempt()
    this.onTransportClosed(this.transport, { unopened: this.transport.type === CHANNEL_TRANSPORT.WS })
  }

  /** The attempt under way sent a request body of `bytes`: it is cut no sooner than that body and its answer take at the
   *  floor, as its RECONCILE's deadline counts them, within the room the next attempt needs, as every cut is. */
  private allowForBody(bytes: number): void {
    const underWay = this.underWay
    if (underWay === null || underWay.cutAt === null) return
    // `fetch` shows no upload's progress: behind a proxy that holds the body, a silent server looks the same till then.
    const cutAt = Math.min(underWay.startedAt + (2 * bytes) / RECONCILE_FLOOR_BYTES_PER_MS, underWay.latestCut)
    if (cutAt <= underWay.cutAt) return
    this.stopCut()
    underWay.cutAt = cutAt
    underWay.cutTimer = setTimeout(() => this.cutAttempt(underWay), cutAt - performance.now())
  }

  /** The server answered the attempt under way; from here its RECONCILE's deadline bounds it. */
  private attemptAnswered(): void {
    const underWay = this.underWay
    if (underWay === null) return
    underWay.cutAt = null
    this.stopCut()
    this.allowance = attemptAllowance(performance.now() - underWay.startedAt)
    this.exchange.restartReconcileDeadline()
  }

  private stopCut(): void {
    const underWay = this.underWay
    if (underWay === null || underWay.cutTimer === null) return
    clearTimeout(underWay.cutTimer)
    underWay.cutTimer = null
  }

  /** The attempt under way ended, with its RECONCILED or its loss: what it was, if one was. */
  private endAttempt(): Attempt | null {
    const underWay = this.underWay
    this.stopCut()
    this.underWay = null
    return underWay
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

  get liveWire(): boolean {
    return this.connected && !this.upgrade.gatesSends && this.exchange.liveWire
  }

  takesLive(record: ChannelRecord): boolean {
    return this.liveWire && this.exchange.takesLive(record)
  }

  /** The wire can't take a RECONCILE now: it isn't open, or an upgrade is between its READY and its handoff. */
  private get reconcileHeld(): boolean {
    return !this.connected || this.upgrade.holdsReconcile
  }

  /** Sends the RECONCILE a registration owes, where the wire takes one; no-op when none is owed. While the wire can't
   *  (connecting, awaiting a RECONCILED, from an upgrade's READY until its handoff) it stays owed: `reconcileOwed` keeps
   *  `sendsOpen` false, so data frames buffer behind it, and the wire's open, an upgrade attempt's end and its join's end
   *  call this again. A RECONCILED names what registered meanwhile in its follow-up, but the handoff's leaves that to the
   *  join's end. */
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
        this.sendQueue.release(record)
        break
      }
    }
    assertUsage(
      this.channels.size < MAX_CHANNELS_PER_CONNECTION,
      `Too many channels on one connection (${MAX_CHANNELS_PER_CONNECTION} max) — open another with \`connectionKey\``,
    )
    this.stopIdle()
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
    if (transport !== this.transport || (!this.sendsOpen && !this.liveWire)) return
    this.sendQueue.sendRunnable()
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
    if (transport !== this.transport) return
    if (this.underWay === null) this.exchange.restartReconcileDeadline()
    else this.attemptAnswered()
  }

  onTransportOpen(transport: ClientChannelTransport, startedAt: number): void {
    if (transport !== this.transport) return
    if (transport.type === CHANNEL_TRANSPORT.WS || this.unopenedInARow !== null) this.unopenedInARow = 0
    // The handshake's sample is the opened socket's own: one that hung beside it adds nothing. SSE's RECONCILED may
    // have come first, and taken it.
    if (this.underWay !== null) this.underWay.openStartedAt = startedAt
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

  onTransportFrame(frame: ReceivedFrame, source: ClientChannelTransport, byteLength: number, abandoned: boolean): void {
    // An abandoned wire still delivers, but what answers its RECONCILE answers one the next wire's has replaced.
    this.readAt = performance.now()
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
  private dispatchFrame(frame: ReceivedFrame): void {
    try {
      this.handleFrame(frame)
    } catch (err) {
      queueMicrotask(() => {
        throw err
      })
    }
  }

  private handleFrame(frame: ReceivedFrame): void {
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
        this.closeRemoteChannel(frame.index, channelErrorFor(frame.reason, this.reconnectTimeoutMs))
        this.startTtlIfIdle()
        return
    }
    // PING/PONG/PIECE/PIECES_ACK/ANNOUNCE/RECONCILE/STREAM_REQUEST_OPEN_ACK never reach `dispatchFrame` —
    // transports peel them off in their own receive paths. Everything that lands
    // here is per-channel and carries `index`.
    const channelFrame = frame as ReceivedFrame<ChannelFrame>
    const record = this.channels.get(channelFrame.index)
    // A closed channel stays listed only for what it still has to tell the server.
    if (record && record.state.tag !== 'closed') record.channel._dispatchFrame(channelFrame)
  }

  onTransportReconcileSent(transport: ClientChannelTransport, bytes: number): void {
    if (this.closed || transport !== this.transport) return
    if (this.underWay === null) return this.exchange.reconcileCarried(bytes)
    this.allowForBody(bytes)
    this.liveness.beatFromReconcile(transport, this.exchange.sessionId)
  }

  get ssePostLimit(): number {
    return this.postLimit
  }

  get stalledChannels(): ReadonlySet<number> {
    const stalled = new Set<number>()
    for (const { ix, replay } of this.channels.values()) if (replay.stall !== null) stalled.add(ix)
    return stalled
  }

  onTransportTooLarge(bodyBytes: number): void {
    // A lone frame's body may be larger than what the page learned: it never raises that.
    this.postLimit = Math.min(this.postLimit, Math.floor(bodyBytes / 2))
  }

  onTransportPong(ended: PongEntry[]): void {
    this.readAt = performance.now()
    this.ownFramesRefusedOn = null
    this.liveness.onPong(ended)
    if (this.idle?.confirming) this.startTtlIfIdle()
  }

  onTransportReconciledPiece(transport: ClientChannelTransport): void {
    if (transport === this.transport) this.exchange.reconciledPieceArrived()
  }

  onReconcileLate(transport: ClientChannelTransport): void {
    // Till the server answered the request that carries it, the attempt's allowance bounds it.
    if (this.underWay !== null && this.underWay.cutTimer !== null) return
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
      refused,
      retryAfterMs = 0,
      lost,
      route,
      framesRefused,
      sessionEnded,
      unopened = false,
    }: TransportEnd = {},
  ): void {
    if (this.closed) return
    transport.detachHeartbeat()
    // What the server ended on the page's session holds whichever of its wires said it.
    if (sessionEnded !== undefined) {
      this.closeAll(channelErrorFor(sessionEnded, this.reconnectTimeoutMs))
      return
    }
    if (framesRefused !== undefined) this.endRefusedChannel(framesRefused)
    // A wire the connection left says nothing more of the one it is on.
    if (transport !== this.transport) {
      if (this.upgrade.isLeftWire(transport)) this.fallbackToSse()
      return
    }
    // No reconnect reaches a server that speaks this page's wire.
    if (otherWireVersion) {
      const err = new NetworkError('The server was updated since this page loaded: reload the page.', true)
      for (const { channel } of this.channels.values()) channel._onConnectionLost(err)
      this.dispose()
      return
    }
    if (route !== undefined) {
      this.leaveSse(new NetworkError(`The server refused the page's SSE requests: ${route}`, true))
      return
    }
    this.stopCut()
    this.upgrade.abortAttempt()
    // One that never opened takes the page to SSE, unless a WebSocket opened before: then only a second in a row does.
    if (unopened && (this.unopenedInARow === null || ++this.unopenedInARow > 1)) this.leaveWsWhereSse()
    const why = refused === undefined ? (lost ?? refusal) : `The server refused what the page sent with ${refused}`
    this.handleTransportLoss(
      why === undefined ? null : new NetworkError(why, true),
      refusal !== undefined,
      retryAfterMs,
    )
  }

  /** The server ended the channel for `cause`, which it ends with at once: nothing of it goes again. A RECONCILE tells
   *  the server, ending it there. */
  private endRefusedChannel({ channel, cause }: { channel: number; cause: number }): void {
    const record = this.channels.get(channel)
    if (record === undefined) return
    this.sendQueue.release(record)
    this.exchange.scheduleRegisterReconcile()
    record.channel._onEnded(channelErrorFor(cause, this.reconnectTimeoutMs))
  }

  /** The server doesn't serve the page's SSE requests: with WebSocket next in the page's order, the connection goes on
   *  over one, as a page whose order starts with it does; else every channel ends with `err`. A new connection tries the
   *  order again. */
  private leaveSse(err: Error): void {
    const order = this.connectionOptions.transports
    if (order.indexOf(CHANNEL_TRANSPORT.WS) <= order.indexOf(CHANNEL_TRANSPORT.SSE)) {
      this.closeAll(err)
      return
    }
    this.transport.abandonActiveTransport()
    this.transport.dispose()
    this.transport = TRANSPORT_REGISTRY[CHANNEL_TRANSPORT.WS](this.telefuncUrl, this.connectionOptions, this)
    this.handleTransportLoss(err, false)
  }

  /** Where the page's transports list SSE, the next attempt goes over it, and upgrades once a WebSocket opens. */
  private leaveWsWhereSse(): void {
    if (!this.connectionOptions.transports.includes(CHANNEL_TRANSPORT.SSE)) return
    if (this.unopenedInARow !== null) this.unopenedInARow = 0
    this.transport.dispose()
    this.transport = TRANSPORT_REGISTRY[CHANNEL_TRANSPORT.SSE](this.telefuncUrl, this.connectionOptions, this)
  }

  /** Counts the refusal against the channels whose sequenced frames are among `frames`, through the highest seq
   *  refused: one whose frames were refused on an earlier wire too, so after the connection was established again, ends.
   *  A 200 for another request clears nothing: what counts is what the server has of each channel. A request that
   *  carried none of their data counts against the connection (`refusedOwnFrames`). */
  onTransportRefused(frames: Uint8Array<ArrayBuffer>[], answer: string): boolean {
    const ownFramesOnly = !frames.some((frame) => frame[0] !== undefined && isSequencedTag(frame[0]))
    if (ownFramesOnly) return this.refusedOwnFrames(answer)
    const refused = new Set<ChannelRecord>()
    const refusedAgain = new Set<ChannelRecord>()
    for (const frame of frames) {
      const tag = frame[0]
      if (tag === undefined || !isSequencedTag(tag)) continue
      const record = this.channels.get(readU16(frame, 1))
      if (record === undefined || refusedAgain.has(record)) continue
      refused.add(record)
      const seq = this.channels.sentThrough(record.ix, readU32(frame, SEQ_OFFSET))
      // A refusal of data the server has since confirmed explains nothing any more.
      const before =
        record.refused !== null && record.replay.acknowledged < record.refused.throughSeq ? record.refused : null
      if (before !== null && before.wire < this.wire && seq <= before.throughSeq) refusedAgain.add(record)
      else record.refused = { wire: this.wire, throughSeq: Math.max(seq, before?.throughSeq ?? 0) }
    }
    // Released, which a RECONCILE tells the server, ending it there: one on this wire, or the next wire's.
    if (refusedAgain.size > 0) this.exchange.scheduleRegisterReconcile()
    for (const record of refusedAgain) {
      this.sendQueue.release(record)
      record.channel._onEnded(
        new NetworkError(
          `Channel closed: what the page sent on it was refused with ${answer}, again after a reconnect. Something between the page and the server refuses it, such as a proxy's limit on the size of a request.`,
          true,
        ),
      )
    }
    return refusedAgain.size === 0 || refused.size > refusedAgain.size
  }

  /** A refused request that carried only the connection's own frames, as its PINGs: it ends once they were refused on
   *  an earlier wire too and no PONG came since, naming `answer`. Whether the connection is to be established again. */
  private refusedOwnFrames(answer: string): boolean {
    const before = this.ownFramesRefusedOn
    if (before === null || before === this.wire) {
      this.ownFramesRefusedOn = this.wire
      return true
    }
    this.closeAll(
      new NetworkError(
        `Connection closed: the page's requests to keep its connection were refused with ${answer}, again after a reconnect. Something between the page and the server refuses them.`,
        true,
      ),
    )
    return false
  }

  private handleReconciled(ctrl: ReconciledPayload): void {
    if (!this.upgrade.onReconciled(ctrl)) return
    const attempt = this.endAttempt()
    this.endRecovery()
    if (attempt !== null) this.handshakeMs = performance.now() - attempt.openStartedAt
    this.transport.applyReconciledSettings(ctrl)
    this.takeServerSettings(ctrl)
    const outcome = this.exchange.applyReconciled(ctrl, {
      deferOmitted: this.upgrade.handingOff,
      holdsReconcile: this.upgrade.holdsReconcile,
    })
    this.upgrade.deferOmitted(outcome.omitted)
    this.liveness.install(this.transport, ctrl.pingInterval)
    this.transport.closeAbandonedTransport()
    for (const frame of outcome.frames) this.transport.sendFrame(frame)
    this.exchange.batchHanded()
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
    this.unopenedInARow = 0
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
    const wire = this.wire
    const to = new WsTransport({ telefuncUrl: this.telefuncUrl, owner: this })
    const end = await this.upgrade.run(
      from,
      to,
      { sessionId, pingInterval: this.liveness.pingInterval, attempt: this.exchange.nextAttempt() },
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
        // The server may have taken the barrier, and its probe is gone: the old wire goes, and the next one upgrades.
        if (this.wire === wire) this.dropWire(from)
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
  private applyCompletionFrame(frame: ReceivedFrame): void {
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
    this.onReconcileDue()
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
    for (const frame of this.sendQueue.drainSendable()) this.transport.sendFrame(frame)
    this.startTtlIfIdle()
  }

  /** A wire lost, or an attempt failed. `lost`: why, where its end said so; `rejected`: the server refused the attempt
   *  with an answer that wasn't a wire; `retryAfterMs`: the least wait before the next attempt that the answer asked for. */
  private handleTransportLoss(lost: Error | null, rejected = false, retryAfterMs = 0): void {
    if (this.closed) return
    if (this.upgrade.flipped) {
      this.fallbackToSse()
      return
    }
    this.wire++
    // The wire is dying: the queued RECONCILE is not sent, since the reconnect's RECONCILE lists every channel.
    this.exchange.cancelRegisterReconcile()
    this.exchange.forgetWire()
    this.stopIdle()

    const now = performance.now()
    // At once with the recovery's timer, its end wins: every channel is told its cause.
    if (this.recovery !== null && now >= this.recovery.deadline) return this.onRecoveryDeadline()
    // Data that has waited since an earlier loss: the attempts go on from where they were, at no less than their step.
    const stalled = this.anyStalled()
    if (this.recovery === null && !stalled) this.attempt = 0
    const recovery = this.recoveryFrom()
    const tries = recovery.tries
    if (lost !== null) recovery.lost = lost
    if (rejected && tries === 0) return this.giveUp()
    // A closed channel whose frames the server all has needs nothing from another wire.
    for (const record of this.channels.values()) {
      if (record.state.tag === 'closed' && record.state.delivered) this.sendQueue.release(record)
      else record.replay.dateStall(now)
    }
    this.endStalled(now, lost ?? new NetworkError('Connection dropped', true))
    if (this.channels.size === 0) {
      this.dispose()
      return
    }
    recovery.tries = tries + 1
    const attempt = this.attempt++
    // An attempt that couldn't reconcile before the deadline doesn't start, and the deadline ends the channels. A loss
    // counts no later than its cut was due, which a late timer passed.
    const ended = this.endAttempt()
    const delay = Math.max(ended?.nextDelay ?? reconnectDelay(attempt, stalled), retryAfterMs)
    const at = Math.min(now, ended?.cutAt ?? Infinity)
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

  /** A POST of stalled channels' data alone got no answer, or one that asks for later, while its wire's event stream is
   *  up (`SseTransport.hold`): the wire goes on, and that try is those channels' loss, judged as at a wire's
   *  (`endStalled`). Returns how long its data waits before it goes again: the stalled schedule's next attempt. */
  onStalledPostLost(cause: string): number {
    const lost = new NetworkError(cause, true)
    const released = this.endStalled(performance.now(), lost)
    // Released: a RECONCILE tells the server, ending it there.
    if (released) this.exchange.scheduleRegisterReconcile()
    return reconnectDelay(this.attempt++, true)
  }

  /** Whether the server lacks sequenced `frame`, of a channel still carried. */
  stillOwed(frame: Uint8Array<ArrayBuffer>): boolean {
    const record = this.channels.get(readU16(frame, 1))
    if (record === undefined) return false
    return this.channels.sentThrough(record.ix, readU32(frame, SEQ_OFFSET)) > record.replay.acknowledged
  }

  /** Whether a channel's data has waited for the server since an earlier loss. */
  private anyStalled(): boolean {
    for (const { replay } of this.channels.values()) if (replay.stall !== null) return true
    return false
  }

  /** Ends each channel, a closed one too, whose data the server hasn't confirmed for over `reconnectTimeout` of
   *  reconnects: dated at a loss, and the page read something from the server since, so a server was there to confirm
   *  it. With nothing read at all, the recovery decides. A RECONCILE leaves it out, which ends it on the server. */
  private endStalled(now: number, lost: Error): boolean {
    let released = false
    for (const record of [...this.channels.values()]) {
      const { stall } = record.replay
      if (stall === null || this.readAt <= stall.since || now - stall.since <= this.reconnectTimeoutMs) continue
      this.sendQueue.release(record)
      record.channel._onEnded(
        new NetworkError(
          `Channel closed: the server hasn't confirmed what the page sent on it for over ${Math.round(this.reconnectTimeoutMs / 1000)} s of reconnects. The last try: ${lost.message}`,
          true,
        ),
      )
      released = true
    }
    return released
  }

  /** The recovery under way, or one from now: the server holds the page's channels until its ping deadline noticed
   *  the loss, then `reconnectTimeout`, so the page tries until then and not past it. */
  private recoveryFrom(): Recovery {
    if (this.recovery) return this.recovery
    const windowMs = this.reconnectWindow()
    const timer = setTimeout(() => this.onRecoveryDeadline(), windowMs)
    this.recovery = { tries: 0, deadline: performance.now() + windowMs, lost: null, timer }
    return this.recovery
  }

  /** No wire reconciled in time: the channels end, and the attempt under way closes with them. */
  private onRecoveryDeadline(): void {
    if (this.closed || !this.recovery) return
    this.giveUp()
  }

  /** The connection gives up: each channel ends with why this recovery's last wire or attempt went where its end said,
   *  else with whether the server ever attached it. */
  private giveUp(): void {
    const lost = this.recovery?.lost ?? null
    for (const record of this.channels.values()) {
      const words = this.channels.isInitial(record) ? 'Connection not established' : 'Connection dropped'
      record.channel._onConnectionLost(lost ?? new NetworkError(words, true))
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
    // The expired timer whose PING asked for the closed channels' frames holds till the server has them all.
    if (this.idle?.confirming && this.channels.delivered) return this.dispose()
    if (!this.connected || this.idle || !this.sendQueue.idle || !this.channels.idle) {
      return
    }
    const idle: IdleTimer = {
      timer: setTimeout(() => void this.disposeOnceDrained(idle), this.idleTimeoutMs),
      confirming: false,
    }
    this.idle = idle
  }

  /** Waits for queued frames to go. If the server still owes confirmation, keep the expired timer and ask with a PING. */
  private async disposeOnceDrained(idle: IdleTimer): Promise<void> {
    await this.transport.drained()
    if (this.idle !== idle || !this.channels.idle) return
    if (this.channels.delivered) return this.dispose()
    idle.confirming = true
    this.liveness.pingNow(this.transport)
  }

  private stopIdle(): void {
    if (this.idle === null) return
    clearTimeout(this.idle.timer)
    this.idle = null
  }

  private dispose(): void {
    if (this.closed) return
    this.stopIdle()
    this.exchange.cancelRegisterReconcile()
    // Tear down any in-flight phase before transitioning to `closed`.
    this.clearReconnectTimer()
    this.stopCut()
    this.endRecovery()
    this.upgrade.abandon()
    this.enterClosed()
    this.transport.detachHeartbeat()
    this.transport.dispose()
    this.channels.clear()
    this.sendQueue.clear()
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
    this.sendQueue.dropQueue(record.ix)
    // What its frames held back may go.
    this.sendQueue.sendRunnable()
  }

  private closeAll(err: Error): void {
    for (const { channel } of this.channels.values()) {
      channel._onEnded(err)
    }
    this.dispose()
  }
}

/** How long the page waits before reconnect `attempt` (0 after a loss): a step that doubles from
 *  `CHANNEL_RECONNECT_INITIAL_DELAY_MS` to `CHANNEL_RECONNECT_MAX_DELAY_MS`, drawn at random so that pages that lost
 *  their wires together, to a proxy or a server that restarted, don't come back together, on the first attempt or at
 *  the cap: anywhere in the step, or, while a channel's data has waited since an earlier loss (`stalled`), between the
 *  step and half a step more, so what can't get through goes again at most once a step. */
function reconnectDelay(attempt: number, stalled: boolean): number {
  const step = Math.min(CHANNEL_RECONNECT_INITIAL_DELAY_MS * 2 ** attempt, CHANNEL_RECONNECT_MAX_DELAY_MS)
  return stalled ? (1 + Math.random() / 2) * step : Math.random() * step
}

/** How long an attempt may go unanswered before it is cut, after one the server answered in `answeredMs`: three times
 *  as long, and never under the minimum, a first connect's too. */
function attemptAllowance(answeredMs: number): number {
  return Math.max(ATTEMPT_ALLOWANCE_MIN_MS, ATTEMPT_ALLOWANCE_OPENS * answeredMs)
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
function channelErrorFor(reason: number, reconnectTimeoutMs: number): Error {
  return endCauseOf(reason)?.error(reconnectTimeoutMs) ?? makeBugError()
}
