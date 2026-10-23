export { ChannelMux, getChannelMux }
export type { ConnectionEntry, ReconcileOutcome, ServerTransport, Wire, WirePieces }

import { assert } from '../../../utils/assert.js'
import { getGlobalObject } from '../../../utils/getGlobalObject.js'
import { getAdapterMaxFrameBytes, getServerConfig, pingDeadlineOf } from '../../../node/server/serverConfig.js'
import { unrefTimer } from '../../../utils/unrefTimer.js'
import { handleTelefunctionBug } from '../../../node/server/runTelefunc/validateTelefunctionError.js'
import {
  CHANNEL_PING_INTERVAL_MIN_MS,
  CREDIT_MSG_WINDOW_MAX,
  CREDIT_WINDOW_MAX_BYTES,
  MAX_CHANNELS_PER_CONNECTION,
  TIMER_DELAY_MAX_MS,
  UPGRADE_MAX_ID_BYTES,
  UPGRADE_MAX_STAGED_BYTES,
  UPGRADE_MAX_STAGED_RECORDS,
  UPGRADE_STAGE_TTL_MS,
  WIRE_MAX_CONN_CTRL_FRAME_BYTES,
  WIRE_MAX_RAW_FRAME_BYTES,
  WIRE_RECV_BACKLOG_BASE_BYTES,
  WIRE_RECV_BACKLOG_BASE_FRAMES,
  type ChannelTransports,
} from '../../constants.js'
import {
  TAG,
  ProtocolViolationError,
  assertProtocol,
  decodeClientFrame,
  encode,
  isOneFrame,
  messageFrames,
  isConnCtrlTag,
  peekTag,
  seqThrough,
  withTook,
} from '../../wire/connectionFrame.js'
import type {
  BarrierPayload,
  ChannelFrame,
  PingEntry,
  PongEntry,
  PreparePayload,
  ReconcileOpenEntry,
  ReconcilePayload,
} from '../../wire/connectionFrame.js'
import { IndexedPeer, type PeerSender } from './IndexedPeer.js'
import { PathRoom } from '../../flowControl/server/PathRoom.js'
import { ServerPieceReceiver } from '../../flowControl/server/ServerPieceReceiver.js'
import { Deliveries } from '../../flowControl/Deliveries.js'
import { SocketWait } from '../../flowControl/server/SocketWait.js'
import { WireSender } from './WireSender.js'
import { AwaitedChannels, type AwaitedChannel, type RecvBacklog } from './AwaitedChannels.js'
import type { ServerChannel } from './channel.js'

/** A transport-owned connection handle. The mux never looks inside one — it only compares them by
 *  identity and hands them back to the transport that created it. */
type Wire = unknown

// Single-instance kernel: owns channels, sessions, per-connection runtime. Transports talk
// to this class via `onConnectionOpen` and from then on identify connections by object
// identity. Multi-instance deployments rely on sticky sessions at the load balancer.

type ServerTransport<TConnection> = {
  getSessionId(connection: TConnection): string | undefined
  setSessionId(connection: TConnection, sessionId: string): void
  /** Stable per-connection id, or `null` for transports that don't multiplex client→server
   *  traffic across requests (WebSocket: every frame already lands on the same socket). */
  getConnId(connection: TConnection): string | null
  sendNow(connection: TConnection, frame: Uint8Array<ArrayBuffer>): void
  /** Its peer gets no part of a message until all of it arrived, as on a WebSocket (see `pieces.ts`). */
  wholeMessages: boolean
  /** Bytes of sent frames still in the connection, never undercounted; `undefined` if the runtime can't tell. */
  bufferedAmount(connection: TConnection): number | undefined
  /** The bytes of those a frame that goes at once waits behind, where it passes some: else `bufferedAmount`'s. */
  queuedAhead?(connection: TConnection): number | undefined
  terminateConnection(connection: TConnection): void
}

/** Emitted only when `handleFrame` consumed a reconcile. Caller threads the payload through
 *  to `sendReconciled` and fires `finalizeUpgrade` once the new wire's reconciled has emitted.
 *  `finalizeUpgrade` is null when this isn't an SSE→WS upgrade. */
type ReconcileOutcome = {
  sessionId: string
  /** Each `lastSeq` is read as the RECONCILED goes out, so it counts what was dispatched before then. */
  attached: ChannelHandle[]
  finalizeUpgrade: (() => void) | null
  /** The wire this RECONCILED belongs on — a barrier reconciles the staged WS, not the sender. */
  deliverTo: Wire
  upgradeId?: string
}

type StagedUpgrade = {
  upgradeId: string
  prevSessionId: string
  bytes: number
  timer: ReturnType<typeof setTimeout>
  phase: 'staged' | 'committing'
}

function retargetToProbe(err: unknown, probe: Wire): unknown {
  if (!(err instanceof ProtocolViolationError) || err.target !== undefined) return err
  return new ProtocolViolationError(err.message, probe)
}

const textEncoder = new TextEncoder()

type MuxServerOptions = {
  reconnectTimeout: number
  idleTimeout: number
  pingInterval: number
  pingDeadline: number
  serverReplayBuffer: number
  serverReplayBufferBinary: number
  clientReplayBuffer: number
  clientReplayBufferBinary: number
  connectTtl: number
  sseFlushThrottle: number
  ssePostIdleFlushDelay: number
  transports: ChannelTransports
}

const DETACH_REASON = {
  TRANSIENT: 0x01 as const,
  PERMANENT: 0x02 as const,
  RECOVERY_FAILED: 0x03 as const,
}
type DetachReason = (typeof DETACH_REASON)[keyof typeof DETACH_REASON]

type ChannelHandle = { channel: ServerChannel; ix: number; peer: IndexedPeer }

type WirePieces = { room: PathRoom; receiver: ServerPieceReceiver }

type ConnectionState = {
  pingTimer: ReturnType<typeof setTimeout> | null
  /** When the wire last delivered a frame, or the ping deadline was last set. */
  lastReceivedAt: number
  /** Each frame, piece and announcement the wire delivered. */
  deliveries: Deliveries
  /** The number of the delivery that completed the frame it's handling. */
  frameDelivery: number
  /** When the last PONG went out. */
  pongedAt: number
  pieces: WirePieces | null
  terminatePermanently: boolean
  recvChain: Promise<unknown> | null
  /** Set by `onConnectionClosed` so an in-flight `reconcile` can see the close and its kind. */
  closed: { isPermanent: boolean } | null
  retiredByBarrier: boolean
  recvBacklog: RecvBacklog
  awaited: AwaitedChannels
  /** Channels attaching in this turn, before its session holds them: what they send counts in the headroom too. */
  attaching: Set<ServerChannel>
  wireSender: WireSender
}

type ConnectionEntry = {
  state: ConnectionState
  transport: ServerTransport<unknown>
  /** One per wire: the peers of every reconcile on this wire share it. */
  sender: PeerSender
  /** The request that opened the wire, where one did. */
  request: Request | undefined
}

function getChannelMux(): ChannelMux {
  return getGlobals().mux
}

class ChannelMux {
  private readonly channels = new Map<string, ServerChannel>()
  /** Waiters registered by `attach` when a reconcile lands before the channel is registered.
   *  Fired synchronously from `registerChannel`. */
  private readonly pendingRegisterWaiters = new Map<string, Set<(channel: ServerChannel) => void>>()
  private readonly sessions = new SessionRegistry()
  /** The wire each session is on: the last to reconcile it, until another reconciles it or the wire closes. */
  private readonly sessionWires = new Map<string, Wire>()
  private readonly connectionEntries = new Map<unknown, ConnectionEntry>()
  /** Reverse index for transports with a stable connId (SSE). Lets data POSTs locate the
   *  live stream connection, and catches a duplicate-connId reconnect racing teardown. */
  private readonly connectionsByConnId = new Map<string, Wire>()
  private readonly stagedUpgrades = new Map<Wire, StagedUpgrade>()
  private readonly stagedByPrevSession = new Map<string, Wire>()
  private stagedBytes = 0
  /** Ended channels their page may still need, each with the timer releasing it, null while the page is attached. */
  private readonly endedChannels = new Map<ServerChannel, ReturnType<typeof setTimeout> | null>()

  /** Resolved lazily so the mux can be constructed at module-load (the globalObject factory
   *  runs before `serverConfig` is initialized). */
  private resolvedOptions: MuxServerOptions | null = null

  private get options(): MuxServerOptions {
    return (this.resolvedOptions ??= resolveMuxServerOptions())
  }

  /** Exposed for transport-level race timers (SSE's `waitForConnection`). */
  get connectTtl(): number {
    return this.options.connectTtl
  }

  // ── ServerChannel registry ──────────────────────────────────────────

  /** Callers must not invoke `channel._registerChannel()` directly. */
  registerChannel(channel: ServerChannel<any, any>): void {
    channel._registerChannel()
    this.channels.set(channel.id, channel)
    if (channel._didShutdown) {
      // It ended before a response carried it: it waits for its page as an ended channel does.
      this.keepEnded(channel, false)
    } else {
      // Before a waiter attaches it: its `onOpen` may end it.
      channel._onShutdown((keep, pageAttached) => {
        if (keep) this.keepEnded(channel, pageAttached)
        else this.unregisterChannel(channel.id)
      })
    }
    const waiters = this.pendingRegisterWaiters.get(channel.id)
    if (waiters) {
      this.pendingRegisterWaiters.delete(channel.id)
      for (const cb of waiters) cb(channel)
    }
  }

  unregisterChannel(channelId: string): void {
    this.channels.delete(channelId)
    this.sessions.removeChannel(channelId)
  }

  /** Keeps an ended channel attachable until its page needs nothing of it, leaves it, or is away `reconnectTimeout`. */
  private keepEnded(channel: ServerChannel, pageAttached: boolean): void {
    const timer = this.endedChannels.get(channel)
    if (timer) clearTimeout(timer)
    this.endedChannels.set(
      channel,
      pageAttached
        ? null
        : unrefTimer(setTimeout(() => this.releaseEnded(channel), getServerConfig().channel.reconnectTimeout)),
    )
  }

  private releaseEnded(channel: ServerChannel): void {
    const timer = this.endedChannels.get(channel)
    if (timer) clearTimeout(timer)
    this.endedChannels.delete(channel)
    this.unregisterChannel(channel.id)
    channel._release()
  }

  hasChannels(): boolean {
    return this.channels.size > 0
  }

  // ── Connection lifecycle (transport-facing) ─────────────────────────

  onConnectionOpen<TConnection>(
    connection: TConnection,
    transport: ServerTransport<TConnection>,
    request?: Request,
  ): void {
    const wait = new SocketWait({
      held: () => this.bufferedAmount(connection),
      ahead: () => this.queuedAhead(connection),
    })
    const recvBacklog = { bytes: 0, frames: 0 }
    const room = transport.wholeMessages
      ? new PathRoom({
          transmit: (message) => transport.sendNow(connection, message),
          queued: () => transport.bufferedAmount(connection) ?? 0,
          pingInterval: this.options.pingInterval,
        })
      : null
    const pieces = room && {
      room,
      receiver: new ServerPieceReceiver({
        send: (frame) => this.send(connection, frame),
        carry: (ack) => room.sender.carry(ack),
      }),
    }
    const attaching = new Set<ServerChannel>()
    const wireSender = new WireSender({
      wire: connection,
      transport: transport as ServerTransport<unknown>,
      pieces,
      wait,
      channels: () => this.wireChannels(transport, connection, attaching),
    })
    const entry: ConnectionEntry = {
      state: {
        pingTimer: null,
        lastReceivedAt: 0,
        deliveries: new Deliveries(),
        frameDelivery: 0,
        pongedAt: performance.now(),
        pieces,
        terminatePermanently: false,
        recvChain: null,
        closed: null,
        retiredByBarrier: false,
        recvBacklog,
        awaited: new AwaitedChannels(recvBacklog),
        attaching,
        wireSender,
      },
      transport: transport as ServerTransport<unknown>,
      sender: {
        send: (frame, onCommit) => this.send(connection, frame as Uint8Array<ArrayBuffer>, onCommit),
        carry: (frame) => {
          if (room) room.sender.carry(() => frame as Uint8Array<ArrayBuffer>)
          else this.send(connection, frame as Uint8Array<ArrayBuffer>)
        },
        bufferedAmount: () => this.bufferedAmount(connection),
        sendWait: () => wait,
        deliveredBeforeFrame: (ms) => {
          const state = this.connectionEntries.get(connection)?.state
          return state ? state.deliveries.before(state.frameDelivery, ms) : 0
        },
      },
      request,
    }
    this.connectionEntries.set(connection, entry)
    const connId = transport.getConnId(connection)
    if (connId !== null) this.connectionsByConnId.set(connId, connection)
    this.resetPingTimer(connection)
  }

  async onConnectionRawMessage(connection: Wire, rawFrame: Uint8Array<ArrayBuffer>): Promise<void> {
    const outcome = await this.dispatchInbound(connection, rawFrame)
    if (outcome) this.sendReconciled(outcome)
  }

  onConnectionRawMessageDeferredReconciled(
    connection: Wire,
    rawFrame: Uint8Array<ArrayBuffer>,
  ): Promise<ReconcileOutcome | null> {
    return this.dispatchInbound(connection, rawFrame)
  }

  sendReconciled(outcome: ReconcileOutcome): void {
    this.connectionEntries.get(outcome.deliverTo)?.state.wireSender.dropProbes()
    this.send(
      outcome.deliverTo,
      encode.reconciled({
        upgradeId: outcome.upgradeId,
        sessionId: outcome.sessionId,
        open: outcome.attached.map((h) => ({ ix: h.ix, lastSeq: h.channel._lastClientSeq })),
        reconnectTimeout: this.options.reconnectTimeout,
        idleTimeout: this.options.idleTimeout,
        pingInterval: this.options.pingInterval,
        maxFrameBytes: getAdapterMaxFrameBytes() ?? WIRE_MAX_RAW_FRAME_BYTES,
        serverReplayBuffer: this.options.serverReplayBuffer,
        serverReplayBufferBinary: this.options.serverReplayBufferBinary,
        clientReplayBuffer: this.options.clientReplayBuffer,
        clientReplayBufferBinary: this.options.clientReplayBufferBinary,
        sseFlushThrottle: this.options.sseFlushThrottle,
        ssePostIdleFlushDelay: this.options.ssePostIdleFlushDelay,
        transports: this.options.transports,
      }),
    )
    // Sends are sync; firing the upgrade finalizer here can't reorder anything on the new wire.
    outcome.finalizeUpgrade?.()
  }

  onConnectionClosed(connection: Wire, { permanent }: { permanent: boolean }): void {
    const entry = this.connectionEntries.get(connection)
    if (!entry) return
    entry.state.closed = { isPermanent: permanent }
    entry.state.pieces?.room.close()
    this.clearPingTimer(entry.state)
    entry.state.awaited.stop()
    this.connectionEntries.delete(connection)
    const connId = entry.transport.getConnId(connection)
    // Identity-equality guards against deleting a *replacement* connection's entry when
    // a duplicate-connId reconnect raced the old wire's teardown.
    if (connId !== null && this.connectionsByConnId.get(connId) === connection) {
      this.connectionsByConnId.delete(connId)
    }
    this.clearStage(connection)
    const sessionId = entry.transport.getSessionId(connection)
    if (!sessionId) return // Closed before reconciling — nothing to clean up.
    const stagedWs = this.stagedByPrevSession.get(sessionId)
    if (stagedWs !== undefined) this.abandonStage(stagedWs)
    // Channels survive a transient close (`_onPeerDisconnect`'s reconnectTimeout grace);
    // permanent tears them down.
    this.detachSession(sessionId, permanent ? DETACH_REASON.PERMANENT : DETACH_REASON.TRANSIENT)
    if (this.sessionWires.get(sessionId) === connection) this.sessionWires.delete(sessionId)
  }

  /** Ends the channels of a session whose wire closed transiently as a permanent close would have: its page left. */
  endSession(sessionId: string): void {
    this.detachSession(sessionId, DETACH_REASON.PERMANENT)
  }

  readPermanentTermination(connection: Wire): boolean {
    return this.connectionEntries.get(connection)?.state.terminatePermanently ?? false
  }

  /** SSE data POST: resolve the stream connection by its stable connId. Undefined when the
   *  connection hasn't reconciled yet or has already torn down. */
  getConnectionByConnId<TConnection>(connId: string): TConnection | undefined {
    return this.connectionsByConnId.get(connId) as TConnection | undefined
  }

  // ── Inbound dispatch ────────────────────────────────────────────────

  private dispatchInbound(connection: Wire, rawFrame: Uint8Array<ArrayBuffer>): Promise<ReconcileOutcome | null> {
    const entry = this.connectionEntries.get(connection)
    if (!entry) return Promise.resolve(null)
    let outcome: Promise<ReconcileOutcome | null> = Promise.resolve(null)
    try {
      if (isOneFrame(rawFrame)) return this.dispatchFrame(entry, connection, rawFrame, 'alone')
      // Every frame but the last arrives riding.
      assertProtocol(entry.state.pieces, 'frames in one message on a wire that takes no pieces')
      const frames = messageFrames(rawFrame)
      let next = frames.next()
      while (!next.done && entry.state.closed === null) {
        const frame = next.value
        next = frames.next()
        const dispatched = this.dispatchFrame(entry, connection, frame, next.done ? 'alone' : 'riding')
        if (frame[0] === TAG.RECONCILE) outcome = dispatched
      }
    } catch (err) {
      if (!(err instanceof ProtocolViolationError)) throw err
      this.terminateWire(connection)
    }
    return outcome
  }

  /** PING and PIECES_ACK skip the recv chain, which would tie liveness to its slowest handler. */
  private dispatchFrame(
    entry: ConnectionEntry,
    connection: Wire,
    rawFrame: Uint8Array<ArrayBuffer>,
    arrival: 'alone' | 'riding' | 'pieced',
  ): Promise<ReconcileOutcome | null> {
    const { state } = entry
    const byteLength = rawFrame.byteLength
    if (this.isOverBudget(entry, connection, rawFrame)) {
      this.terminateWire(connection)
      return Promise.resolve(null)
    }
    const tag = peekTag(rawFrame)
    if (tag === TAG.PIECE) return this.receivePiece(entry, connection, rawFrame)
    if (tag === TAG.ANNOUNCE) return this.receiveAnnounce(entry, rawFrame)
    const now = performance.now()
    if (arrival !== 'pieced') {
      state.pieces?.receiver.arrived(tag as number, byteLength, now, arrival)
      state.deliveries.note(now, byteLength)
    }
    state.recvBacklog.bytes += byteLength
    state.recvBacklog.frames++
    // Taken as it arrives: it's handled once those before it were, after later ones arrived.
    const delivery = state.deliveries.last
    state.lastReceivedAt = now
    const exec = (): Promise<ReconcileOutcome | null> =>
      this.runInboundTurn(entry, connection, rawFrame, byteLength, delivery)
    if (tag === TAG.PING || tag === TAG.PIECES_ACK) return exec()
    this.answerArrival(entry, connection)
    return this.chainRecv(entry, exec)
  }

  /** The frame a piece completes is dispatched as one that came whole. */
  private receivePiece(
    entry: ConnectionEntry,
    connection: Wire,
    rawFrame: Uint8Array<ArrayBuffer>,
  ): Promise<ReconcileOutcome | null> {
    const { state } = entry
    const now = performance.now()
    state.lastReceivedAt = now
    state.deliveries.note(now, rawFrame.byteLength)
    this.answerArrival(entry, connection)
    const piece = decodeClientFrame(rawFrame, WIRE_MAX_CONN_CTRL_FRAME_BYTES)
    assert(piece.tag === TAG.PIECE)
    assertProtocol(state.pieces, 'PIECE on a wire that takes no pieces')
    // As large as the runtime takes in one message: its pieces get past what it would refuse whole.
    const cap = getAdapterMaxFrameBytes() ?? WIRE_MAX_RAW_FRAME_BYTES
    assertProtocol(piece.total <= cap, 'PIECE of a frame over the cap')
    const { receiver } = state.pieces
    const heldBefore = receiver.held
    const frame = receiver.add(piece.total, piece.piece, now)
    // What it holds of a frame counts in the recv backlog as the frame does once whole.
    state.recvBacklog.bytes += receiver.held - heldBefore
    state.recvBacklog.frames += Number(receiver.held > 0) - Number(heldBefore > 0)
    if (frame === null) return Promise.resolve(null)
    return this.dispatchFrame(entry, connection, frame, 'pieced')
  }

  private receiveAnnounce(entry: ConnectionEntry, rawFrame: Uint8Array<ArrayBuffer>): Promise<ReconcileOutcome | null> {
    const now = performance.now()
    entry.state.lastReceivedAt = now
    entry.state.deliveries.note(now, rawFrame.byteLength)
    const announce = decodeClientFrame(rawFrame, WIRE_MAX_CONN_CTRL_FRAME_BYTES)
    assert(announce.tag === TAG.ANNOUNCE)
    assertProtocol(entry.state.pieces, 'ANNOUNCE on a wire that takes no pieces')
    entry.state.pieces.receiver.announce(announce.bytes, now, announce.floor)
    return Promise.resolve(null)
  }

  /** Bytes reached `connection`: a frame slower than the ping deadline to arrive shows the wire alive meanwhile. */
  onConnectionBytes(connection: Wire): void {
    const entry = this.connectionEntries.get(connection)
    if (!entry) return
    entry.state.lastReceivedAt = performance.now()
    this.answerArrival(entry, connection)
  }

  /** A wire whose bytes keep arriving gets a PONG each ping interval, as its PING may wait behind an upload. */
  private answerArrival(entry: ConnectionEntry, connection: Wire): void {
    if (performance.now() - entry.state.pongedAt >= this.options.pingInterval) this.pong(entry, connection, [])
  }

  /** Only data frames and pieces get the raw frame cap; the backlog adds a full window per channel of the session. */
  private isOverBudget(entry: ConnectionEntry, connection: Wire, rawFrame: Uint8Array<ArrayBuffer>): boolean {
    const tag = peekTag(rawFrame)
    const maxFrameBytes =
      tag !== undefined && isConnCtrlTag(tag) && tag !== TAG.PIECE
        ? WIRE_MAX_CONN_CTRL_FRAME_BYTES
        : WIRE_MAX_RAW_FRAME_BYTES
    const byteLength = rawFrame.byteLength
    if (byteLength > maxFrameBytes) return true
    const { state, transport } = entry
    const backlogBytes = state.recvBacklog.bytes + byteLength
    const backlogFrames = state.recvBacklog.frames
    if (backlogBytes <= WIRE_RECV_BACKLOG_BASE_BYTES && backlogFrames < WIRE_RECV_BACKLOG_BASE_FRAMES) return false
    const sessionId = transport.getSessionId(connection)
    const channels = sessionId === undefined ? 0 : (this.sessions.peekSession(sessionId)?.size ?? 0)
    return (
      backlogBytes > WIRE_RECV_BACKLOG_BASE_BYTES + channels * CREDIT_WINDOW_MAX_BYTES ||
      backlogFrames >= WIRE_RECV_BACKLOG_BASE_FRAMES + channels * CREDIT_MSG_WINDOW_MAX
    )
  }

  private async runInboundTurn(
    entry: ConnectionEntry,
    connection: Wire,
    rawFrame: Uint8Array<ArrayBuffer>,
    byteLength: number,
    delivery: number,
  ): Promise<ReconcileOutcome | null> {
    try {
      entry.state.frameDelivery = delivery
      return (await this.handleFrame(entry, connection, rawFrame)) ?? null
    } catch (err) {
      if (!(err instanceof ProtocolViolationError)) throw err
      this.terminateWire(err.target ?? connection)
      return null
    } finally {
      entry.state.recvBacklog.bytes -= byteLength
      entry.state.recvBacklog.frames--
    }
  }

  /** `connection` may be a wire other than the one that sent the offending frame — a barrier's
   *  violation is the staged probe's. */
  private terminateWire(connection: Wire): void {
    const entry = this.connectionEntries.get(connection)
    if (!entry) return
    if (this.stagedUpgrades.get(connection)?.phase === 'staged') this.clearStage(connection)
    entry.state.terminatePermanently = true
    entry.transport.terminateConnection(connection)
  }

  /** Returns a `ReconcileOutcome` only on reconcile (so the caller decides when to send
   *  `reconciled`). Anything but reconcile/ping before reconciliation is a violation. */
  private handleFrame(
    entry: ConnectionEntry,
    connection: Wire,
    rawFrame: Uint8Array<ArrayBuffer>,
  ): null | Promise<ReconcileOutcome | null> {
    const frame = decodeClientFrame(rawFrame, WIRE_MAX_CONN_CTRL_FRAME_BYTES)
    if (frame.tag === TAG.PING) {
      const pieces = entry.state.pieces
      if (pieces)
        assertProtocol(pieces.room.sender.heartbeat(frame.took, performance.now()), 'PING took more than sent')
      this.resetPingTimer(connection)
      this.acknowledgeArrivals(entry, connection)
      this.pong(entry, connection, this.answerPing(entry, connection, frame.ended))
      return null
    }
    if (frame.tag === TAG.PIECES_ACK) {
      const known = entry.state.pieces?.room.acknowledged(frame)
      assertProtocol(known, 'PIECES_ACK for nothing sent')
      return null
    }
    assertProtocol(!entry.state.retiredByBarrier, 'frame on a wire retired by its barrier')
    assertProtocol(!this.stagedUpgrades.has(connection), 'frame on a staged probe')
    if (frame.tag === TAG.PREPARE) return this.handlePrepare(entry, connection, frame.payload, rawFrame.byteLength)
    if (frame.tag === TAG.BARRIER) return this.handleBarrier(entry, connection, frame.payload)
    if (frame.tag === TAG.RECONCILE) {
      this.claimSessionForReconcile(frame.payload, entry, connection)
      return this.reconcile(entry, connection, frame.payload)
    }
    const sessionId = entry.transport.getSessionId(connection)
    assertProtocol(sessionId, 'frame before reconcile')
    const channelFrame = frame as ChannelFrame
    // One for a channel the wire awaits is held for after its attach, and stays in the recv backlog until then.
    const awaited = entry.state.awaited.get(channelFrame.index)
    if (awaited && awaited.phase !== 'expired') {
      entry.state.awaited.hold(awaited, channelFrame, rawFrame.byteLength)
      return null
    }
    this.dispatchChannelFrame(sessionId, channelFrame)
    return null
  }

  /** Answers each closed channel the page lists on its session's wire: how far the server has it, or null once gone. */
  private answerPing(entry: ConnectionEntry, connection: Wire, ended: PingEntry[]): PongEntry[] {
    assertProtocol(ended.length <= MAX_CHANNELS_PER_CONNECTION, 'PING over entry cap')
    const sessionId = entry.transport.getSessionId(connection)
    if (sessionId === undefined || this.sessionWires.get(sessionId) !== connection) return []
    const answers: PongEntry[] = []
    for (const { ix, lastSeq } of ended) {
      const channel = this.sessions.get(sessionId, ix)?.channel
      if (channel === undefined) {
        answers.push({ ix, lastSeq: null })
        continue
      }
      channel._onPageClosed(seqThrough(lastSeq, channel._replayBuffer?.seq ?? 0))
      const released = this.endedChannels.has(channel)
      if (released) this.releaseEnded(channel)
      answers.push({ ix, lastSeq: released ? null : channel._lastClientSeq })
    }
    return answers
  }

  /** At each page heartbeat, its session's channels acknowledge what arrived, so the page's replay lets it go. */
  private acknowledgeArrivals(entry: ConnectionEntry, connection: Wire): void {
    const sessionId = entry.transport.getSessionId(connection)
    if (sessionId === undefined || this.sessionWires.get(sessionId) !== connection) return
    for (const { channel } of this.sessions.peekSession(sessionId)?.values() ?? []) channel._acknowledge()
  }

  private pong(entry: ConnectionEntry, connection: Wire, ended: PongEntry[]): void {
    entry.state.pongedAt = performance.now()
    this.send(connection, withTook(encode.pong(ended), entry.state.pieces?.receiver.took ?? 0))
  }

  private dispatchChannelFrame(sessionId: string, frame: ChannelFrame): void {
    // Frame for an ix that's no longer in the session — client closed the channel and the
    // server reconciled it out, but a frame was still in flight. Drop silently.
    this.sessions.get(sessionId, frame.index)?.channel._dispatchFrame(frame)
  }

  /** Abandons the probes staged on the session it names and its wire's, refused mid-commit, unless it names its own. */
  private claimSessionForReconcile(ctrl: ReconcilePayload, entry: ConnectionEntry, connection: Wire): void {
    const own = entry.transport.getSessionId(connection)
    if (ctrl.sessionId !== undefined && ctrl.sessionId === own) return
    for (const claimed of [ctrl.sessionId, own]) {
      if (claimed === undefined) continue
      const staleProbe = this.stagedByPrevSession.get(claimed)
      if (staleProbe === undefined) continue
      // A committing barrier already owns this session; a concurrent claim on it is refused rather
      // than allowed to abandon the stage out from under the in-flight commit.
      assertProtocol(this.stagedUpgrades.get(staleProbe)?.phase !== 'committing', 'session claimed mid-commit')
      this.abandonStage(staleProbe)
    }
  }

  private handlePrepare(
    entry: ConnectionEntry,
    connection: Wire,
    payload: PreparePayload,
    rawByteLength: number,
  ): null {
    assertProtocol(!entry.transport.getSessionId(connection), 'PREPARE on a reconciled wire')
    assertProtocol(this.sessions.peekSession(payload.sessionId), 'PREPARE for an unknown session')
    // A page stages one attempt at a time, so a newer PREPARE replaces what an earlier attempt left staged.
    const staged = this.stagedByPrevSession.get(payload.sessionId)
    if (staged !== undefined) {
      assertProtocol(this.stagedUpgrades.get(staged)?.phase === 'staged', 'session already committing')
      this.abandonStage(staged)
    }
    assertProtocol(this.stagedUpgrades.size < UPGRADE_MAX_STAGED_RECORDS, 'staged record budget')
    assertProtocol(this.stagedBytes + rawByteLength <= UPGRADE_MAX_STAGED_BYTES, 'staged byte budget')

    const timer = unrefTimer(setTimeout(() => this.abandonStage(connection), UPGRADE_STAGE_TTL_MS))
    this.stagedUpgrades.set(connection, {
      upgradeId: payload.upgradeId,
      prevSessionId: payload.sessionId,
      bytes: rawByteLength,
      timer,
      phase: 'staged',
    })
    this.stagedByPrevSession.set(payload.sessionId, connection)
    this.stagedBytes += rawByteLength
    this.send(connection, encode.ready({ upgradeId: payload.upgradeId }))
    return null
  }

  private handleBarrier(
    entry: ConnectionEntry,
    connection: Wire,
    ctrl: BarrierPayload,
  ): Promise<ReconcileOutcome> | null {
    const wsConnection = this.stagedByPrevSession.get(ctrl.sessionId)
    const stage = wsConnection === undefined ? undefined : this.stagedUpgrades.get(wsConnection)
    // A barrier for an unknown or already-committing stage is refused SILENTLY (the client's attempt
    // deadline is the only watchdog) — erroring would tear down a wire this frame has no claim on.
    if (wsConnection === undefined || stage?.phase !== 'staged') return null

    try {
      // Admission policy — `decodeClientFrame` has already established the frame's shape.
      for (const channel of ctrl.open) {
        assertProtocol(textEncoder.encode(channel.id).byteLength <= UPGRADE_MAX_ID_BYTES, 'channel id over byte cap')
      }
      assertProtocol(ctrl.upgradeId === stage.upgradeId, 'barrier upgradeId mismatch', wsConnection)
      assertProtocol(
        entry.transport.getSessionId(connection) === stage.prevSessionId,
        'barrier session mismatch',
        wsConnection,
      )

      const wsEntry = this.connectionEntries.get(wsConnection)
      assert(wsEntry, 'staged probe has no connection entry')

      stage.phase = 'committing'
      entry.state.retiredByBarrier = true
      this.moveAwaited(entry, wsEntry, wsConnection, ctrl.open)
      return this.settleBarrierCommit(entry, wsEntry, wsConnection, ctrl, stage.upgradeId)
    } catch (err) {
      this.clearStage(wsConnection)
      throw retargetToProbe(err, wsConnection)
    }
  }

  private async settleBarrierCommit(
    oldEntry: ConnectionEntry,
    wsEntry: ConnectionEntry,
    wsConnection: Wire,
    ctrl: BarrierPayload,
    upgradeId: string,
  ): Promise<ReconcileOutcome> {
    try {
      // `reconcile` ran against the staged WS, so the outcome already carries it as `deliverTo`.
      const outcome = await this.reconcile(wsEntry, wsConnection, ctrl, true)
      return { ...outcome, upgradeId }
    } catch (err) {
      oldEntry.state.retiredByBarrier = false
      throw retargetToProbe(err, wsConnection)
    } finally {
      this.clearStage(wsConnection)
    }
  }

  /** Forgets the stage AND kills the probe holding it. No-op once the stage is committing. */
  private abandonStage(wsConnection: Wire): void {
    if (this.stagedUpgrades.get(wsConnection)?.phase !== 'staged') return
    this.clearStage(wsConnection)
    const entry = this.connectionEntries.get(wsConnection)
    if (!entry) return
    entry.state.terminatePermanently = true
    entry.transport.terminateConnection(wsConnection)
  }

  /** Bookkeeping only — the probe wire is left alone. */
  private clearStage(wsConnection: Wire): void {
    const stage = this.stagedUpgrades.get(wsConnection)
    if (!stage) return
    clearTimeout(stage.timer)
    this.stagedUpgrades.delete(wsConnection)
    if (this.stagedByPrevSession.get(stage.prevSessionId) === wsConnection) {
      this.stagedByPrevSession.delete(stage.prevSessionId)
    }
    this.stagedBytes -= stage.bytes
  }

  // ── Reconcile + attach ──────────────────────────────────────────────

  /** `isBarrier`: the old wire's session is retired by this commit, so it gets a finalizer that
   *  FINs it once the new wire's RECONCILED has gone out. */
  private async reconcile(
    entry: ConnectionEntry,
    connection: Wire,
    ctrl: ReconcilePayload,
    isBarrier = false,
  ): Promise<ReconcileOutcome> {
    const { state, transport } = entry
    const oldWire = isBarrier && ctrl.sessionId ? this.sessionWires.get(ctrl.sessionId) : undefined
    const finalizeUpgrade = oldWire === undefined ? null : () => this.send(oldWire, encode.fin())
    this.resetPingTimer(connection)
    // One on the wire that holds the session it names keeps it, and so what is bound to it: a staged upgrade.
    const newSessionId =
      ctrl.sessionId !== undefined && ctrl.sessionId === transport.getSessionId(connection)
        ? ctrl.sessionId
        : crypto.randomUUID()
    // Replayed only on a reconnect, as the session's own wire and a barrier's old one (read to its FIN) still deliver.
    const replay = !isBarrier && newSessionId !== ctrl.sessionId
    const { released } = ctrl
    // The page names what changed only on the wire that has its session: elsewhere it can't know what the server holds.
    assertProtocol(released === undefined || newSessionId === ctrl.sessionId, 'RECONCILE released on another wire')
    const attached =
      released === undefined
        ? this.reconcileSession(ctrl.sessionId, newSessionId, ctrl.open, entry, connection, replay)
        : this.reconcileChanges(newSessionId, ctrl.open, released, entry, connection)
    // Its wire no longer has the session it names, which is gone with the handles it had.
    if (ctrl.sessionId) this.sessionWires.delete(ctrl.sessionId)

    // Closed since this frame arrived: the session goes, but a transient close keeps its channels' reattach grace.
    if (state.closed) {
      state.attaching.clear()
      const reason = state.closed.isPermanent ? DETACH_REASON.PERMANENT : DETACH_REASON.TRANSIENT
      const session = this.sessions.removeSession(newSessionId)
      if (session) for (const handle of session.values()) this.detachHandle(handle, reason)
      state.awaited.stop()
      throw new ProtocolViolationError('connection closed mid-reconcile')
    }

    this.sessionWires.set(newSessionId, connection)
    transport.setSessionId(connection, newSessionId)
    state.attaching.clear()
    return { sessionId: newSessionId, attached, finalizeUpgrade, deliverTo: connection }
  }

  private reconcileSession(
    prevSessionId: string | undefined,
    newSessionId: string,
    open: ReconcilePayload['open'],
    entry: ConnectionEntry,
    connection: Wire,
    replay: boolean,
  ): ChannelHandle[] {
    const handles = open
      .map((each) => this.attach(each, entry, connection, replay))
      .filter((h): h is ChannelHandle => h !== null)

    // Channels in the previous session that the client did NOT re-include are recovery-failed.
    if (prevSessionId) {
      const prev = this.sessions.removeSession(prevSessionId)
      if (prev) {
        const keptIxes = new Set(handles.map((h) => h.ix))
        for (const [ix, prevHandle] of prev)
          if (!keptIxes.has(ix)) this.detachHandle(prevHandle, DETACH_REASON.RECOVERY_FAILED)
      }
    }
    entry.state.awaited.forgetUnnamed(open)
    this.sessions.setSession(newSessionId, handles)
    return handles
  }

  /** Attaches the channels the RECONCILE names to the session, and ends there each one the page released. */
  private reconcileChanges(
    sessionId: string,
    open: ReconcileOpenEntry[],
    released: number[],
    entry: ConnectionEntry,
    connection: Wire,
  ): ChannelHandle[] {
    const handles: ChannelHandle[] = []
    for (const each of open) {
      const handle = this.attach(each, entry, connection, false)
      if (handle === null) continue
      handles.push(handle)
      this.sessions.add(sessionId, handle)
    }
    for (const ix of released) {
      const handle = this.sessions.remove(sessionId, ix)
      if (handle) this.detachHandle(handle, DETACH_REASON.RECOVERY_FAILED)
    }
    entry.state.awaited.forget(released)
    return handles
  }

  /** Null leaves it out of the RECONCILED; an initial channel not yet registered is awaited until its ATTACH_RESULT. */
  private attach(
    open: ReconcileOpenEntry,
    entry: ConnectionEntry,
    connection: Wire,
    replay: boolean,
  ): ChannelHandle | null {
    // Answered ahead of the channel's frames, and of its registration where awaited: its round trip is the path's.
    if (open.probe !== undefined) entry.state.wireSender.addProbe(open.ix, open.probe)
    const awaited = entry.state.awaited.get(open.ix)
    if (awaited) {
      awaited.open = open
      if (awaited.phase !== 'attached') return null
    }
    const existing = this.channels.get(open.id)
    if (existing) {
      entry.state.attaching.add(existing)
      return this.attachChannel(existing, open, entry, replay)
    }
    if (open.initial && !awaited) this.awaitChannel(open, entry, connection)
    return null
  }

  private awaitChannel(open: ReconcileOpenEntry, entry: ConnectionEntry, connection: Wire): void {
    const onResult = (channel: ServerChannel | null): void => {
      if (channel) this.attachAwaited(awaited, channel)
      else this.expireAwaited(awaited)
    }
    const awaited: AwaitedChannel = {
      open,
      entry,
      wire: connection,
      held: [],
      phase: 'waiting',
      stopWaiting: this.waitForChannelRegistration(open.id, this.options.connectTtl, onResult),
    }
    entry.state.awaited.add(awaited)
  }

  /** Runs in `registerChannel`, between two reconciles: waiters attach in the order they began; the latest keeps it. */
  private attachAwaited(awaited: AwaitedChannel, channel: ServerChannel): void {
    const { entry, wire } = awaited
    const sessionId = entry.transport.getSessionId(wire)
    assert(sessionId, 'a channel awaited on a wire that never reconciled')
    entry.state.attaching.add(channel)
    const handle = this.attachChannel(channel, awaited.open, entry, false)
    entry.state.attaching.delete(channel)
    if (!handle) {
      this.expireAwaited(awaited)
      return
    }
    this.sessions.add(sessionId, handle)
    awaited.phase = 'attached'
    // Once the registering code added its listeners; the ATTACH_RESULT follows, so its lastSeq counts what was held.
    const settle = async (): Promise<void> => {
      if (entry.state.awaited.get(awaited.open.ix) !== awaited) return
      if (this.dispatchHeld(awaited)) this.send(wire, encode.attachResult(awaited.open.ix, channel._lastClientSeq))
    }
    void this.chainRecv(entry, settle).catch(handleTelefunctionBug)
  }

  /** Returns false if what it held broke the protocol, which ends its wire. */
  private dispatchHeld(awaited: AwaitedChannel): boolean {
    const { entry, wire } = awaited
    const held = entry.state.awaited.take(awaited)
    const sessionId = entry.transport.getSessionId(wire)
    assert(sessionId)
    try {
      for (const { frame } of held) this.dispatchChannelFrame(sessionId, frame)
      return true
    } catch (err) {
      if (!(err instanceof ProtocolViolationError)) throw err
      this.terminateWire(wire)
      return false
    }
  }

  /** Not registered within `connectTtl`, or shut down as it registered. */
  private expireAwaited(awaited: AwaitedChannel): void {
    awaited.phase = 'expired'
    awaited.entry.state.awaited.drop(awaited)
    this.send(awaited.wire, encode.attachResult(awaited.open.ix, null))
  }

  /** Awaited channels the barrier lists move to the WebSocket; an attached one first gets what the old wire held. */
  private moveAwaited(
    oldEntry: ConnectionEntry,
    wsEntry: ConnectionEntry,
    wsConnection: Wire,
    open: ReconcileOpenEntry[],
  ): void {
    for (const { ix } of open) {
      const awaited = oldEntry.state.awaited.get(ix)
      if (!awaited) continue
      if (awaited.phase === 'attached') this.dispatchHeld(awaited)
      else oldEntry.state.awaited.move(awaited, wsEntry, wsConnection)
    }
  }

  /** With `replay`, sends what the page missed, or ends the channel if the replay lost it; null if it kept nothing. */
  private attachChannel(
    channel: ServerChannel,
    open: ReconcileOpenEntry,
    { sender, request }: ConnectionEntry,
    replay: boolean,
  ): ChannelHandle | null {
    const buffer = channel._replayBuffer
    if (buffer === null) return null
    channel._onPageHas(open.lastSeq)
    channel._wireRequest = request
    const peer = new IndexedPeer({ sender, index: open.ix, replay: buffer })
    const missed = replay ? buffer.getAfter(open.lastSeq) : []
    if (missed === null) {
      channel._onReplayLost(peer)
    } else {
      for (const frame of missed) sender.send(frame)
      channel._attachPeer(peer, open)
    }
    if (this.endedChannels.has(channel)) this.keepEnded(channel, true)
    return { channel, ix: open.ix, peer }
  }

  /** Returns what ends the wait without a result. */
  private waitForChannelRegistration(
    channelId: string,
    ttlMs: number,
    onResult: (channel: ServerChannel | null) => void,
  ): () => void {
    let settled = false
    let timer: ReturnType<typeof setTimeout>
    const waiterSet = this.pendingRegisterWaiters.get(channelId) ?? new Set()
    this.pendingRegisterWaiters.set(channelId, waiterSet)

    const stop = (): boolean => {
      if (settled) return false
      settled = true
      waiterSet.delete(waiter)
      if (waiterSet.size === 0) this.pendingRegisterWaiters.delete(channelId)
      clearTimeout(timer)
      return true
    }
    const waiter = (channel: ServerChannel): void => {
      if (stop()) onResult(channel)
    }
    waiterSet.add(waiter)
    timer = setTimeout(() => {
      if (stop()) onResult(null)
    }, ttlMs)
    return () => void stop()
  }

  /** Transient: leave registry entries so the next reconcile's prev-comparison can fire
   *  `recovery-failed` on abandoned channels. Permanent: remove outright — no resume. */
  private detachSession(sessionId: string, reason: DetachReason): void {
    const session =
      reason === DETACH_REASON.PERMANENT ? this.sessions.removeSession(sessionId) : this.sessions.peekSession(sessionId)
    if (!session) return
    for (const handle of session.values()) this.detachHandle(handle, reason)
  }

  private detachHandle(h: ChannelHandle, reason: DetachReason): void {
    // An ended channel waits out a lost wire for its page, which lets it go by releasing it, leaving it out of a new
    // wire's RECONCILE, or leaving for good.
    if (this.endedChannels.has(h.channel)) {
      if (reason === DETACH_REASON.TRANSIENT) this.keepEnded(h.channel, false)
      else this.releaseEnded(h.channel)
      return
    }
    switch (reason) {
      case DETACH_REASON.PERMANENT:
        h.channel._onPeerClose()
        return
      case DETACH_REASON.TRANSIENT:
        h.channel._onPeerDisconnect(h.peer, getServerConfig().channel.reconnectTimeout)
        return
      case DETACH_REASON.RECOVERY_FAILED:
        h.channel._onPeerRecoveryFailure()
        return
    }
  }

  // ── Per-connection plumbing (send, recv chain, ping) ────────────────

  /** Sole send path, sync so wire order is call order; a frame for a closed or over-backlog wire commits anyway. */
  private send(connection: Wire, frame: Uint8Array<ArrayBuffer>, onCommit?: () => void): void {
    onCommit?.()
    this.connectionEntries.get(connection)?.state.wireSender.send(frame)
  }

  /** The channels the wire's send headroom counts: its session's, and those attaching in this turn. */
  private wireChannels<TConnection>(
    transport: ServerTransport<TConnection>,
    connection: TConnection,
    attaching: Set<ServerChannel>,
  ): Set<ServerChannel> {
    const sessionId = transport.getSessionId(connection)
    const session = sessionId === undefined ? undefined : this.sessions.peekSession(sessionId)
    const channels = new Set(attaching)
    for (const { channel } of session?.values() ?? []) channels.add(channel)
    return channels
  }

  /** A wire that's gone holds nothing. */
  private bufferedAmount(connection: Wire): number | undefined {
    const entry = this.connectionEntries.get(connection)
    if (!entry) return 0
    return entry.state.wireSender.bufferedAmount()
  }

  /** What a frame that goes at once counts as ahead of it: the transport's `queuedAhead` where it has one. */
  private queuedAhead(connection: Wire): number | undefined {
    const transport = this.connectionEntries.get(connection)?.transport
    return transport?.queuedAhead ? transport.queuedAhead(connection) : this.bufferedAmount(connection)
  }

  private chainRecv<T>(entry: ConnectionEntry, fn: () => Promise<T>): Promise<T> {
    const prev = entry.state.recvChain ?? Promise.resolve()
    const next = prev.then(fn, fn).finally(() => {
      if (entry.state.recvChain === next) entry.state.recvChain = null
    })
    entry.state.recvChain = next
    return next
  }

  private clearPingTimer(state: ConnectionState): void {
    if (!state.pingTimer) return
    clearTimeout(state.pingTimer)
    state.pingTimer = null
  }

  private resetPingTimer(connection: Wire): void {
    const entry = this.connectionEntries.get(connection)
    if (!entry) return
    entry.state.lastReceivedAt = performance.now()
    this.armPingDeadline(connection, entry, this.options.pingDeadline)
  }

  private armPingDeadline(connection: Wire, entry: ConnectionEntry, ms: number): void {
    const { state, transport } = entry
    this.clearPingTimer(state)
    state.pingTimer = unrefTimer(
      setTimeout(() => {
        state.pingTimer = null
        // A PING waits behind what the page queued: dead past the deadline after its last bytes and its announced data.
        const deadAt = Math.max(
          state.lastReceivedAt + this.options.pingDeadline,
          state.pieces?.receiver.expectedBy(this.options.pingDeadline) ?? 0,
        )
        const now = performance.now()
        if (now < deadAt) return this.armPingDeadline(connection, entry, Math.min(deadAt - now, TIMER_DELAY_MAX_MS))
        // Transient close so each channel gets its `reconnectTimeout` grace via
        // `_onPeerDisconnect`. Connection-level state is rebuilt by the next reconcile.
        transport.terminateConnection(connection)
      }, ms),
    )
  }
}

/** Forward (`bySession`: sessionId → ix → handle) for per-frame routing; reverse
 *  (`byChannel`: channelId → sessionId → ix) for O(bindings) channel eviction.
 *  Mutations stay atomic across both maps. */
class SessionRegistry {
  private readonly bySession = new Map<string, Map<number, ChannelHandle>>()
  private readonly byChannel = new Map<string, Map<string, number>>()

  get(sessionId: string, ix: number): ChannelHandle | undefined {
    return this.bySession.get(sessionId)?.get(ix)
  }

  /** Read without mutating. Used by transient-close handling so the next reconcile's
   *  prev-comparison can still detect channels the client dropped. */
  peekSession(sessionId: string): Map<number, ChannelHandle> | undefined {
    return this.bySession.get(sessionId)
  }

  /** Stores no empty session: a transient close leaves it, and no channel's removal deletes it. */
  setSession(sessionId: string, handles: Iterable<ChannelHandle>): void {
    this.removeSession(sessionId)
    for (const h of handles) this.add(sessionId, h)
  }

  add(sessionId: string, h: ChannelHandle): void {
    let session = this.bySession.get(sessionId)
    if (!session) {
      session = new Map()
      this.bySession.set(sessionId, session)
    }
    session.set(h.ix, h)
    let bindings = this.byChannel.get(h.channel.id)
    if (!bindings) {
      bindings = new Map()
      this.byChannel.set(h.channel.id, bindings)
    }
    bindings.set(sessionId, h.ix)
  }

  /** Takes the channel at `ix` out of the session and returns it; a session left empty goes, as in `removeChannel`. */
  remove(sessionId: string, ix: number): ChannelHandle | undefined {
    const session = this.bySession.get(sessionId)
    const handle = session?.get(ix)
    if (session === undefined || handle === undefined) return undefined
    session.delete(ix)
    if (session.size === 0) this.bySession.delete(sessionId)
    const bindings = this.byChannel.get(handle.channel.id)!
    bindings.delete(sessionId)
    if (bindings.size === 0) this.byChannel.delete(handle.channel.id)
    return handle
  }

  /** Returns the removed session so callers can drive per-handle lifecycle side effects. */
  removeSession(sessionId: string): Map<number, ChannelHandle> | undefined {
    const session = this.bySession.get(sessionId)
    if (!session) return undefined
    this.bySession.delete(sessionId)
    for (const handle of session.values()) {
      const bindings = this.byChannel.get(handle.channel.id)
      if (!bindings) continue
      bindings.delete(sessionId)
      if (bindings.size === 0) this.byChannel.delete(handle.channel.id)
    }
    return session
  }

  removeChannel(channelId: string): void {
    const bindings = this.byChannel.get(channelId)
    if (!bindings) return
    this.byChannel.delete(channelId)
    for (const [sessionId, ix] of bindings) {
      const session = this.bySession.get(sessionId)
      if (!session) continue
      session.delete(ix)
      // Last channel gone: drop the session, or it outlives every reconcile that could
      // ever name it (transient-closed sessions are otherwise only removed by reconcile).
      if (session.size === 0) this.bySession.delete(sessionId)
    }
  }
}

function resolveMuxServerOptions(): MuxServerOptions {
  const c = getServerConfig().channel
  const pingInterval = Math.max(c.pingInterval, CHANNEL_PING_INTERVAL_MIN_MS)
  return {
    reconnectTimeout: c.reconnectTimeout,
    idleTimeout: c.idleTimeout,
    pingInterval,
    pingDeadline: pingDeadlineOf(c),
    serverReplayBuffer: c.serverReplayBuffer,
    serverReplayBufferBinary: c.serverReplayBufferBinary,
    clientReplayBuffer: c.clientReplayBuffer,
    clientReplayBufferBinary: c.clientReplayBufferBinary,
    connectTtl: c.connectTtl,
    sseFlushThrottle: c.sseFlushThrottle,
    ssePostIdleFlushDelay: c.ssePostIdleFlushDelay,
    transports: c.transports,
  }
}

// Lazy because `getGlobalObject` evaluates its factory eagerly — the factory needs
// `ChannelMux` fully initialized, so defer until first access.
function getGlobals(): { mux: ChannelMux } {
  return getGlobalObject('wire-protocol/channel/server/mux.ts', () => ({ mux: new ChannelMux() }))
}
