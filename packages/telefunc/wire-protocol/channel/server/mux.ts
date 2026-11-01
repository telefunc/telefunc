export { ChannelMux, getChannelMux }
export type { WireEntry, ReconcileOutcome, ServerTransport, Wire, WirePieces }

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
import { decodeClientFrame, encode, isOneFrame, messageFrames, peekTag, withTook } from '../../wire/frame.js'
import { TAG, isConnCtrlTag } from '../../wire/tags.js'
import { seqThrough } from '../../wire/seq.js'
import { ProtocolViolationError, assertProtocol, isProtocolViolationError } from '../../wire/protocolViolation.js'
import type { ChannelFrame, PingEntry, PongEntry } from '../../wire/frame.js'
import type { BarrierPayload, PreparePayload, ReconcileOpenEntry, ReconcilePayload } from '../../wire/payloads.js'
import { IndexedPeer } from './IndexedPeer.js'
import { ServerHeadroom } from '../../flowControl/server/headroom.js'
import type { ServerPieceReceiver } from '../../flowControl/server/pieceReceiver.js'
import { Deliveries } from '../../flowControl/Deliveries.js'
import { WireSender, type WireSenderOwner } from './WireSender.js'
import { TransportSocket } from './TransportSocket.js'
import { AwaitedChannels, type AwaitedChannel, type RecvBacklog } from './AwaitedChannels.js'
import { RegistrationWaits, type RegistrationWaitsOwner } from './RegistrationWaits.js'
import type { ServerChannel } from './channel.js'

/** A transport-owned wire handle. The mux never looks inside one — it only compares them by
 *  identity and hands them back to the transport that created it. */
type Wire = unknown

// Single-instance kernel: owns channels, sessions, per-wire runtime. Transports talk
// to this class via `onWireOpen` and from then on identify wires by object
// identity. Multi-instance deployments rely on sticky sessions at the load balancer.

type ServerTransport<TWire> = {
  getSessionId(wire: TWire): string | undefined
  setSessionId(wire: TWire, sessionId: string): void
  /** Stable per-wire id, or `null` for transports that don't multiplex client→server
   *  traffic across requests (WebSocket: every frame already lands on the same socket). */
  getConnId(wire: TWire): string | null
  sendNow(wire: TWire, frame: Uint8Array<ArrayBuffer>): void
  /** Its peer gets no part of a message until all of it arrived, as on a WebSocket (see `pieces.ts`). */
  wholeMessages: boolean
  /** Bytes of sent frames still in the wire, never undercounted; `undefined` if the runtime can't tell. */
  bufferedAmount(wire: TWire): number | undefined
  /** The bytes of those a frame that goes at once waits behind, where it passes some: else `bufferedAmount`'s. */
  queuedAhead?(wire: TWire): number | undefined
  terminateWire(wire: TWire): void
}

/** What a RECONCILE or a barrier settled, which `sendReconciled` answers with the RECONCILED. */
type ReconcileOutcome = {
  sessionId: string
  /** Each `lastSeq` is read as the RECONCILED goes out, so it counts what was dispatched before then. */
  attached: ChannelHandle[]
  /** The wire a barrier retired, which gets its FIN once this RECONCILED went out; null but on an SSE→WS upgrade. */
  retiredWire: Wire | null
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
  if (!isProtocolViolationError(err) || err.target !== undefined) return err
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

type DetachReason = 'transient' | 'permanent' | 'recoveryFailed'

type ChannelHandle = { channel: ServerChannel; ix: number; peer: IndexedPeer }

type WirePieces = { headroom: ServerHeadroom; receiver: ServerPieceReceiver }

type WireState = {
  pingTimer: ReturnType<typeof setTimeout> | null
  /** When the wire last delivered a frame, or the ping deadline was last set. */
  lastReceivedAt: number
  /** Each frame, piece and announcement the wire delivered. */
  deliveries: Deliveries
  /** When the last PONG went out. */
  pongedAt: number
  terminatePermanently: boolean
  recvChain: Promise<unknown> | null
  /** Set by `onWireClosed` so an in-flight `reconcile` can see the close and its kind. */
  closed: { isPermanent: boolean } | null
  retiredByBarrier: boolean
  recvBacklog: RecvBacklog
  awaited: AwaitedChannels
  /** Channels attaching in this turn, before its session holds them: what they send counts in the headroom too. */
  attaching: Set<ServerChannel>
}

type WireEntry = {
  wire: Wire
  state: WireState
  transport: ServerTransport<unknown>
  /** One per wire: the peers of every reconcile on this wire share it. */
  sender: WireSender
  /** The request that opened the wire, where one did. */
  request: Request | undefined
}

function getChannelMux(): ChannelMux {
  return getGlobals().mux
}

class ChannelMux implements RegistrationWaitsOwner, WireSenderOwner {
  private readonly channels = new Map<string, ServerChannel>()
  /** The channels a reconcile named before they registered, which `registerChannel` attaches. */
  private readonly registrationWaits = new RegistrationWaits({ owner: this })
  private readonly sessions = new SessionRegistry()
  /** The wire each session is on: the last to reconcile it, until another reconciles it or the wire closes. */
  private readonly sessionWires = new Map<string, Wire>()
  private readonly wires = new Map<unknown, WireEntry>()
  /** Reverse index for transports with a stable connId (SSE). Lets data POSTs locate the
   *  live stream wire, and catches a duplicate-connId reconnect racing teardown. */
  private readonly wiresByConnId = new Map<string, Wire>()
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
    // Before a waiter attaches it: its `onOpen` may end it, which it tells this mux.
    channel._registerChannel(this)
    this.channels.set(channel.id, channel)
    // It ended before a response carried it: it waits for its page as an ended channel does.
    if (channel._phase === 'ended') this.keepEnded(channel, false)
    for (const awaited of this.registrationWaits.take(channel.id)) {
      if (this.registrationWaits.stop(awaited)) this.attachAwaited(awaited, channel)
    }
  }

  /** `RegistrationWaitsOwner`: its channel didn't register within `connectTtl`. */
  onRegistrationTimeout(awaited: AwaitedChannel): void {
    this.expireAwaited(awaited)
  }

  unregisterChannel(channelId: string): void {
    this.channels.delete(channelId)
    this.sessions.removeChannel(channelId)
  }

  /** An ended channel its page may still need (`keep`) stays attachable; the mux forgets any other. */
  onChannelEnded(
    channel: ServerChannel<any, any>,
    { keep, pageAttached }: { keep: boolean; pageAttached: boolean },
  ): void {
    if (keep) this.keepEnded(channel, pageAttached)
    else this.unregisterChannel(channel.id)
  }

  /** Keeps an ended channel attachable until its page needs nothing of it, leaves it, or is away `reconnectTimeout`. */
  private keepEnded(channel: ServerChannel, pageAttached: boolean): void {
    const timer = this.endedChannels.get(channel)
    if (timer) clearTimeout(timer)
    this.endedChannels.set(
      channel,
      pageAttached ? null : unrefTimer(setTimeout(() => this.releaseEnded(channel), this.options.reconnectTimeout)),
    )
  }

  private releaseEnded(channel: ServerChannel): void {
    const timer = this.endedChannels.get(channel)
    if (timer) clearTimeout(timer)
    this.endedChannels.delete(channel)
    this.unregisterChannel(channel.id)
    channel._release()
  }

  // ── Wire lifecycle (transport-facing) ───────────────────────────────

  onWireOpen<TWire>(wire: TWire, transport: ServerTransport<TWire>, request?: Request): void {
    const recvBacklog = { bytes: 0, frames: 0 }
    const headroom = transport.wholeMessages
      ? new ServerHeadroom({
          socket: new TransportSocket({ transport: transport as ServerTransport<unknown>, wire }),
          pingInterval: this.options.pingInterval,
        })
      : null
    const deliveries = new Deliveries()
    const entry: WireEntry = {
      wire,
      state: {
        pingTimer: null,
        lastReceivedAt: 0,
        deliveries,
        pongedAt: performance.now(),
        terminatePermanently: false,
        recvChain: null,
        closed: null,
        retiredByBarrier: false,
        recvBacklog,
        awaited: new AwaitedChannels({ backlog: recvBacklog, waits: this.registrationWaits }),
        attaching: new Set(),
      },
      transport: transport as ServerTransport<unknown>,
      sender: new WireSender({
        owner: this,
        wire,
        transport: transport as ServerTransport<unknown>,
        pieceHeadroom: headroom,
        deliveries,
      }),
      request,
    }
    this.wires.set(wire, entry)
    const connId = transport.getConnId(wire)
    if (connId !== null) this.wiresByConnId.set(connId, wire)
    this.resetPingTimer(entry)
  }

  async onWireMessage(wire: Wire, rawFrame: Uint8Array<ArrayBuffer>): Promise<void> {
    const outcome = await this.dispatchInbound(wire, rawFrame)
    if (outcome) this.sendReconciled(outcome)
  }

  /** As `onWireMessage`, but a RECONCILE's outcome is returned for the caller to send: SSE sends the RECONCILED once
   *  the stream it answers is open. */
  dispatchWireMessage(wire: Wire, rawFrame: Uint8Array<ArrayBuffer>): Promise<ReconcileOutcome | null> {
    return this.dispatchInbound(wire, rawFrame)
  }

  sendReconciled(outcome: ReconcileOutcome): void {
    this.wires.get(outcome.deliverTo)?.sender.dropProbes()
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
    // Sends are sync, so the FIN can't pass anything on the new wire.
    if (outcome.retiredWire !== null) this.send(outcome.retiredWire, encode.fin())
  }

  onWireClosed(wire: Wire, { permanent }: { permanent: boolean }): void {
    const entry = this.wires.get(wire)
    if (!entry) return
    entry.state.closed = { isPermanent: permanent }
    entry.sender.pieces?.headroom.close()
    this.clearPingTimer(entry.state)
    entry.state.awaited.stop()
    entry.sender.close()
    this.wires.delete(wire)
    const connId = entry.transport.getConnId(wire)
    // Identity-equality guards against deleting a *replacement* wire's entry when
    // a duplicate-connId reconnect raced the old wire's teardown.
    if (connId !== null && this.wiresByConnId.get(connId) === wire) {
      this.wiresByConnId.delete(connId)
    }
    this.clearStage(wire)
    const sessionId = entry.transport.getSessionId(wire)
    if (!sessionId) return // Closed before reconciling — nothing to clean up.
    const stagedWs = this.stagedByPrevSession.get(sessionId)
    if (stagedWs !== undefined) this.abandonStage(stagedWs)
    // Channels survive a transient close (`_onPeerDisconnect`'s reconnectTimeout grace);
    // permanent tears them down.
    this.detachSession(sessionId, permanent ? 'permanent' : 'transient')
    if (this.sessionWires.get(sessionId) === wire) this.sessionWires.delete(sessionId)
  }

  /** Ends the channels of a session whose wire closed transiently as a permanent close would have: its page left. */
  endSession(sessionId: string): void {
    this.detachSession(sessionId, 'permanent')
  }

  readPermanentTermination(wire: Wire): boolean {
    return this.wires.get(wire)?.state.terminatePermanently ?? false
  }

  /** SSE data POST: resolve the stream wire by its stable connId. Undefined when the
   *  wire hasn't reconciled yet or has already torn down. */
  getWireByConnId<TWire>(connId: string): TWire | undefined {
    return this.wiresByConnId.get(connId) as TWire | undefined
  }

  // ── Inbound dispatch ────────────────────────────────────────────────

  private dispatchInbound(wire: Wire, rawFrame: Uint8Array<ArrayBuffer>): Promise<ReconcileOutcome | null> {
    const entry = this.wires.get(wire)
    if (!entry) return Promise.resolve(null)
    let outcome: Promise<ReconcileOutcome | null> = Promise.resolve(null)
    try {
      if (isOneFrame(rawFrame)) return this.dispatchFrame(entry, rawFrame, 'alone')
      // Every frame but the last arrives riding.
      assertProtocol(entry.sender.pieces, 'frames in one message on a wire that takes no pieces')
      const frames = messageFrames(rawFrame)
      let next = frames.next()
      while (!next.done && entry.state.closed === null) {
        const frame = next.value
        next = frames.next()
        const dispatched = this.dispatchFrame(entry, frame, next.done ? 'alone' : 'riding')
        if (frame[0] === TAG.RECONCILE) outcome = dispatched
      }
    } catch (err) {
      if (!isProtocolViolationError(err)) throw err
      this.terminateWire(wire)
    }
    return outcome
  }

  /** PING and PIECES_ACK skip the recv chain, which would tie liveness to its slowest handler. */
  private dispatchFrame(
    entry: WireEntry,
    rawFrame: Uint8Array<ArrayBuffer>,
    arrival: 'alone' | 'riding' | 'pieced',
  ): Promise<ReconcileOutcome | null> {
    const { state } = entry
    const byteLength = rawFrame.byteLength
    if (this.isOverBudget(entry, rawFrame)) {
      this.terminateWire(entry.wire)
      return Promise.resolve(null)
    }
    const tag = peekTag(rawFrame)
    if (tag === TAG.PIECE) return this.receivePiece(entry, rawFrame)
    if (tag === TAG.ANNOUNCE) return this.receiveAnnounce(entry, rawFrame)
    const now = performance.now()
    if (arrival !== 'pieced') {
      entry.sender.pieces?.receiver.arrived(tag as number, byteLength, now, arrival)
      state.deliveries.note(now, byteLength)
    }
    state.recvBacklog.bytes += byteLength
    state.recvBacklog.frames++
    // Taken as it arrives: it's handled once those before it were, after later ones arrived.
    const delivery = state.deliveries.last
    state.lastReceivedAt = now
    const exec = (): Promise<ReconcileOutcome | null> => this.runInboundTurn(entry, rawFrame, byteLength, delivery)
    if (tag === TAG.PING || tag === TAG.PIECES_ACK) return exec()
    this.answerArrival(entry)
    return this.chainRecv(entry, exec)
  }

  /** The frame a piece completes is dispatched as one that came whole. */
  private receivePiece(entry: WireEntry, rawFrame: Uint8Array<ArrayBuffer>): Promise<ReconcileOutcome | null> {
    const { state } = entry
    const now = performance.now()
    state.lastReceivedAt = now
    state.deliveries.note(now, rawFrame.byteLength)
    this.answerArrival(entry)
    const piece = decodeClientFrame(rawFrame, WIRE_MAX_CONN_CTRL_FRAME_BYTES)
    assert(piece.tag === TAG.PIECE)
    assertProtocol(entry.sender.pieces, 'PIECE on a wire that takes no pieces')
    // As large as the runtime takes in one message: its pieces get past what it would refuse whole.
    const cap = getAdapterMaxFrameBytes() ?? WIRE_MAX_RAW_FRAME_BYTES
    assertProtocol(piece.total <= cap, 'PIECE of a frame over the cap')
    const { receiver } = entry.sender.pieces
    const heldBefore = receiver.held
    const frame = receiver.add(piece.total, piece.piece, now)
    // What it holds of a frame counts in the recv backlog as the frame does once whole.
    state.recvBacklog.bytes += receiver.held - heldBefore
    state.recvBacklog.frames += Number(receiver.held > 0) - Number(heldBefore > 0)
    if (frame === null) return Promise.resolve(null)
    return this.dispatchFrame(entry, frame, 'pieced')
  }

  private receiveAnnounce(entry: WireEntry, rawFrame: Uint8Array<ArrayBuffer>): Promise<ReconcileOutcome | null> {
    const now = performance.now()
    entry.state.lastReceivedAt = now
    entry.state.deliveries.note(now, rawFrame.byteLength)
    const announce = decodeClientFrame(rawFrame, WIRE_MAX_CONN_CTRL_FRAME_BYTES)
    assert(announce.tag === TAG.ANNOUNCE)
    assertProtocol(entry.sender.pieces, 'ANNOUNCE on a wire that takes no pieces')
    entry.sender.pieces.receiver.announce(announce.bytes, now, announce.floor)
    return Promise.resolve(null)
  }

  /** Bytes reached `wire`: a frame slower than the ping deadline to arrive shows the wire alive meanwhile. */
  onWireBytes(wire: Wire): void {
    const entry = this.wires.get(wire)
    if (!entry) return
    entry.state.lastReceivedAt = performance.now()
    this.answerArrival(entry)
  }

  /** A wire whose bytes keep arriving gets a PONG each ping interval, as its PING may wait behind an upload. */
  private answerArrival(entry: WireEntry): void {
    if (performance.now() - entry.state.pongedAt >= this.options.pingInterval) this.pong(entry, [])
  }

  /** Only data frames and pieces get the raw frame cap; the backlog adds a full window per channel of the session. */
  private isOverBudget(entry: WireEntry, rawFrame: Uint8Array<ArrayBuffer>): boolean {
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
    const sessionId = transport.getSessionId(entry.wire)
    const channels = sessionId === undefined ? 0 : (this.sessions.peekSession(sessionId)?.size ?? 0)
    return (
      backlogBytes > WIRE_RECV_BACKLOG_BASE_BYTES + channels * CREDIT_WINDOW_MAX_BYTES ||
      backlogFrames >= WIRE_RECV_BACKLOG_BASE_FRAMES + channels * CREDIT_MSG_WINDOW_MAX
    )
  }

  private async runInboundTurn(
    entry: WireEntry,
    rawFrame: Uint8Array<ArrayBuffer>,
    byteLength: number,
    delivery: number,
  ): Promise<ReconcileOutcome | null> {
    try {
      entry.state.deliveries.handle(delivery)
      return (await this.handleFrame(entry, rawFrame)) ?? null
    } catch (err) {
      if (!isProtocolViolationError(err)) throw err
      this.terminateWire(err.target ?? entry.wire)
      return null
    } finally {
      entry.state.recvBacklog.bytes -= byteLength
      entry.state.recvBacklog.frames--
    }
  }

  /** `wire` may be a wire other than the one that sent the offending frame — a barrier's
   *  violation is the staged probe's. */
  private terminateWire(wire: Wire): void {
    const entry = this.wires.get(wire)
    if (!entry) return
    if (this.stagedUpgrades.get(wire)?.phase === 'staged') this.clearStage(wire)
    entry.state.terminatePermanently = true
    entry.transport.terminateWire(wire)
  }

  /** Returns a `ReconcileOutcome` only on reconcile (so the caller decides when to send
   *  `reconciled`). Anything but reconcile/ping before reconciliation is a violation. */
  private handleFrame(
    entry: WireEntry,
    rawFrame: Uint8Array<ArrayBuffer>,
  ): null | ReconcileOutcome | Promise<ReconcileOutcome | null> {
    const frame = decodeClientFrame(rawFrame, WIRE_MAX_CONN_CTRL_FRAME_BYTES)
    if (frame.tag === TAG.PING) {
      const pieces = entry.sender.pieces
      if (pieces)
        assertProtocol(pieces.headroom.sender.heartbeat(frame.took, performance.now()), 'PING took more than sent')
      this.resetPingTimer(entry)
      this.acknowledgeArrivals(entry)
      this.pong(entry, this.answerPing(entry, frame.ended))
      return null
    }
    if (frame.tag === TAG.PIECES_ACK) {
      const known = entry.sender.pieces?.headroom.acknowledged(frame)
      assertProtocol(known, 'PIECES_ACK for nothing sent')
      return null
    }
    assertProtocol(!entry.state.retiredByBarrier, 'frame on a wire retired by its barrier')
    assertProtocol(!this.stagedUpgrades.has(entry.wire), 'frame on a staged probe')
    if (frame.tag === TAG.PREPARE) return this.handlePrepare(entry, frame.payload, rawFrame.byteLength)
    if (frame.tag === TAG.BARRIER) return this.handleBarrier(entry, frame.payload)
    if (frame.tag === TAG.RECONCILE) {
      this.claimSessionForReconcile(frame.payload, entry)
      return this.reconcile(entry, frame.payload)
    }
    const sessionId = entry.transport.getSessionId(entry.wire)
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
  private answerPing(entry: WireEntry, ended: PingEntry[]): PongEntry[] {
    assertProtocol(ended.length <= MAX_CHANNELS_PER_CONNECTION, 'PING over entry cap')
    const sessionId = entry.transport.getSessionId(entry.wire)
    if (sessionId === undefined || this.sessionWires.get(sessionId) !== entry.wire) return []
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
  private acknowledgeArrivals(entry: WireEntry): void {
    const sessionId = entry.transport.getSessionId(entry.wire)
    if (sessionId === undefined || this.sessionWires.get(sessionId) !== entry.wire) return
    for (const { channel } of this.sessions.peekSession(sessionId)?.values() ?? []) channel._acknowledge()
  }

  private pong(entry: WireEntry, ended: PongEntry[]): void {
    entry.state.pongedAt = performance.now()
    entry.sender.send(withTook(encode.pong(ended), entry.sender.pieces?.receiver.took ?? 0))
  }

  private dispatchChannelFrame(sessionId: string, frame: ChannelFrame): void {
    // Frame for an ix that's no longer in the session — client closed the channel and the
    // server reconciled it out, but a frame was still in flight. Drop silently.
    this.sessions.get(sessionId, frame.index)?.channel._dispatchFrame(frame)
  }

  /** Abandons the probes staged on the session it names and its wire's, refused mid-commit, unless it names its own. */
  private claimSessionForReconcile(ctrl: ReconcilePayload, entry: WireEntry): void {
    const own = entry.transport.getSessionId(entry.wire)
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

  private handlePrepare(entry: WireEntry, payload: PreparePayload, rawByteLength: number): null {
    const { wire } = entry
    assertProtocol(!entry.transport.getSessionId(wire), 'PREPARE on a reconciled wire')
    assertProtocol(this.sessions.peekSession(payload.sessionId), 'PREPARE for an unknown session')
    // A page stages one attempt at a time, so a newer PREPARE replaces what an earlier attempt left staged.
    const staged = this.stagedByPrevSession.get(payload.sessionId)
    if (staged !== undefined) {
      assertProtocol(this.stagedUpgrades.get(staged)?.phase === 'staged', 'session already committing')
      this.abandonStage(staged)
    }
    assertProtocol(this.stagedUpgrades.size < UPGRADE_MAX_STAGED_RECORDS, 'staged record budget')
    assertProtocol(this.stagedBytes + rawByteLength <= UPGRADE_MAX_STAGED_BYTES, 'staged byte budget')

    const timer = unrefTimer(setTimeout(() => this.abandonStage(wire), UPGRADE_STAGE_TTL_MS))
    this.stagedUpgrades.set(wire, {
      upgradeId: payload.upgradeId,
      prevSessionId: payload.sessionId,
      bytes: rawByteLength,
      timer,
      phase: 'staged',
    })
    this.stagedByPrevSession.set(payload.sessionId, wire)
    this.stagedBytes += rawByteLength
    entry.sender.send(encode.ready({ upgradeId: payload.upgradeId }))
    return null
  }

  private handleBarrier(entry: WireEntry, ctrl: BarrierPayload): Promise<ReconcileOutcome> | null {
    const probe = this.stagedByPrevSession.get(ctrl.sessionId)
    const stage = probe === undefined ? undefined : this.stagedUpgrades.get(probe)
    // A barrier for an unknown or already-committing stage is refused SILENTLY (the client's attempt
    // deadline is the only watchdog) — erroring would tear down a wire this frame has no claim on.
    if (probe === undefined || stage?.phase !== 'staged') return null

    try {
      // Admission policy — `decodeClientFrame` has already established the frame's shape.
      for (const channel of ctrl.open) {
        assertProtocol(textEncoder.encode(channel.id).byteLength <= UPGRADE_MAX_ID_BYTES, 'channel id over byte cap')
      }
      assertProtocol(ctrl.upgradeId === stage.upgradeId, 'barrier upgradeId mismatch', probe)
      assertProtocol(
        entry.transport.getSessionId(entry.wire) === stage.prevSessionId,
        'barrier session mismatch',
        probe,
      )

      const wsEntry = this.wires.get(probe)
      assert(wsEntry, 'staged probe has no wire entry')

      stage.phase = 'committing'
      entry.state.retiredByBarrier = true
      this.moveAwaited(entry, wsEntry, ctrl.open)
      return this.attachBarrier(entry, wsEntry, ctrl, stage.upgradeId)
    } catch (err) {
      this.clearStage(probe)
      throw retargetToProbe(err, probe)
    }
  }

  /** Attaches the channels a barrier names to the WebSocket it commits. */
  private async attachBarrier(
    oldEntry: WireEntry,
    wsEntry: WireEntry,
    ctrl: BarrierPayload,
    upgradeId: string,
  ): Promise<ReconcileOutcome> {
    const probe = wsEntry.wire
    try {
      // `reconcile` ran against the staged WS, so the outcome already carries it as `deliverTo`. Awaited: the stage
      // stays committing for a microtask, so a claim of the old session that runs in it is refused.
      const outcome = await this.reconcile(wsEntry, ctrl, true)
      return { ...outcome, upgradeId }
    } catch (err) {
      oldEntry.state.retiredByBarrier = false
      throw retargetToProbe(err, probe)
    } finally {
      this.clearStage(probe)
    }
  }

  /** Forgets the stage AND kills the probe holding it. No-op once the stage is committing. */
  private abandonStage(probe: Wire): void {
    if (this.stagedUpgrades.get(probe)?.phase !== 'staged') return
    this.clearStage(probe)
    const entry = this.wires.get(probe)
    if (!entry) return
    entry.state.terminatePermanently = true
    entry.transport.terminateWire(probe)
  }

  /** Bookkeeping only — the probe wire is left alone. */
  private clearStage(probe: Wire): void {
    const stage = this.stagedUpgrades.get(probe)
    if (!stage) return
    clearTimeout(stage.timer)
    this.stagedUpgrades.delete(probe)
    if (this.stagedByPrevSession.get(stage.prevSessionId) === probe) {
      this.stagedByPrevSession.delete(stage.prevSessionId)
    }
    this.stagedBytes -= stage.bytes
  }

  // ── Reconcile + attach ──────────────────────────────────────────────

  /** `isBarrier`: this commit retires the old wire's session, so the outcome names that wire, which gets a FIN once
   *  the new wire's RECONCILED has gone out. */
  private reconcile(entry: WireEntry, ctrl: ReconcilePayload, isBarrier = false): ReconcileOutcome {
    const { wire, state, transport } = entry
    const retiredWire = isBarrier && ctrl.sessionId ? (this.sessionWires.get(ctrl.sessionId) ?? null) : null
    this.resetPingTimer(entry)
    // One on the wire that holds the session it names keeps it, and so what is bound to it: a staged upgrade.
    const newSessionId =
      ctrl.sessionId !== undefined && ctrl.sessionId === transport.getSessionId(wire)
        ? ctrl.sessionId
        : crypto.randomUUID()
    // Replayed only on a reconnect, as the session's own wire and a barrier's old one (read to its FIN) still deliver.
    const replay = !isBarrier && newSessionId !== ctrl.sessionId
    const { released } = ctrl
    // The page names what changed only on the wire that has its session: elsewhere it can't know what the server holds.
    assertProtocol(released === undefined || newSessionId === ctrl.sessionId, 'RECONCILE released on another wire')
    const attached =
      released === undefined
        ? this.attachSession(ctrl.sessionId, newSessionId, ctrl.open, entry, replay)
        : this.attachChanges(newSessionId, ctrl.open, released, entry)
    // Its wire no longer has the session it names, which is gone with the handles it had.
    if (ctrl.sessionId) this.sessionWires.delete(ctrl.sessionId)

    // Closed since this frame arrived: the session goes, but a transient close keeps its channels' reattach grace.
    if (state.closed) {
      state.attaching.clear()
      const reason = state.closed.isPermanent ? 'permanent' : 'transient'
      const session = this.sessions.removeSession(newSessionId)
      if (session) for (const handle of session.values()) this.detachHandle(handle, reason)
      state.awaited.stop()
      throw new ProtocolViolationError('connection closed mid-reconcile')
    }

    this.sessionWires.set(newSessionId, wire)
    transport.setSessionId(wire, newSessionId)
    state.attaching.clear()
    return { sessionId: newSessionId, attached, retiredWire, deliverTo: wire }
  }

  /** Attaches the channels a RECONCILE of the whole session names; one of the session before it doesn't name ends. */
  private attachSession(
    prevSessionId: string | undefined,
    newSessionId: string,
    open: ReconcilePayload['open'],
    entry: WireEntry,
    replay: boolean,
  ): ChannelHandle[] {
    const handles = open.map((each) => this.attach(each, entry, replay)).filter((h): h is ChannelHandle => h !== null)

    // Channels in the previous session that the client did NOT re-include are recovery-failed.
    if (prevSessionId) {
      const prev = this.sessions.removeSession(prevSessionId)
      if (prev) {
        const keptIxes = new Set(handles.map((h) => h.ix))
        for (const [ix, prevHandle] of prev) if (!keptIxes.has(ix)) this.detachHandle(prevHandle, 'recoveryFailed')
      }
    }
    entry.state.awaited.forgetUnnamed(open)
    this.sessions.setSession(newSessionId, handles)
    return handles
  }

  /** Attaches the channels a RECONCILE of what changed names to the session, and ends each one the page released. */
  private attachChanges(
    sessionId: string,
    open: ReconcileOpenEntry[],
    released: number[],
    entry: WireEntry,
  ): ChannelHandle[] {
    const session = this.sessions.peekSession(sessionId)
    const { awaited } = entry.state
    const heldId = (ix: number): string | undefined => session?.get(ix)?.channel.id ?? awaited.get(ix)?.open.id
    // At least the channels the wire holds once this frame applies, counted before any is allocated.
    let held = (session?.size ?? 0) + awaited.unattached
    for (const ix of new Set(released)) if (heldId(ix) !== undefined) held--
    for (const { id, ix } of open) {
      const heldAs = heldId(ix)
      // An index names one channel until the page releases it.
      assertProtocol(heldAs === undefined || heldAs === id, 'RECONCILE names a held index under another id')
      if (heldAs === undefined) held++
    }
    assertProtocol(held <= MAX_CHANNELS_PER_CONNECTION, 'RECONCILE over the wire channel cap')
    const handles: ChannelHandle[] = []
    for (const each of open) {
      const handle = this.attach(each, entry, false)
      if (handle === null) continue
      handles.push(handle)
      this.sessions.add(sessionId, handle)
    }
    for (const ix of released) {
      const handle = this.sessions.remove(sessionId, ix)
      if (handle) this.detachHandle(handle, 'recoveryFailed')
    }
    entry.state.awaited.forget(released)
    return handles
  }

  /** Null leaves it out of the RECONCILED; an initial channel not yet registered is awaited until its ATTACH_RESULT. */
  private attach(open: ReconcileOpenEntry, entry: WireEntry, replay: boolean): ChannelHandle | null {
    // Answered ahead of the channel's frames, and of its registration where awaited: its round trip is the path's.
    if (open.probe !== undefined) entry.sender.addProbe(open.ix, open.probe)
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
    if (open.initial && !awaited) this.awaitChannel(open, entry)
    return null
  }

  private awaitChannel(open: ReconcileOpenEntry, entry: WireEntry): void {
    const awaited: AwaitedChannel = {
      channelId: open.id,
      open,
      entry,
      held: [],
      phase: 'waiting',
      ttl: null,
    }
    this.registrationWaits.wait(awaited, this.options.connectTtl)
    entry.state.awaited.add(awaited)
  }

  /** Runs in `registerChannel`, between two reconciles: waiters attach in the order they began; the latest keeps it. */
  private attachAwaited(awaited: AwaitedChannel, channel: ServerChannel): void {
    const { entry } = awaited
    const sessionId = entry.transport.getSessionId(entry.wire)
    assert(sessionId, 'a channel awaited on a wire that never reconciled')
    entry.state.attaching.add(channel)
    const handle = this.attachChannel(channel, awaited.open, entry, false)
    entry.state.attaching.delete(channel)
    if (!handle) {
      this.expireAwaited(awaited)
      return
    }
    this.sessions.add(sessionId, handle)
    entry.state.awaited.attach(awaited)
    // Once the registering code added its listeners; the ATTACH_RESULT follows, so its lastSeq counts what was held.
    const settle = async (): Promise<void> => {
      if (entry.state.awaited.get(awaited.open.ix) !== awaited) return
      if (this.dispatchHeld(awaited)) entry.sender.send(encode.attachResult(awaited.open.ix, channel._lastClientSeq))
    }
    void this.chainRecv(entry, settle).catch(handleTelefunctionBug)
  }

  /** Returns false if what it held broke the protocol, which ends its wire. */
  private dispatchHeld(awaited: AwaitedChannel): boolean {
    const { entry } = awaited
    const held = entry.state.awaited.take(awaited)
    const sessionId = entry.transport.getSessionId(entry.wire)
    assert(sessionId)
    try {
      for (const { frame } of held) this.dispatchChannelFrame(sessionId, frame)
      return true
    } catch (err) {
      if (!isProtocolViolationError(err)) throw err
      this.terminateWire(entry.wire)
      return false
    }
  }

  /** Not registered within `connectTtl`, or shut down as it registered. */
  private expireAwaited(awaited: AwaitedChannel): void {
    awaited.entry.state.awaited.expire(awaited)
    awaited.entry.sender.send(encode.attachResult(awaited.open.ix, null))
  }

  /** Awaited channels the barrier lists move to the WebSocket; an attached one first gets what the old wire held. */
  private moveAwaited(oldEntry: WireEntry, wsEntry: WireEntry, open: ReconcileOpenEntry[]): void {
    for (const { ix } of open) {
      const awaited = oldEntry.state.awaited.get(ix)
      if (!awaited) continue
      if (awaited.phase === 'attached') this.dispatchHeld(awaited)
      else {
        // The WebSocket awaits it from now on, with what it held.
        const held = oldEntry.state.awaited.take(awaited)
        awaited.entry = wsEntry
        wsEntry.state.awaited.add(awaited, held)
      }
    }
  }

  /** With `replay`, sends what the page missed, or ends the channel if the replay lost it; null if it kept nothing. */
  private attachChannel(
    channel: ServerChannel,
    open: ReconcileOpenEntry,
    { sender, request }: WireEntry,
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

  /** Transient: leave registry entries so the next reconcile's prev-comparison can fire
   *  `recovery-failed` on abandoned channels. Permanent: remove outright — no resume. */
  private detachSession(sessionId: string, reason: DetachReason): void {
    const session =
      reason === 'permanent' ? this.sessions.removeSession(sessionId) : this.sessions.peekSession(sessionId)
    if (!session) return
    for (const handle of session.values()) this.detachHandle(handle, reason)
  }

  private detachHandle(h: ChannelHandle, reason: DetachReason): void {
    // An ended channel waits out a lost wire for its page, which lets it go by releasing it, leaving it out of a new
    // wire's RECONCILE, or leaving for good.
    if (this.endedChannels.has(h.channel)) {
      if (reason === 'transient') this.keepEnded(h.channel, false)
      else this.releaseEnded(h.channel)
      return
    }
    switch (reason) {
      case 'permanent':
        h.channel._onPeerClose()
        return
      case 'transient':
        h.channel._onPeerDisconnect(h.peer, this.options.reconnectTimeout)
        return
      case 'recoveryFailed':
        h.channel._onPeerRecoveryFailure()
        return
    }
  }

  // ── Per-wire plumbing (send, recv chain, ping) ────────────────

  /** Sole send path, sync so wire order is call order; a wire closed or past its backlog drops the frame. */
  private send(wire: Wire, frame: Uint8Array<ArrayBuffer>): void {
    this.wires.get(wire)?.sender.send(frame)
  }

  /** The channels the wire's send headroom counts: its session's, and those attaching in this turn. */
  channelsOn(wire: Wire): Set<ServerChannel> {
    const entry = this.wires.get(wire)
    if (entry === undefined) return new Set()
    const sessionId = entry.transport.getSessionId(wire)
    const session = sessionId === undefined ? undefined : this.sessions.peekSession(sessionId)
    const channels = new Set(entry.state.attaching)
    for (const { channel } of session?.values() ?? []) channels.add(channel)
    return channels
  }

  private chainRecv<T>(entry: WireEntry, fn: () => Promise<T>): Promise<T> {
    const prev = entry.state.recvChain ?? Promise.resolve()
    const next = prev.then(fn, fn).finally(() => {
      if (entry.state.recvChain === next) entry.state.recvChain = null
    })
    entry.state.recvChain = next
    return next
  }

  private clearPingTimer(state: WireState): void {
    if (!state.pingTimer) return
    clearTimeout(state.pingTimer)
    state.pingTimer = null
  }

  /** A wire that closed keeps no timer. */
  private resetPingTimer(entry: WireEntry): void {
    if (entry.state.closed !== null) return
    entry.state.lastReceivedAt = performance.now()
    this.armPingDeadline(entry, this.options.pingDeadline)
  }

  private armPingDeadline(entry: WireEntry, ms: number): void {
    const { wire, state, transport } = entry
    this.clearPingTimer(state)
    state.pingTimer = unrefTimer(
      setTimeout(() => {
        state.pingTimer = null
        // A PING waits behind what the page queued: dead past the deadline after its last bytes and its announced data.
        const deadAt = Math.max(
          state.lastReceivedAt + this.options.pingDeadline,
          entry.sender.pieces?.receiver.expectedBy(this.options.pingDeadline) ?? 0,
        )
        const now = performance.now()
        if (now < deadAt) return this.armPingDeadline(entry, Math.min(deadAt - now, TIMER_DELAY_MAX_MS))
        // Transient close so each channel gets its `reconnectTimeout` grace via
        // `_onPeerDisconnect`. Wire-level state is rebuilt by the next reconcile.
        transport.terminateWire(wire)
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
