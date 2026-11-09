export { ChannelMux, getChannelMux }
export type { WireEntry, ReconcileOutcome, ServerTransport, Wire }

import { assert } from '../../../utils/assert.js'
import { getGlobalObject } from '../../../utils/getGlobalObject.js'
import { getAdapterMaxFrameBytes, getChannelConfig } from '../../../node/server/serverConfig.js'
import { unrefTimer } from '../../../utils/unrefTimer.js'
import { handleTelefunctionBug } from '../../../node/server/runTelefunc/validateTelefunctionError.js'
import {
  CREDIT_MSG_WINDOW_MAX,
  CREDIT_WINDOW_MAX_BYTES,
  MAX_CHANNELS_PER_CONNECTION,
  TIMER_DELAY_MAX_MS,
  UPGRADE_MAX_ID_BYTES,
  WIRE_MAX_CTRL_FRAME_BYTES,
  WIRE_MAX_RAW_FRAME_BYTES,
  WIRE_RECV_BACKLOG_BASE_BYTES,
  WIRE_RECV_BACKLOG_BASE_FRAMES,
  type ChannelTransports,
} from '../../constants.js'
import { decodeClientFrame, encode, isOneFrame, messageFrames, peekTag, withTook } from '../../wire/frame.js'
import { TAG, isWireCtrlTag } from '../../wire/tags.js'
import { pingDeadline } from '../../wire/pingDeadline.js'
import { textEncoder } from '../../wire/envelope.js'
import { ProtocolViolationError, assertProtocol, isProtocolViolationError } from '../../wire/protocolViolation.js'
import type { ChannelFrame, PingEntry, PongEntry } from '../../wire/frame.js'
import type { BarrierPayload, PreparePayload, ReconcileOpenEntry, ReconcilePayload } from '../../wire/payloads.js'
import { IndexedPeer } from './IndexedPeer.js'
import type { WireQueue } from '../../flowControl/server/waitingPublishes.js'
import { Deliveries } from '../../flowControl/Deliveries.js'
import { creditWindowOf } from '../../flowControl/credit.js'
import { WireSender } from './WireSender.js'
import { EndedChannels } from './EndedChannels.js'
import { UpgradeStages, type UpgradeStagesOwner } from './UpgradeStages.js'
import { SessionRegistry, type ChannelHandle } from './SessionRegistry.js'
import { AwaitedChannels, expireAwaited, type AwaitedChannel, type RecvBacklog } from './AwaitedChannels.js'
import { RegistrationWaits } from './RegistrationWaits.js'
import type { ServerChannel } from './ServerChannel.js'

/** A transport-owned wire handle. The mux never looks inside one — it only compares them by
 *  identity and hands them back to the transport that created it. */
type Wire = unknown

type ServerTransport<TWire> = {
  getSessionId(wire: TWire): string | undefined
  setSessionId(wire: TWire, sessionId: string): void
  /** Stable per-wire id, or `null` for transports that don't multiplex client→server
   *  traffic across requests (WebSocket: every frame already lands on the same socket). */
  getConnId(wire: TWire): string | null
  sendNow(wire: TWire, frame: Uint8Array<ArrayBuffer>): void
  /** True if the peer gets a message only once all of it has arrived, as on a WebSocket (see `pieces.ts`). */
  wholeMessages: boolean
  /** True if the runtime keeps a copy of each message it's handed, as Bun, Deno and workerd do: Node keeps the bytes. */
  copiesOnSend?: boolean
  /** Bytes of sent frames still in the wire, never undercounted; `undefined` if the runtime can't tell. */
  bufferedAmount(wire: TWire): number | undefined
  /** The part of `bufferedAmount` a frame sent now waits behind, where it can pass the rest (SSE). Without this
   *  method, a frame waits behind all of `bufferedAmount`. */
  queuedAhead?(wire: TWire): number | undefined
  /** What the wire holds back for its page, on a transport that isn't `wholeMessages`. */
  queueOf?(wire: TWire): WireQueue
  terminateWire(wire: TWire): void
}

/** What a RECONCILE or a barrier settled, which `sendReconciled` answers with the RECONCILED. */
type ReconcileOutcome = {
  sessionId: string
  /** Each `lastSeq` is read as the RECONCILED goes out, so it counts what was dispatched before then. */
  attached: ChannelHandle[]
  /** The wire a barrier retired, which gets its FIN once this RECONCILED went out; null except on an SSE→WS upgrade. */
  retiredWire: Wire | null
  /** The wire this RECONCILED belongs on — a barrier reconciles the staged WS, not the sender. */
  deliverTo: Wire
  upgradeId?: string
}

function retargetToProbe(err: unknown, probe: Wire): unknown {
  if (!isProtocolViolationError(err) || err.target !== undefined) return err
  return new ProtocolViolationError(err.message, probe)
}

type MuxServerOptions = {
  reconnectTimeout: number
  idleTimeout: number
  pingInterval: number
  pingDeadline: number
  creditWindow: number
  connectTtl: number
  sseFlushThrottle: number
  ssePostIdleFlushDelay: number
  transports: ChannelTransports
}

type DetachReason = 'transient' | 'permanent' | 'recoveryFailed'

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

/** The server's channel kernel: it owns the channels, the sessions and each wire's state. Transports report a wire
 *  through `onWireOpen` and name it by identity from then on; a deployment with several instances relies on sticky
 *  sessions at its load balancer. */
class ChannelMux implements UpgradeStagesOwner {
  private readonly channels = new Map<string, ServerChannel>()
  /** The channels a reconcile named before they registered, which `registerChannel` attaches. */
  private readonly registrationWaits = new RegistrationWaits()
  private readonly sessions = new SessionRegistry()
  /** The wire each session is on: the last to reconcile it, until another reconciles it or the wire closes. */
  private readonly sessionWires = new Map<string, Wire>()
  private readonly wires = new Map<unknown, WireEntry>()
  /** Reverse index for transports with a stable connId (SSE). Lets data POSTs locate the
   *  live stream wire, and catches a duplicate-connId reconnect racing teardown. */
  private readonly wiresByConnId = new Map<string, Wire>()
  /** The SSE→WS upgrades staged on probe WebSockets. */
  private readonly stages = new UpgradeStages({ owner: this })
  private readonly ended = new EndedChannels({
    onRelease: (channel) => {
      this.unregisterChannel(channel.id)
      channel._release()
    },
  })

  /** Resolved lazily so the mux can be constructed at module-load (the globalObject factory
   *  runs before `serverConfig` is initialized). */
  private resolvedOptions: MuxServerOptions | null = null

  private get options(): MuxServerOptions {
    return (this.resolvedOptions ??= resolveMuxServerOptions())
  }

  /** Exposed for transport-level race timers (SSE's `waitForWire`). */
  get connectTtl(): number {
    return this.options.connectTtl
  }

  // ── ServerChannel registry ──────────────────────────────────────────

  /** Callers must not invoke `channel._registerChannel()` directly. */
  registerChannel(channel: ServerChannel<any, any>): void {
    // Registered before any waiter attaches it: its `onOpen` may end it, and it tells this mux when it does.
    channel._registerChannel(this)
    this.channels.set(channel.id, channel)
    // A channel that ended before a response carried it waits for its page, as any ended channel does.
    if (channel._phase === 'ended') this.ended.keep(channel, this.options.reconnectTimeout)
    for (const awaited of this.registrationWaits.take(channel.id)) {
      if (this.registrationWaits.stop(awaited)) this.attachAwaited(awaited, channel)
    }
  }

  private unregisterChannel(channelId: string): void {
    this.channels.delete(channelId)
    this.sessions.removeChannel(channelId)
  }

  /** An ended channel its page may still need (`keep`) stays attachable; the mux forgets any other. */
  onChannelEnded(
    channel: ServerChannel<any, any>,
    { keep, pageAttached }: { keep: boolean; pageAttached: boolean },
  ): void {
    // Kept until its page needs nothing more from it, leaves, or stays away for `reconnectTimeout`.
    if (keep) this.ended.keep(channel, pageAttached ? null : this.options.reconnectTimeout)
    else this.unregisterChannel(channel.id)
  }

  // ── Wire lifecycle (transport-facing) ───────────────────────────────

  onWireOpen<TWire>(wire: TWire, transport: ServerTransport<TWire>, request?: Request): void {
    const recvBacklog = { bytes: 0, frames: 0 }
    const deliveries = new Deliveries()
    const attaching = new Set<ServerChannel>()
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
        attaching,
      },
      transport: transport as ServerTransport<unknown>,
      sender: new WireSender({
        wire,
        transport: transport as ServerTransport<unknown>,
        pingInterval: this.options.pingInterval,
        deliveries,
        sessions: this.sessions,
        attaching,
      }),
      request,
    }
    this.wires.set(wire, entry)
    const connId = transport.getConnId(wire)
    if (connId !== null) this.wiresByConnId.set(connId, wire)
    this.resetPingTimer(entry)
  }

  async onWireMessage(wire: Wire, rawFrame: Uint8Array<ArrayBuffer>): Promise<void> {
    const outcome = await this.dispatchWireMessage(wire, rawFrame)
    if (outcome) this.sendReconciled(outcome)
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
        creditWindow: this.options.creditWindow,
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
    this.stages.clear(wire)
    const sessionId = entry.transport.getSessionId(wire)
    if (!sessionId) return // Closed before reconciling — nothing to clean up.
    const stagedProbe = this.stages.probeFor(sessionId)
    if (stagedProbe !== undefined) this.stages.abandon(stagedProbe)
    // Channels survive a transient close (`_onPeerDisconnect`'s reconnectTimeout grace);
    // permanent tears them down.
    this.detachSession(sessionId, permanent ? 'permanent' : 'transient')
    if (this.sessionWires.get(sessionId) === wire) this.sessionWires.delete(sessionId)
  }

  /** The page of a session whose wire closed transiently left: ends its channels as a permanent close would. */
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

  /** As `onWireMessage`, but a RECONCILE's outcome is returned for the caller to send: SSE sends the RECONCILED once
   *  the stream it answers is open. */
  dispatchWireMessage(wire: Wire, rawFrame: Uint8Array<ArrayBuffer>): Promise<ReconcileOutcome | null> {
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
    // Read now, as the frame arrives: it is handled only after the frames before it, when later ones may have arrived.
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
    const piece = decodeClientFrame(rawFrame, WIRE_MAX_CTRL_FRAME_BYTES)
    assert(piece.tag === TAG.PIECE)
    assertProtocol(entry.sender.pieces, 'PIECE on a wire that takes no pieces')
    // Pieces let a frame exceed the runtime's limit on one message, so its total is checked against that limit here.
    const cap = getAdapterMaxFrameBytes() ?? WIRE_MAX_RAW_FRAME_BYTES
    assertProtocol(piece.total <= cap, 'PIECE of a frame over the cap')
    const { receiver } = entry.sender.pieces
    const heldBefore = receiver.held
    const frame = receiver.add(piece.total, piece.piece, now)
    // The part of a frame the receiver holds counts in the recv backlog, as the whole frame will.
    state.recvBacklog.bytes += receiver.held - heldBefore
    state.recvBacklog.frames += Number(receiver.held > 0) - Number(heldBefore > 0)
    if (frame === null) return Promise.resolve(null)
    return this.dispatchFrame(entry, frame, 'pieced')
  }

  private receiveAnnounce(entry: WireEntry, rawFrame: Uint8Array<ArrayBuffer>): Promise<ReconcileOutcome | null> {
    const now = performance.now()
    entry.state.lastReceivedAt = now
    entry.state.deliveries.note(now, rawFrame.byteLength)
    const announce = decodeClientFrame(rawFrame, WIRE_MAX_CTRL_FRAME_BYTES)
    assert(announce.tag === TAG.ANNOUNCE)
    assertProtocol(entry.sender.pieces, 'ANNOUNCE on a wire that takes no pieces')
    entry.sender.pieces.receiver.announce(announce.bytes, now, announce.floor)
    return Promise.resolve(null)
  }

  /** Bytes reached `wire`: while a frame takes longer than the ping deadline to arrive, they show the wire is alive. */
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
      tag !== undefined && isWireCtrlTag(tag) && tag !== TAG.PIECE
        ? WIRE_MAX_CTRL_FRAME_BYTES
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
  terminateWire(wire: Wire): void {
    const entry = this.wires.get(wire)
    if (!entry) return
    if (this.stages.get(wire)?.phase === 'staged') this.stages.clear(wire)
    entry.state.terminatePermanently = true
    entry.transport.terminateWire(wire)
  }

  /** Returns a RECONCILE's or barrier's `ReconcileOutcome` for the caller to send. Before a RECONCILE, a channel frame
   *  is a violation; PING, PIECES_ACK, PREPARE and BARRIER go through their own checks. */
  private handleFrame(
    entry: WireEntry,
    rawFrame: Uint8Array<ArrayBuffer>,
  ): null | ReconcileOutcome | Promise<ReconcileOutcome | null> {
    const frame = decodeClientFrame(rawFrame, WIRE_MAX_CTRL_FRAME_BYTES)
    if (frame.tag === TAG.PING) {
      const pieces = entry.sender.pieces
      // A PING counts the bytes the page took off this wire: more than it was sent is a broken peer, and the wire ends.
      if (pieces) {
        const known = pieces.headroom.heartbeat(frame.took, performance.now())
        assertProtocol(known, 'PING says the page took more bytes than this wire sent it')
      }
      this.resetPingTimer(entry)
      this.acknowledgeArrivals(entry)
      this.pong(entry, this.answerPing(entry, frame.ended))
      return null
    }
    if (frame.tag === TAG.PIECES_ACK) {
      const known = entry.sender.acknowledged(frame)
      assertProtocol(known, 'PIECES_ACK for nothing sent')
      return null
    }
    assertProtocol(!entry.state.retiredByBarrier, 'frame on a wire retired by its barrier')
    assertProtocol(this.stages.get(entry.wire) === undefined, 'frame on a staged probe')
    if (frame.tag === TAG.PREPARE) return this.handlePrepare(entry, frame.payload, rawFrame.byteLength)
    if (frame.tag === TAG.BARRIER) return this.handleBarrier(entry, frame.payload)
    if (frame.tag === TAG.RECONCILE) {
      this.claimSessionForReconcile(frame.payload, entry)
      return this.reconcile(entry, frame.payload)
    }
    const sessionId = entry.transport.getSessionId(entry.wire)
    assertProtocol(sessionId, 'frame before reconcile')
    const channelFrame = frame as ChannelFrame
    // A frame for a channel the wire awaits is held until the channel attaches, counted in the recv backlog meanwhile.
    const awaited = entry.state.awaited.get(channelFrame.index)
    if (awaited && awaited.phase !== 'expired') {
      entry.state.awaited.hold(awaited, channelFrame, rawFrame.byteLength)
      return null
    }
    this.dispatchChannelFrame(sessionId, channelFrame)
    return null
  }

  /** Answers each closed channel the page lists on its session's wire: the last seq the server has from it, or null
   *  once the channel is gone. */
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
      channel._onPageClosed(lastSeq)
      const released = this.ended.release(channel)
      answers.push({ ix, lastSeq: released ? null : channel._lastClientSeq })
    }
    return answers
  }

  /** On each page heartbeat, the session's channels acknowledge what arrived, so the page's replay lets it go. */
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

  /** A RECONCILE that doesn't name its own wire's session abandons the probes staged for both sessions: the one it
   *  names and its wire's. A claim on a stage that is committing is refused. */
  private claimSessionForReconcile(ctrl: ReconcilePayload, entry: WireEntry): void {
    const own = entry.transport.getSessionId(entry.wire)
    if (ctrl.sessionId !== undefined && ctrl.sessionId === own) return
    for (const claimed of [ctrl.sessionId, own]) if (claimed !== undefined) this.stages.claim(claimed)
  }

  private handlePrepare(entry: WireEntry, payload: PreparePayload, rawByteLength: number): null {
    const { wire } = entry
    assertProtocol(!entry.transport.getSessionId(wire), 'PREPARE on a reconciled wire')
    assertProtocol(this.sessions.peekSession(payload.sessionId), 'PREPARE for an unknown session')
    this.stages.stage(wire, payload, rawByteLength)
    entry.sender.send(encode.ready({ upgradeId: payload.upgradeId }))
    return null
  }

  private handleBarrier(entry: WireEntry, ctrl: BarrierPayload): Promise<ReconcileOutcome> | null {
    const probe = this.stages.probeFor(ctrl.sessionId)
    const stage = probe === undefined ? undefined : this.stages.get(probe)
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

      this.stages.commit(probe)
      entry.state.retiredByBarrier = true
      this.moveAwaited(entry, wsEntry, ctrl.open)
      return this.attachBarrier(entry, wsEntry, ctrl, stage.upgradeId)
    } catch (err) {
      this.stages.clear(probe)
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
      this.stages.clear(probe)
    }
  }

  // ── Reconcile + attach ──────────────────────────────────────────────

  /** `isBarrier`: this commit retires the old wire's session, so the outcome names that wire, which gets a FIN once
   *  the new wire's RECONCILED has gone out. */
  private reconcile(entry: WireEntry, ctrl: ReconcilePayload, isBarrier = false): ReconcileOutcome {
    const { wire, state, transport } = entry
    const retiredWire = isBarrier && ctrl.sessionId ? (this.sessionWires.get(ctrl.sessionId) ?? null) : null
    this.resetPingTimer(entry)
    // A RECONCILE on the wire that holds the session it names keeps that session, and what is bound to it, such as a
    // staged upgrade.
    const newSessionId =
      ctrl.sessionId !== undefined && ctrl.sessionId === transport.getSessionId(wire)
        ? ctrl.sessionId
        : crypto.randomUUID()
    // Missed frames are replayed only on a reconnect: the session's own wire, and a barrier's old wire read up to its
    // FIN, still deliver them.
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

    // Closed since this frame arrived: the session goes, but a transient close keeps its channels' reconnect grace.
    if (state.closed) {
      state.attaching.clear()
      const reason = state.closed.isPermanent ? 'permanent' : 'transient'
      const session = this.sessions.removeSession(newSessionId)
      if (session) for (const handle of session.values()) this.detachHandle(handle, reason)
      state.awaited.stop()
      throw new ProtocolViolationError('wire closed mid-reconcile')
    }

    this.sessionWires.set(newSessionId, wire)
    transport.setSessionId(wire, newSessionId)
    state.attaching.clear()
    return { sessionId: newSessionId, attached, retiredWire, deliverTo: wire }
  }

  /** Attaches the channels a full-session RECONCILE names; the previous session's channels it leaves out end. */
  private attachSession(
    prevSessionId: string | undefined,
    newSessionId: string,
    open: ReconcilePayload['open'],
    entry: WireEntry,
    replay: boolean,
  ): ChannelHandle[] {
    const handles = open.map((each) => this.attach(each, entry, replay)).filter((h): h is ChannelHandle => h !== null)

    // Channels in the previous session that the client did NOT re-include are recovery-failed.
    const prev = prevSessionId ? this.sessions.removeSession(prevSessionId) : undefined
    if (prev) {
      const keptIxes = new Set(handles.map((h) => h.ix))
      for (const [ix, prevHandle] of prev) if (!keptIxes.has(ix)) this.detachHandle(prevHandle, 'recoveryFailed')
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
    // A lower bound on the channels the wire holds once this frame applies, counted before any is allocated.
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

  /** Returns null to leave the channel out of the RECONCILED; an initial channel not registered yet is awaited until
   *  its ATTACH_RESULT. */
  private attach(open: ReconcileOpenEntry, entry: WireEntry, replay: boolean): ChannelHandle | null {
    // The probe is answered ahead of the channel's frames, and of its registration where the channel is awaited, so its
    // round trip measures the path alone.
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

  /** Runs in `registerChannel`, between two reconciles. Waiters attach in the order they began; the latest one keeps
   *  the channel. */
  private attachAwaited(awaited: AwaitedChannel, channel: ServerChannel): void {
    const { entry } = awaited
    const sessionId = entry.transport.getSessionId(entry.wire)
    assert(sessionId, 'a channel awaited on a wire that never reconciled')
    entry.state.attaching.add(channel)
    const handle = this.attachChannel(channel, awaited.open, entry, false)
    entry.state.attaching.delete(channel)
    if (!handle) {
      expireAwaited(awaited)
      return
    }
    this.sessions.add(sessionId, handle)
    entry.state.awaited.attach(awaited)
    // Runs once the registering code has added its listeners. The ATTACH_RESULT goes after the held frames, so its
    // lastSeq counts them.
    const settle = async (): Promise<void> => {
      if (entry.state.awaited.get(awaited.open.ix) !== awaited) return
      if (this.dispatchHeld(awaited)) entry.sender.send(encode.attachResult(awaited.open.ix, channel._lastClientSeq))
    }
    void this.chainRecv(entry, settle).catch(handleTelefunctionBug)
  }

  /** Dispatches the frames held for `awaited`; returns false if they broke the protocol, which ends its wire. */
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

  /** Moves the barrier's awaited channels to the WebSocket; an attached one first gets what the old wire held. */
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

  /** Attaches `channel` to the wire. With `replay`, first sends what the page missed. Null once the channel keeps
   *  nothing for a page (its replay was released). */
  private attachChannel(
    channel: ServerChannel,
    open: ReconcileOpenEntry,
    { sender, request }: WireEntry,
    replay: boolean,
  ): ChannelHandle | null {
    const buffer = channel._replayBuffer
    if (buffer === null) return null
    channel._onPageAttach(open.lastSeq, request)
    const peer = new IndexedPeer({ sender, index: open.ix, replay: buffer })
    if (replay) for (const frame of buffer.getAfter(open.lastSeq)) sender.send(frame)
    channel._attachPeer(peer, open)
    if (this.ended.has(channel)) this.ended.keep(channel, null)
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
    if (this.ended.has(h.channel)) {
      if (reason === 'transient') this.ended.keep(h.channel, this.options.reconnectTimeout)
      else this.ended.release(h.channel)
      return
    }
    switch (reason) {
      case 'permanent':
        h.channel._onPeerGone()
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

  /** `recheck`: the deadline had passed when the timer last ran, and is judged again once the server read its sockets. */
  private armPingDeadline(entry: WireEntry, ms: number, recheck = false): void {
    const { wire, state, transport } = entry
    this.clearPingTimer(state)
    state.pingTimer = unrefTimer(
      setTimeout(() => {
        state.pingTimer = null
        // A PING waits behind what the page queued, so the wire is dead only a deadline after its last bytes and after
        // the data it announced.
        const deadAt = Math.max(
          state.lastReceivedAt + this.options.pingDeadline,
          entry.sender.pieces?.receiver.expectedBy(this.options.pingDeadline) ?? 0,
        )
        const now = performance.now()
        if (now < deadAt) return this.armPingDeadline(entry, Math.min(deadAt - now, TIMER_DELAY_MAX_MS))
        // Timers run before the sockets are read: after server code that held the event loop past the deadline, the
        // page's PING may be waiting unread. The next turn reads it first.
        if (!recheck) return this.armPingDeadline(entry, 0, true)
        // Transient close so each channel gets its `reconnectTimeout` grace via
        // `_onPeerDisconnect`. Wire-level state is rebuilt by the next reconcile.
        transport.terminateWire(wire)
      }, ms),
    )
  }
}

function resolveMuxServerOptions(): MuxServerOptions {
  const c = getChannelConfig()
  return {
    reconnectTimeout: c.reconnectTimeout,
    idleTimeout: c.idleTimeout,
    pingInterval: c.pingInterval,
    pingDeadline: pingDeadline(c.pingInterval),
    creditWindow: creditWindowOf(c.maxMemory, c.maxPendingBytes),
    connectTtl: c.connectTtl,
    sseFlushThrottle: c.sseFlushThrottle,
    ssePostIdleFlushDelay: c.ssePostIdleFlushDelay,
    transports: c.transports,
  }
}

// Lazy because `getGlobalObject` evaluates its factory eagerly — the factory needs
// `ChannelMux` fully initialized, so defer until first access.
function getGlobals(): { mux: ChannelMux } {
  return getGlobalObject('wire-protocol/channel/server/ChannelMux.ts', () => ({ mux: new ChannelMux() }))
}
