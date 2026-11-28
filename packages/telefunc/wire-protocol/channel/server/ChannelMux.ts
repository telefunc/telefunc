export { ChannelMux, getChannelMux }
export type {
  Fault,
  WireEntry,
  ReconcileOutcome,
  ServerEnd,
  ServerTransport,
  SessionHold,
  TakesUpgrades,
  Wire,
  WireClose,
}

import { assert } from '../../../utils/assert.js'
import { getGlobalObject } from '../../../utils/getGlobalObject.js'
import { getAdapterMaxFrameBytes, getChannelConfig } from '../../../node/server/serverConfig.js'
import { DeadlineTimer } from '../DeadlineTimer.js'
import { handleTelefunctionBug } from '../../../node/server/runTelefunc/validateTelefunctionError.js'
import {
  CREDIT_MSG_WINDOW_MAX,
  CREDIT_WINDOW_MAX_BYTES,
  MAX_CHANNELS_PER_CONNECTION,
  WIRE_MAX_RAW_FRAME_BYTES,
  WIRE_RECV_BACKLOG_BASE_BYTES,
  WIRE_RECV_BACKLOG_BASE_FRAMES,
  CHANNEL_TRANSPORT,
  type ChannelTransport,
  type ChannelTransports,
} from '../../constants.js'
import {
  channelOf,
  decodeClientFrame,
  encode,
  isOneFrame,
  messageFrames,
  peekTag,
  tookOf,
  withTook,
} from '../../wire/frame.js'
import { TAG, isWireCtrlTag } from '../../wire/tags.js'
import { keptEndWindowOf, pingDeadline } from '../../wire/pingDeadline.js'
import { getSessionToken } from '../../wire/requestKind.js'
import { ProtocolViolationError, assertProtocol } from '../../wire/protocolViolation.js'
import { frameRefusal, type Refusal } from '../../refusal.js'
import { ChannelPageLeftError } from '../channelErrors.js'
import { END_CAUSE, type FrameRefusalCause, type SessionCause } from '../endCause.js'
import type { ChannelFrame, PingEntry, PongEntry } from '../../wire/frame.js'
import { WIRE_MAX_CTRL_FRAME_BYTES, isChannelId } from '../../wire/payloads.js'
import type {
  BarrierPayload,
  CallWant,
  PreparePayload,
  ReconcileOpenEntry,
  ReconcilePayload,
} from '../../wire/payloads.js'
import { IndexedPeer } from './IndexedPeer.js'
import type { WireQueue } from '../../flowControl/server/waitingPublishes.js'
import { Deliveries } from '../../flowControl/Deliveries.js'
import { creditWindowOf } from '../../flowControl/credit.js'
import { WireSender } from './WireSender.js'
import { EndedChannels } from './EndedChannels.js'
import { UpgradeStages, type UpgradeStagesOwner } from './UpgradeStages.js'
import { SessionRegistry, type ChannelHandle, type Session } from './SessionRegistry.js'
import { AwaitedChannels, expireAwaited, type AwaitedChannel, type RecvBacklog } from './AwaitedChannels.js'
import { RegistrationWaits } from './RegistrationWaits.js'
import type { ServerChannel } from './ServerChannel.js'

/** A transport-owned wire handle. The mux never looks inside one — it only compares them by
 *  identity and hands them back to the transport that created it. */
type Wire = unknown

type ServerTransport<TWire> = {
  /** Stable per-wire id, or `null` for transports that don't multiplex client→server
   *  traffic across requests (WebSocket: every frame already lands on the same socket). */
  getConnId(wire: TWire): string | null
  sendNow(wire: TWire, frame: Uint8Array<ArrayBuffer>): void
  /** Which transport the wire is, whose ping deadline allows for the PING its page may hold. */
  type: ChannelTransport
  /** True if the runtime keeps a copy of each message it's handed, as Bun, Deno and workerd do: Node keeps the bytes. */
  copiesOnSend?: boolean
  /** Bytes of sent frames still in the wire, never undercounted; `undefined` if the runtime can't tell. */
  bufferedAmount(wire: TWire): number | undefined
  /** The part of `bufferedAmount` a frame sent now waits behind, where it can pass the rest (SSE). Without this
   *  method, a frame waits behind all of `bufferedAmount`. */
  queuedAhead?(wire: TWire): number | undefined
  /** Ends the wire as `end` says, and tells the mux nothing: the mux forgot the wire before. `held`: the session the
   *  wire held, which an HTTP transport keeps for the page's close POST. */
  close(wire: TWire, end: ServerEnd, held: SessionHold | null): void
} & (
  | {
      /** True if the peer gets a message only once all of it has arrived, as on a WebSocket (see `pieces.ts`). */
      wholeMessages: true
    }
  | {
      wholeMessages: false
      /** What the wire holds back for its page. */
      queueOf(wire: TWire): WireQueue
    }
)

/** How the server ends a wire: lost, or for good for a session cause. `refusal`: the end the server decided, which the
 *  wire's own end carries (a WebSocket's close, the event stream's last event); null for a plain loss, which tells
 *  nothing. */
type ServerEnd = { how: Exclude<WireClose, 'pageLeft'>; refusal: Refusal | null }

/** What the server can't go on with on a wire, which `endFault` ends. */
type Fault =
  /** A frame of the channel at `ix` it can't take: that channel ends with `cause`. `wire`: whether the wire goes on, or
   *  closes as a loss, the others reattaching under their session. An `ix` the session holds in no form (undefined
   *  where the frame names none) makes it a fault of the session. */
  | { scope: 'channel'; ix: number | undefined; cause: FrameRefusalCause; wire: 'kept' | 'loss' }
  /** Every channel of the page's session ends with `cause`, and the wire for good. */
  | { scope: 'session'; cause: SessionCause }
  /** The wire is lost: its channels wait `reconnectTimeout` for their page. */
  | { scope: 'loss' }

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
  if (!(err instanceof ProtocolViolationError) || err.target !== undefined) return err
  return new ProtocolViolationError(err.message, probe)
}

type MuxServerOptions = {
  reconnectTimeout: number
  idleTimeout: number
  pingInterval: number
  creditWindow: number
  connectTtl: number
  sseFlushThrottle: number
  ssePostIdleFlushDelay: number
  /** What `config.channel.transports` allows, and the same less `'ws'` for a wire whose server takes no upgrades. */
  transports: ChannelTransports
  transportsWithoutWs: ChannelTransports
}

/** How a wire closed: lost (its page may reconnect), its page left for good, or ended for good by the server for a
 *  session cause, which each channel of the session ends with. */
type WireClose = 'transient' | 'pageLeft' | SessionCause

/** A session as one wire held it: its id, and the page's number for that wire. */
type SessionHold = { sessionId: string; attempt: number }

/** Whether the server a request arrived on takes WebSocket upgrades, asked each time the page that the request connected
 *  is offered its transports: a Node server may get its upgrade listener, or lose it, later. Only the adapter that was
 *  handed the request can say, as for `ClientLeft`. */
type TakesUpgrades = () => boolean

type DetachReason = WireClose | 'recoveryFailed'

type WireState = {
  pingTimer: DeadlineTimer
  /** When the wire last delivered a frame, or the ping deadline was last set. */
  lastReceivedAt: number
  /** Each frame, piece and announcement the wire delivered. */
  deliveries: Deliveries
  /** When the last PONG went out. */
  pongedAt: number
  recvChain: Promise<unknown> | null
  /** The turn of the RECONCILE or BARRIER the message being dispatched completed: a message completes one at most. */
  messageReconcile: Promise<ReconcileOutcome | null> | null
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
  /** The session the wire holds, which it alone speaks for; null on a wire that holds none: one before its first
   *  RECONCILE, a staged probe, and one whose session a reopen or an upgrade's commit took. */
  holds: Session | null
  /** How the wire closed, decided by the server or seen by its transport; null while it is open. A turn queued on the
   *  wire reads it at its start. */
  closed: WireClose | null
  /** Whether the server that opened the wire takes WebSocket upgrades: its page is offered `'ws'` only where it does. */
  takesUpgrades: TakesUpgrades
}

function getChannelMux(): ChannelMux {
  return getGlobals().mux
}

/** The server's channel kernel: it owns the channels, the sessions and each wire's state. Transports report a wire
 *  through `onWireOpen` and name it by identity from then on; a deployment with several instances relies on sticky
 *  sessions at its load balancer. */
class ChannelMux implements UpgradeStagesOwner {
  /** Only calls from the session that carried a channel can change what its page wants. */
  private readonly channels = new Map<string, { channel: ServerChannel; session?: string }>()
  /** The channels a reconcile named before they registered, which `registerChannel` attaches. */
  private readonly registrationWaits = new RegistrationWaits()
  private readonly sessions = new SessionRegistry({
    retiredLeavesAt: () =>
      performance.now() + keptEndWindowOf(this.options.pingInterval, this.options.reconnectTimeout),
  })
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

  /** Exposed for SSE's read-off of a closed wire's bodies, bounded by the wire's ping deadline. */
  get pingInterval(): number {
    return this.options.pingInterval
  }

  // ── Channel registry ──────────────────────────────────────────

  /** Makes a channel available for the client to attach. */
  registerChannel(channel: ServerChannel<any, any>, session?: string): void {
    // Registered before any waiter attaches it: its `onOpen` may end it, and it tells this mux when it does.
    channel._registerChannel(this)
    this.channels.set(channel.id, { channel, session })
    // A channel that ended before a response carried it waits for its page, as any ended channel does.
    if (channel._phase === 'ended') this.ended.keep(channel, this.options.reconnectTimeout)
    for (const awaited of this.registrationWaits.take(channel.id)) {
      if (this.registrationWaits.stop(awaited)) this.attachAwaited(awaited, channel)
    }
  }

  /** Applies the wants `call` carries to the channels its session's earlier calls returned, and settles once each is in
   *  effect or its channel ended (`_applyCallWant`, which ends one still on its way after `connectTtl`). A want of a
   *  channel another session holds, or none, changes nothing.
   *  Undefined where nothing has to happen first. */
  applyCallWants(call: Request, wants: CallWant[]): Promise<unknown> | undefined {
    const session = getSessionToken(call)
    const pending: Promise<void>[] = []
    for (const want of wants) {
      const entry = this.channels.get(want.channel)
      if (!entry || session === undefined || entry.session !== session) continue
      const inEffect = entry.channel._applyCallWant(want, call.signal, this.options.connectTtl)
      if (inEffect) pending.push(inEffect)
    }
    return pending.length === 0 ? undefined : Promise.all(pending)
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

  /** Lets go of the ended channel `id`, kept for its page, where the memory ledger needs the room. */
  letGoEnded(id: string): void {
    const channel = this.channels.get(id)?.channel
    assert(channel)
    this.ended.release(channel)
  }

  // ── Wire lifecycle (transport-facing) ───────────────────────────────

  onWireOpen<TWire>(
    wire: TWire,
    transport: ServerTransport<TWire>,
    takesUpgrades: TakesUpgrades,
    request?: Request,
  ): void {
    const recvBacklog = { bytes: 0, frames: 0 }
    const deliveries = new Deliveries()
    const attaching = new Set<ServerChannel>()
    const entry: WireEntry = {
      wire,
      state: {
        pingTimer: new DeadlineTimer({ unref: true }),
        lastReceivedAt: 0,
        deliveries,
        pongedAt: performance.now(),
        recvChain: null,
        messageReconcile: null,
        recvBacklog,
        awaited: new AwaitedChannels({ backlog: recvBacklog, waits: this.registrationWaits }),
        attaching,
      },
      transport: transport as ServerTransport<unknown>,
      sender: new WireSender({
        wire,
        transport: transport as ServerTransport<unknown>,
        pingInterval: this.options.pingInterval,
        sseFlushThrottle: this.options.sseFlushThrottle,
        deliveries,
        heldChannels: () => entry.holds?.channels,
        attaching,
        onPastBacklog: () => this.endFault(wire, { scope: 'session', cause: 'sendBudget' }),
      }),
      request,
      holds: null,
      closed: null,
      takesUpgrades,
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
    const entry = this.wires.get(outcome.deliverTo)
    if (entry) {
      entry.sender.sendReconciled(
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
          transports: entry.takesUpgrades() ? this.options.transports : this.options.transportsWithoutWs,
        }),
      )
    }
    for (const { channel } of outcome.attached) channel._confirmWant()
    // Sends are sync, so the FIN can't pass anything on the new wire.
    if (outcome.retiredWire !== null) this.wires.get(outcome.retiredWire)?.sender.send(encode.fin())
  }

  /** The wire's socket sent what it held: what the ledger holds for the wire and the socket no longer holds goes. */
  onWireDrain(wire: Wire): void {
    this.wires.get(wire)?.sender.rebalanceMemory()
  }

  /** The wire's transport saw it close: the mux forgets it. Returns the session the wire held, if it held one. */
  onWireClosed(wire: Wire, reason: WireClose): SessionHold | undefined {
    const entry = this.wires.get(wire)
    if (!entry) return undefined
    const held = this.decideClose(entry, reason)
    this.dispose(entry, reason)
    return held ?? undefined
  }

  /** The one close the server decides on a wire, as `end` says, the only caller of `transport.close`. The mux forgets
   *  the wire first, so a later decision, its sender's included, finds nothing; the transport then gets the channels'
   *  ends its sender kept, and ends it; what the wire held goes last, once its bytes were handed, and also where the
   *  transport throws. */
  private closeWire(entry: WireEntry, end: ServerEnd): void {
    if (this.wires.get(entry.wire) !== entry) return
    const held = this.decideClose(entry, end.how)
    try {
      entry.sender.handLastEnds()
      entry.transport.close(entry.wire, end, held)
    } finally {
      this.dispose(entry, end.how)
    }
  }

  /** The wire is closed, as `reason` says: it leaves the mux's indexes. Returns the session it held, read before the
   *  wire releases it. */
  private decideClose(entry: WireEntry, reason: WireClose): SessionHold | null {
    const { wire } = entry
    entry.closed = reason
    this.wires.delete(wire)
    const connId = entry.transport.getConnId(wire)
    // Identity-equality guards against deleting a *replacement* wire's entry when
    // a duplicate-connId reconnect raced the old wire's teardown.
    if (connId !== null && this.wiresByConnId.get(connId) === wire) {
      this.wiresByConnId.delete(connId)
    }
    const session = entry.holds
    // The wire that holds a session is the page's newest to take it, so its number is the session's highest.
    return session === null ? null : { sessionId: session.id, attempt: session.high }
  }

  /** Drops what the closed wire held: its timer, what it awaits, its sender, its stage, its session, whose channels
   *  detach as `reason` says, the app's callbacks running there, and last the probe its page was moving to, whose
   *  transport may throw as it closes. */
  private dispose(entry: WireEntry, reason: WireClose): void {
    const { wire } = entry
    entry.state.pingTimer.disarm()
    entry.state.awaited.stop()
    entry.sender.close()
    this.stages.clear(wire)
    // A wire that holds no session speaks for no channel: its page's channels are another wire's, or none.
    const session = entry.holds
    if (session === null) return
    const stagedProbe = this.stages.probeFor(session.id)
    this.sessions.release(entry)
    // Channels survive a transient close (`_onPeerDisconnect`'s reconnectTimeout grace); the others end them.
    this.detachSession(session, reason)
    if (stagedProbe !== undefined) this.stages.abandon(stagedProbe)
  }

  /** The page of a session that no wire holds left, `hold` naming the wire its leaving came for: ends its channels as
   *  that wire's close would have, where that wire was the last to hold the session. A session a later wire took is
   *  that wire's to end, held or not. */
  endSession(hold: SessionHold): void {
    const session = this.sessions.get(hold.sessionId)
    if (session === undefined || session.holder !== null || session.high !== hold.attempt) return
    this.detachSession(session, 'pageLeft')
  }

  /** SSE data POST: resolve the stream wire by its stable connId. Undefined when the
   *  wire hasn't reconciled yet or has already torn down. */
  getWireByConnId<TWire>(connId: string): TWire | undefined {
    return this.wiresByConnId.get(connId) as TWire | undefined
  }

  // ── Inbound dispatch ────────────────────────────────────────────────

  /** As `onWireMessage`, but a RECONCILE's or BARRIER's outcome is returned for the caller to send: SSE sends the
   *  RECONCILED once the stream it answers is open. */
  dispatchWireMessage(wire: Wire, rawFrame: Uint8Array<ArrayBuffer>): Promise<ReconcileOutcome | null> {
    const entry = this.wires.get(wire)
    if (!entry) return Promise.resolve(null)
    entry.state.messageReconcile = null
    try {
      if (isOneFrame(rawFrame)) return this.dispatchFrame(entry, rawFrame, 'alone')
      // Every frame but the last arrives riding.
      assertProtocol(entry.sender.pieces, 'frames in one message on a wire that takes no pieces')
      const frames = messageFrames(rawFrame)
      let next = frames.next()
      while (!next.done && entry.closed === null) {
        const frame = next.value
        next = frames.next()
        void this.dispatchFrame(entry, frame, next.done ? 'alone' : 'riding')
      }
    } catch (err) {
      if (!(err instanceof ProtocolViolationError)) throw err
      this.endFault(wire, { scope: 'session', cause: 'unreadable' })
    }
    // Whichever frame completed it, a piece of it too.
    return entry.state.messageReconcile ?? Promise.resolve(null)
  }

  /** PING and PIECES_ACK skip the recv chain, which would tie liveness to its slowest handler. */
  private dispatchFrame(
    entry: WireEntry,
    rawFrame: Uint8Array<ArrayBuffer>,
    arrival: 'alone' | 'riding' | 'pieced',
  ): Promise<ReconcileOutcome | null> {
    const { state } = entry
    const byteLength = rawFrame.byteLength
    const over = this.overBudget(entry, rawFrame)
    if (over !== null) {
      this.endFault(
        entry.wire,
        over === 'frame'
          ? { scope: 'channel', ix: channelOf(rawFrame), cause: 'frameOverLimit', wire: 'loss' }
          : { scope: 'session', cause: 'receiveBudget' },
      )
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
    if (tag !== TAG.RECONCILE && tag !== TAG.BARRIER) {
      this.answerArrival(entry)
      return this.chainRecv(entry, exec)
    }
    // Each one owes a RECONCILED, and a page has one out at a time.
    assertProtocol(state.messageReconcile === null, 'a second RECONCILE or BARRIER in one message')
    if (tag === TAG.BARRIER) {
      this.answerArrival(entry)
      state.messageReconcile = this.chainRecv(entry, exec)
      return state.messageReconcile
    }
    this.answerArrival(entry)
    if (entry.sender.pieces) {
      const receipt = encode.reconcileTaken()
      // A RECONCILE waiting behind other frames starts its reply deadline on its own receipt.
      if (state.recvChain !== null) entry.sender.send(receipt)
      else entry.sender.carry(receipt)
    }
    state.messageReconcile = this.chainRecv(entry, exec)
    return state.messageReconcile
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

  /** What `rawFrame` is over, if anything: its cap (only data frames and pieces get the raw frame cap), or the backlog
   *  of what the wire received and hasn't handled, which adds a full window per channel of the session. */
  private overBudget(entry: WireEntry, rawFrame: Uint8Array<ArrayBuffer>): 'frame' | 'backlog' | null {
    const tag = peekTag(rawFrame)
    const maxFrameBytes =
      tag !== undefined && isWireCtrlTag(tag) && tag !== TAG.PIECE
        ? WIRE_MAX_CTRL_FRAME_BYTES
        : WIRE_MAX_RAW_FRAME_BYTES
    const byteLength = rawFrame.byteLength
    if (byteLength > maxFrameBytes) return 'frame'
    const { state } = entry
    const backlogBytes = state.recvBacklog.bytes + byteLength
    const backlogFrames = state.recvBacklog.frames
    if (backlogBytes <= WIRE_RECV_BACKLOG_BASE_BYTES && backlogFrames < WIRE_RECV_BACKLOG_BASE_FRAMES) return null
    const channels = entry.holds?.channels.size ?? 0
    const over =
      backlogBytes > WIRE_RECV_BACKLOG_BASE_BYTES + channels * CREDIT_WINDOW_MAX_BYTES ||
      backlogFrames >= WIRE_RECV_BACKLOG_BASE_FRAMES + channels * CREDIT_MSG_WINDOW_MAX
    return over ? 'backlog' : null
  }

  private async runInboundTurn(
    entry: WireEntry,
    rawFrame: Uint8Array<ArrayBuffer>,
    byteLength: number,
    delivery: number,
  ): Promise<ReconcileOutcome | null> {
    try {
      entry.state.deliveries.handle(delivery)
      // A turn queued on a wire that closed since takes nothing: the close dealt with what the wire held, and what the
      // frame names the page names again on its next wire. Once the wire ended its session, the page names its channels
      // on no other wire, and those a RECONCILE queued behind that end names end with its cause.
      if (entry.closed !== null) {
        if (entry.closed !== 'transient' && entry.closed !== 'pageLeft')
          this.refuseReconciled(entry, rawFrame, entry.closed)
        return null
      }
      return (await this.handleFrame(entry, rawFrame)) ?? null
    } catch (err) {
      // A bug is reported here, as no WebSocket host awaits a message's handling. The wire goes on, unless its page
      // waits for the RECONCILED that won't come: then it ends as a lost one, and the page resumes on the next.
      if (!(err instanceof ProtocolViolationError)) {
        handleTelefunctionBug(err)
        if (peekTag(rawFrame) === TAG.RECONCILE) this.endFault(entry.wire, { scope: 'loss' })
      } else {
        const fault: Fault = { scope: 'channel', ix: channelOf(rawFrame), cause: 'frameRefused', wire: 'loss' }
        this.endFault(err.target ?? entry.wire, fault)
      }
      return null
    } finally {
      entry.state.recvBacklog.bytes -= byteLength
      entry.state.recvBacklog.frames--
    }
  }

  /** Every end the server decides on `wire` is decided here: what `fault` ends and how the wire goes. `wire` may be a
   *  wire other than the one that sent the offending frame: a barrier's violation is the staged probe's. An end that
   *  keeps the wire is its channel's, told on its own down stream; one that closes the wire rides the wire's own end,
   *  its refusal (see `ServerTransport.close`). What the others sent that the server didn't confirm goes again on the
   *  next wire. */
  endFault(wire: Wire, fault: Fault): void {
    const entry = this.wires.get(wire)
    if (!entry) return
    const session = entry.holds
    // A wire that holds no session speaks for no channel, so whatever it did it goes as a loss, and its page reopens: a
    // wire before its first RECONCILE, a staged probe, one whose session another wire took.
    if (session === null || fault.scope === 'loss') {
      entry.sender.close('dropped')
      return this.closeWire(entry, { how: 'transient', refusal: null })
    }
    // The instant a session's end, where this is one, is kept until: counted from the decision, before its teardown.
    const keptUntil = performance.now() + keptEndWindowOf(this.options.pingInterval, this.options.reconnectTimeout)
    // A wire the end closes says it as its last word: what its channels send on it from here goes no more, kept in
    // their replay for their next attach.
    if (fault.scope !== 'channel' || fault.wire !== 'kept') entry.sender.close('first')
    const channelEnded = fault.scope === 'channel' && this.endChannelAt(entry, fault.ix, fault.cause)
    if (channelEnded && fault.wire === 'kept') return
    if (fault.scope === 'channel' && channelEnded) {
      return this.closeWire(entry, { how: 'transient', refusal: frameRefusal(fault.cause, fault.ix) })
    }
    const cause = fault.scope === 'session' ? fault.cause : 'unreadable'
    // Its page, should it miss the wire's last word, reads the end at its reopen.
    this.sessions.keepEnd(session.id, cause, session.high, keptUntil)
    this.endSessionOn(entry, cause)
  }

  /** The session ends for `cause` on `entry`'s wire, which closes for good with it as its last word: its channels end as
   *  the wire closes (`detachHandle`), and those it awaits, as they register. */
  private endSessionOn(entry: WireEntry, cause: SessionCause): void {
    entry.sender.close('first')
    entry.state.awaited.refuseAll(cause)
    this.closeWire(entry, { how: cause, refusal: frameRefusal(cause, undefined) })
  }

  /** Ends the channel at `ix` with `cause` where the session holds it, or keeps the cause for its registration where
   *  the wire awaits it; false where neither, as for a frame that names no channel. */
  private endChannelAt(entry: WireEntry, ix: number | undefined, cause: FrameRefusalCause): boolean {
    if (ix === undefined) return false
    const handle = entry.holds?.channels.get(ix)
    if (handle) {
      handle.channel._onServerEnd(cause)
      return true
    }
    const awaited = entry.state.awaited.get(ix)
    if (!awaited) return false
    entry.state.awaited.refuse(awaited, cause)
    return true
  }

  /** Returns a RECONCILE's or barrier's `ReconcileOutcome` for the caller to send. Before a RECONCILE, a channel frame
   *  is a violation; PING, PIECES_ACK, PREPARE and BARRIER go through their own checks. */
  private handleFrame(
    entry: WireEntry,
    rawFrame: Uint8Array<ArrayBuffer>,
  ): null | ReconcileOutcome | Promise<ReconcileOutcome | null> {
    const frame = decodeClientFrame(rawFrame, WIRE_MAX_CTRL_FRAME_BYTES)
    this.countTook(entry, rawFrame)
    if (frame.tag === TAG.PING) {
      this.handlePing(entry, frame.ended)
      return null
    }
    if (frame.tag === TAG.PIECES_ACK) {
      const known = entry.sender.pieces?.headroom.acknowledged(frame)
      assertProtocol(known, 'PIECES_ACK for nothing sent')
      entry.sender.rebalanceMemory()
      return null
    }
    assertProtocol(this.stages.get(entry.wire) === undefined, 'frame on a staged probe')
    if (frame.tag === TAG.PREPARE) return this.handlePrepare(entry, frame.payload, rawFrame.byteLength)
    if (frame.tag === TAG.BARRIER) return this.handleBarrier(entry, frame.payload)
    if (frame.tag === TAG.RECONCILE) return this.handleReconcile(entry, frame.payload)
    const session = entry.holds
    assertProtocol(session, 'frame on a wire that holds no session')
    const channelFrame = frame as ChannelFrame
    // A frame for a channel the wire awaits is held until the channel attaches, counted in the recv backlog meanwhile.
    const awaited = entry.state.awaited.get(channelFrame.index)
    if (awaited && awaited.phase !== 'expired') {
      entry.state.awaited.hold(awaited, channelFrame, rawFrame.byteLength)
      return null
    }
    this.dispatchChannelFrame(session, channelFrame)
    return null
  }

  /** The bytes the page says it took off this wire, which a frame may carry: more than it was sent is a broken peer, and
   *  the wire ends. */
  private countTook(entry: WireEntry, rawFrame: Uint8Array<ArrayBuffer>): void {
    const took = tookOf(rawFrame)
    const pieces = entry.sender.pieces
    if (took === null || !pieces) return
    const known = pieces.headroom.heartbeat(took, performance.now())
    assertProtocol(known, 'the page says it took more bytes than this wire sent it')
    entry.sender.rebalanceMemory()
  }

  /** A PING is answered with a PONG, whose answers for the channels it names wait for the frames the page sent ahead
   *  of it. */
  private handlePing(entry: WireEntry, ended: PingEntry[]): void {
    assertProtocol(ended.length <= MAX_CHANNELS_PER_CONNECTION, 'PING over entry cap')
    this.resetPingTimer(entry)
    // The session's channels acknowledge what arrived, so the page's replay lets it go.
    for (const { channel } of entry.holds?.channels.values() ?? []) channel._acknowledge()
    if (ended.length === 0 || entry.state.recvChain === null) return this.pong(entry, this.answerPing(entry, ended))
    // What it names, the page sent before it: answered once the frames ahead of it had their turn.
    this.pong(entry, [])
    void this.chainRecv(
      entry,
      async () => void (entry.closed === null && this.pong(entry, this.answerPing(entry, ended))),
    )
  }

  /** A RECONCILE opens or reopens the page's session on `entry`, or, for an attempt older than one answered or for a
   *  session the server ended, ends the wire or the session instead. */
  private handleReconcile(entry: WireEntry, ctrl: ReconcilePayload): null | ReconcileOutcome {
    // An open its wire can't send is refused before anything kept is answered or any channel it names is touched.
    this.admitReconcile(entry, ctrl)
    const kept = this.sessions.keptOf(ctrl.sessionId)
    if (kept !== undefined) {
      // An attempt not above the highest the record holds is older than one the server answered: it takes nothing.
      if (ctrl.attempt === undefined || ctrl.attempt <= kept.high) {
        this.endFault(entry.wire, { scope: 'loss' })
        return null
      }
      // A reopen of a session the server ended gets the same end, before anything of it is claimed, attached or
      // awaited, and so does each channel it names. The record is not written again: only its highest attempt
      // rises, so that one between this and the last, read later still, takes nothing; its instant stays.
      if (kept.end !== 'retired') {
        kept.high = ctrl.attempt
        this.refuseNamed(ctrl.open, entry, kept.end)
        this.endSessionOn(entry, kept.end)
        return null
      }
      // A newer attempt of a session that retired takes it again.
      this.sessions.revive(ctrl.sessionId)
    }
    if (this.isStale(entry, ctrl)) {
      this.endFault(entry.wire, { scope: 'loss' })
      return null
    }
    this.claimSessionForReconcile(ctrl, entry)
    return this.reconcile(entry, ctrl)
  }

  /** The channels a RECONCILE on `entry` names, where it is a RECONCILE, end with `cause`: its wire ended its session
   *  before its turn. */
  private refuseReconciled(entry: WireEntry, rawFrame: Uint8Array<ArrayBuffer>, cause: SessionCause): void {
    if (peekTag(rawFrame) !== TAG.RECONCILE) return
    const frame = decodeClientFrame(rawFrame, WIRE_MAX_CTRL_FRAME_BYTES)
    if (frame.tag === TAG.RECONCILE) this.refuseNamed(frame.payload.open, entry, cause)
  }

  /** A RECONCILE an end of its session covers, `cause`'s, refuses each channel it names, registered or not: one the
   *  server has ends now, and an initial one it hasn't registered yet ends as it registers, within `connectTtl`. */
  private refuseNamed(open: ReconcileOpenEntry[], entry: WireEntry, cause: SessionCause): void {
    for (const each of open) {
      const channel = this.channels.get(each.id)?.channel
      if (channel) channel._onServerEnd(cause)
      // One refused wait per channel, however many reopens name it: the first refusal stands, its deadline too.
      else if (each.initial && !this.registrationWaits.refuses(each.id)) {
        const refused: AwaitedChannel = {
          channelId: each.id,
          open: each,
          entry,
          held: [],
          phase: 'waiting',
          ttl: null,
          refused: cause,
        }
        this.registrationWaits.wait(refused, this.options.connectTtl)
      }
    }
  }

  /** Refuses a RECONCILE its wire can't send, before anything of a session is made, claimed or handed over: a refused
   *  wire leaves nothing behind. */
  private admitReconcile(entry: WireEntry, ctrl: ReconcilePayload): void {
    const held = entry.holds
    // A page has one session, and names it on every wire: a wire that holds one speaks for that one alone.
    assertProtocol(held === null || ctrl.sessionId === held.id, "RECONCILE names another session than its wire's")
    // The page names what changed only on the wire that holds its session: elsewhere it can't know what the server holds.
    assertProtocol(ctrl.released === undefined || held !== null, 'RECONCILE released on another wire')
    assertProtocol(
      held !== null || ctrl.attempt !== undefined,
      'RECONCILE without its attempt on a wire that holds no session',
    )
  }

  /** Whether a RECONCILE on a wire that doesn't hold the session comes from an older attempt of the page's than the one
   *  that took the session: read after it, it takes nothing, claims no stage, and its wire goes as a loss. */
  private isStale(entry: WireEntry, ctrl: ReconcilePayload): boolean {
    if (entry.holds !== null || ctrl.attempt === undefined) return false
    const named = this.sessions.get(ctrl.sessionId)
    return named !== undefined && ctrl.attempt <= named.high
  }

  /** Answers each closed channel the page lists on its session's wire: the last seq the server has from it, or null
   *  once the channel is gone. */
  private answerPing(entry: WireEntry, ended: PingEntry[]): PongEntry[] {
    const session = entry.holds
    if (session === null) return []
    const answers: PongEntry[] = []
    for (const { ix, lastSeq } of ended) {
      const channel = session.channels.get(ix)?.channel
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

  private pong(entry: WireEntry, ended: PongEntry[]): void {
    entry.state.pongedAt = performance.now()
    entry.sender.send(withTook(encode.pong(ended), entry.sender.pieces?.receiver.took ?? 0))
  }

  private dispatchChannelFrame(session: Session, frame: ChannelFrame): void {
    // Frame for an ix that's no longer in the session — client closed the channel and the
    // server reconciled it out, but a frame was still in flight. Drop silently.
    session.channels.get(frame.index)?.channel._dispatchFrame(frame)
  }

  /** A RECONCILE on a wire that holds no session abandons the probe staged for the session it names (`admitReconcile`
   *  refused one that names another than its wire's). A claim on a stage that is committing is refused. */
  private claimSessionForReconcile(ctrl: ReconcilePayload, entry: WireEntry): void {
    if (ctrl.sessionId === entry.holds?.id) return
    this.stages.claim(ctrl.sessionId)
  }

  private handlePrepare(entry: WireEntry, payload: PreparePayload, rawByteLength: number): null {
    const { wire } = entry
    assertProtocol(entry.holds === null, 'PREPARE on a wire that holds a session')
    const session = this.sessions.get(payload.sessionId)
    assertProtocol(session, 'PREPARE for an unknown session')
    // An older attempt's than the session's wire or its stage: the probe goes as a loss, and nothing else is touched.
    if (payload.attempt <= session.high || !this.stages.stage(wire, payload, rawByteLength)) {
      this.endFault(wire, { scope: 'loss' })
      return null
    }
    // From the READY on, the page holds back what it would send on its old wire, its confirmations included.
    for (const { channel } of session.channels.values()) channel._onPageMoving()
    entry.sender.send(encode.ready({ upgradeId: payload.upgradeId }))
    return null
  }

  /** A barrier is admitted before any stage or channel is touched: its wire holds the session it names, and that
   *  session's stage is staged under its `upgradeId`, for an attempt newer than the session's. */
  private handleBarrier(entry: WireEntry, ctrl: BarrierPayload): Promise<ReconcileOutcome> | null {
    const probe = this.stages.probeFor(ctrl.sessionId)
    const stage = probe === undefined ? undefined : this.stages.get(probe)
    const session = entry.holds
    // A newer reopen took the session from the barrier's wire: the barrier is stale. Its own stage goes, and so does
    // its wire, which holds nothing; a stage under another `upgradeId` is another attempt's.
    if (session === null || session.id !== ctrl.sessionId) {
      if (probe !== undefined && stage?.upgradeId === ctrl.upgradeId) this.stages.abandon(probe)
      this.endFault(entry.wire, { scope: 'loss' })
      return null
    }
    // A barrier for no stage, a committing one or another upgrade's is refused SILENTLY (the page's attempt deadline is
    // its only watchdog): its wire still holds the session, and the stage waits for its own barrier.
    if (probe === undefined || stage?.phase !== 'staged' || stage.upgradeId !== ctrl.upgradeId) return null

    try {
      // Admission policy — `decodeClientFrame` has already established the frame's shape.
      for (const channel of ctrl.open) {
        assertProtocol(isChannelId(channel.id), 'channel id over byte cap')
      }
      // A page numbers its probe after the wire it moves off.
      assertProtocol(stage.attempt > session.high, "staged attempt not above the session's", probe)

      const wsEntry = this.wires.get(probe)
      assert(wsEntry, 'staged probe has no wire entry')

      this.stages.commit(probe)
      this.moveAwaited(entry, wsEntry, ctrl.open)
      return this.attachBarrier(wsEntry, { ...ctrl, attempt: stage.attempt }, stage.upgradeId)
    } catch (err) {
      this.stages.clear(probe)
      throw retargetToProbe(err, probe)
    }
  }

  /** Attaches the channels a barrier names to the WebSocket it commits. */
  private async attachBarrier(
    wsEntry: WireEntry,
    ctrl: ReconcilePayload,
    upgradeId: string,
  ): Promise<ReconcileOutcome> {
    const probe = wsEntry.wire
    try {
      // `reconcile` ran against the staged WS, so the outcome already carries it as `deliverTo`. Awaited: the stage
      // stays committing for a microtask, so a claim of the old session that runs in it is refused.
      const outcome = await this.reconcile(wsEntry, ctrl, true)
      return { ...outcome, upgradeId }
    } catch (err) {
      throw retargetToProbe(err, probe)
    } finally {
      this.stages.clear(probe)
    }
  }

  // ── Reconcile + attach ──────────────────────────────────────────────

  /** `isBarrier`: this commit retires the old wire's session, so the outcome names that wire, which gets a FIN once
   *  the new wire's RECONCILED has gone out. */
  private reconcile(entry: WireEntry, ctrl: ReconcilePayload, isBarrier = false): ReconcileOutcome {
    entry.sender.owesReconciled()
    try {
      return this.settleReconcile(entry, ctrl, isBarrier)
    } catch (err) {
      // A bug: no RECONCILED will go, and what the wire holds for it would stay held on a wire that is up. It ends as a
      // lost wire does, its channels keeping their grace; a protocol violation ends it for good where it is caught.
      if (!(err instanceof ProtocolViolationError)) this.endFault(entry.wire, { scope: 'loss' })
      throw err
    }
  }

  private settleReconcile(entry: WireEntry, ctrl: ReconcilePayload, isBarrier: boolean): ReconcileOutcome {
    const { state } = entry
    this.resetPingTimer(entry)
    const held = entry.holds
    // The session keeps its id for its life. One the server has no record of starts under the id the page named, which
    // the page made: its end is then kept under that id from its first wire on.
    const session = held ?? this.sessions.get(ctrl.sessionId) ?? this.sessions.create(ctrl.sessionId)
    // Missed frames are replayed where the RECONCILE comes on a wire that doesn't hold the session yet: the wire that
    // holds it, and a barrier's old wire read up to its FIN, still deliver them.
    const replay = !isBarrier && session.holder !== entry
    const { released } = ctrl
    // The wire that held the session holds nothing from now on: what it reads later is a loss of its own, and the
    // channels it awaited attach where the new holder's RECONCILE names them.
    const previous = this.sessions.handOver(session, entry, ctrl.attempt ?? session.high)
    previous?.state.awaited.stop()
    const retiredWire = isBarrier ? (previous?.wire ?? null) : null
    const attached =
      released === undefined
        ? this.attachSession(session, ctrl.open, entry, replay)
        : this.attachChanges(session, ctrl.open, released, entry)

    // Closed while this RECONCILE settled: a transient close keeps its channels' reconnect grace, the others end them.
    if (entry.closed !== null) {
      state.attaching.clear()
      this.sessions.release(entry)
      this.detachSession(session, entry.closed)
      state.awaited.stop()
      throw new ProtocolViolationError('wire closed mid-reconcile')
    }

    state.attaching.clear()
    return { sessionId: session.id, attached, retiredWire, deliverTo: entry.wire }
  }

  /** Attaches the channels a full-session RECONCILE names; the session's channels it leaves out end. */
  private attachSession(
    session: Session,
    open: ReconcilePayload['open'],
    entry: WireEntry,
    replay: boolean,
  ): ChannelHandle[] {
    const handles = open.map((each) => this.attach(each, entry, replay)).filter((h): h is ChannelHandle => h !== null)
    const previous = this.sessions.setChannels(session, handles)
    // Channels of the session that the page did NOT re-include are recovery-failed.
    const keptIxes = new Set(handles.map((h) => h.ix))
    for (const [ix, prevHandle] of previous) if (!keptIxes.has(ix)) this.detachHandle(prevHandle, 'recoveryFailed')
    entry.state.awaited.forgetUnnamed(open)
    return handles
  }

  /** Attaches the channels a RECONCILE of what changed names to the session, and ends each one the page released. */
  private attachChanges(
    session: Session,
    open: ReconcileOpenEntry[],
    released: number[],
    entry: WireEntry,
  ): ChannelHandle[] {
    const { awaited } = entry.state
    const heldId = (ix: number): string | undefined => session.channels.get(ix)?.channel.id ?? awaited.get(ix)?.open.id
    // A lower bound on the channels the wire holds once this frame applies, counted before any is allocated.
    let held = session.channels.size + awaited.unattached
    for (const ix of new Set(released)) if (heldId(ix) !== undefined) held--
    for (const { id, ix } of open) {
      const heldAs = heldId(ix)
      // An index names one channel until the page releases it, and a page holds one end of a channel.
      assertProtocol(heldAs === undefined || heldAs === id, 'RECONCILE names a held index under another id')
      const heldAt = this.sessions.indexOf(session.id, id)
      assertProtocol(heldAt === undefined || heldAt === ix, 'RECONCILE names a held channel under another index')
      if (heldAs === undefined) held++
    }
    assertProtocol(held <= MAX_CHANNELS_PER_CONNECTION, 'RECONCILE over the wire channel cap')
    const handles: ChannelHandle[] = []
    for (const each of open) {
      const handle = this.attach(each, entry, false)
      if (handle === null) continue
      handles.push(handle)
      this.sessions.add(session, handle)
    }
    for (const ix of released) {
      const handle = this.sessions.remove(session, ix)
      if (handle) this.detachHandle(handle, 'recoveryFailed')
    }
    entry.state.awaited.forget(released)
    return handles
  }

  /** Returns null to leave the channel out of the RECONCILED; an initial channel not registered yet is awaited until
   *  its ATTACH_RESULT. */
  private attach(open: ReconcileOpenEntry, entry: WireEntry, replay: boolean): ChannelHandle | null {
    if (open.probe !== undefined) entry.sender.answerProbe(open.ix, open.probe)
    const awaited = entry.state.awaited.get(open.ix)
    if (awaited) {
      awaited.open = open
      if (awaited.phase !== 'attached') return null
    }
    const existing = this.channels.get(open.id)?.channel
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
      refused: null,
    }
    this.registrationWaits.wait(awaited, this.options.connectTtl)
    entry.state.awaited.add(awaited)
  }

  /** Runs in `registerChannel`, between two reconciles. Waiters attach in the order they began; the latest one keeps
   *  the channel. */
  private attachAwaited(awaited: AwaitedChannel, channel: ServerChannel): void {
    // Its page sent it a frame the server can't take while it was awaited: it ends with that cause, unattached.
    if (awaited.refused !== null) return channel._onServerEnd(awaited.refused)
    const { entry } = awaited
    const session = entry.holds
    // A wire awaits only while it holds its session: the handover and the wire's close stop its waits.
    assert(session !== null, 'a wire that holds no session awaits a channel')
    // A page holds one end of a channel: one that awaits it at a second index broke the protocol.
    if (this.sessions.indexOf(session.id, channel.id) !== undefined) {
      return this.endFault(entry.wire, { scope: 'session', cause: 'unreadable' })
    }
    entry.state.attaching.add(channel)
    const handle = this.attachChannel(channel, awaited.open, entry, false)
    entry.state.attaching.delete(channel)
    if (!handle) {
      expireAwaited(awaited)
      return
    }
    this.sessions.add(session, handle)
    entry.state.awaited.attach(awaited)
    // Runs once the registering code has added its listeners. The ATTACH_RESULT goes after the held frames, so its
    // lastSeq counts them.
    const settle = async (): Promise<void> => {
      if (entry.state.awaited.get(awaited.open.ix) !== awaited) return
      this.dispatchHeld(awaited)
      entry.sender.send(encode.attachResult(awaited.open.ix, channel._lastClientSeq))
      channel._confirmWant()
    }
    void this.chainRecv(entry, settle).catch(handleTelefunctionBug)
  }

  /** Dispatches the frames held for `awaited`. One that breaks the protocol ends its channel alone: what the page sent
   *  meanwhile was all read, so the wire goes on. */
  private dispatchHeld(awaited: AwaitedChannel): void {
    const { entry } = awaited
    const held = entry.state.awaited.take(awaited)
    const session = entry.holds
    assert(session !== null, 'a wire that holds no session awaits a channel')
    try {
      for (const { frame } of held) this.dispatchChannelFrame(session, frame)
    } catch (err) {
      if (!(err instanceof ProtocolViolationError)) throw err
      this.endFault(entry.wire, { scope: 'channel', ix: awaited.open.ix, cause: 'frameRefused', wire: 'kept' })
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
   *  `recovery-failed` on abandoned channels. The page left or the server ended the session: remove outright — no
   *  resume. */
  private detachSession(session: Session, reason: DetachReason): void {
    const channels =
      reason === 'transient' ? [...session.channels.values()] : this.sessions.removeSession(session).values()
    for (const handle of channels) this.detachHandle(handle, reason)
  }

  private detachHandle(h: ChannelHandle, reason: DetachReason): void {
    const { peer } = h
    // The session keeps its channels for the page's reconnect, and nothing of the wire that went.
    h.peer = null
    // An ended channel waits out a lost wire for its page, which lets it go by releasing it, leaving it out of a new
    // wire's RECONCILE, or leaving for good.
    if (this.ended.has(h.channel)) {
      if (peer) h.channel._onPeerDisconnect(peer, this.options.reconnectTimeout)
      if (reason === 'transient') this.ended.keep(h.channel, this.options.reconnectTimeout)
      else this.ended.release(h.channel)
      return
    }
    switch (reason) {
      case 'pageLeft':
        h.channel._onPeerGone(new ChannelPageLeftError())
        return
      case 'transient':
        if (peer) h.channel._onPeerDisconnect(peer, this.options.reconnectTimeout)
        return
      case 'recoveryFailed':
        h.channel._onPeerRecoveryFailure()
        return
      default:
        h.channel._onPeerGone(END_CAUSE[reason].error())
    }
  }

  // ── Per-wire plumbing (recv chain, ping) ────────────────

  private chainRecv<T>(entry: WireEntry, fn: () => Promise<T>): Promise<T> {
    const prev = entry.state.recvChain ?? Promise.resolve()
    const next = prev.then(fn, fn).finally(() => {
      if (entry.state.recvChain === next) entry.state.recvChain = null
    })
    entry.state.recvChain = next
    return next
  }

  private pingDeadlineOf(entry: WireEntry): number {
    return pingDeadline(this.options.pingInterval, entry.transport.type)
  }

  /** A wire that closed keeps no timer. */
  private resetPingTimer(entry: WireEntry): void {
    if (entry.closed !== null) return
    entry.state.lastReceivedAt = performance.now()
    this.armPingDeadline(entry, entry.state.lastReceivedAt + this.pingDeadlineOf(entry))
  }

  /** Judges the wire at `dueAt`, on the `performance` clock. `recheck`: the deadline had passed when the timer last
   *  ran, and is judged again once the server read its sockets. */
  private armPingDeadline(entry: WireEntry, dueAt: number, recheck = false): void {
    const { wire, state } = entry
    state.pingTimer.arm(dueAt, () => {
      // A PING waits behind what the page queued, so the wire is dead only a deadline after its last bytes and after
      // the data it announced.
      const deadline = this.pingDeadlineOf(entry)
      const deadAt = Math.max(state.lastReceivedAt + deadline, entry.sender.pieces?.receiver.expectedBy(deadline) ?? 0)
      const now = performance.now()
      if (now < deadAt) return this.armPingDeadline(entry, deadAt)
      // Timers run before the sockets are read: after server code that held the event loop past the deadline, the
      // page's PING may be waiting unread, on a POST the server reads only some turns later. The page gets back the
      // time the timer ran late, when the server read nothing.
      if (!recheck) return this.armPingDeadline(entry, now + (now - dueAt), true)
      // Transient close so each channel gets its `reconnectTimeout` grace via
      // `_onPeerDisconnect`. Wire-level state is rebuilt by the next reconcile.
      this.endFault(wire, { scope: 'loss' })
    })
  }
}

function resolveMuxServerOptions(): MuxServerOptions {
  const c = getChannelConfig()
  return {
    reconnectTimeout: c.reconnectTimeout,
    idleTimeout: c.idleTimeout,
    pingInterval: c.pingInterval,
    creditWindow: creditWindowOf(c.maxMemory, c.maxPendingBytes),
    connectTtl: c.connectTtl,
    sseFlushThrottle: c.sseFlushThrottle,
    ssePostIdleFlushDelay: c.ssePostIdleFlushDelay,
    transports: c.transports,
    transportsWithoutWs: c.transports.filter((t) => t !== CHANNEL_TRANSPORT.WS),
  }
}

// Lazy because `getGlobalObject` evaluates its factory eagerly — the factory needs
// `ChannelMux` fully initialized, so defer until first access.
function getGlobals(): { mux: ChannelMux } {
  return getGlobalObject('wire-protocol/channel/server/ChannelMux.ts', () => ({ mux: new ChannelMux() }))
}
