export { ChannelMux, getChannelMux, CHANNEL_MUX }
export type { ReconcileOutcome, ServerTransport }

import { assert } from '../../utils/assert.js'
import { getGlobalObject } from '../../utils/getGlobalObject.js'
import { getRawContext } from '../../node/server/context/context.js'
import { getServerConfig } from '../../node/server/serverConfig.js'
import { unrefTimer } from '../../utils/unrefTimer.js'
import { handleTelefunctionBug } from '../../node/server/runTelefunc/validateTelefunctionError.js'
import {
  CHANNEL_PING_INTERVAL_MIN_MS,
  CREDIT_MSG_WINDOW_MAX,
  CREDIT_WINDOW_MAX_BYTES,
  MAX_CHANNELS_PER_CONNECTION,
  UPGRADE_MAX_ID_BYTES,
  UPGRADE_MAX_STAGED_BYTES,
  UPGRADE_MAX_STAGED_RECORDS,
  UPGRADE_STAGE_TTL_MS,
  WIRE_MAX_CONN_CTRL_FRAME_BYTES,
  WIRE_MAX_RAW_FRAME_BYTES,
  WIRE_RECV_BACKLOG_BASE_BYTES,
  WIRE_RECV_BACKLOG_BASE_FRAMES,
  WIRE_SEND_BACKLOG_BASE_BYTES,
  type ChannelTransports,
} from '../constants.js'
import {
  TAG,
  ProtocolViolationError,
  assertProtocol,
  decodeClientFrame,
  encode,
  isConnCtrlTag,
  peekTag,
} from '../shared-ws.js'
import type {
  BarrierPayload,
  ChannelFrame,
  PingEntry,
  PongEntry,
  PreparePayload,
  ReconcileOpenEntry,
  ReconcilePayload,
  SeqReader,
} from '../shared-ws.js'
import { IndexedPeer, type PeerSender } from './IndexedPeer.js'
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
  /** Bytes of the frames sent that still wait in the connection, never fewer than there are, or `undefined` where the
   *  runtime doesn't report them. */
  bufferedAmount(connection: TConnection): number | undefined
  terminateConnection(connection: TConnection): void
}

/** Emitted only when `handleFrame` consumed a reconcile. Caller threads the payload through
 *  to `sendReconciled` and fires `finalizeUpgrade` once the new wire's reconciled has emitted.
 *  `finalizeUpgrade` is null when this isn't an SSE→WS upgrade. */
type ReconcileOutcome = {
  sessionId: string
  /** Each one's `lastSeq` is read as the RECONCILED goes out, so it counts what the batch carried behind the RECONCILE
   *  when the transport sends it after that batch. */
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

/** An initial channel a RECONCILE on this wire named before the server registered it. The RECONCILED leaves it out,
 *  and an ATTACH_RESULT settles it. */
type AwaitedChannel = {
  /** The latest RECONCILE entry naming it. */
  entry: ReconcileOpenEntry
  /** The wire awaiting it: the one whose RECONCILE named it, until a barrier moves it to the WebSocket. */
  conn: ConnectionEntry
  wire: Wire
  /** What the page sent it meanwhile, dispatched after its attach. */
  held: { frame: ChannelFrame; bytes: number }[]
  /** `attached` until what it holds is dispatched. `expired` once `connectTtl` passed, and kept until a RECONCILE no
   *  longer names it, so one that crossed its ATTACH_RESULT doesn't await it again. */
  phase: 'waiting' | 'attached' | 'expired'
  stopWaiting: () => void
}

type ConnectionState = {
  pingTimer: ReturnType<typeof setTimeout> | null
  /** When the wire last delivered a frame, or the ping deadline was last set. */
  lastReceivedAt: number
  /** When the last PONG went out. */
  pongedAt: number
  terminatePermanently: boolean
  recvChain: Promise<unknown> | null
  /** Set by `onConnectionClosed` so an in-flight `reconcile` can see the close and its kind. */
  closed: { isPermanent: boolean } | null
  retiredByBarrier: boolean
  recvBacklogBytes: number
  recvBacklogFrames: number
  awaited: Map<number, AwaitedChannel>
  /** What may be written to the wire before its backlog, which only writes grow, can pass what its channels' flow
   *  control allowed when it was last read (see `WIRE_SEND_BACKLOG_BASE_BYTES`), and is read again. */
  sendHeadroom: number
  /** The largest frame sent on the wire. */
  largestSent: number
  /** Its backlog passed that, and the wire is being terminated. */
  pastSendBacklog: boolean
  /** Channels attaching to the wire in this turn, before its session holds them: what they send as they attach (their
   *  replay, what they buffered, what `onOpen` sends) counts under their flow control too. */
  attaching: Set<ServerChannel>
}

type ConnectionEntry = {
  state: ConnectionState
  transport: ServerTransport<unknown>
  /** One per wire: the peers of every reconcile on this wire share it. */
  sender: PeerSender
  /** Where the server stands on each channel of the wire's session, from which a frame's seqs are read. */
  seqs: SeqReader
}

/** The context key of a server that hosts its own channels: a Cloudflare session DO's end with it. */
const CHANNEL_MUX = Symbol('telefunc.channelMux')

function getChannelMux(): ChannelMux {
  return (getRawContext()?.[CHANNEL_MUX] as ChannelMux | undefined) ?? getGlobals().mux
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
  /** Channels that ended while their page may still lack their last frames, each with the timer that lets it go once
   *  its page stays away: none while the page is attached. */
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
    // A shutdown channel's `_onShutdown` callback never fires, so inserting it would leave a
    // permanent zombie entry whose later attach trips `attachChannel`'s replay-buffer assert.
    if (channel._didShutdown) return
    channel._registerChannel()
    this.channels.set(channel.id, channel)
    // Before a waiter attaches it: its `onOpen` may end it.
    channel._onShutdown((keep, pageAttached) =>
      keep ? this.keepEnded(channel, pageAttached) : this.unregisterChannel(channel.id),
    )
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

  /** An ended channel stays attachable, so what its page lacks of it replays, until the page takes nothing more of it
   *  (see `_pageNeedsNothing`), leaves it out of a RECONCILE or closes its wire for good, or stays away
   *  `reconnectTimeout`, since it ended or since its page left, as a channel that lives waits for it. Called again as
   *  its page attaches it or leaves it. */
  private keepEnded(channel: ServerChannel, pageAttached: boolean): void {
    const timer = this.endedChannels.get(channel)
    if (timer) clearTimeout(timer)
    const { reconnectTimeout } = getServerConfig().channel
    this.endedChannels.set(
      channel,
      pageAttached ? null : unrefTimer(setTimeout(() => this.releaseEnded(channel), reconnectTimeout)),
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

  onConnectionOpen<TConnection>(connection: TConnection, transport: ServerTransport<TConnection>): void {
    const channelOn = (ix: number): ServerChannel | undefined => {
      const sessionId = transport.getSessionId(connection)
      return sessionId === undefined ? undefined : this.sessions.get(sessionId, ix)?.channel
    }
    this.connectionEntries.set(connection, {
      state: {
        pingTimer: null,
        lastReceivedAt: 0,
        pongedAt: performance.now(),
        terminatePermanently: false,
        recvChain: null,
        closed: null,
        retiredByBarrier: false,
        recvBacklogBytes: 0,
        recvBacklogFrames: 0,
        awaited: new Map(),
        sendHeadroom: 0,
        largestSent: 0,
        pastSendBacklog: false,
        attaching: new Set(),
      },
      transport: transport as ServerTransport<unknown>,
      sender: {
        send: (frame, onCommit) => this.send(connection, frame as Uint8Array<ArrayBuffer>, onCommit),
        bufferedAmount: () => this.bufferedAmount(connection),
      },
      seqs: {
        received: (ix) => channelOn(ix)?._lastClientSeq ?? 0,
        sent: (ix) => channelOn(ix)?._replayBuffer?.seq ?? 0,
      },
    })
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
    this.send(
      outcome.deliverTo,
      encode.reconciled({
        upgradeId: outcome.upgradeId,
        sessionId: outcome.sessionId,
        open: outcome.attached.map((h) => ({ ix: h.ix, lastSeq: h.channel._lastClientSeq })),
        reconnectTimeout: this.options.reconnectTimeout,
        idleTimeout: this.options.idleTimeout,
        pingInterval: this.options.pingInterval,
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
    this.clearPingTimer(entry.state)
    this.stopAwaiting(entry.state)
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

  readPermanentTermination(connection: Wire): boolean {
    return this.connectionEntries.get(connection)?.state.terminatePermanently ?? false
  }

  /** SSE data POST: resolve the stream connection by its stable connId. Undefined when the
   *  connection hasn't reconciled yet or has already torn down. */
  getConnectionByConnId<TConnection>(connId: string): TConnection | undefined {
    return this.connectionsByConnId.get(connId) as TConnection | undefined
  }

  // ── Inbound dispatch ────────────────────────────────────────────────

  /** PING bypasses the recv chain — serializing it would tie liveness to the slowest
   *  awaitable on the connection. */
  private dispatchInbound(connection: Wire, rawFrame: Uint8Array<ArrayBuffer>): Promise<ReconcileOutcome | null> {
    const entry = this.connectionEntries.get(connection)
    if (!entry) return Promise.resolve(null)
    const { state } = entry
    const byteLength = rawFrame.byteLength
    if (this.isOverBudget(entry, connection, rawFrame)) {
      this.terminateWire(connection)
      return Promise.resolve(null)
    }
    state.recvBacklogBytes += byteLength
    state.recvBacklogFrames++
    state.lastReceivedAt = performance.now()
    const tag = peekTag(rawFrame)
    const exec = (): Promise<ReconcileOutcome | null> => this.runInboundTurn(entry, connection, rawFrame, byteLength)
    if (tag === TAG.PING) return exec()
    // A PING waits behind what the page sent before it, as an upload on a slow link: a wire whose frames keep arriving
    // is answered all the same, once a ping interval, so the page knows they arrive.
    if (performance.now() - state.pongedAt >= this.options.pingInterval) this.pong(entry, connection, [])
    return this.chainRecv(entry, exec)
  }

  /** Control frames are bounded by what the protocol itself can describe; only the data plane
   *  carries user payloads, and only it gets the multi-megabyte allowance. The backlog allows a full
   *  window per channel attached to the wire, on top of the base (see `WIRE_RECV_BACKLOG_BASE_BYTES`). */
  private isOverBudget(entry: ConnectionEntry, connection: Wire, rawFrame: Uint8Array<ArrayBuffer>): boolean {
    const tag = peekTag(rawFrame)
    const maxFrameBytes =
      tag !== undefined && isConnCtrlTag(tag) ? WIRE_MAX_CONN_CTRL_FRAME_BYTES : WIRE_MAX_RAW_FRAME_BYTES
    const byteLength = rawFrame.byteLength
    if (byteLength > maxFrameBytes) return true
    const { state, transport } = entry
    const sessionId = transport.getSessionId(connection)
    const channels = sessionId === undefined ? 0 : (this.sessions.peekSession(sessionId)?.size ?? 0)
    return (
      state.recvBacklogBytes + byteLength > WIRE_RECV_BACKLOG_BASE_BYTES + channels * CREDIT_WINDOW_MAX_BYTES ||
      state.recvBacklogFrames >= WIRE_RECV_BACKLOG_BASE_FRAMES + channels * CREDIT_MSG_WINDOW_MAX
    )
  }

  private async runInboundTurn(
    entry: ConnectionEntry,
    connection: Wire,
    rawFrame: Uint8Array<ArrayBuffer>,
    byteLength: number,
  ): Promise<ReconcileOutcome | null> {
    try {
      return (await this.handleFrame(entry, connection, rawFrame)) ?? null
    } catch (err) {
      if (!(err instanceof ProtocolViolationError)) throw err
      this.terminateWire(err.target ?? connection)
      return null
    } finally {
      entry.state.recvBacklogBytes -= byteLength
      entry.state.recvBacklogFrames--
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
    const frame = decodeClientFrame(rawFrame, WIRE_MAX_CONN_CTRL_FRAME_BYTES, entry.seqs)
    if (frame.tag === TAG.PING) {
      this.resetPingTimer(connection)
      this.acknowledgeArrivals(entry, connection)
      this.pong(entry, connection, this.answerPing(entry, connection, frame.ended))
      return null
    }
    assertProtocol(!entry.state.retiredByBarrier, 'frame on a wire retired by its barrier')
    assertProtocol(!this.stagedUpgrades.has(connection), 'frame on a staged probe')
    if (frame.tag === TAG.PREPARE) return this.handlePrepare(entry, connection, frame.payload, rawFrame.byteLength)
    if (frame.tag === TAG.BARRIER) return this.handleBarrier(entry, connection, frame.payload, rawFrame.byteLength)
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
      awaited.held.push({ frame: channelFrame, bytes: rawFrame.byteLength })
      entry.state.recvBacklogBytes += rawFrame.byteLength
      entry.state.recvBacklogFrames++
      return null
    }
    this.dispatchChannelFrame(sessionId, channelFrame)
    return null
  }

  /** The page lists its closed channels the server attached, each with how far it has what the server sent on it. Each
   *  is answered with how far the server has what the page sent on it, or that the server no longer holds it: one that
   *  ended here is let go, as the page takes nothing more of it. Only the wire its session is on answers: that session
   *  has each channel the page lists that the server holds. */
  private answerPing(entry: ConnectionEntry, connection: Wire, ended: PingEntry[]): PongEntry[] {
    assertProtocol(ended.length <= MAX_CHANNELS_PER_CONNECTION, 'PING over entry cap')
    const sessionId = entry.transport.getSessionId(connection)
    if (sessionId === undefined || this.sessionWires.get(sessionId) !== connection) return []
    return ended.map(({ ix, lastSeq }) => {
      const channel = this.sessions.get(sessionId, ix)?.channel
      if (channel === undefined) return { ix, lastSeq: null }
      channel._onPageClosed(lastSeq)
      if (!this.endedChannels.has(channel)) return { ix, lastSeq: channel._lastClientSeq }
      this.releaseEnded(channel)
      return { ix, lastSeq: null }
    })
  }

  /** At each of the page's heartbeats, the channels of the session on this wire acknowledge what arrived since their
   *  last WINDOW, so the page's replay lets it go while a channel is quiet. */
  private acknowledgeArrivals(entry: ConnectionEntry, connection: Wire): void {
    const sessionId = entry.transport.getSessionId(connection)
    if (sessionId === undefined || this.sessionWires.get(sessionId) !== connection) return
    for (const { channel } of this.sessions.peekSession(sessionId)?.values() ?? []) channel._acknowledge()
  }

  private pong(entry: ConnectionEntry, connection: Wire, ended: PongEntry[]): void {
    entry.state.pongedAt = performance.now()
    this.send(connection, encode.pong(ended))
  }

  private dispatchChannelFrame(sessionId: string, frame: ChannelFrame): void {
    // Frame for an ix that's no longer in the session — client closed the channel and the
    // server reconciled it out, but a frame was still in flight. Drop silently.
    this.sessions.get(sessionId, frame.index)?.channel._dispatchFrame(frame)
  }

  /** An ordinary reconcile claims its session, abandoning any probe staged on it — unless a barrier
   *  is mid-commit on that session, in which case the claim is refused instead. One on the wire that
   *  holds the session is that wire's own, and leaves the page's upgrade attempt be. */
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
    rawByteLength: number,
  ): Promise<ReconcileOutcome> | null {
    const wsConnection = this.stagedByPrevSession.get(ctrl.sessionId)
    const stage = wsConnection === undefined ? undefined : this.stagedUpgrades.get(wsConnection)
    // A barrier for an unknown or already-committing stage is refused SILENTLY (the client's attempt
    // deadline is the only watchdog) — erroring would tear down a wire this frame has no claim on.
    if (wsConnection === undefined || stage?.phase !== 'staged') return null

    try {
      // Admission policy — `decodeClientFrame` has already established the frame's shape.
      assertProtocol(rawByteLength <= WIRE_MAX_CONN_CTRL_FRAME_BYTES, 'barrier frame over byte cap')
      assertProtocol(ctrl.open.length <= MAX_CHANNELS_PER_CONNECTION, 'barrier over entry cap')
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
    // What the server sent the page is on a wire that delivers it, unless the page reconnected: the one that holds the
    // session, or a barrier's old wire, which the page reads to its FIN first.
    const replay = !isBarrier && newSessionId !== ctrl.sessionId
    const attached = this.reconcileSession(ctrl.sessionId, newSessionId, ctrl.open, entry, connection, replay)
    // Its wire no longer has the session it names, which is gone with the handles it had.
    if (ctrl.sessionId) this.sessionWires.delete(ctrl.sessionId)

    // The connection may have closed since this frame arrived, so its RECONCILED never goes out.
    // Remove the session outright, one the client never received or its own, but preserve the
    // close kind: a transient close leaves the channels their `_onPeerDisconnect` grace so the
    // client's retry can re-attach them.
    if (state.closed) {
      state.attaching.clear()
      const reason = state.closed.isPermanent ? DETACH_REASON.PERMANENT : DETACH_REASON.TRANSIENT
      const session = this.sessions.removeSession(newSessionId)
      if (session) for (const handle of session.values()) this.detachHandle(handle, reason)
      this.stopAwaiting(state)
      throw new ProtocolViolationError('connection closed mid-reconcile')
    }

    // What the wire awaits is forgotten once a RECONCILE no longer names it: the page has let it go.
    const named = new Set(ctrl.open.map((open) => open.ix))
    for (const [ix, awaited] of state.awaited) if (!named.has(ix)) this.forgetAwaited(state, ix, awaited)

    this.sessionWires.set(newSessionId, connection)
    transport.setSessionId(connection, newSessionId)
    state.attaching.clear()
    return { sessionId: newSessionId, attached, finalizeUpgrade, deliverTo: connection }
  }

  private reconcileSession(
    prevSessionId: string | undefined,
    newSessionId: string,
    open: ReconcilePayload['open'],
    conn: ConnectionEntry,
    connection: Wire,
    replay: boolean,
  ): ChannelHandle[] {
    const handles = open
      .map((entry) => this.attach(entry, conn, connection, replay))
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
    this.sessions.setSession(newSessionId, handles)
    return handles
  }

  /** Null leaves the channel out of the RECONCILED. The wire awaits an initial one the server hasn't registered, and
   *  one it awaits stays out until attached; its ATTACH_RESULT settles it. Later reconciles fail fast if the channel
   *  is gone. */
  private attach(
    entry: ReconcileOpenEntry,
    conn: ConnectionEntry,
    connection: Wire,
    replay: boolean,
  ): ChannelHandle | null {
    // Ahead of every frame of the channel, and of its registration where the wire awaits it: its round trip is the
    // path's. It measures, so it says nothing starved, and names no round trip.
    if (entry.probe !== undefined) conn.sender.send(encode.bdpPingAck(entry.ix, entry.probe, false, Infinity))
    const awaited = conn.state.awaited.get(entry.ix)
    if (awaited) {
      awaited.entry = entry
      if (awaited.phase !== 'attached') return null
    }
    const existing = this.channels.get(entry.id)
    if (existing) {
      conn.state.attaching.add(existing)
      return this.attachChannel(existing, entry, conn.sender, replay)
    }
    if (entry.initial && !awaited) this.awaitChannel(entry, conn, connection)
    return null
  }

  private awaitChannel(entry: ReconcileOpenEntry, conn: ConnectionEntry, connection: Wire): void {
    const onResult = (channel: ServerChannel | null): void => {
      if (channel) this.attachAwaited(awaited, channel)
      else this.expireAwaited(awaited)
    }
    const awaited: AwaitedChannel = {
      entry,
      conn,
      wire: connection,
      held: [],
      phase: 'waiting',
      stopWaiting: this.waitForChannelRegistration(entry.id, this.options.connectTtl, onResult),
    }
    conn.state.awaited.set(entry.ix, awaited)
  }

  /** Runs in `registerChannel`, so the waiters of several wires attach in the order they began waiting and the latest
   *  keeps the channel. A reconcile is one synchronous turn, so this lands between two, never within one. */
  private attachAwaited(awaited: AwaitedChannel, channel: ServerChannel): void {
    const { conn, wire } = awaited
    const sessionId = conn.transport.getSessionId(wire)
    assert(sessionId, 'a channel awaited on a wire that never reconciled')
    conn.state.attaching.add(channel)
    const handle = this.attachChannel(channel, awaited.entry, conn.sender, false)
    conn.state.attaching.delete(channel)
    if (!handle) {
      this.expireAwaited(awaited)
      return
    }
    this.sessions.add(sessionId, handle)
    awaited.phase = 'attached'
    // On the wire's recv chain, once the code that registered the channel has added its listeners; the wire holds
    // what arrives meanwhile. The ATTACH_RESULT follows, so its lastSeq counts what the wire held.
    const settle = async (): Promise<void> => {
      if (conn.state.awaited.get(awaited.entry.ix) !== awaited) return
      if (this.dispatchHeld(awaited)) this.send(wire, encode.attachResult(awaited.entry.ix, channel._lastClientSeq))
    }
    void this.chainRecv(conn, settle).catch(handleTelefunctionBug)
  }

  /** Returns false if what it held broke the protocol, which ends its wire. */
  private dispatchHeld(awaited: AwaitedChannel): boolean {
    const { conn, wire } = awaited
    conn.state.awaited.delete(awaited.entry.ix)
    const sessionId = conn.transport.getSessionId(wire)
    assert(sessionId)
    try {
      for (const { frame } of awaited.held) this.dispatchChannelFrame(sessionId, frame)
      return true
    } catch (err) {
      if (!(err instanceof ProtocolViolationError)) throw err
      this.terminateWire(wire)
      return false
    } finally {
      this.chargeHeld(awaited, -1)
      awaited.held = []
    }
  }

  /** Not registered within `connectTtl`, or shut down as it registered. */
  private expireAwaited(awaited: AwaitedChannel): void {
    awaited.phase = 'expired'
    this.chargeHeld(awaited, -1)
    awaited.held = []
    this.send(awaited.wire, encode.attachResult(awaited.entry.ix, null))
  }

  /** The wire awaits it no more, and drops what it held for it. One attached meanwhile, the RECONCILE left out of the
   *  session, which ended it. */
  private forgetAwaited(state: ConnectionState, ix: number, awaited: AwaitedChannel): void {
    awaited.stopWaiting()
    this.chargeHeld(awaited, -1)
    awaited.held = []
    state.awaited.delete(ix)
  }

  /** What a wire holds counts against its recv backlog. */
  private chargeHeld(awaited: AwaitedChannel, sign: 1 | -1): void {
    for (const { bytes } of awaited.held) {
      awaited.conn.state.recvBacklogBytes += sign * bytes
      awaited.conn.state.recvBacklogFrames += sign
    }
  }

  /** A channel the old wire awaits moves with the barrier listing it, so the WebSocket awaits it from then on. One
   *  attached already gets what the old wire held for it first, and the barrier's reconcile moves it as it is. */
  private moveAwaited(
    oldEntry: ConnectionEntry,
    wsEntry: ConnectionEntry,
    wsConnection: Wire,
    open: ReconcileOpenEntry[],
  ): void {
    for (const entry of open) {
      const awaited = oldEntry.state.awaited.get(entry.ix)
      if (!awaited) continue
      if (awaited.phase === 'attached') {
        this.dispatchHeld(awaited)
        continue
      }
      oldEntry.state.awaited.delete(entry.ix)
      this.chargeHeld(awaited, -1)
      awaited.conn = wsEntry
      awaited.wire = wsConnection
      this.chargeHeld(awaited, 1)
      wsEntry.state.awaited.set(entry.ix, awaited)
    }
  }

  private stopAwaiting(state: ConnectionState): void {
    for (const awaited of state.awaited.values()) awaited.stopWaiting()
    state.awaited.clear()
  }

  /** Attaches an `IndexedPeer`, after draining, with `replay`, the replay frames missed since `lastSeq` (sends are
   *  sync, see `send`). If the replay no longer holds them the channel ends instead. Returns null if the channel shut
   *  down and kept nothing for its page. */
  private attachChannel(
    channel: ServerChannel,
    entry: ReconcileOpenEntry,
    sender: PeerSender,
    replay: boolean,
  ): ChannelHandle | null {
    const buffer = channel._replayBuffer
    if (buffer === null) return null
    channel._onPageHas(entry.lastSeq)
    const peer = new IndexedPeer(sender, entry.ix, buffer)
    const missed = replay ? buffer.getAfter(entry.lastSeq) : []
    if (missed === null) {
      channel._onReplayLost(peer)
    } else {
      for (const frame of missed) sender.send(frame)
      channel._attachPeer(peer, entry)
    }
    if (this.endedChannels.has(channel)) this.keepEnded(channel, true)
    return { channel, ix: entry.ix, peer }
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
    // An ended channel waits out a lost wire for its page, which lets it go by leaving it out or leaving for good.
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

  /** Sole server→client send path; sync so wire order = call order. What a channel's sends and publishes queue on it
   *  is bounded by the channel's credit (see `flow-control/`) and, past that, by how far behind it lets its peer be. A
   *  frame for a wire that closed is committed all the same: it replays as one a dying wire lost does. So is one for a
   *  wire found holding more than that allows, which takes no more frames and is terminated after this turn as the
   *  ping deadline terminates one: the page reconnects, and what it lost replays from there. */
  private send(connection: Wire, frame: Uint8Array<ArrayBuffer>, onCommit?: () => void): void {
    onCommit?.()
    const entry = this.connectionEntries.get(connection)
    if (!entry) return
    const { state } = entry
    if (state.pastSendBacklog) return
    if (frame.byteLength > state.largestSent) state.largestSent = frame.byteLength
    if (frame.byteLength > state.sendHeadroom) {
      state.sendHeadroom = this.sendHeadroom(entry, connection)
      if (state.sendHeadroom < 0) {
        state.pastSendBacklog = true
        queueMicrotask(() => entry.transport.terminateConnection(connection))
        return
      }
    }
    state.sendHeadroom -= frame.byteLength
    entry.transport.sendNow(connection, frame)
  }

  /** What its channels' flow control allows the wire to hold, less what it holds: `Infinity` where the runtime can't
   *  tell. */
  private sendHeadroom(entry: ConnectionEntry, connection: Wire): number {
    const backlog = entry.transport.bufferedAmount(connection)
    if (backlog === undefined) return Infinity
    const sessionId = entry.transport.getSessionId(connection)
    const session = sessionId === undefined ? undefined : this.sessions.peekSession(sessionId)
    const channels = new Set(entry.state.attaching)
    for (const { channel } of session?.values() ?? []) channels.add(channel)
    let allowed = WIRE_SEND_BACKLOG_BASE_BYTES
    for (const channel of channels) allowed += channel._sendAllowance() + entry.state.largestSent
    return allowed - backlog
  }

  /** A wire that's gone holds nothing. */
  private bufferedAmount(connection: Wire): number | undefined {
    const entry = this.connectionEntries.get(connection)
    if (!entry) return 0
    return entry.transport.bufferedAmount(connection)
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
        // A PING arrives behind what the page queued before it, as an upload on a slow link: the wire is dead once it
        // has delivered nothing for the deadline.
        const quiet = performance.now() - state.lastReceivedAt
        if (quiet < this.options.pingDeadline)
          return this.armPingDeadline(connection, entry, this.options.pingDeadline - quiet)
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

  setSession(sessionId: string, handles: Iterable<ChannelHandle>): void {
    this.removeSession(sessionId)
    for (const h of handles) this.add(sessionId, h)
  }

  /** An empty session is never stored: it has nothing to route, detach, or recovery-fail, and storing
   *  it would leak, as only `removeSession` (a future reconcile naming this id, or a permanent close)
   *  ever deletes entries, and a session abandoned by a transient close sees neither. */
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
    pingDeadline: pingInterval * 2,
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
  return getGlobalObject('wire-protocol/server/mux.ts', () => ({ mux: new ChannelMux() }))
}
