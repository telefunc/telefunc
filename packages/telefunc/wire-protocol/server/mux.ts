export { ChannelMux, getChannelMux }
export type { ReconcileOutcome, ServerTransport, MuxResourceLimits, UpgradeResourceSnapshot, BacklogSnapshot }

import { assert } from '../../utils/assert.js'
import { getGlobalObject } from '../../utils/getGlobalObject.js'
import { getServerConfig } from '../../node/server/serverConfig.js'
import { unrefTimer } from '../../utils/unrefTimer.js'
import {
  CHANNEL_PING_INTERVAL_MIN_MS,
  UPGRADE_MAX_FRAME_BYTES,
  UPGRADE_MAX_ID_BYTES,
  UPGRADE_MAX_OPEN_ENTRIES,
  UPGRADE_MAX_STAGED_BYTES,
  UPGRADE_MAX_STAGED_RECORDS,
  SSE_METADATA_MAX_BYTES,
  UPGRADE_STAGE_TTL_MS,
  WIRE_MAX_RAW_FRAME_BYTES,
  WIRE_MAX_RECV_BACKLOG_BYTES,
  WIRE_MAX_RECV_BACKLOG_FRAMES,
  type ChannelTransports,
} from '../constants.js'
import { TAG, ProtocolViolationError, decodeClientFrame, encode } from '../shared-ws.js'
import type { ChannelFrame, PreparePayload, ReconcilePayload, ReconciledPayload } from '../shared-ws.js'
import { IndexedPeer, type PeerSender } from './IndexedPeer.js'
import { recordUpgradeCommitted, recordUpgradePrepared } from './upgrade-observer.js'
import type { ServerChannel } from './channel.js'

// Single-instance kernel: owns channels, sessions, per-connection runtime. Transports talk
// to this class via `onConnectionOpen` and from then on identify connections by object
// identity. Multi-instance deployments rely on sticky sessions at the load balancer.

type SendFn = (frame: Uint8Array<ArrayBuffer>, onCommit?: () => void) => void

type ServerTransport<TConnection> = {
  getSessionId(connection: TConnection): string | undefined
  setSessionId(connection: TConnection, sessionId: string): void
  /** Stable per-connection id, or `null` for transports that don't multiplex client→server
   *  traffic across requests (WebSocket: every frame already lands on the same socket). */
  getConnId(connection: TConnection): string | null
  sendNow(connection: TConnection, frame: Uint8Array<ArrayBuffer>): void
  terminateConnection(connection: TConnection): void
}

/** Emitted only when `handleFrame` consumed a reconcile. Caller threads the payload through
 *  to `sendReconciled` and fires `finalizeUpgrade` once the new wire's reconciled has emitted.
 *  `finalizeUpgrade` is null when this isn't an SSE→WS upgrade. */
type ReconcileOutcome = {
  sessionId: string
  openList: ReconciledPayload['open']
  finalizeUpgrade: (() => void) | null
  /** Barrier commits only: the reconcile ran on the OLD wire's recv chain but belongs to the staged
   *  probe wire, so its `reconciled` goes there rather than to the connection that hosted the turn. */
  deliverTo?: unknown
  /** Barrier commits only — what makes the frame a COMMITTED rather than an ordinary RECONCILED, so
   *  a delayed one can never be consumed as the commit of an attempt. */
  upgradeId?: string
}

/** The `maxFrameBytes`…`stageTtlMs` group admits the barrier upgrade; the `maxRaw…`/`maxRecv…` group
 *  bounds inbound raw frames on EVERY wire. Tests construct a `ChannelMux` with small values so the
 *  mechanism runs through the REAL enforcement path rather than a parallel accountant. */
type MuxResourceLimits = {
  maxFrameBytes: number
  maxOpenEntries: number
  maxIdBytes: number
  maxStagedRecords: number
  maxStagedBytes: number
  stageTtlMs: number
  maxRawFrameBytes: number
  maxRecvBacklogBytes: number
  maxRecvBacklogFrames: number
  maxMetadataBytes: number
}

const DEFAULT_MUX_LIMITS: MuxResourceLimits = Object.freeze({
  maxFrameBytes: UPGRADE_MAX_FRAME_BYTES,
  maxOpenEntries: UPGRADE_MAX_OPEN_ENTRIES,
  maxIdBytes: UPGRADE_MAX_ID_BYTES,
  maxStagedRecords: UPGRADE_MAX_STAGED_RECORDS,
  maxStagedBytes: UPGRADE_MAX_STAGED_BYTES,
  stageTtlMs: UPGRADE_STAGE_TTL_MS,
  maxRawFrameBytes: WIRE_MAX_RAW_FRAME_BYTES,
  maxRecvBacklogBytes: WIRE_MAX_RECV_BACKLOG_BYTES,
  maxRecvBacklogFrames: WIRE_MAX_RECV_BACKLOG_FRAMES,
  maxMetadataBytes: SSE_METADATA_MAX_BYTES,
})

/** A PREPARE the server accepted, awaiting its barrier on the OLD wire. Metadata only — no
 *  application payload is ever staged. */
type StagedUpgrade = {
  upgradeId: string
  /** The session the barrier must still name AND the old wire must still hold. */
  prevSessionId: string
  /** Wire bytes charged against the global staged budget; refunded on every cleanup path. */
  bytes: number
  timer: ReturnType<typeof setTimeout>
  /** ⚠️ The record doubles as the probe wire's PHASE MARKER — while it exists that wire may send only
   *  PING and its first PREPARE. Releasing it at the START of the awaitable commit would leave the
   *  probe neither staged nor sessioned, where a plain reconcile would run a second destructive
   *  rotation alongside the one committing. It also makes the record single-use, so a duplicate
   *  barrier cannot re-enter a commit already in flight. */
  phase: 'staged' | 'committing'
}

/** Read-only view of the staged-upgrade accounting, for tests. */
type UpgradeResourceSnapshot = {
  /** Compared against `maxStagedRecords`. */
  records: number
  /** `stagedByPrevSession.size`. Equal to `records` unless a cleanup path leaked one side. */
  reverseRecords: number
  /** Compared against `maxStagedBytes`. */
  bytes: number
}

/** Read-only view of ONE connection's recv-chain backlog, for tests. */
type BacklogSnapshot = {
  bytes: number
  frames: number
}

/** Past the stage lookup every commit failure is the STAGED wire's, including the ones thrown by
 *  helpers that know nothing about the upgrade (`validateUpgradeFrame`, `reconcile`'s closed-wire
 *  check). A rejected commit must leave the old session fully intact, so the wire whose chain merely
 *  hosted the turn is never the victim. Already-targeted violations pass through unchanged. */
function retargetToProbe(err: unknown, probe: unknown): unknown {
  if (!(err instanceof ProtocolViolationError) || err.target !== undefined) return err
  return new ProtocolViolationError(probe)
}

const textEncoder = new TextEncoder()

type MuxServerOptions = {
  reconnectTimeout: number
  idleTimeout: number
  pingInterval: number
  pingDeadline: number
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

type ChannelHandle = { channel: ServerChannel; ix: number }
type SessionFinalizer = () => void

type ConnectionState = {
  pingTimer: ReturnType<typeof setTimeout> | null
  terminatePermanently: boolean | null
  reconciling: boolean
  recvChain: Promise<unknown> | null
  /** Set by `onConnectionClosed` so an in-flight `reconcile` can see the close and its kind. */
  closed: { isPermanent: boolean } | null
  /** Set on the OLD wire once its barrier has committed. The barrier is that wire's final frame
   *  by contract, so anything after it is a violation — and it has to be caught by name, because
   *  the routing lookup below would otherwise swallow it silently (the connection still reports
   *  the rotated-away session id, so `sessions.get` misses for every ix). */
  retiredByBarrier: boolean
  /** Raw bytes and frames admitted to this wire's recv chain whose turn has not finished.
   *  Written only by `chargeBacklog`/`refundBacklog`, so the pair cannot drift. */
  recvBacklogBytes: number
  recvBacklogFrames: number
}

function chargeBacklog(state: ConnectionState, byteLength: number): void {
  state.recvBacklogBytes += byteLength
  state.recvBacklogFrames++
}

function refundBacklog(state: ConnectionState, byteLength: number): void {
  state.recvBacklogBytes -= byteLength
  state.recvBacklogFrames--
}

type ConnectionEntry = {
  state: ConnectionState
  transport: ServerTransport<unknown>
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
  private readonly sessionFinalizers = new Map<string, SessionFinalizer>()
  private readonly connectionEntries = new Map<unknown, ConnectionEntry>()
  /** Reverse index for transports with a stable connId (SSE). Lets data POSTs locate the
   *  live stream connection, and catches a duplicate-connId reconnect racing teardown. */
  private readonly connectionsByConnId = new Map<string, unknown>()
  /** Accepted PREPARE stages awaiting their barrier, keyed by the staged probe (WS) connection. */
  private readonly stagedUpgrades = new Map<unknown, StagedUpgrade>()
  /** REQUIRED reverse index, not an optimization: the barrier arrives on the OLD wire carrying
   *  only a session id, and the server has no other way back to the staged probe wire. */
  private readonly stagedByPrevSession = new Map<string, unknown>()
  private stagedBytes = 0
  private readonly limits: MuxResourceLimits

  /** Resolved lazily so the mux can be constructed at module-load (the globalObject factory
   *  runs before `serverConfig` is initialized). */
  private resolvedOptions: MuxServerOptions | null = null

  constructor(limits: Partial<MuxResourceLimits> = {}) {
    this.limits = { ...DEFAULT_MUX_LIMITS, ...limits }
  }

  private get options(): MuxServerOptions {
    return (this.resolvedOptions ??= resolveMuxServerOptions())
  }

  /** Exposed for transport-level race timers (SSE's `waitForConnection`). */
  get connectTtl(): number {
    return this.options.connectTtl
  }

  /** The mux is the single authority for inbound ceilings, so a test-constructed limit governs SSE
   *  ingress too. Reading the module constants directly at the transport left a mux built with small
   *  limits still facing a 64 MiB ingress, and the two could drift apart silently. */
  get maxRawFrameBytes(): number {
    return this.limits.maxRawFrameBytes
  }

  get maxMetadataBytes(): number {
    return this.limits.maxMetadataBytes
  }

  // ── ServerChannel registry ──────────────────────────────────────────

  /** Callers must not invoke `channel._registerChannel()` directly. */
  registerChannel(channel: ServerChannel<any, any>): void {
    // A shutdown channel's `_onShutdown` callback never fires, so inserting it would leave a
    // permanent zombie entry whose later attach trips `attachChannel`'s replay-buffer assert.
    if (channel._didShutdown) return
    channel._registerChannel()
    this.channels.set(channel.id, channel)
    const waiters = this.pendingRegisterWaiters.get(channel.id)
    if (waiters) {
      this.pendingRegisterWaiters.delete(channel.id)
      for (const cb of waiters) cb(channel)
    }
    channel._onShutdown(() => this.unregisterChannel(channel.id))
  }

  unregisterChannel(channelId: string): void {
    this.channels.delete(channelId)
    this.sessions.removeChannel(channelId)
  }

  hasChannels(): boolean {
    return this.channels.size > 0
  }

  // ── Connection lifecycle (transport-facing) ─────────────────────────

  onConnectionOpen<TConnection>(connection: TConnection, transport: ServerTransport<TConnection>): void {
    this.connectionEntries.set(connection, {
      state: {
        pingTimer: null,
        terminatePermanently: null,
        reconciling: false,
        recvChain: null,
        closed: null,
        retiredByBarrier: false,
        recvBacklogBytes: 0,
        recvBacklogFrames: 0,
      },
      transport: transport as ServerTransport<unknown>,
    })
    const connId = transport.getConnId(connection)
    if (connId !== null) this.connectionsByConnId.set(connId, connection)
    this.resetPingTimer(connection)
  }

  async onConnectionRawMessage(connection: unknown, rawFrame: Uint8Array<ArrayBuffer>): Promise<void> {
    const outcome = await this.dispatchInbound(connection, rawFrame)
    if (outcome) this.sendReconciled(connection, outcome)
  }

  onConnectionRawMessageDeferredReconciled(
    connection: unknown,
    rawFrame: Uint8Array<ArrayBuffer>,
  ): Promise<ReconcileOutcome | null> {
    return this.dispatchInbound(connection, rawFrame)
  }

  sendReconciled(connection: unknown, outcome: ReconcileOutcome): void {
    // A barrier commit ran on the old wire's chain but reconciled the STAGED wire, so its
    // `reconciled` belongs there. Ordinary reconciles leave `deliverTo` absent and are unaffected.
    this.send(
      outcome.deliverTo !== undefined ? outcome.deliverTo : connection,
      encode.reconciled({
        upgradeId: outcome.upgradeId,
        sessionId: outcome.sessionId,
        open: outcome.openList,
        reconnectTimeout: this.options.reconnectTimeout,
        idleTimeout: this.options.idleTimeout,
        pingInterval: this.options.pingInterval,
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

  onConnectionClosed(connection: unknown, isPermanent: boolean): void {
    const entry = this.connectionEntries.get(connection)
    if (!entry) return
    entry.state.closed = { isPermanent }
    this.clearPingTimer(entry.state)
    this.connectionEntries.delete(connection)
    const connId = entry.transport.getConnId(connection)
    // Identity-equality guards against deleting a *replacement* connection's entry when
    // a duplicate-connId reconnect raced the old wire's teardown.
    if (connId !== null && this.connectionsByConnId.get(connId) === connection) {
      this.connectionsByConnId.delete(connId)
    }
    // ⚠️ Must stay ABOVE the session-less early return below: a staged probe wire has no session id
    // by definition, so any lower this never runs and every abandoned attempt leaks two map entries
    // and a live timer, permanently, keyed by a dead object.
    this.clearStage(connection)
    const sessionId = entry.transport.getSessionId(connection)
    if (!sessionId) return // Closed before reconciling — nothing to clean up.
    // The OLD wire died with a stage pending on it: the barrier can never arrive now. ⚠️ ABANDON,
    // not clear — the probe has to go with the record. `clearStage` alone leaves a session-less WS
    // that PING keeps alive indefinitely, and the record it drops is also the phase marker
    // `handleFrame` reads, so the wire would be left free to establish an ordinary WS session
    // through a plain RECONCILE. This is the same rule every other cleanup path here follows; the
    // `clearStage` above is the exception only because there the closing wire IS the probe.
    const stagedWs = this.stagedByPrevSession.get(sessionId)
    if (stagedWs !== undefined) this.abandonStage(stagedWs)
    // Channels survive a transient close (`_onPeerDisconnect`'s reconnectTimeout grace);
    // permanent tears them down. The session-level finalizer is dropped on any close;
    // reconcile rebuilds it on next attach.
    this.detachSession(sessionId, isPermanent ? DETACH_REASON.PERMANENT : DETACH_REASON.TRANSIENT)
    this.sessionFinalizers.delete(sessionId)
  }

  readPermanentTermination(connection: unknown): boolean | null {
    return this.connectionEntries.get(connection)?.state.terminatePermanently ?? null
  }

  /** SSE data POST: resolve the stream connection by its stable connId. Undefined when the
   *  connection hasn't reconciled yet or has already torn down. */
  getConnectionByConnId<TConnection>(connId: string): TConnection | undefined {
    return this.connectionsByConnId.get(connId) as TConnection | undefined
  }

  // ── Inbound dispatch ────────────────────────────────────────────────

  /** PING bypasses the recv chain — serializing it would tie liveness to the slowest
   *  awaitable on the connection. */
  private dispatchInbound(connection: unknown, rawFrame: Uint8Array<ArrayBuffer>): Promise<ReconcileOutcome | null> {
    const entry = this.connectionEntries.get(connection)
    if (!entry) return Promise.resolve(null)
    const { state } = entry
    const byteLength = rawFrame.byteLength
    if (!this.admitInboundFrame(state, byteLength)) {
      this.terminateWire(entry, connection)
      return Promise.resolve(null)
    }
    chargeBacklog(state, byteLength)
    const exec = (): Promise<ReconcileOutcome | null> => this.runInboundTurn(entry, connection, rawFrame, byteLength)
    if (rawFrame[0] === TAG.PING) return exec()
    return this.chainRecv(entry, exec)
  }

  /** Admission runs ahead of everything — decode, dispatch, and the chain itself. The raw-frame
   *  ceiling bounds what `decode` is asked to allocate; the backlog pair bounds what a client can
   *  leave QUEUED by stalling the chain and continuing to send. Rejection kills only the wire that
   *  overran, so a flood costs its author its own connection and nobody else theirs. */
  private admitInboundFrame(state: ConnectionState, byteLength: number): boolean {
    // ⚠️ Stated as REJECTION and negated: for a non-finite limit `>=` is not the complement of `<`,
    // so an admit-form comparison would refuse every frame instead of admitting them. Bytes compare
    // post-add and frames pre-increment because this frame is not charged yet.
    const overBudget =
      byteLength > this.limits.maxRawFrameBytes ||
      state.recvBacklogBytes + byteLength > this.limits.maxRecvBacklogBytes ||
      state.recvBacklogFrames >= this.limits.maxRecvBacklogFrames
    return !overBudget
  }

  /** One admitted frame's turn, refund included. A protocol violation kills the offending wire —
   *  `err.target` when the barrier named another, otherwise the wire the frame arrived on.
   *
   *  Everything else RETHROWS. Past the decode seam a client can no longer produce a `TypeError`
   *  here, so one is our bug, and executing the client's wire for it would hide the fault while
   *  costing them a session.
   *
   *  ⚠️ The refund rides THIS function's `finally`, not the caller's. A frame that completes without
   *  ever awaiting (every PING) must refund SYNCHRONOUSLY, before `dispatchInbound` returns. Wrapping
   *  this in a second `async` layer defers the refund by a microtask, and same-stack PINGs then pile
   *  up as unfinished backlog until the cap terminates a perfectly healthy wire. */
  private async runInboundTurn(
    entry: ConnectionEntry,
    connection: unknown,
    rawFrame: Uint8Array<ArrayBuffer>,
    byteLength: number,
  ): Promise<ReconcileOutcome | null> {
    try {
      const pending = this.handleFrame(entry, connection, rawFrame)
      return pending ? ((await pending) ?? null) : null
    } catch (err) {
      if (!(err instanceof ProtocolViolationError)) throw err
      const target = err.target ?? connection
      // Only a barrier retargets, and then the wire may already be gone.
      const targetEntry = target === connection ? entry : this.connectionEntries.get(target)
      if (!targetEntry) return null
      this.terminateWire(targetEntry, target)
      return null
    } finally {
      refundBacklog(entry.state, byteLength)
    }
  }

  /** Kill one wire and abandon any upgrade staged on it: a wire being torn down can never commit. */
  private terminateWire(entry: ConnectionEntry, connection: unknown): void {
    // A `committing` stage is the continuation's to release; killing this wire only marks intent,
    // and the continuation unwinds into its own `finally` either way.
    if (this.stagedUpgrades.get(connection)?.phase === 'staged') this.clearStage(connection)
    entry.state.terminatePermanently = true
    entry.transport.terminateConnection(connection)
  }

  /** Returns a `ReconcileOutcome` only on reconcile (so the caller decides when to send
   *  `reconciled`). Anything but reconcile/ping before reconciliation is a violation. */
  private handleFrame(
    entry: ConnectionEntry,
    connection: unknown,
    rawFrame: Uint8Array<ArrayBuffer>,
  ): null | Promise<ReconcileOutcome | null> {
    const frame = decodeClientFrame(rawFrame, this.limits.maxFrameBytes)
    if (frame.tag === TAG.PING) {
      this.resetPingTimer(connection)
      this.send(connection, encode.pong())
      return null
    }
    // Everything below is a SEMANTIC frame. The barrier is the old wire's final one by contract, so
    // this sits above the reconcile dispatch: a late reconcile on a committed-away wire would mint a
    // fresh session and reattach the channels away from the probe that just won them.
    if (entry.state.retiredByBarrier) throw new ProtocolViolationError()
    // A staged probe may send nothing but PING. One check for both dispatches below — RECONCILE is
    // handled above the sessionless guard, so without it a reconcile on a probe would run the
    // destructive rotation and step around the stage entirely.
    if (this.stagedUpgrades.has(connection)) throw new ProtocolViolationError()
    if (frame.tag === TAG.PREPARE) return this.handlePrepare(entry, connection, frame.payload, rawFrame.byteLength)
    if (frame.tag === TAG.RECONCILE) {
      // Sound truthiness: the decode seam guaranteed the flat union, so `true` here means both legs.
      if (frame.payload.barrier === true) {
        return this.handleBarrier(entry, connection, frame.payload, rawFrame.byteLength)
      }
      this.releaseStagesForReconcile(frame.payload, entry, connection)
      return this.reconcile(entry, connection, frame.payload)
    }
    const sessionId = entry.transport.getSessionId(connection)
    if (!sessionId) throw new ProtocolViolationError()
    // Frame for an ix that's no longer in the session — client closed the channel and the
    // server reconciled it out, but a frame was still in flight. Drop silently.
    this.sessions.get(sessionId, (frame as ChannelFrame).index)?.channel._dispatchFrame(frame as ChannelFrame)
    return null
  }

  /** A stage cannot outlive the session it commits against. ⚠️ TWO sessions qualify and they are NOT
   *  the same one: the session this reconcile CLAIMS (destroyed whichever connection asked — so
   *  keying only off the reconciling wire misses a reconnect naming it from elsewhere, whose later
   *  barrier would then pass every equality leg against a session already dead), and the reconciling
   *  wire's CURRENT session, which it is about to rotate away from and whose record would otherwise
   *  leak until the TTL, since close cleanup looks up the wire's NEW id.
   *
   *  A reconcile naming NO session is deliberately not covered: it destroys nothing, and releasing
   *  there would abort a healthy upgrade whenever an unrelated connection attached by channel id. */
  private releaseStagesForReconcile(ctrl: ReconcilePayload, entry: ConnectionEntry, connection: unknown): void {
    for (const doomed of [ctrl.sessionId, entry.transport.getSessionId(connection)]) {
      if (doomed === undefined) continue
      const staleProbe = this.stagedByPrevSession.get(doomed)
      if (staleProbe !== undefined) this.abandonStage(staleProbe)
    }
  }

  // ── Barrier upgrade: staging + commit ───────────────────────────────

  /** Validates, installs ONE metadata-only record, enqueues `READY` and RETURNS. Nothing is ever
   *  awaited on the probe wire's recv chain: leaving a turn pending there would retain every later
   *  raw frame in a closure on the promise chain, which is the unbounded holding area this design
   *  exists not to build. */
  private handlePrepare(
    entry: ConnectionEntry,
    connection: unknown,
    payload: PreparePayload,
    rawByteLength: number,
  ): null {
    const limits = this.limits
    // No cap check here: PREPARE carries no membership, so its only bound is the frame-byte one the
    // decode seam already applied pre-parse.
    // A PREPARE belongs on a probe wire that is not yet anyone's transport.
    if (entry.transport.getSessionId(connection)) throw new ProtocolViolationError()
    // The named session must exist. It is only ever compared for equality, at barrier time, so
    // without this a client can pin a record and a socket against any string it invents and the
    // stage is reaped only by its TTL. A legitimate client names a session the server just minted.
    if (!this.sessions.peekSession(payload.sessionId)) throw new ProtocolViolationError()
    // One stage per old session; a compliant client never sends a second. (The one-per-probe-wire
    // half is the staged-probe guard in `handleFrame` — a probe with a stage may send only PING.)
    if (this.stagedByPrevSession.has(payload.sessionId)) throw new ProtocolViolationError()
    // Global budget: rejects the NEW probe only, so a flood cannot evict an in-progress upgrade.
    if (this.stagedUpgrades.size >= limits.maxStagedRecords) throw new ProtocolViolationError()
    if (this.stagedBytes + rawByteLength > limits.maxStagedBytes) throw new ProtocolViolationError()

    // Expiry releases the probe as well as the record: PING alone would otherwise keep a useless
    // session-less socket alive indefinitely.
    const timer = unrefTimer(setTimeout(() => this.abandonStage(connection), limits.stageTtlMs))
    this.stagedUpgrades.set(connection, {
      upgradeId: payload.upgradeId,
      prevSessionId: payload.sessionId,
      bytes: rawByteLength,
      timer,
      phase: 'staged',
    })
    this.stagedByPrevSession.set(payload.sessionId, connection)
    this.stagedBytes += rawByteLength
    // Counts an ACCEPTED prepare, not one that merely arrived — every rejection throws above.
    recordUpgradePrepared()
    this.send(connection, encode.ready({ upgradeId: payload.upgradeId }))
    return null
  }

  /** The barrier arrives on the OLD wire and commits the staged probe wire, as an ordinary turn on
   *  that wire's `recvChain` — so the happens-before edge over every earlier old-wire frame exists by
   *  construction. It commits by handing the STAGED entry to the unmodified `reconcile`, which binds
   *  the send closure, `setSessionId` and the finalizer to the probe wire; FIN still binds to the old
   *  wire, because `buildUpgradeFinalizer` reads the PREVIOUS session's. */
  private handleBarrier(
    entry: ConnectionEntry,
    connection: unknown,
    ctrl: ReconcilePayload,
    rawByteLength: number,
  ): Promise<ReconcileOutcome> | null {
    const sessionId = ctrl.sessionId
    assert(sessionId, 'barrier without a sessionId reached handleBarrier')
    const wsConnection = this.stagedByPrevSession.get(sessionId)
    const stage = wsConnection === undefined ? undefined : this.stagedUpgrades.get(wsConnection)
    // A stale barrier is not an error: its stage expired or its probe closed, and there is no wire
    // it would be right to kill — the probe is gone, and the old session must not pay for a timeout.
    // The client's own attempt deadline ends the upgrade.
    if (wsConnection === undefined || stage?.phase !== 'staged') return null

    try {
      // Caps only; the shape was settled at decode.
      this.validateUpgradeFrame(ctrl.open, rawByteLength)
      // Established channels only: an `initial: true` entry naming an unregistered channel is the
      // one thing `attach` waits for, and it would park the commit mid-flight. That refusal is also
      // what keeps the commit a bounded microtask chain, which is what makes owning the stage
      // across the await safe.
      for (const channel of ctrl.open) if (channel.initial) throw new ProtocolViolationError(wsConnection)
      // TWO checks for two distinct defects; neither substitutes for the other. The id closes stale
      // settlement (a delayed ordinary reconciled consumed as this attempt's commit). The
      // live-session equality closes concurrent rotation — it is what requires the barrier to arrive
      // on the wire that still OWNS the staged session, so a copy landing elsewhere cannot commit.
      if (ctrl.upgradeId !== stage.upgradeId) throw new ProtocolViolationError(wsConnection)
      if (entry.transport.getSessionId(connection) !== stage.prevSessionId) {
        throw new ProtocolViolationError(wsConnection)
      }

      // A true internal invariant, not a client-reachable state: `onConnectionClosed` clears the
      // stage in the same breath as it deletes the entry, so a stage for a vanished connection
      // means a cleanup path was missed. Surfacing it beats executing a wire for our bug.
      const wsEntry = this.connectionEntries.get(wsConnection)
      assert(wsEntry, 'staged probe has no connection entry')

      // Committed to. The TTL stays armed, so the attempt deadline still bounds an upgrade that
      // wedges inside `reconcile`.
      stage.phase = 'committing'
      // The old wire is spent: the barrier is its final semantic frame by contract.
      entry.state.retiredByBarrier = true
      return this.settleBarrierCommit(entry, wsEntry, wsConnection, ctrl, stage.upgradeId)
    } catch (err) {
      this.clearStage(wsConnection)
      throw retargetToProbe(err, wsConnection)
    }
  }

  /** The awaitable half of the commit, split out so `handleBarrier` can keep throwing its validation
   *  failures SYNCHRONOUSLY. The stage is released however `reconcile` settles — on success that is
   *  the session-set boundary, past which the probe legitimately owns a session. */
  private async settleBarrierCommit(
    oldEntry: ConnectionEntry,
    wsEntry: ConnectionEntry,
    wsConnection: unknown,
    ctrl: ReconcilePayload,
    upgradeId: string,
  ): Promise<ReconcileOutcome> {
    try {
      const outcome = await this.reconcile(wsEntry, wsConnection, ctrl)
      // Recorded on the RESOLVED reconcile rather than at the caller's COMMITTED send, so a future
      // change to how COMMITTED is delivered cannot quietly detach the e2e oracle from the commit.
      recordUpgradeCommitted()
      return { ...outcome, deliverTo: wsConnection, upgradeId }
    } catch (err) {
      // The barrier was the old wire's final frame only if the commit happened. It did not, so the
      // old session is whole and its wire must answer ordinary traffic again — above all the
      // client's own recovery reconcile, which the stale flag is exactly what would refuse.
      oldEntry.state.retiredByBarrier = false
      throw retargetToProbe(err, wsConnection)
    } finally {
      this.clearStage(wsConnection)
    }
  }

  /** Release a stage that can no longer commit, and let go of its probe with it. Clearing the record
   *  alone would leave a session-less WS that PING keeps alive indefinitely, waiting on a commit the
   *  server will never perform. The OLD session is never touched here — that is the entire pre-barrier
   *  rule: an abandoned attempt costs the established client nothing.
   *
   *  Refuses a `committing` stage: only the commit continuation may release one. */
  private abandonStage(wsConnection: unknown): void {
    if (this.stagedUpgrades.get(wsConnection)?.phase !== 'staged') return
    this.clearStage(wsConnection)
    const entry = this.connectionEntries.get(wsConnection)
    if (!entry) return
    entry.state.terminatePermanently = true
    entry.transport.terminateConnection(wsConnection)
  }

  /** Idempotent, and the ONLY writer of the staged accounting — so every cleanup path refunds the
   *  byte charge and both map entries by construction rather than by discipline. */
  private clearStage(wsConnection: unknown): void {
    const stage = this.stagedUpgrades.get(wsConnection)
    if (!stage) return
    clearTimeout(stage.timer)
    this.stagedUpgrades.delete(wsConnection)
    // Identity-guarded for the same reason `connectionsByConnId` is: a stale clear must not evict
    // a later stage that legitimately re-used this session id.
    if (this.stagedByPrevSession.get(stage.prevSessionId) === wsConnection) {
      this.stagedByPrevSession.delete(stage.prevSessionId)
    }
    this.stagedBytes -= stage.bytes
  }

  /** Applies to the `barrier: true` RECONCILE only — an ordinary reconcile keeps its existing
   *  uncapped contract. */
  private validateUpgradeFrame(open: ReconcilePayload['open'], rawByteLength: number): void {
    const limits = this.limits
    if (rawByteLength > limits.maxFrameBytes) throw new ProtocolViolationError()
    if (!Array.isArray(open)) throw new ProtocolViolationError()
    if (open.length > limits.maxOpenEntries) throw new ProtocolViolationError()
    for (const channel of open) {
      if (typeof channel?.id !== 'string') throw new ProtocolViolationError()
      // UTF-8 bytes rather than UTF-16 units: the cap bounds what the decoder actually allocated.
      if (textEncoder.encode(channel.id).byteLength > limits.maxIdBytes) throw new ProtocolViolationError()
    }
  }

  // ── Reconcile + attach ──────────────────────────────────────────────

  private async reconcile(
    entry: ConnectionEntry,
    connection: unknown,
    ctrl: ReconcilePayload,
  ): Promise<ReconcileOutcome> {
    const { state, transport } = entry
    // Reads `barrier` rather than firing on every sessioned reconcile: only a barrier retires a
    // wire, and the FIN it owes is the finalizer the OLD session registered on its own reconcile.
    const finalizeUpgrade = ctrl.barrier && ctrl.sessionId ? this.buildUpgradeFinalizer(ctrl.sessionId) : null
    state.reconciling = true
    this.resetPingTimer(connection)
    const send: SendFn = (frame, onCommit) => this.send(connection, frame, onCommit)
    const newSessionId = crypto.randomUUID()
    const openList = await this.reconcileSession(ctrl.sessionId, newSessionId, ctrl.open, send)

    // The connection may have closed during the await. The client never received this
    // session's id (`reconciled` was never sent), so no future reconcile can reference it —
    // remove the session outright, but preserve the close kind: a transient close leaves the
    // channels their `_onPeerDisconnect` grace so the client's retry can re-attach them.
    if (state.closed) {
      const reason = state.closed.isPermanent ? DETACH_REASON.PERMANENT : DETACH_REASON.TRANSIENT
      const session = this.sessions.removeSession(newSessionId)
      if (session) for (const handle of session.values()) this.detachHandle(handle, reason)
      throw new ProtocolViolationError()
    }

    if (ctrl.sessionId) this.sessionFinalizers.delete(ctrl.sessionId)
    this.sessionFinalizers.set(newSessionId, () => this.send(connection, encode.fin()))
    transport.setSessionId(connection, newSessionId)
    state.reconciling = false
    this.resetPingTimer(connection)
    return { sessionId: newSessionId, openList, finalizeUpgrade }
  }

  private buildUpgradeFinalizer(prevSessionId: string): (() => void) | null {
    return this.sessionFinalizers.get(prevSessionId) ?? null
  }

  private async reconcileSession(
    prevSessionId: string | undefined,
    newSessionId: string,
    open: ReconcilePayload['open'],
    send: SendFn,
  ): Promise<ReconciledPayload['open']> {
    const handles = (await Promise.all(open.map((entry) => this.attach(entry, send)))).filter(
      (h): h is ChannelHandle => h !== null,
    )

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
    return handles.map((h) => ({ ix: h.ix, lastSeq: h.channel._lastClientSeq }))
  }

  /** First reconcile (`initial:true`) races channel registration against `connectTtl`; later
   *  reconciles fail fast if the channel is gone. */
  private async attach(entry: ReconcilePayload['open'][number], send: SendFn): Promise<ChannelHandle | null> {
    const existing = this.channels.get(entry.id)
    if (existing) return this.attachChannel(existing, entry.ix, entry.lastSeq, send)
    if (!entry.initial) return null
    return new Promise<ChannelHandle | null>((resolve) => {
      this.waitForChannelRegistration(entry.id, this.options.connectTtl, (channel) => {
        resolve(channel ? this.attachChannel(channel, entry.ix, entry.lastSeq, send) : null)
      })
    })
  }

  /** Drains replay frames missed since `lastSeq` (sends are sync — see `send`), then
   *  attaches an `IndexedPeer`. Returns null if the channel already shut down. */
  private attachChannel(channel: ServerChannel, ix: number, lastSeq: number, send: SendFn): ChannelHandle | null {
    if (channel._didShutdown) return null
    const replay = channel._replayBuffer
    assert(replay !== null, `ServerChannel "${channel.id}" attached without a replay buffer`)
    for (const frame of replay.getAfter(lastSeq)) send(frame as Uint8Array<ArrayBuffer>)
    const sender: PeerSender = { send }
    channel._attachPeer(new IndexedPeer(sender, ix, replay))
    return { channel, ix }
  }

  private waitForChannelRegistration(
    channelId: string,
    ttlMs: number,
    onResult: (channel: ServerChannel | null) => void,
  ): void {
    let settled = false
    let timer: ReturnType<typeof setTimeout>
    const waiterSet = this.pendingRegisterWaiters.get(channelId) ?? new Set()
    this.pendingRegisterWaiters.set(channelId, waiterSet)

    const settle = (channel: ServerChannel | null): void => {
      if (settled) return
      settled = true
      waiterSet.delete(waiter)
      if (waiterSet.size === 0) this.pendingRegisterWaiters.delete(channelId)
      clearTimeout(timer)
      onResult(channel)
    }
    const waiter = (channel: ServerChannel): void => settle(channel)
    waiterSet.add(waiter)
    timer = setTimeout(() => settle(null), ttlMs)
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
    switch (reason) {
      case DETACH_REASON.PERMANENT:
        h.channel._onPeerClose()
        return
      case DETACH_REASON.TRANSIENT:
        h.channel._onPeerDisconnect(getServerConfig().channel.reconnectTimeout)
        return
      case DETACH_REASON.RECOVERY_FAILED:
        h.channel._onPeerRecoveryFailure()
        return
    }
  }

  // ── Per-connection plumbing (send, recv chain, ping) ────────────────

  /** Sole server→client send path; sync so wire order = call order. Per-channel
   *  byte+msg credit (see `flow-control/`) bounds queue growth. */
  private send(connection: unknown, frame: Uint8Array<ArrayBuffer>, onCommit?: () => void): void {
    const entry = this.connectionEntries.get(connection)
    if (!entry) return
    onCommit?.()
    entry.transport.sendNow(connection, frame)
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

  private resetPingTimer(connection: unknown): void {
    const entry = this.connectionEntries.get(connection)
    if (!entry) return
    const { state, transport } = entry
    this.clearPingTimer(state)
    state.pingTimer = unrefTimer(
      setTimeout(() => {
        state.pingTimer = null
        if (state.reconciling) return
        // Transient close so each channel gets its `reconnectTimeout` grace via
        // `_onPeerDisconnect`. Connection-level state is rebuilt by the next reconcile.
        transport.terminateConnection(connection)
        state.terminatePermanently = false
      }, this.options.pingDeadline),
    )
  }

  // ── Test-only ───────────────────────────────────────────────────────

  dispose(): void {
    this.channels.clear()
    this.pendingRegisterWaiters.clear()
    this.sessions.clear()
    this.sessionFinalizers.clear()
    this.connectionEntries.clear()
    this.connectionsByConnId.clear()
    for (const stage of this.stagedUpgrades.values()) clearTimeout(stage.timer)
    this.stagedUpgrades.clear()
    this.stagedByPrevSession.clear()
    this.stagedBytes = 0
  }

  /** @internal @test-only Server-global staged accounting, read from the enforcement fields. */
  _getUpgradeResourceSnapshot(): UpgradeResourceSnapshot {
    return {
      records: this.stagedUpgrades.size,
      reverseRecords: this.stagedByPrevSession.size,
      bytes: this.stagedBytes,
    }
  }

  /** @internal @test-only Per-connection backlog, read from the enforcement fields. Null once the
   *  wire is gone. */
  _getBacklogSnapshot(connection: unknown): BacklogSnapshot | null {
    const entry = this.connectionEntries.get(connection)
    if (!entry) return null
    return { bytes: entry.state.recvBacklogBytes, frames: entry.state.recvBacklogFrames }
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
    const session = new Map<number, ChannelHandle>()
    for (const h of handles) {
      session.set(h.ix, h)
      let bindings = this.byChannel.get(h.channel.id)
      if (!bindings) {
        bindings = new Map()
        this.byChannel.set(h.channel.id, bindings)
      }
      bindings.set(sessionId, h.ix)
    }
    // An empty session has nothing to route, detach, or recovery-fail — storing it would
    // leak: only `removeSession` (a future reconcile naming this id, or a permanent close)
    // ever deletes entries, and a session abandoned by a transient close sees neither.
    if (session.size === 0) return
    this.bySession.set(sessionId, session)
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

  clear(): void {
    this.bySession.clear()
    this.byChannel.clear()
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
