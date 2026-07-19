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
  UPGRADE_STAGE_TTL_MS,
  WIRE_MAX_RAW_FRAME_BYTES,
  WIRE_MAX_RECV_BACKLOG_BYTES,
  WIRE_MAX_RECV_BACKLOG_FRAMES,
  type ChannelTransports,
} from '../constants.js'
import { TAG, decode, encode, isConnCtrlTag } from '../shared-ws.js'
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
  /** Set only by a barrier commit: the reconcile ran on the OLD wire's recv chain but belongs to
   *  the staged probe wire, so its `reconciled` must be delivered there instead of to the
   *  connection that hosted the turn. Absent on every ordinary reconcile. */
  deliverTo?: unknown
  /** Set only by a barrier commit — this is what makes the frame a COMMITTED rather than an
   *  ordinary RECONCILED, so a delayed one can never be consumed as the commit of an attempt. */
  upgradeId?: string
}

/** Resource limits the mux enforces. The `maxFrameBytes`…`stageTtlMs` group admits the barrier
 *  upgrade; the `maxRaw…`/`maxRecv…` group is not upgrade-specific and bounds inbound raw frames on
 *  EVERY wire. Named immutable defaults in production; tests construct a `ChannelMux` with small
 *  values so the mechanism runs through the REAL enforcement path rather than a parallel accountant. */
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
  /** `staged` until a barrier commits to this record; `committing` from then until the commit
   *  settles. The record is the probe wire's PHASE MARKER — while it exists that wire may send only
   *  PING and its first PREPARE — so releasing it at the start of an awaitable commit would open a
   *  window in which the probe is neither staged nor sessioned, and a plain reconcile there would
   *  run a second destructive rotation alongside the one committing. The phase also makes the record
   *  single-use: a duplicate barrier can never re-enter a commit already in flight. */
  phase: 'staged' | 'committing'
}

/** Read-only view of the staged-upgrade accounting, for tests. Derived from the SAME fields
 *  admission reads — a snapshot maintained in parallel would stay correct while enforcement
 *  broke, which is precisely the bug it exists to reveal. */
type UpgradeResourceSnapshot = {
  /** `stagedUpgrades.size` — the record count admission compares against `maxStagedRecords`. */
  records: number
  /** `stagedByPrevSession.size`. Equal to `records` unless a cleanup path leaked one side. */
  reverseRecords: number
  /** The running charge admission compares against `maxStagedBytes`. */
  bytes: number
}

/** Read-only view of ONE connection's recv-chain backlog, for tests. Same discipline as
 *  `UpgradeResourceSnapshot`: these are the very fields the overflow check compares, not a tally
 *  kept beside them — a mirrored counter would stay right while enforcement went wrong. */
type BacklogSnapshot = {
  bytes: number
  frames: number
}

/** Which wire dies if this turn throws. Defaults to the wire the frame arrived on; the barrier
 *  branch retargets it to the staged probe wire, because a rejected commit must leave the old SSE
 *  session untouched — killing the connection whose chain merely HOSTED the turn inverts the rule. */
type ViolationTarget = { connection: unknown }

const textEncoder = new TextEncoder()

type MuxServerOptions = {
  reconnectTimeout: number
  idleTimeout: number
  pingInterval: number
  pingDeadline: number
  clientReplayBuffer: number
  clientReplayBufferBinary: number
  connectTtl: number
  bufferLimit: number
  bufferLimitBinary: number
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
  /** Raw bytes and frames currently charged to this wire's recv chain — every frame that has been
   *  admitted but whose turn has not finished. Charged before the frame is chained and refunded in
   *  that turn's own `finally`, so the pair is one integer op each on the dispatch path and cannot
   *  drift: there is exactly one increment site and exactly one decrement site. */
  recvBacklogBytes: number
  recvBacklogFrames: number
}

type ConnectionEntry = {
  state: ConnectionState
  transport: ServerTransport<unknown>
}

class ProtocolViolationError extends Error {}

/** A barrier the server cannot honour because its stage is simply GONE — expired, or its probe
 *  closed. Distinct from a violation because there is no correct wire to kill: the probe no longer
 *  exists to be terminated, and terminating the OLD wire would cost an established client a healthy
 *  session because a probe timed out. The commit is refused, both wires are left exactly as they
 *  were, and the client's own attempt deadline aborts the upgrade.
 *
 *  This is a refusal, not the silent drop the design forbids: nothing rotates and nothing the client
 *  sent is consumed, which is precisely what distinguishes it from the loss PC1 records. */
class StaleBarrierError extends Error {}

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
  /** Accepted PREPAREs awaiting their barrier, keyed by the staged probe (WS) connection. */
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
    // `reconciled` belongs there. Every ordinary reconcile leaves `deliverTo` absent and is
    // unaffected; resolution stays inside `connectionEntries` either way (see `send`).
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
        // Capability advertisement. Safe to send unconditionally: neither payload is schema-
        // validated, so an older client simply ignores the unknown field.
        barrierUpgrade: true,
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
    // ⚠️ Placement is load-bearing: this must sit ABOVE the session-less early return below. A
    // staged probe wire has no session id by definition, so cleanup placed any lower would never
    // run and every abandoned attempt would leak two map entries and a live timer, permanently,
    // keyed by a dead object.
    this.clearStage(connection)
    const sessionId = entry.transport.getSessionId(connection)
    if (!sessionId) return // Closed before reconciling — nothing to clean up.
    // The OLD wire died with a stage pending on it: the barrier can never arrive now. Cleared
    // before `detachSession` so the ordering is unambiguous rather than incidental.
    const stagedWs = this.stagedByPrevSession.get(sessionId)
    if (stagedWs !== undefined) this.clearStage(stagedWs)
    // Channels survive a transient close (`_onPeerDisconnect`'s reconnectTimeout grace);
    // permanent tears them down. The session-level finalizer is dropped on any close;
    // reconcile rebuilds it on next attach.
    this.detachSession(sessionId, isPermanent ? DETACH_REASON.PERMANENT : DETACH_REASON.TRANSIENT)
    this.sessionFinalizers.delete(sessionId)
  }

  consumePermanentTermination(connection: unknown): boolean | null {
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
    // Resource admission, ahead of everything — decode, dispatch, and the chain itself. The raw-frame
    // ceiling bounds what `decode` below is asked to allocate; the backlog pair bounds what a client
    // can leave QUEUED by stalling the chain and continuing to send. Both terminate only the wire
    // that overran, so a flood costs its author its own connection and nobody else theirs.
    //
    // The backlog charge is deliberately two integer adds with no allocation and no async hop: it
    // sits on the path every ordinary data frame takes, and anything heavier here would be a real
    // throughput cost paid on normal traffic to bound abnormal traffic.
    if (
      byteLength > this.limits.maxRawFrameBytes ||
      state.recvBacklogBytes + byteLength > this.limits.maxRecvBacklogBytes ||
      state.recvBacklogFrames >= this.limits.maxRecvBacklogFrames
    ) {
      this.terminateWire(entry, connection)
      return Promise.resolve(null)
    }
    state.recvBacklogBytes += byteLength
    state.recvBacklogFrames++
    const exec = async (): Promise<ReconcileOutcome | null> => {
      const violation: ViolationTarget = { connection }
      try {
        const pending = this.handleFrame(entry, connection, rawFrame, violation)
        return pending ? ((await pending) ?? null) : null
      } catch (err) {
        // Nobody's wire to kill — see `StaleBarrierError`. Refuse and leave both wires alone.
        if (err instanceof StaleBarrierError) return null
        const target = violation.connection
        // `target === connection` is every pre-existing path, and resolves to the entry captured
        // above exactly as before. Only a barrier retargets, and then the wire may already be gone.
        const targetEntry = target === connection ? entry : this.connectionEntries.get(target)
        if (!targetEntry) return null
        this.terminateWire(targetEntry, target)
        return null
      } finally {
        // The refund rides the turn's OWN try, so it costs no extra promise link and cannot be
        // skipped by any exit — including the violation path above, whose wire is dead but whose
        // entry may be re-registered by a reconnect on the same object in a pooled adapter.
        state.recvBacklogBytes -= byteLength
        state.recvBacklogFrames--
      }
    }
    if (rawFrame[0] === TAG.PING) return exec()
    return this.chainRecv(entry, exec)
  }

  /** Kill one wire and abandon any upgrade staged on it: a wire being torn down can never commit. */
  private terminateWire(entry: ConnectionEntry, connection: unknown): void {
    this.clearStage(connection)
    entry.state.terminatePermanently = true
    entry.transport.terminateConnection(connection)
  }

  /** Returns a `ReconcileOutcome` only on reconcile (so the caller decides when to send
   *  `reconciled`). Anything but reconcile/ping before reconciliation is a violation. */
  private handleFrame(
    entry: ConnectionEntry,
    connection: unknown,
    rawFrame: Uint8Array<ArrayBuffer>,
    violation: ViolationTarget,
  ): null | Promise<ReconcileOutcome | null> {
    // Pre-decode admission for PREPARE. The tag is byte 0, so an oversized stage frame is refused
    // before `decode` materializes its JSON — which is what makes this cap bound decoder ALLOCATION
    // rather than merely bound what is retained afterwards. A barrier cannot be screened the same
    // way: only its decoded payload distinguishes it from an ordinary, deliberately uncapped
    // reconcile. So the division of labour is explicit — for the barrier the 256 KiB cap bounds
    // staged/commit state only, and decode-time allocation is bounded separately by the pre-decode
    // raw-frame ceiling (D3/T7).
    if (rawFrame[0] === TAG.PREPARE && rawFrame.byteLength > this.limits.maxFrameBytes) {
      throw new ProtocolViolationError()
    }
    const frame = decode(rawFrame)
    if (frame.tag === TAG.PING) {
      this.resetPingTimer(connection)
      this.send(connection, encode.pong())
      return null
    }
    // Everything below this line is a SEMANTIC frame, and the barrier is the old wire's FINAL
    // semantic frame by contract. The check has to sit above the reconcile dispatch rather than down
    // with the data frames: a late reconcile on a committed-away wire would otherwise mint a fresh
    // session there and reattach the channels away from the probe that just won them. PING stays
    // legal above — the wire is spent, not dead, and the client keeps it alive until it sees FIN.
    if (entry.state.retiredByBarrier) throw new ProtocolViolationError()
    if (frame.tag === TAG.PREPARE) return this.handlePrepare(entry, connection, frame.payload, rawFrame.byteLength)
    if (frame.tag === TAG.RECONCILE) {
      // The dispatch hole: RECONCILE is handled here, ABOVE the session-less guard below, so
      // without this line a reconcile on a staged probe wire would run the destructive rotation
      // and step around the stage entirely. A barrier-flagged copy is equally wrong on this wire —
      // the barrier belongs on the OLD one — and saying so by name beats relying on the
      // live-session check downstream to reject it as a side effect.
      if (this.stagedUpgrades.has(connection)) throw new ProtocolViolationError()
      if (frame.payload.barrier) {
        return this.handleBarrier(entry, connection, frame.payload, rawFrame.byteLength, violation)
      }
      // A stage cannot outlive the session it commits against, so this reconcile must release any
      // stage bound to a session it is about to consume or vacate. TWO sessions qualify, and they
      // are NOT the same one:
      //
      //   · `ctrl.sessionId` — the session this reconcile CLAIMS. `reconcileSession` removes it from
      //     the registry and deletes its FIN finalizer, and it does so no matter which connection
      //     asked. A reconnect can name it from a wire that holds no session of its own, so keying
      //     only off the reconciling wire misses it entirely — and the stage then survives something
      //     that already consumed everything it depends on. The original wire still reports the old
      //     id (nothing wrote to its transport), so its later barrier passes every equality leg while
      //     being STRING-equal to a session that is semantically dead: it would commit a second
      //     rotation, emit COMMITTED with no FIN available, and leave the claimant holding stale
      //     handles whose close detaches the freshly committed probe peer.
      //   · the reconciling wire's CURRENT session — which it is about to rotate away from. Nothing
      //     destroys that session here, but the stage can never commit against it again (live-session
      //     equality would refuse the barrier), and close cleanup looks up the wire's NEW id, so the
      //     record would leak with its bytes and its timer until the TTL.
      //
      // A reconcile naming NO session is deliberately not covered: it destroys nothing — the
      // previous-session block is skipped entirely, so the staged session keeps both its registry
      // entry and its finalizer and a later commit against it is still coherent. Releasing there
      // would abort a healthy upgrade every time an unrelated connection attached by channel id.
      const claimedSessionId = frame.payload.sessionId
      const vacatedSessionId = entry.transport.getSessionId(connection)
      for (const doomed of [claimedSessionId, vacatedSessionId]) {
        if (doomed === undefined) continue
        const staleProbe = this.stagedByPrevSession.get(doomed)
        if (staleProbe !== undefined) this.abandonStage(staleProbe)
      }
      return this.reconcile(entry, connection, frame.payload)
    }
    const sessionId = entry.transport.getSessionId(connection)
    if (!sessionId) throw new ProtocolViolationError()
    // PONG/FIN/RECONCILED are server→client only; a client sending one is a violation.
    if (isConnCtrlTag(frame.tag)) throw new ProtocolViolationError()
    // Frame for an ix that's no longer in the session — client closed the channel and the
    // server reconciled it out, but a frame was still in flight. Drop silently.
    this.sessions.get(sessionId, (frame as ChannelFrame).index)?.channel._dispatchFrame(frame as ChannelFrame)
    return null
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
    // Charged before anything is installed, so a rejected PREPARE leaves no trace to clean up.
    this.validateUpgradeFrame(payload.open, rawByteLength)
    if (!payload.upgradeId || typeof payload.upgradeId !== 'string') throw new ProtocolViolationError()
    if (!payload.sessionId || typeof payload.sessionId !== 'string') throw new ProtocolViolationError()
    // A PREPARE belongs on a probe wire that is not yet anyone's transport. On an already-
    // sessioned connection it is newly reachable (this branch sits above the session-less guard)
    // and always wrong.
    if (entry.transport.getSessionId(connection)) throw new ProtocolViolationError()
    // One stage per probe wire AND one per old session. Replace-semantics would have to cancel the
    // old timer anyway, and a compliant client never sends a second PREPARE.
    if (this.stagedUpgrades.has(connection)) throw new ProtocolViolationError()
    if (this.stagedByPrevSession.has(payload.sessionId)) throw new ProtocolViolationError()
    // Global budget: rejects the NEW probe only. Existing stages and every SSE session are
    // untouched, so a flood cannot evict an in-progress upgrade.
    if (this.stagedUpgrades.size >= limits.maxStagedRecords) throw new ProtocolViolationError()
    if (this.stagedBytes + rawByteLength > limits.maxStagedBytes) throw new ProtocolViolationError()

    // Expiry releases the probe as well as the record, per the timeout table: the attempt it was
    // opened for is over, nothing it can now send is legal, and PING alone would otherwise keep a
    // useless session-less socket alive indefinitely.
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
    // Recorded once the stage is installed and about to be acknowledged — an accepted PREPARE,
    // not merely one that arrived. Every rejection above throws before reaching this line.
    recordUpgradePrepared()
    this.send(connection, encode.ready({ upgradeId: payload.upgradeId }))
    return null
  }

  /** The barrier arrives on the OLD wire and commits the staged probe wire. It runs as an ordinary
   *  turn on that wire's `recvChain`, so the happens-before edge over every earlier old-wire frame
   *  already exists by construction — nothing here awaits anything cross-wire.
   *
   *  Commits by handing the STAGED connection's entry to the existing, unmodified `reconcile`: the
   *  send closure, `setSessionId`, the new finalizer and `state.reconciling` all bind to the probe
   *  wire because that is the entry they are given. The one thing that must bind to the old wire —
   *  FIN — does so untouched, since `buildUpgradeFinalizer` reads the PREVIOUS session's finalizer,
   *  registered against the old connection on its own reconcile. */
  private handleBarrier(
    entry: ConnectionEntry,
    connection: unknown,
    ctrl: ReconcilePayload,
    rawByteLength: number,
    violation: ViolationTarget,
  ): Promise<ReconcileOutcome> {
    const sessionId = ctrl.sessionId
    if (!sessionId) throw new ProtocolViolationError()
    const wsConnection = this.stagedByPrevSession.get(sessionId)
    const stage = wsConnection === undefined ? undefined : this.stagedUpgrades.get(wsConnection)
    // No stage at all, or one already committing: nothing here is any wire's fault to die for.
    if (wsConnection === undefined || !stage) throw new StaleBarrierError()
    if (stage.phase !== 'staged') throw new StaleBarrierError()

    // From here every failure is the STAGED wire's: a rejected commit must leave the old session
    // fully intact, so the wire whose chain merely HOSTS this turn is never the victim.
    violation.connection = wsConnection
    try {
      this.validateUpgradeFrame(ctrl.open, rawByteLength)
      // Established channels only. `attach` is the one awaitable inside `reconcile`, and it waits
      // only for an `initial: true` entry naming an unregistered channel — which would park the
      // commit mid-flight. The barrier's membership is authoritative, so a channel it names that the
      // server does not have is omitted, never waited for.
      for (const channel of ctrl.open) if (channel.initial) throw new ProtocolViolationError()
      // TWO checks for two distinct defects; neither substitutes for the other. The id closes stale
      // settlement (a delayed ordinary reconciled consumed as this attempt's commit). The live-session
      // equality closes concurrent rotation — it is what requires the barrier to arrive on the wire
      // that still OWNS the staged session, so a copy landing anywhere else can never commit.
      if (ctrl.upgradeId !== stage.upgradeId) throw new ProtocolViolationError()
      if (entry.transport.getSessionId(connection) !== stage.prevSessionId) throw new ProtocolViolationError()

      // `state.closed` is deliberately NOT also checked: `onConnectionClosed` deletes the entry in
      // the same breath as it sets that flag, so a closed wire is always simply absent here. A
      // second condition would read as defence and be unreachable — verified by mutation.
      const wsEntry = this.connectionEntries.get(wsConnection)
      if (!wsEntry) throw new ProtocolViolationError()

      // Committed to. The record now survives as the probe's phase marker for the whole of the
      // awaitable commit — and its TTL stays armed, so the 10 s attempt deadline still bounds an
      // upgrade that wedges inside `reconcile` rather than being cancelled at the last moment.
      stage.phase = 'committing'
      // The old wire is spent: the barrier is its final semantic frame by contract.
      entry.state.retiredByBarrier = true
      const release = () => this.clearStage(wsConnection)
      return this.reconcile(wsEntry, wsConnection, ctrl).then(
        (outcome) => {
          // Released at the session-set boundary — `reconcile` has run `setSession`/`setSessionId`,
          // so the probe now legitimately owns a session and the pre-COMMITTED policy no longer
          // applies to it. (The caller enqueues COMMITTED a microtask later; in that sliver the wire
          // is an ordinary sessioned transport, on which a reconcile is legal anyway.)
          release()
          // The authoritative "this upgrade is done" event: the reconcile RESOLVED, so the probe
          // wire owns the session. Recorded here rather than at the caller's COMMITTED send so a
          // future change to how COMMITTED is delivered cannot quietly detach the e2e oracle from
          // the commit it names.
          recordUpgradeCommitted()
          return { ...outcome, deliverTo: wsConnection, upgradeId: stage.upgradeId }
        },
        (err) => {
          release()
          throw err
        },
      )
    } catch (err) {
      this.clearStage(wsConnection)
      throw err
    }
  }

  /** Release a stage that can no longer commit, and let go of its probe with it. Clearing the record
   *  alone would leave a session-less WS that PING keeps alive indefinitely, waiting on a commit the
   *  server will never perform. The OLD session is never touched here — that is the entire pre-barrier
   *  rule: an abandoned attempt costs the established client nothing. */
  private abandonStage(wsConnection: unknown): void {
    if (!this.stagedUpgrades.has(wsConnection)) return
    this.clearStage(wsConnection)
    const entry = this.connectionEntries.get(wsConnection)
    if (!entry) return
    entry.state.terminatePermanently = true
    entry.transport.terminateConnection(wsConnection)
  }

  /** Idempotent, and the ONLY writer of the staged accounting — so every one of the six cleanup
   *  paths refunds the byte charge and both map entries by construction rather than by discipline. */
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

  /** Applies to PREPARE and `barrier: true` RECONCILE only — an ordinary reconcile keeps its
   *  existing uncapped contract. */
  private validateUpgradeFrame(open: ReconcilePayload['open'] | PreparePayload['open'], rawByteLength: number): void {
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
    const finalizeUpgrade = ctrl.upgrade && ctrl.sessionId ? this.buildUpgradeFinalizer(ctrl.sessionId) : null
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

  /** @internal @test-only Deliberately absent from the package's documented API. Reads the SAME
   *  fields admission enforces on — a snapshot kept in parallel would stay correct while
   *  enforcement broke, hiding exactly the bug it was added to reveal. */
  _getUpgradeResourceSnapshot(): UpgradeResourceSnapshot {
    return {
      records: this.stagedUpgrades.size,
      reverseRecords: this.stagedByPrevSession.size,
      bytes: this.stagedBytes,
    }
  }

  /** @internal @test-only The resolved limits object ITSELF — the very reference every comparison
   *  above dereferences, not a copy of it. Exists because the mechanism/value split has a blind spot
   *  neither half can see: injected-limit tests prove enforcement reads `limits.<field>`, and
   *  constant tests prove each exported constant holds its agreed number, but NOTHING between them
   *  observes which constant `DEFAULT_MUX_LIMITS` bound to which field. Swapping two of them leaves
   *  both halves true and the shipped ceiling wrong. */
  _getResourceLimits(): Readonly<MuxResourceLimits> {
    return this.limits
  }

  /** @internal @test-only Beside `_getUpgradeResourceSnapshot` rather than folded into it: that one
   *  is server-global, this one is per-connection, and merging them would force a shape where one
   *  half is meaningless. Reads the enforcement fields themselves. Null once the wire is gone. */
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
    bufferLimit: c.bufferLimit,
    bufferLimitBinary: c.bufferLimitBinary,
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
