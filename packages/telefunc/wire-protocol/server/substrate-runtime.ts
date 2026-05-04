export { ChannelMux }
export type { SendFn, ServerTransport }

import { assert } from '../../utils/assert.js'
import { unrefTimer } from '../../utils/unrefTimer.js'
import { getGlobalObject } from '../../utils/getGlobalObject.js'
import { getServerConfig } from '../../node/server/serverConfig.js'
import { setChannelDefaults } from './channel.js'
import { TAG, decode, encode, isConnCtrlTag } from '../shared-ws.js'
import type { ChannelFrame, ReconcilePayload, ReconciledPayload } from '../shared-ws.js'
import { CHANNEL_PING_INTERVAL_MIN_MS, type ChannelTransports, SUBSTRATE_ATTACH_ACK_TIMEOUT_MS } from '../constants.js'
import { IndexedPeer, type PeerSender } from './IndexedPeer.js'
import type { ServerChannel } from './channel.js'
import { DETACH_REASON, ENVELOPE_KIND, PROXY_DIRECTION } from './substrate.js'
import type { ChannelSubstrate, ConnectionRecord, DetachReason, ProxyAttachPayload } from './substrate.js'

/** Invoking preserves wire order via the connection's serialised send chain. */
type SendFn = (frame: Uint8Array<ArrayBuffer>, onCommit?: () => void) => void | Promise<void>

type ChannelHandle =
  | { kind: 'local'; channel: ServerChannel; ix: number }
  | { kind: 'proxy'; channelId: string; homeInstance: string; ix: number; lastClientSeq: number }

type ProxyChannelState = {
  homeInstance: string
  /** Client-allocated wire ix for this channel — needed when synthesizing an `abort`
   *  ctrl frame on the local wire after detecting the home instance has died. */
  ix: number
  writeFrame: SendFn
}

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

/** Returned by `handleFrame` only when a reconcile completed: lets the caller branch
 *  on object identity (`if (result)`) rather than a magic `string | null` value. */
type ReconcileOutcome = { sessionId: string }

type SessionState = {
  /** `ReconciledPayload.open[]` snapshot returned by the most recent `reconcileSession`.
   *  Each entry carries `home` so the client can mirror it into data POST metadata for
   *  per-frame routing. */
  openList: ReconciledPayload['open']
  /** Waits until the currently attached transport has fully drained its send chain. */
  drainActiveTransport: (() => Promise<void>) | null
  /** Sends a fin ctrl frame on the current transport. Set at end of each reconcile. */
  sendFin: (() => void | Promise<void>) | null
  /** Set when this reconcile carries an `upgrade`: drains the previous transport and sends
   *  fin on it. Fired by `sendReconciled` once the new transport has emitted reconciled. */
  finalizeUpgrade: (() => void) | null
}

type ConnectionState = {
  pingTimer: ReturnType<typeof setTimeout> | null
  terminatePermanently: boolean | null
  reconciling: boolean
  sendChain: Promise<void> | null
}

/** Per-connection runtime state plus a back-reference to the transport-specific hooks
 *  that produced this connection. The mux is heterogeneous over `TConnection` types
 *  (one process can run SSE + WS); the back-reference lets `sendNow`/`terminate`/etc.
 *  reach the right transport without the mux being generic. */
type ConnectionEntry = {
  state: ConnectionState
  transport: ServerTransport<unknown>
}

type ServerTransport<TConnection> = {
  getSessionId(connection: TConnection): string | undefined
  setSessionId(connection: TConnection, sessionId: string): void
  /** Stable per-connection identifier the cluster uses to route data POSTs back to the
   *  owner instance. Returns `null` for transports that don't multiplex client→server
   *  traffic across requests (e.g. WebSocket — every frame already lands on the owner). */
  getConnId(connection: TConnection): string | null
  sendNow(connection: TConnection, frame: Uint8Array<ArrayBuffer>): void | Promise<void>
  terminateConnection(connection: TConnection): void
}

class ProtocolViolationError extends Error {}

const globalObject = getGlobalObject('wire-protocol/server/substrate-runtime.ts', {
  sessionStates: new Map<string, SessionState>(),
})

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

/** Forward (`bySession`: sessionId → ix → handle) for per-frame routing, reverse
 *  (`byChannel`: channelId → sessionId → ix) for O(bindings) channel eviction.
 *  Mutations are atomic across both. Proxy handles aren't reverse-indexed. */
class SessionRegistry {
  private readonly bySession = new Map<string, Map<number, ChannelHandle>>()
  private readonly byChannel = new Map<string, Map<string, number>>()

  get(sessionId: string, ix: number): ChannelHandle | undefined {
    return this.bySession.get(sessionId)?.get(ix)
  }

  /** Read the handle map for a session without mutating. Used by transient-close handling
   *  so the next reconcile's prev-comparison can still detect channels the client dropped. */
  peekSession(sessionId: string): Map<number, ChannelHandle> | undefined {
    return this.bySession.get(sessionId)
  }

  setSession(sessionId: string, handles: Iterable<ChannelHandle>): void {
    this.removeSession(sessionId)
    const session = new Map<number, ChannelHandle>()
    for (const h of handles) {
      session.set(h.ix, h)
      if (h.kind !== 'local') continue
      let bindings = this.byChannel.get(h.channel.id)
      if (!bindings) {
        bindings = new Map()
        this.byChannel.set(h.channel.id, bindings)
      }
      bindings.set(sessionId, h.ix)
    }
    this.bySession.set(sessionId, session)
  }

  /** Returns the removed session so callers can drive per-handle lifecycle side effects. */
  removeSession(sessionId: string): Map<number, ChannelHandle> | undefined {
    const session = this.bySession.get(sessionId)
    if (!session) return undefined
    this.bySession.delete(sessionId)
    for (const handle of session.values()) {
      if (handle.kind !== 'local') continue
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
    for (const [sessionId, ix] of bindings) this.bySession.get(sessionId)?.delete(ix)
  }

  clear(): void {
    this.bySession.clear()
    this.byChannel.clear()
  }
}

/** The kernel: owns channels, sessions, connection runtime state, and the substrate
 *  reference. Transports (SSE, WS, Cloudflare) talk to this class — they hand it the
 *  transport-specific hooks at `onConnectionOpen` and from then on identify connections
 *  by object identity. The substrate has exactly one parent (this class). */
class ChannelMux {
  // ServerChannel runtime
  private readonly substrate: ChannelSubstrate
  private readonly localChannels = new Map<string, ServerChannel>()
  /** Same-process attach waiters, fired synchronously by `registerChannel` so a
   *  deferred-attach race resolves locally without involving the substrate. */
  private readonly localWaiters = new Map<string, Set<(channel: ServerChannel) => void>>()
  /** sessionId → ix → handle. ServerChannel-attach registry, separate from per-session
   *  connection state (`sessionStates`). */
  private readonly sessions = new SessionRegistry()
  /** Channels substrate-attached here (HOME role) → owning proxy instance. The
   *  `fromInstance` check rejects stale frames/detaches from a handed-off proxy. */
  private readonly homeAttached = new Map<string, string>()
  private readonly proxyStates = new Map<string, ProxyChannelState>()
  /** Connections this instance owns (it holds the SSE response wire). Refreshed each
   *  heartbeat so non-owner POST receivers can locate `owner` via `locateConnection`. */
  private readonly ownedConnections = new Set<string>()

  // Connection runtime. `options` is resolved lazily on first use so the mux can be
  // constructed at module-load (substrate.ts's globalObject) before `serverConfig` has
  // been initialized.
  private resolvedOptions: MuxServerOptions | null = null
  private readonly sessionStates = globalObject.sessionStates
  private readonly connectionStates = new Map<unknown, ConnectionEntry>()
  /** Reverse map keyed by `connId`, populated for transports that report a stable connId
   *  (SSE). Lets cross-instance `CONNECTION_FRAME` envelopes resolve the local connection. */
  private readonly connectionsByConnId = new Map<string, unknown>()

  private readonly unsubscribe: () => void
  private readonly heartbeatTimer: ReturnType<typeof setInterval>

  constructor(substrate: ChannelSubstrate) {
    this.substrate = substrate
    this.unsubscribe = substrate.listen({
      onAttach: (env, payload) => this.attachHome(env.channelId, env.fromInstance, payload),
      onDetach: (env, payload) => this.detachHome(env.channelId, env.fromInstance, payload.reason),
      onHomeFrame: (env, payload) => this.dispatchHomeFrame(env.channelId, payload.frame),
      onPeerFrame: (env, payload) => this.writePeerFrame(env.channelId, payload.frame),
      onPeerDetach: (env) => this.proxyStates.delete(env.channelId),
      // Cross-instance CONNECTION_FRAME envelopes: a non-owner POST receiver forwarded an
      // alias-0 frame here for re-injection on this owner's local connection. Vacuous for
      // transports without connId (WS), since the lookup map stays empty.
      onConnectionFrame: (env, payload) => {
        const connection = this.connectionsByConnId.get(env.channelId)
        if (connection !== undefined) {
          void this.onConnectionRawMessage(connection, payload.frame as Uint8Array<ArrayBuffer>)
        }
      },
      // `onAttachAck` is registered ad-hoc by `awaitAttachAck` for the duration of one call.
    })
    this.heartbeatTimer = unrefTimer(
      setInterval(() => void this.heartbeat().catch(() => {}), substrate.heartbeatIntervalMs),
    )
    void this.substrate.pinInstance()
  }

  private get options(): MuxServerOptions {
    if (this.resolvedOptions) return this.resolvedOptions
    this.resolvedOptions = resolveMuxServerOptions()
    setChannelDefaults({
      connectTtl: this.resolvedOptions.connectTtl,
      bufferLimit: this.resolvedOptions.bufferLimit,
      bufferLimitBinary: this.resolvedOptions.bufferLimitBinary,
    })
    return this.resolvedOptions
  }

  /** First-frame timeout for new channels (ms). Exposed so transports can apply it to
   *  same-instance race timers (e.g. SSE's `waitForConnection`). */
  get connectTtl(): number {
    return this.options.connectTtl
  }

  // ── ServerChannel registry ──────────────────────────────────────────────────

  /** Callers must not invoke `channel._registerChannel()` directly. */
  registerChannel(channel: ServerChannel<any, any>): void {
    channel._registerChannel()
    this.localChannels.set(channel.id, channel)
    const waiters = this.localWaiters.get(channel.id)
    if (waiters) {
      this.localWaiters.delete(channel.id)
      for (const cb of waiters) cb(channel)
    }
    channel._onShutdown(() => this.unregisterChannel(channel.id))
    void this.substrate.pinChannel(channel.id)
  }

  unregisterChannel(channelId: string): void {
    this.localChannels.delete(channelId)
    this.sessions.removeChannel(channelId)
    const proxyInstance = this.homeAttached.get(channelId)
    if (proxyInstance !== undefined) {
      this.homeAttached.delete(channelId)
      void this.substrate.forward(proxyInstance, {
        channelId,
        fromInstance: this.substrate.selfInstanceId,
        direction: PROXY_DIRECTION.TO_PEER,
        payload: { kind: ENVELOPE_KIND.DETACH, reason: DETACH_REASON.PERMANENT },
      })
    } else {
    }
    void this.substrate.unpinChannel(channelId)
  }

  findLocal(channelId: string): ServerChannel | null {
    return this.localChannels.get(channelId) ?? null
  }

  hasChannels(): boolean {
    return this.localChannels.size > 0
  }

  // ── Cluster bridge ────────────────────────────────────────────────────
  // The mux is the only consumer of `substrate`. Components above the mux reach the
  // cluster through these methods.

  /** This server's cluster-instance identifier. */
  get selfInstanceId(): string {
    return this.substrate.selfInstanceId
  }

  /** Read this connection's cluster-visible record (owner + sessionId). Returns null if
   *  the record is absent (never reconciled, unpinned, or TTL'd out). Used at reconcile
   *  time only — not on the data hot path. */
  locateConnection(connId: string): Promise<ConnectionRecord | null> {
    return this.substrate.locateConnection(connId)
  }

  /** Forward a connection-level wire frame (ping, in-body reconcile) to the connection's
   *  owner so it re-injects on its local connection. Used by SSE data POSTs that landed
   *  on a non-owner instance. */
  forwardConnectionFrame(ownerInstance: string, connId: string, rawFrame: Uint8Array<ArrayBuffer>): Promise<void> {
    return this.substrate.forward(ownerInstance, {
      channelId: connId,
      fromInstance: this.substrate.selfInstanceId,
      direction: PROXY_DIRECTION.TO_HOME,
      payload: { kind: ENVELOPE_KIND.CONNECTION_FRAME, frame: rawFrame },
    })
  }

  /** Dispatch a client→server channel frame to its home: locally if `home` is this
   *  instance (skip the substrate entirely), otherwise forward to the home as a FRAME
   *  envelope. Used by SSE data POSTs that route per-frame from client-supplied metadata
   *  — channel-data frames bypass the connection owner and reach the home in 1 hop. */
  async routeClientFrame(channelId: string, home: string, rawFrame: Uint8Array<ArrayBuffer>): Promise<void> {
    if (home === this.substrate.selfInstanceId) {
      this.findLocal(channelId)?._dispatchFrame(decode(rawFrame) as ChannelFrame)
      return
    }
    await this.forwardClientFrameToHome(home, channelId, rawFrame)
  }

  private forwardClientFrameToHome(
    homeInstance: string,
    channelId: string,
    frame: Uint8Array<ArrayBuffer>,
  ): Promise<void> {
    return this.substrate.forward(homeInstance, {
      channelId,
      fromInstance: this.substrate.selfInstanceId,
      direction: PROXY_DIRECTION.TO_HOME,
      payload: { kind: ENVELOPE_KIND.FRAME, frame },
    })
  }

  // ── Connection lifecycle ──────────────────────────────────────────────

  /** Register a connection with its transport hooks. Subsequent calls (`onConnectionRawMessage`,
   *  `sendReconciled`, `onConnectionClosed`) identify the connection by object identity and
   *  reach back through the stored hooks for `sendNow`/`terminateConnection`/etc. */
  onConnectionOpen<TConnection>(connection: TConnection, transport: ServerTransport<TConnection>): void {
    this.connectionStates.set(connection, {
      state: { pingTimer: null, terminatePermanently: null, reconciling: false, sendChain: null },
      transport: transport as ServerTransport<unknown>,
    })
    const connId = transport.getConnId(connection)
    if (connId !== null) this.connectionsByConnId.set(connId, connection)
    this.resetPingTimer(connection)
  }

  async onConnectionRawMessage(connection: unknown, rawFrame: Uint8Array<ArrayBuffer>): Promise<void> {
    const entry = this.getEntry(connection)
    if (!entry) return
    try {
      const pending = this.handleFrame(entry, connection, rawFrame)
      if (!pending) return
      // If the frame was a `reconcile`, `handleFrame` resolves to the new sessionId — emit
      // `reconciled` immediately. SSE takes the deferred path and emits later (after
      // draining concurrent data POSTs); see `onConnectionRawMessageDeferredReconciled`.
      const outcome = await pending
      if (outcome) this.sendReconciled(connection, outcome.sessionId)
    } catch {
      entry.state.terminatePermanently = true
      entry.transport.terminateConnection(connection)
    }
  }

  async onConnectionRawMessageDeferredReconciled(
    connection: unknown,
    rawFrame: Uint8Array<ArrayBuffer>,
  ): Promise<string | null> {
    const entry = this.getEntry(connection)
    if (!entry) return null
    try {
      const pending = this.handleFrame(entry, connection, rawFrame)
      if (!pending) return null
      return (await pending)?.sessionId ?? null
    } catch {
      entry.state.terminatePermanently = true
      entry.transport.terminateConnection(connection)
      return null
    }
  }

  onConnectionClosed(connection: unknown, isPermanent: boolean): void {
    const entry = this.connectionStates.get(connection)
    if (!entry) return
    this.clearPingTimer(entry.state)
    this.connectionStates.delete(connection)
    const connId = entry.transport.getConnId(connection)
    if (connId !== null && this.connectionsByConnId.get(connId) === connection) {
      this.connectionsByConnId.delete(connId)
    }
    this.handleConnectionClose(entry, connection, isPermanent)
  }

  consumePermanentTermination(connection: unknown): boolean | null {
    return this.getEntry(connection)?.state.terminatePermanently ?? null
  }

  getConnectionByConnId<TConnection>(connId: string): TConnection | undefined {
    return this.connectionsByConnId.get(connId) as TConnection | undefined
  }

  sendReconciled(connection: unknown, sessionId: string): void | Promise<void> {
    const sessionState = this.getSessionStateOrThrow(sessionId)
    const pending = this.send(
      connection,
      encode.reconciled({
        sessionId,
        open: sessionState.openList,
        ownerInstance: this.substrate.selfInstanceId,
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
    // Upgrade carries over from the previous transport: drain and `fin` it now that the
    // new transport has accepted `reconciled`. Each transport has its own send chain, so
    // firing this synchronously is safe — it doesn't reorder anything on the new wire.
    const finalizeUpgrade = sessionState.finalizeUpgrade
    if (finalizeUpgrade) {
      sessionState.finalizeUpgrade = null
      finalizeUpgrade()
    }
    return pending
  }

  // ── Per-session/per-connection internals ──────────────────────────────

  /** Connection-scoped outbound send gate. Preserves wire order across the whole
   *  connection: every server→client frame for one transport flows through here, whether
   *  it originates from mux control handling (`reconcile` replay, `reconciled`, `pong`) or
   *  from a `ServerChannel` via the `SendFn` closure passed into `reconcileSession`. */
  private send(connection: unknown, frame: Uint8Array<ArrayBuffer>, onCommit?: () => void): void | Promise<void> {
    const entry = this.getEntry(connection)
    if (!entry) return
    const { state, transport } = entry
    if (!state.sendChain) {
      onCommit?.()
      const pending = transport.sendNow(connection, frame)
      if (!pending) return
      const chain = pending.finally(() => {
        if (state.sendChain === chain) state.sendChain = null
      })
      state.sendChain = chain
      return chain
    }
    const chain = state.sendChain
      .then(() => {
        onCommit?.()
        return transport.sendNow(connection, frame)
      })
      .finally(() => {
        if (state.sendChain === chain) state.sendChain = null
      })
    state.sendChain = chain
    return chain
  }

  private handleConnectionClose(entry: ConnectionEntry, connection: unknown, permanent: boolean): void {
    // The owner pin is independent of sessionState — clean it up first, unconditionally.
    // Otherwise an SSE→WS upgrade leaves the SSE wire's `connId` orphaned: WS reconcile
    // deletes the SSE session's `sessionState`, then SSE closes, and the early-return on
    // missing `sessionState` would skip the unregister, leaving `ownedConnections` (and
    // the `tf:conn:<connId>` pin) refreshing forever.
    const connId = entry.transport.getConnId(connection)
    if (connId !== null) this.unregisterOwnedConnection(connId)

    // No sessionId: the connection closed before reconciling (handshake error or immediate
    // disconnect). No session state to clean up.
    const sessionId = entry.transport.getSessionId(connection)
    if (!sessionId) return
    const sessionState = this.sessionStates.get(sessionId)
    if (!sessionState) return

    sessionState.drainActiveTransport = null
    sessionState.sendFin = null

    // The transient/permanent distinction governs *channel* grace inside `detachSession` —
    // channels survive a transient close via `_onPeerDisconnect`'s reconnectTimeout.
    // Connection-level state (sessionState closures) is cluster-coordinated through the
    // substrate; clearing it eagerly on any close is correct because reconcile rebuilds
    // it from scratch on the next attach. SSE only ever fires permanent close on a
    // protocol violation (`handleFrame` throw) — every network drop, page close, ping
    // timeout, and stream cancel is transient.
    this.detachSession(sessionId, permanent ? DETACH_REASON.PERMANENT : DETACH_REASON.TRANSIENT)
    this.sessionStates.delete(sessionId)
  }

  private async reconcile(
    entry: ConnectionEntry,
    connection: unknown,
    ctrl: ReconcilePayload,
  ): Promise<ReconcileOutcome> {
    const { state, transport } = entry
    const connId = transport.getConnId(connection)
    const sessionState = await this.resumeOrCreateSessionState(ctrl.sessionId, connId)
    state.reconciling = true
    this.resetPingTimer(connection)
    // Snapshot the previous transport's drain/fin handlers BEFORE we overwrite them with
    // the new transport's handlers below — `prepareUpgradeFinalizer` reads from sessionState.
    const finalizeUpgrade = ctrl.upgrade ? this.prepareUpgradeFinalizer(sessionState) : null
    const send: SendFn = (frame, onCommit) => this.send(connection, frame, onCommit)
    const newSessionId = crypto.randomUUID()

    // The mux attaches every channel in `ctrl.open` (parallel home-lookups + parallel
    // local replay drains and proxy attach-acks), detaches anything from the previous
    // session that the client did NOT re-include, and returns the open list for ReconciledPayload.
    sessionState.openList = await this.reconcileSession({
      prevSessionId: ctrl.sessionId,
      newSessionId,
      open: ctrl.open,
      send,
    })

    // The connection may have closed during the await
    if (!this.connectionStates.has(connection)) {
      this.detachSession(newSessionId, DETACH_REASON.PERMANENT)
      throw new ProtocolViolationError()
    }

    if (ctrl.sessionId) this.sessionStates.delete(ctrl.sessionId)
    this.sessionStates.set(newSessionId, sessionState)
    transport.setSessionId(connection, newSessionId)
    sessionState.drainActiveTransport = async () => {
      const pending = state.sendChain
      if (pending) await pending
    }
    sessionState.sendFin = () => this.send(connection, encode.fin())
    sessionState.finalizeUpgrade = finalizeUpgrade

    state.reconciling = false
    this.resetPingTimer(connection)
    // Pin the connection record cluster-wide so a future reconcile on any instance can
    // verify the client's claimed `prevSessionId`. The record is keyed by `connId`
    // (stable across sessionId rotations); the write replaces any prior reconcile's record.
    if (connId !== null) this.registerOwnedConnection(connId, newSessionId)
    return { sessionId: newSessionId }
  }

  /** Verify and resume a session that may live on any instance. Local hit returns the
   *  in-memory `SessionState` (carries this-instance transport closures used by the upgrade
   *  flow). Local miss + cluster match (Redis records `prevSessionId` as the connection's
   *  current sessionId) means the session was issued on a different instance — build a
   *  fresh local `SessionState` (the closures only describe transports on the previous
   *  instance, all dead by the time we're here). Local miss + cluster mismatch means the
   *  client is replaying a rotated-out sessionId — throw. No prevSessionId means a fresh
   *  reconcile, build empty. */
  private async resumeOrCreateSessionState(
    prevSessionId: string | undefined,
    connId: string | null,
  ): Promise<SessionState> {
    if (prevSessionId) {
      const local = this.sessionStates.get(prevSessionId)
      if (local) return local
      if (connId !== null) {
        const clusterRecord = await this.substrate.locateConnection(connId)
        if (clusterRecord !== null && clusterRecord.sessionId !== prevSessionId) throw new ProtocolViolationError()
      }
    }
    return { openList: [], drainActiveTransport: null, sendFin: null, finalizeUpgrade: null }
  }

  /** Snapshot the previous transport's drain + fin handlers, then return a finalizer the
   *  caller invokes after the new transport has emitted ReconciledPayload. */
  private prepareUpgradeFinalizer(sessionState: SessionState): () => void {
    const drainPreviousTransport = sessionState.drainActiveTransport
    const sendPreviousFin = sessionState.sendFin
    sessionState.drainActiveTransport = null
    sessionState.sendFin = null
    return () => {
      void (async () => {
        if (drainPreviousTransport) await drainPreviousTransport()
        const finPending = sendPreviousFin?.()
        if (finPending) await finPending
      })()
    }
  }

  private getSessionStateOrThrow(sessionId: string | undefined): SessionState {
    if (!sessionId) throw new ProtocolViolationError()
    const sessionState = this.sessionStates.get(sessionId)
    if (!sessionState) throw new ProtocolViolationError()
    return sessionState
  }

  private getEntry(connection: unknown): ConnectionEntry | undefined {
    return this.connectionStates.get(connection)
  }

  private clearPingTimer(state: ConnectionState): void {
    if (!state.pingTimer) return
    clearTimeout(state.pingTimer)
    state.pingTimer = null
  }

  private resetPingTimer(connection: unknown): void {
    const entry = this.connectionStates.get(connection)
    if (!entry) return
    const { state, transport } = entry
    this.clearPingTimer(state)
    state.pingTimer = unrefTimer(
      setTimeout(() => {
        state.pingTimer = null
        if (state.reconciling) return
        // Ping deadline elapsed — close transient so each channel gets its `reconnectTimeout`
        // grace at the home (handled by `_onPeerDisconnect`). Connection-level state
        // (sessionState, owned-connection pin) is cleaned eagerly in `handleConnectionClose`
        // regardless of permanence, since a future reconcile rebuilds it from scratch.
        transport.terminateConnection(connection)
        state.terminatePermanently = false
      }, this.options.pingDeadline),
    )
  }

  /** Single dispatch for an inbound wire frame. Data frames go to the per-ix routing;
   *  ctrl frames branch by `t`. Returns a `ReconcileOutcome` only on reconcile (so the
   *  caller can decide when to send `reconciled`); null in every other case.
   *
   *  Any frame other than `reconcile` or `ping` arriving before this connection has
   *  reconciled is a protocol violation — the spec says the first frame must be `reconcile`. */
  private handleFrame(
    entry: ConnectionEntry,
    connection: unknown,
    rawFrame: Uint8Array<ArrayBuffer>,
  ): null | Promise<ReconcileOutcome | null> {
    const frame = decode(rawFrame)

    // Pre-session ctrls: only `reconcile` (creates the session) and `ping` (heartbeat) are
    // legal before reconcile. Handle them first so the session-existence check below can
    // be unconditional for everything that follows.
    if (frame.tag === TAG.RECONCILE) return this.reconcile(entry, connection, frame.payload)
    if (frame.tag === TAG.PING) {
      this.resetPingTimer(connection)
      this.send(connection, encode.pong())
      return null
    }

    // Everything else requires an established session.
    const sessionId = entry.transport.getSessionId(connection)
    if (!sessionId) throw new ProtocolViolationError()

    // PONG/FIN/RECONCILED are server→client only; a client sending one is a protocol violation.
    if (isConnCtrlTag(frame.tag)) throw new ProtocolViolationError()
    this.handleClientFrame(sessionId, frame as ChannelFrame, rawFrame)
    return null
  }

  // ── Owned-connection directory pin ────────────────────────────────────

  /** Atomically write the connection record (owner + sessionId) and track the connId for
   *  heartbeat refresh. Rotates the cluster-visible sessionId in the same write so prior
   *  sessionIds become unreplay-able. */
  private registerOwnedConnection(connId: string, sessionId: string): void {
    this.ownedConnections.add(connId)
    void this.substrate.pinConnection(connId, { owner: this.substrate.selfInstanceId, sessionId })
  }

  private unregisterOwnedConnection(connId: string): void {
    if (!this.ownedConnections.delete(connId)) return
    void this.substrate.unpinConnection(connId)
  }

  // ── Heartbeat + peer liveness ─────────────────────────────────────────

  /** Refresh own pins and detect dead peer instances. The instance-alive pin is refreshed
   *  unconditionally so peers can ride this single TTL to learn we're still up. ServerChannel
   *  and connection pins are batched into pipelined calls — O(1) round trips regardless
   *  of locally-hosted count. After refreshing, we check every unique remote instance
   *  this mux talks to: dead OWNERs (in `homeAttached`) trigger per-channel
   *  `_onPeerDisconnect` here on the home; dead HOMEs (in `proxyStates`) get an
   *  `abort` ctrl synthesized on the local wire so just that channel closes on the
   *  client without disturbing the rest of the connection. */
  private async heartbeat(): Promise<void> {
    const channelIds = Array.from(this.localChannels.keys())
    const connIds = Array.from(this.ownedConnections)
    await Promise.all([
      this.substrate.pinInstance(),
      this.substrate.refreshChannels(channelIds),
      this.substrate.refreshConnections(connIds),
    ])
    await this.checkPeerLiveness()
  }

  private async checkPeerLiveness(): Promise<void> {
    const peers = new Set<string>()
    for (const owner of this.homeAttached.values()) peers.add(owner)
    for (const state of this.proxyStates.values()) peers.add(state.homeInstance)
    if (peers.size === 0) return
    const aliveness = await Promise.all(
      Array.from(peers, async (peer) => [peer, await this.substrate.isInstanceAlive(peer)] as const),
    )
    for (const [peer, alive] of aliveness) {
      if (alive) continue
      this.onPeerInstanceDead(peer)
    }
  }

  /** Roles aren't mutually exclusive — the dead instance might have been an owner for
   *  some channels (we're their home) and a home for others (we're their owner). Handle
   *  both, idempotently. */
  private onPeerInstanceDead(deadInstance: string): void {
    const reconnectTimeout = getServerConfig().channel.reconnectTimeout
    for (const [channelId, owner] of this.homeAttached) {
      if (owner !== deadInstance) continue
      this.homeAttached.delete(channelId)
      this.localChannels.get(channelId)?._onPeerDisconnect(reconnectTimeout)
    }
    for (const [channelId, state] of this.proxyStates) {
      if (state.homeInstance !== deadInstance) continue
      this.proxyStates.delete(channelId)
      void state.writeFrame(encode.error(state.ix))
    }
  }

  // ── Session attach + per-frame dispatch (channel routing) ─────────────

  async reconcileSession(args: {
    prevSessionId: string | undefined
    newSessionId: string
    open: ReconcilePayload['open']
    send: SendFn
  }): Promise<ReconciledPayload['open']> {
    const handles = (await Promise.all(args.open.map((entry) => this.attach(entry, args.send)))).filter(
      (h): h is ChannelHandle => h !== null,
    )

    // Channels in the previous session that the client did NOT re-include are recovery-failed.
    if (args.prevSessionId) {
      const prev = this.sessions.removeSession(args.prevSessionId)
      if (prev) {
        const keptIxes = new Set(handles.map((h) => h.ix))
        for (const [ix, prevHandle] of prev)
          if (!keptIxes.has(ix)) this.detachHandle(prevHandle, DETACH_REASON.RECOVERY_FAILED)
      }
    }
    this.sessions.setSession(args.newSessionId, handles)

    const self = this.substrate.selfInstanceId
    return handles.map((h) =>
      h.kind === 'local'
        ? { id: h.channel.id, ix: h.ix, lastSeq: h.channel._lastClientSeq, home: self }
        : { id: h.channelId, ix: h.ix, lastSeq: h.lastClientSeq, home: h.homeInstance },
    )
  }

  /** All client frames carry an index (channel ix) post-reconcile — both data tags and
   *  per-channel ctrl tags. Connection-level ctrls are handled by `handleFrame` upstream. */
  handleClientFrame(sessionId: string, frame: ChannelFrame, rawFrame: Uint8Array<ArrayBuffer>): void {
    const h = this.sessions.get(sessionId, frame.index)
    // Race: client closed a channel and the server reconciled it out, but a frame for the dropped
    // ix is still in flight. Indistinguishable from a bogus-ix violation without a per-session
    // history of detached ixs — drop, don't escalate.
    if (!h) return
    if (h.kind === 'local') h.channel._dispatchFrame(frame)
    else void this.forwardProxiedClientFrame(h.channelId, rawFrame)
  }

  /** Notify every handle in this session that the wire is gone. On transient close the
   *  registry entries are LEFT IN PLACE so the next reconcile's prev-comparison can still
   *  detect abandoned channels (those the client doesn't re-include) and fire `recovery-failed`
   *  on them immediately. On permanent close the session is removed outright — there will
   *  be no resume, so there's nothing for a future reconcile to compare against. */
  detachSession(sessionId: string, reason: DetachReason): void {
    const session =
      reason === DETACH_REASON.PERMANENT ? this.sessions.removeSession(sessionId) : this.sessions.peekSession(sessionId)
    if (!session) return
    for (const handle of session.values()) this.detachHandle(handle, reason)
  }

  /** No `_didShutdown` guard needed: `attachLocalChannel` returns null on shutdown during
   *  its replay-drain await, and `unregisterChannel` synchronously evicts the reverse-indexed
   *  session entry — stale handles never reach this path. */
  private detachHandle(h: ChannelHandle, reason: DetachReason): void {
    if (h.kind === 'proxy') {
      this.detachProxyChannel(h.channelId, reason)
      return
    }
    this.dispatchPeerDetach(h.channel, reason)
  }

  /** Single source for `DetachReason → _onPeer*()` so local and home-from-proxy paths agree. */
  private dispatchPeerDetach(channel: ServerChannel, reason: DetachReason): void {
    switch (reason) {
      case DETACH_REASON.PERMANENT:
        channel._onPeerClose()
        return
      case DETACH_REASON.TRANSIENT:
        channel._onPeerDisconnect(getServerConfig().channel.reconnectTimeout)
        return
      case DETACH_REASON.RECOVERY_FAILED:
        channel._onPeerRecoveryFailure()
        return
    }
  }

  dispose(): void {
    this.unsubscribe()
    clearInterval(this.heartbeatTimer)
    this.localChannels.clear()
    this.localWaiters.clear()
    this.homeAttached.clear()
    this.proxyStates.clear()
    this.ownedConnections.clear()
    this.sessions.clear()
    this.connectionStates.clear()
    this.connectionsByConnId.clear()
    void this.substrate.unpinInstance()
  }

  /** Substrate contract: `locateRemoteHome` MUST filter self pins (the runtime's local
   *  waiter handles same-instance). A faulty substrate would silently bounce envelopes. */
  private async locateRemoteHome(channelId: string, timeoutMs: number): Promise<string | null> {
    const home = await this.substrate.locateRemoteHome(channelId, timeoutMs)
    assert(
      home !== this.substrate.selfInstanceId,
      `Substrate returned self instance "${home}" from locateRemoteHome — implementations must filter self pins`,
    )
    return home
  }

  /** Two regimes:
   *   - `initial: true` — first reconcile for this channel; server creation may still be in
   *     flight. Race local-create + cross-instance pin + TTL up to `connectTtl`.
   *   - falsy — established channel; one bounded pin lookup, fail fast if absent. */
  private async attach(entry: ReconcilePayload['open'][number], send: SendFn): Promise<ChannelHandle | null> {
    const local = this.findLocal(entry.id)
    if (local) {
      return attachLocalChannel(local, { ix: entry.ix, lastSeq: entry.lastSeq, send })
    }

    if (!entry.initial) {
      // `timeoutMs=0` = synchronous lookup, no fanout wait. Fails fast if not pinned.
      const home = await this.locateRemoteHome(entry.id, 0)
      if (home === null) {
        return null
      }
      return this.attachAsProxy(entry, home, send).then(
        (h) => h,
        () => null,
      )
    }

    const ttl = getServerConfig().channel.connectTtl
    return new Promise<ChannelHandle | null>((resolve) => {
      let settled = false
      let waiterSet!: Set<(channel: ServerChannel) => void>
      let timer!: ReturnType<typeof setTimeout>

      const localWaiter = (channel: ServerChannel) => {
        if (!claim()) return
        attachLocalChannel(channel, { ix: entry.ix, lastSeq: entry.lastSeq, send }).then(resolve, () => resolve(null))
      }

      /** Wins the race exactly once and tears down the loser branches. */
      const claim = (): boolean => {
        if (settled) return false
        settled = true
        waiterSet.delete(localWaiter)
        if (waiterSet.size === 0) this.localWaiters.delete(entry.id)
        clearTimeout(timer)
        return true
      }

      waiterSet = this.localWaiters.get(entry.id) ?? new Set()
      this.localWaiters.set(entry.id, waiterSet)
      waiterSet.add(localWaiter)
      timer = setTimeout(() => {
        if (claim()) {
          resolve(null)
        }
      }, ttl)

      void this.locateRemoteHome(entry.id, ttl).then((home) => {
        if (home === null || !claim()) return
        // Surface attach-ack timeout as `null` so the rest of the reconcile proceeds.
        this.attachAsProxy(entry, home, send).then(resolve, () => resolve(null))
      })
    })
  }

  /** Routing entry must be installed before the ack arrives — the home sends replay frames
   *  ahead of the ack. Identity-equality cleanup on error prevents clobbering a superseding
   *  `attachAsProxy` for the same channel. */
  private async attachAsProxy(
    entry: ReconcilePayload['open'][number],
    homeInstance: string,
    send: SendFn,
  ): Promise<ChannelHandle> {
    const myProxyState: ProxyChannelState = { homeInstance, ix: entry.ix, writeFrame: send }
    this.proxyStates.set(entry.id, myProxyState)
    try {
      const lastClientSeq = await this.awaitAttachAck(entry.id, homeInstance, entry.ix, entry.lastSeq)
      return { kind: 'proxy', channelId: entry.id, homeInstance, ix: entry.ix, lastClientSeq }
    } catch (err) {
      if (this.proxyStates.get(entry.id) === myProxyState) this.proxyStates.delete(entry.id)
      throw err
    }
  }

  private awaitAttachAck(channelId: string, homeInstance: string, ix: number, lastSeq: number): Promise<number> {
    return new Promise<number>((resolve, reject) => {
      const cleanup = (): void => {
        clearTimeout(timer)
        unsubscribe()
      }
      const timer = setTimeout(() => {
        cleanup()
        reject(new Error(`attach-ack for channel "${channelId}" timed out after ${SUBSTRATE_ATTACH_ACK_TIMEOUT_MS}ms`))
      }, SUBSTRATE_ATTACH_ACK_TIMEOUT_MS)
      const unsubscribe = this.substrate.listen({
        onAttachAck: (env, payload) => {
          if (env.channelId !== channelId) return
          cleanup()
          resolve(payload.lastClientSeq)
        },
      })
      void this.substrate.forward(homeInstance, {
        channelId,
        fromInstance: this.substrate.selfInstanceId,
        direction: PROXY_DIRECTION.TO_HOME,
        payload: {
          kind: ENVELOPE_KIND.ATTACH,
          reconnectTimeout: getServerConfig().channel.reconnectTimeout,
          ix,
          lastSeq,
        },
      })
    })
  }

  /** Forward a client→server wire frame to a remote channel home (we're the proxy). The
   *  proxy state must exist for this `channelId` — `handleClientFrame`/`handleClientCtrl`
   *  only call here when the session registry resolves the ix to a proxy handle. */
  private forwardProxiedClientFrame(channelId: string, frame: Uint8Array<ArrayBuffer>): Promise<void> {
    const state = this.proxyStates.get(channelId)
    assert(state, `forwardProxiedClientFrame called for unknown proxy channel "${channelId}"`)
    return this.forwardClientFrameToHome(state.homeInstance, channelId, frame)
  }

  private detachProxyChannel(channelId: string, reason: DetachReason): void {
    const state = this.proxyStates.get(channelId)
    if (!state) return
    this.proxyStates.delete(channelId)
    void this.substrate.forward(state.homeInstance, {
      channelId,
      fromInstance: this.substrate.selfInstanceId,
      direction: PROXY_DIRECTION.TO_HOME,
      payload: { kind: ENVELOPE_KIND.DETACH, reason },
    })
  }

  private writePeerFrame(channelId: string, frame: Uint8Array): void {
    const state = this.proxyStates.get(channelId)
    if (!state) {
      return // proxy connection has gone away; envelope is stale
    }
    void state.writeFrame(frame as Uint8Array<ArrayBuffer>)
  }

  private attachHome(channelId: string, proxyInstance: string, payload: ProxyAttachPayload): void {
    const channel = this.findLocal(channelId)
    if (!channel) {
      return // channel shut down between proxy lookup and envelope arrival
    }
    const replay = channel._replayBuffer
    assert(replay !== null, `Substrate attachHome on unregistered channel "${channel.id}"`)

    // Replay frames the client missed. They land on the wire ahead of `ReconciledPayload`
    // because the proxy's per-connection sendChain serialises by enqueue order.
    let replayCount = 0
    for (const frame of replay.getAfter(payload.lastSeq)) {
      replayCount++
      void this.substrate.forward(proxyInstance, {
        channelId: channel.id,
        fromInstance: this.substrate.selfInstanceId,
        direction: PROXY_DIRECTION.TO_PEER,
        payload: { kind: ENVELOPE_KIND.FRAME, frame },
      })
    }

    void this.substrate.forward(proxyInstance, {
      channelId: channel.id,
      fromInstance: this.substrate.selfInstanceId,
      direction: PROXY_DIRECTION.TO_PEER,
      payload: { kind: ENVELOPE_KIND.ATTACH_ACK, lastClientSeq: channel._lastClientSeq },
    })

    const sender: PeerSender = {
      send: (frame, onCommit) => {
        // Commit-on-send: matches the local connection's semantics. The frame is "sent"
        // the moment the substrate accepts the envelope.
        onCommit?.()
        return this.substrate.forward(proxyInstance, {
          channelId: channel.id,
          fromInstance: this.substrate.selfInstanceId,
          direction: PROXY_DIRECTION.TO_PEER,
          payload: { kind: ENVELOPE_KIND.FRAME, frame },
        })
      },
    }
    this.homeAttached.set(channel.id, proxyInstance)
    channel._attachPeer(new IndexedPeer(sender, payload.ix, replay))
  }

  private detachHome(channelId: string, fromInstance: string, reason: DetachReason): void {
    if (this.homeAttached.get(channelId) !== fromInstance) {
      return
    }
    this.homeAttached.delete(channelId)
    const channel = this.findLocal(channelId)
    if (!channel) return
    this.dispatchPeerDetach(channel, reason)
  }

  /** Inbound client→server FRAMEs are dispatched as long as the channel exists locally —
   *  any cluster instance is a valid forwarder for client frames since data POSTs round-robin
   *  freely across instances. The `homeAttached` map is only consulted for outbound TO_PEER
   *  routing (server→client direction); inbound dedup/ordering is enforced by the channel's
   *  own seq logic. */
  private dispatchHomeFrame(channelId: string, rawFrame: Uint8Array): void {
    const channel = this.findLocal(channelId)
    if (!channel) return
    channel._dispatchFrame(decode(rawFrame as Uint8Array<ArrayBuffer>) as ChannelFrame)
  }
}

/** Drains replay frames missed since `lastSeq` then attaches an `IndexedPeer`. Returns null
 *  if the channel shut down during the drain. */
async function attachLocalChannel(
  channel: ServerChannel,
  args: { ix: number; lastSeq: number; send: SendFn },
): Promise<ChannelHandle | null> {
  const replay = channel._replayBuffer
  assert(replay !== null, `ServerChannel "${channel.id}" attached without a replay buffer`)
  for (const frame of replay.getAfter(args.lastSeq)) {
    const pending = args.send(frame as Uint8Array<ArrayBuffer>)
    if (pending) await pending
  }
  if (channel._didShutdown) return null
  const sender: PeerSender = { send: args.send }
  channel._attachPeer(new IndexedPeer(sender, args.ix, replay))
  return { kind: 'local', channel, ix: args.ix }
}
