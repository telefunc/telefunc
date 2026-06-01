export { ChannelMux }
export type { ReconcileOutcome, SendFn, ServerTransport, ChannelHandle, ConnectionEntry, MuxServerOptions }

import { assert } from '../../../utils/assert.js'
import { getGlobalObject } from '../../../utils/getGlobalObject.js'
import { getServerConfig } from '../../../node/server/serverConfig.js'
import { unrefTimer } from '../../../utils/unrefTimer.js'
import { CHANNEL_PING_INTERVAL_MIN_MS, type ChannelTransports } from '../../constants.js'
import { TAG, decode, encode, isConnCtrlTag } from '../../shared-ws.js'
import type { ChannelFrame, ReconcilePayload, ReconciledPayload } from '../../shared-ws.js'
import { IndexedPeer, type PeerSender } from '../IndexedPeer.js'
import { setChannelDefaults, type ServerChannel } from '../channel.js'
import { DETACH_REASON, type DetachReason } from './protocol.js'

// ── Public types ─────────────────────────────────────────────────────────

/** Invoking preserves wire order via the connection's serialised send chain. */
type SendFn = (frame: Uint8Array<ArrayBuffer>, onCommit?: () => void) => void | Promise<void>

/** Returned by `handleFrame` only when a reconcile completed. The caller threads the
 *  payload through to `sendReconciled` and fires `finalizeUpgrade` once the new wire's
 *  reconciled has emitted. `finalizeUpgrade` is null when this isn't an upgrade. */
type ReconcileOutcome = {
  sessionId: string
  openList: ReconciledPayload['open']
  finalizeUpgrade: (() => void) | null
}

type ServerTransport<TConnection> = {
  getSessionId(connection: TConnection): string | undefined
  setSessionId(connection: TConnection, sessionId: string): void
  /** Stable per-connection identifier the cluster uses to route data POSTs back to the
   *  owner instance. Returns `null` for transports that don't multiplex client→server
   *  traffic across requests (e.g. WebSocket — every frame already lands on the owner). */
  getConnId(connection: TConnection): string | null
  sendNow(connection: TConnection, frame: Uint8Array<ArrayBuffer>): void
  terminateConnection(connection: TConnection): void
}

// ── Internal types ───────────────────────────────────────────────────────

/** A session slot pointing at a channel that may live locally or, in the substrate-extended
 *  subclass, on a remote instance. Single-instance mux only ever creates `'local'`. */
type ChannelHandle =
  | { kind: 'local'; channel: ServerChannel; ix: number }
  | { kind: 'remote'; channelId: string; homeInstance: string; ix: number; lastClientSeq: number }

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

/** Per-session closure that drains the currently-attached transport's send chain, queues
 *  `fin` on it, and drains again. Captured by the next reconcile (when `ctrl.upgrade`) to
 *  retire the previous transport. */
type SessionFinalizer = () => void

type ConnectionState = {
  pingTimer: ReturnType<typeof setTimeout> | null
  terminatePermanently: boolean | null
  reconciling: boolean
  recvChain: Promise<unknown> | null
}

/** The transport back-reference keeps the mux non-generic over `TConnection` while
 *  still letting it reach `sendNow`/`terminate` on heterogeneous transports (SSE + WS). */
type ConnectionEntry = {
  state: ConnectionState
  transport: ServerTransport<unknown>
}

class ProtocolViolationError extends Error {}

const globalObject = getGlobalObject('wire-protocol/server/substrate/mux.ts', {
  sessionFinalizers: new Map<string, SessionFinalizer>(),
})

// ── Helpers ──────────────────────────────────────────────────────────────

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
 *  Mutations are atomic across both. Remote handles aren't reverse-indexed. */
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

// Single-instance kernel: owns channels, sessions, connection runtime. Transports talk
// to this class via `onConnectionOpen` and from then on identify connections by object
// identity. `SubstrateMux` subclass adds cross-instance routing.

class ChannelMux {
  protected readonly localChannels = new Map<string, ServerChannel>()
  /** Attach waiters fired synchronously by `registerChannel` so a deferred-attach race
   *  resolves locally without going through any external lookup. */
  protected readonly localWaiters = new Map<string, Set<(channel: ServerChannel) => void>>()
  protected readonly sessions = new SessionRegistry()
  protected readonly sessionFinalizers = globalObject.sessionFinalizers
  protected readonly connectionStates = new Map<unknown, ConnectionEntry>()
  /** Reverse map keyed by `connId`, populated only for transports that report a stable
   *  connId (SSE). Used for duplicate-connection detection and, in `SubstrateMux`, for
   *  routing cross-instance `CONNECTION_FRAME` envelopes to local connections. */
  protected readonly connectionsByConnId = new Map<string, unknown>()

  /** Options are resolved lazily on first use so the mux can be constructed at module-load
   *  (install.ts's globalObject) before `serverConfig` has been initialized. */
  private resolvedOptions: MuxServerOptions | null = null

  /** This server's cluster-instance identifier. Stamped into every `reconciled`'s
   *  `ownerInstance` and every channel's `home`. Base lazily picks a per-process UUID
   *  on first read (deferring `crypto.randomUUID()` past module load — Cloudflare
   *  Workers disallow it in global scope). `SubstrateMux` injects the substrate's
   *  cluster-wide id via the constructor. */
  private _selfInstanceId: string | null
  get selfInstanceId(): string {
    if (this._selfInstanceId === null) this._selfInstanceId = crypto.randomUUID()
    return this._selfInstanceId
  }

  constructor(instanceId: string | null = null) {
    this._selfInstanceId = instanceId
  }

  protected get options(): MuxServerOptions {
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

  // ── ServerChannel registry ──────────────────────────────────────────

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
    this.onChannelRegistered(channel.id)
  }

  unregisterChannel(channelId: string): void {
    this.localChannels.delete(channelId)
    this.sessions.removeChannel(channelId)
    this.onChannelUnregistered(channelId)
  }

  hasChannels(): boolean {
    return this.localChannels.size > 0
  }

  // ── Connection lifecycle (transport-facing) ─────────────────────────

  /** Connection identity is the object itself; the stored hooks let later mux methods
   *  reach the right transport without being generic over `TConnection`. */
  onConnectionOpen<TConnection>(connection: TConnection, transport: ServerTransport<TConnection>): void {
    this.connectionStates.set(connection, {
      state: { pingTimer: null, terminatePermanently: null, reconciling: false, recvChain: null },
      transport: transport as ServerTransport<unknown>,
    })
    const connId = transport.getConnId(connection)
    if (connId !== null) this.connectionsByConnId.set(connId, connection)
    this.onConnectionOpened(connection, connId)
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
    this.send(
      connection,
      encode.reconciled({
        sessionId: outcome.sessionId,
        open: outcome.openList,
        ownerInstance: this.selfInstanceId,
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
    // Sends are synchronous; firing the upgrade finalizer here can't reorder
    // anything on the new wire.
    outcome.finalizeUpgrade?.()
  }

  onConnectionClosed(connection: unknown, isPermanent: boolean): void {
    const entry = this.connectionStates.get(connection)
    if (!entry) return
    this.clearPingTimer(entry.state)
    this.connectionStates.delete(connection)
    const connId = entry.transport.getConnId(connection)
    // Identity-equality guards against deleting a *replacement* connection's entry when
    // a duplicate-connId reconnect raced the old wire's teardown.
    if (connId !== null && this.connectionsByConnId.get(connId) === connection) {
      this.connectionsByConnId.delete(connId)
    }
    this.onConnectionClosing(connection, connId)
    this.handleConnectionClose(entry, connection, isPermanent)
  }

  consumePermanentTermination(connection: unknown): boolean | null {
    return this.connectionStates.get(connection)?.state.terminatePermanently ?? null
  }

  /** Looks up a local connection by its transport-stable `connId`. SSE uses this for
   *  duplicate-connection detection (a reconnect with the same connId must close the
   *  stale wire). Returns undefined for transports that don't report a connId (WS). */
  getConnectionByConnId<TConnection>(connId: string): TConnection | undefined {
    return this.connectionsByConnId.get(connId) as TConnection | undefined
  }

  // ── Cross-instance-aware routing (single-instance path in base) ─────
  //
  // SSE's data POST path calls these regardless of cluster topology. In single-instance,
  // `home`/`ownerInstance` always equals `selfInstanceId` (the only instance that ever
  // stamped them), so the base routes to local state. `SubstrateMux` overrides to fan
  // out across instances when the target is remote.

  /** Dispatch a client→server channel frame to its home. Base: home is always self. */
  async routeClientFrame(channelId: string, home: string, rawFrame: Uint8Array<ArrayBuffer>): Promise<void> {
    assert(home === this.selfInstanceId, 'base mux only routes to local channels')
    this.localChannels.get(channelId)?._dispatchFrame(decode(rawFrame) as ChannelFrame)
  }

  /** Push a server→client connection-level frame onto the client's local wire. */
  async sendConnectionFrameToClient(
    ownerInstance: string,
    connId: string,
    frame: Uint8Array<ArrayBuffer>,
  ): Promise<void> {
    assert(ownerInstance === this.selfInstanceId, 'base mux only sends to local connections')
    const connection = this.connectionsByConnId.get(connId)
    if (connection !== undefined) this.send(connection, frame)
  }

  /** Re-inject a client→server alias-0 frame on the connection's owner. In single-instance,
   *  this is only called when the local-connection lookup raced with a close — drop silently. */
  async forwardConnectionFrameToServer(
    ownerInstance: string,
    _connId: string,
    _payload: Uint8Array<ArrayBuffer>,
  ): Promise<void> {
    assert(ownerInstance === this.selfInstanceId, 'base mux cannot forward across instances')
  }

  // ── Per-connection outbound / recv chain / ping ─────────────────────

  /** Sole server→client send path; sync so wire order = call order. Per-channel byte+msg
   *  credit (see `flow-control/`) bounds queue growth. */
  protected send(connection: unknown, frame: Uint8Array<ArrayBuffer>, onCommit?: () => void): void {
    const entry = this.connectionStates.get(connection)
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
    const entry = this.connectionStates.get(connection)
    if (!entry) return
    const { state, transport } = entry
    this.clearPingTimer(state)
    state.pingTimer = unrefTimer(
      setTimeout(() => {
        state.pingTimer = null
        if (state.reconciling) return
        // Close transient so each channel gets its `reconnectTimeout` grace at the home
        // (handled by `_onPeerDisconnect`). Connection-level state is cleaned eagerly in
        // `handleConnectionClose` regardless of permanence — a future reconcile rebuilds
        // it from scratch.
        transport.terminateConnection(connection)
        state.terminatePermanently = false
      }, this.options.pingDeadline),
    )
  }

  private handleConnectionClose(entry: ConnectionEntry, connection: unknown, permanent: boolean): void {
    const sessionId = entry.transport.getSessionId(connection)
    // No sessionId: the connection closed before reconciling (handshake error or
    // immediate disconnect). Nothing to clean up.
    if (!sessionId) return
    // The transient/permanent distinction governs *channel* grace inside `detachSession`:
    // channels survive a transient close via `_onPeerDisconnect`'s reconnectTimeout. The
    // session-level finalizer is dropped on any close — reconcile rebuilds one on next
    // attach. SSE only fires permanent on a protocol violation; every network drop, page
    // close, ping timeout, and stream cancel is transient.
    this.detachSession(sessionId, permanent ? DETACH_REASON.PERMANENT : DETACH_REASON.TRANSIENT)
    this.sessionFinalizers.delete(sessionId)
  }

  // ── Inbound dispatch ────────────────────────────────────────────────

  /** PING bypasses the chain — serialising it would tie liveness to the slowest awaitable
   *  on the connection. */
  private dispatchInbound(connection: unknown, rawFrame: Uint8Array<ArrayBuffer>): Promise<ReconcileOutcome | null> {
    const entry = this.connectionStates.get(connection)
    if (!entry) return Promise.resolve(null)
    const exec = async (): Promise<ReconcileOutcome | null> => {
      try {
        const pending = this.handleFrame(entry, connection, rawFrame)
        if (!pending) return null
        return (await pending) ?? null
      } catch {
        entry.state.terminatePermanently = true
        entry.transport.terminateConnection(connection)
        return null
      }
    }
    if (rawFrame[0] === TAG.PING) return exec()
    return this.chainRecv(entry, exec)
  }

  /** Returns a `ReconcileOutcome` only on reconcile (so the caller decides when to send
   *  `reconciled`). Anything but reconcile/ping before reconciliation is a violation. */
  private handleFrame(
    entry: ConnectionEntry,
    connection: unknown,
    rawFrame: Uint8Array<ArrayBuffer>,
  ): null | Promise<ReconcileOutcome | null> {
    const frame = decode(rawFrame)

    if (frame.tag === TAG.RECONCILE) return this.reconcile(entry, connection, frame.payload)
    if (frame.tag === TAG.PING) {
      this.resetPingTimer(connection)
      this.send(connection, encode.pong())
      return null
    }

    const sessionId = entry.transport.getSessionId(connection)
    if (!sessionId) throw new ProtocolViolationError()
    // PONG/FIN/RECONCILED are server→client only; a client sending one is a protocol violation.
    if (isConnCtrlTag(frame.tag)) throw new ProtocolViolationError()
    this.handleClientFrame(sessionId, frame as ChannelFrame, rawFrame)
    return null
  }

  /** All client frames carry an index (channel ix) post-reconcile — both data tags and
   *  per-channel ctrl tags. Connection-level ctrls are handled by `handleFrame`. */
  handleClientFrame(sessionId: string, frame: ChannelFrame, rawFrame: Uint8Array<ArrayBuffer>): void {
    const h = this.sessions.get(sessionId, frame.index)
    // Race: client closed a channel and the server reconciled it out, but a frame for the
    // dropped ix is still in flight. Indistinguishable from a bogus-ix violation without a
    // per-session history of detached ixs — drop, don't escalate.
    if (!h) return
    if (h.kind === 'local') h.channel._dispatchFrame(frame)
    else this.onProxiedClientFrame(h.channelId, rawFrame)
  }

  // ── Reconcile + per-session attach ──────────────────────────────────

  private async reconcile(
    entry: ConnectionEntry,
    connection: unknown,
    ctrl: ReconcilePayload,
  ): Promise<ReconcileOutcome> {
    const { state, transport } = entry
    const connId = transport.getConnId(connection)
    const finalizeUpgrade = ctrl.upgrade ? await this.buildUpgradeFinalizer(ctrl, connId) : null

    state.reconciling = true
    this.resetPingTimer(connection)
    const send: SendFn = (frame, onCommit) => this.send(connection, frame, onCommit)
    const newSessionId = crypto.randomUUID()

    const openList = await this.reconcileSession({
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

    if (ctrl.sessionId) this.sessionFinalizers.delete(ctrl.sessionId)
    this.sessionFinalizers.set(newSessionId, () => {
      this.send(connection, encode.fin())
    })
    transport.setSessionId(connection, newSessionId)

    state.reconciling = false
    this.resetPingTimer(connection)
    if (connId !== null) this.onSessionRegistered(connId, newSessionId)
    return { sessionId: newSessionId, openList, finalizeUpgrade }
  }

  /** Build the closure that retires the previous wire on `ctrl.upgrade`. Base handles the
   *  local-previous-wire case; `SubstrateMux` overrides to also forward UPGRADE_FINALIZE
   *  when the previous wire lived on another instance. */
  protected async buildUpgradeFinalizer(ctrl: ReconcilePayload, _connId: string | null): Promise<(() => void) | null> {
    if (!ctrl.sessionId) return null
    const local = this.sessionFinalizers.get(ctrl.sessionId)
    return local ? () => void local() : null
  }

  protected async reconcileSession(args: {
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

    return handles.map((h) =>
      h.kind === 'local'
        ? { id: h.channel.id, ix: h.ix, lastSeq: h.channel._lastClientSeq, home: this.selfInstanceId }
        : { id: h.channelId, ix: h.ix, lastSeq: h.lastClientSeq, home: h.homeInstance },
    )
  }

  /** `initial:true` races local-create against `connectTtl`; otherwise fails fast if
   *  absent locally (subclass extends with remote lookup). */
  protected async attach(entry: ReconcilePayload['open'][number], send: SendFn): Promise<ChannelHandle | null> {
    const local = this.localChannels.get(entry.id)
    if (local) return this.attachLocalChannel(local, entry.ix, entry.lastSeq, send)
    if (!entry.initial) return null
    return new Promise<ChannelHandle | null>((resolve) => {
      this.waitForLocalChannel(entry.id, this.options.connectTtl, (channel) => {
        if (!channel) return resolve(null)
        this.attachLocalChannel(channel, entry.ix, entry.lastSeq, send).then(resolve, () => resolve(null))
      })
    })
  }

  /** Drains replay frames missed since `lastSeq`, then attaches an `IndexedPeer`. Returns
   *  null if the channel shut down during the drain. Reused by `SubstrateMux` after a
   *  local-waiter race. */
  protected async attachLocalChannel(
    channel: ServerChannel,
    ix: number,
    lastSeq: number,
    send: SendFn,
  ): Promise<ChannelHandle | null> {
    const replay = channel._replayBuffer
    assert(replay !== null, `ServerChannel "${channel.id}" attached without a replay buffer`)
    for (const frame of replay.getAfter(lastSeq)) {
      const pending = send(frame as Uint8Array<ArrayBuffer>)
      if (pending) await pending
    }
    if (channel._didShutdown) return null
    const sender: PeerSender = { send }
    channel._attachPeer(new IndexedPeer(sender, ix, replay))
    return { kind: 'local', channel, ix }
  }

  /** Race local register against `ttl`; `onResult(channel|null)`. Returned `stop` cancels
   *  when another path resolved first (used by `SubstrateMux`'s local + remote race). */
  protected waitForLocalChannel(
    channelId: string,
    ttlMs: number,
    onResult: (channel: ServerChannel | null) => void,
  ): () => void {
    let settled = false
    let timer: ReturnType<typeof setTimeout>
    const waiterSet = this.localWaiters.get(channelId) ?? new Set()
    this.localWaiters.set(channelId, waiterSet)

    const settle = (channel: ServerChannel | null): void => {
      if (settled) return
      settled = true
      waiterSet.delete(waiter)
      if (waiterSet.size === 0) this.localWaiters.delete(channelId)
      clearTimeout(timer)
      onResult(channel)
    }
    const waiter = (channel: ServerChannel): void => settle(channel)
    waiterSet.add(waiter)
    timer = setTimeout(() => settle(null), ttlMs)
    return () => settle(null)
  }

  /** Transient: leave registry entries so the next reconcile's prev-comparison can fire
   *  `recovery-failed` on abandoned channels. Permanent: remove outright — no resume. */
  detachSession(sessionId: string, reason: DetachReason): void {
    const session =
      reason === DETACH_REASON.PERMANENT ? this.sessions.removeSession(sessionId) : this.sessions.peekSession(sessionId)
    if (!session) return
    for (const handle of session.values()) this.detachHandle(handle, reason)
  }

  /** Routes a handle's wire-loss to the correct channel callback. Base only handles
   *  local; `SubstrateMux` overrides to also handle remote handles. */
  protected detachHandle(h: ChannelHandle, reason: DetachReason): void {
    assert(h.kind === 'local', 'base mux only manages local channel handles')
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

  // ── Lifecycle notification hooks (pure no-ops in base; `SubstrateMux` uses them
  //    to keep cluster pins in sync). Base never has anything to do with these.
  //    The asserting `onProxiedClientFrame` is a should-never-happen guard, not a hook.

  protected onChannelRegistered(_channelId: string): void {}
  protected onChannelUnregistered(_channelId: string): void {}
  protected onConnectionOpened(_connection: unknown, _connId: string | null): void {}
  protected onConnectionClosing(_connection: unknown, _connId: string | null): void {}
  protected onSessionRegistered(_connId: string, _sessionId: string): void {}
  protected onProxiedClientFrame(_channelId: string, _rawFrame: Uint8Array<ArrayBuffer>): void {
    // Only fires when `handleClientFrame` resolves an ix to a 'remote' handle, which the
    // base never creates. Reaching here in base would mean the session registry stored a
    // remote handle without `SubstrateMux` being installed.
    assert(false, 'base mux received a frame for a remote handle')
  }

  // ── Dispose ─────────────────────────────────────────────────────────

  dispose(): void {
    this.localChannels.clear()
    this.localWaiters.clear()
    this.sessions.clear()
    this.connectionStates.clear()
    this.connectionsByConnId.clear()
  }
}
