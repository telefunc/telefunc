export { ServerConnection, ProtocolViolationError }
export type { ServerTransport }

import { unrefTimer } from '../../utils/unrefTimer.js'
import { getGlobalObject } from '../../utils/getGlobalObject.js'
import { getServerConfig } from '../../node/server/serverConfig.js'
import { setChannelDefaults } from './channel.js'
import { getChannelMux, type ChannelMux, type SendFn } from './substrate.js'
import { TAG, decode, encodeCtrl } from '../shared-ws.js'
import type { CtrlReconcile } from '../shared-ws.js'
import { CHANNEL_PING_INTERVAL_MIN_MS, type ChannelTransports } from '../constants.js'

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

/** Returned by `handleFrame`/`handleCtrl` only when a reconcile completed: lets the caller
 *  branch on object identity (`if (result)`) rather than a magic `string | null` value. */
type ReconcileOutcome = { sessionId: string }

type SessionState = {
  /** `CtrlReconciled.open[]` snapshot returned by the most recent `reconcileSession`. */
  openList: CtrlReconcile['open']
  /** Waits until the currently attached transport has fully drained its send chain. */
  drainActiveTransport: (() => Promise<void>) | null
  /** Sends a fin ctrl frame on the current transport. Set at end of each reconcile. */
  sendFin: (() => void | Promise<void>) | null
  /** Set when this reconcile carries an `upgrade`: drains the previous transport and sends
   *  fin on it. Fired by `sendReconciled` once the new transport has emitted reconciled. */
  finalizeUpgrade: (() => void) | null
}

const globalObject = getGlobalObject('wire-protocol/server/connection.ts', {
  sessionStates: new Map<string, SessionState>(),
})

class ProtocolViolationError extends Error {}

function resolveMuxServerOptions(): MuxServerOptions {
  const c = getServerConfig().channel
  const pingInterval = Math.max(c.pingInterval, CHANNEL_PING_INTERVAL_MIN_MS)
  const pingDeadline = pingInterval * 2
  return {
    reconnectTimeout: c.reconnectTimeout,
    idleTimeout: c.idleTimeout,
    pingInterval,
    pingDeadline,
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

type ConnectionState = {
  pingTimer: ReturnType<typeof setTimeout> | null
  terminatePermanently: boolean | null
  reconciling: boolean
  sendChain: Promise<void> | null
}

type ServerTransport<TConnection> = {
  getSessionId(connection: TConnection): string | undefined
  setSessionId(connection: TConnection, sessionId: string): void
  sendNow(connection: TConnection, frame: Uint8Array<ArrayBuffer>): void | Promise<void>
  terminateConnection(connection: TConnection): void
}

class ServerConnection<TConnection> {
  readonly connectTtl: number
  private readonly options: MuxServerOptions
  private readonly transport: ServerTransport<TConnection>
  private readonly sessionStates = globalObject.sessionStates
  private readonly connectionStates = new Map<TConnection, ConnectionState>()
  /** `installChannelSubstrate` is boot-time only (asserts no channels exist), so caching the
   *  mux once per connection is safe and avoids re-resolving the global on every frame. */
  private readonly mux: ChannelMux = getChannelMux()

  constructor(transport: ServerTransport<TConnection>) {
    this.transport = transport
    const resolvedOptions = resolveMuxServerOptions()
    this.options = resolvedOptions
    this.connectTtl = resolvedOptions.connectTtl
    setChannelDefaults({
      connectTtl: resolvedOptions.connectTtl,
      bufferLimit: resolvedOptions.bufferLimit,
      bufferLimitBinary: resolvedOptions.bufferLimitBinary,
    })
  }

  onConnectionOpen(connection: TConnection): void {
    this.getOrCreateConnectionState(connection)
    this.resetPingTimer(connection)
  }

  async onConnectionRawMessage(connection: TConnection, rawFrame: Uint8Array<ArrayBuffer>): Promise<void> {
    const state = this.getOrCreateConnectionState(connection)
    try {
      const pending = this.handleFrame(connection, rawFrame)
      if (!pending) return
      // If the frame was a `reconcile`, `handleFrame` resolves to the new sessionId — emit
      // `reconciled` immediately. SSE takes the deferred path and emits later (after
      // draining concurrent data POSTs); see `onConnectionRawMessageDeferredReconciled`.
      const outcome = await pending
      if (outcome) this.sendReconciled(connection, outcome.sessionId)
    } catch {
      state.terminatePermanently = true
      this.transport.terminateConnection(connection)
    }
  }

  async onConnectionRawMessageDeferredReconciled(
    connection: TConnection,
    rawFrame: Uint8Array<ArrayBuffer>,
  ): Promise<string | null> {
    const state = this.getOrCreateConnectionState(connection)
    try {
      const pending = this.handleFrame(connection, rawFrame)
      if (!pending) return null
      return (await pending)?.sessionId ?? null
    } catch {
      state.terminatePermanently = true
      this.transport.terminateConnection(connection)
      return null
    }
  }

  onConnectionClosed(connection: TConnection, isPermanent: boolean): void {
    this.clearPingTimer(connection)
    this.connectionStates.delete(connection)
    this.handleConnectionClose(connection, isPermanent)
  }

  consumePermanentTermination(connection: TConnection): boolean | null {
    return this.getOrCreateConnectionState(connection).terminatePermanently
  }

  /** Connection-scoped outbound send gate. Preserves wire order across the whole
   *  connection: every server→client frame for one transport flows through here, whether
   *  it originates from mux control handling (`reconcile` replay, `reconciled`, `pong`) or
   *  from a `ServerChannel` via the `SendFn` closure passed into `reconcileSession`.
   *
   *  - if no prior send is in flight, calls `_sendNow()` immediately;
   *  - if `_sendNow()` returns a promise, that promise becomes the connection's active send chain;
   *  - while a send chain exists, later sends append behind it and cannot overtake earlier frames. */
  protected send(connection: TConnection, frame: Uint8Array<ArrayBuffer>, onCommit?: () => void): void | Promise<void> {
    const state = this.getOrCreateConnectionState(connection)
    if (!state.sendChain) {
      onCommit?.()
      const pending = this.transport.sendNow(connection, frame)
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
        return this.transport.sendNow(connection, frame)
      })
      .finally(() => {
        if (state.sendChain === chain) state.sendChain = null
      })
    state.sendChain = chain
    return chain
  }

  handleConnectionClose(connection: TConnection, permanent: boolean): void {
    const sessionId = this.transport.getSessionId(connection)
    // No sessionId: the connection closed before reconciling (e.g., handshake error or immediate
    // disconnect). No session state to clean up.
    if (!sessionId) return
    // No sessionState: a previous permanent close already deleted it (e.g., this transport was
    // bound to a sessionId that's since been replaced or finalized by another close).
    const sessionState = this.sessionStates.get(sessionId)
    if (!sessionState) return

    sessionState.drainActiveTransport = null
    sessionState.sendFin = null

    this.mux.detachSession(sessionId, permanent ? 'permanent' : 'transient')
    if (permanent) this.sessionStates.delete(sessionId)
  }

  private async reconcile(connection: TConnection, ctrl: CtrlReconcile): Promise<ReconcileOutcome> {
    const state = this.getOrCreateConnectionState(connection)
    const sessionState = this.resumeOrCreateSessionState(ctrl.sessionId)
    state.reconciling = true
    this.resetPingTimer(connection)
    // Snapshot the previous transport's drain/fin handlers BEFORE we overwrite them with
    // the new transport's handlers below — `prepareUpgradeFinalizer` reads from sessionState.
    const finalizeUpgrade = ctrl.upgrade ? this.prepareUpgradeFinalizer(sessionState) : null
    const send: SendFn = (frame, onCommit) => this.send(connection, frame, onCommit)
    const newSessionId = crypto.randomUUID()

    // The mux attaches every channel in `ctrl.open` (parallel home-lookups + parallel
    // local replay drains and proxy attach-acks), detaches anything from the previous
    // session that the client did NOT re-include, and returns the open list for CtrlReconciled.
    sessionState.openList = await this.mux.reconcileSession({
      prevSessionId: ctrl.sessionId,
      newSessionId,
      open: ctrl.open,
      send,
    })

    // The connection may have closed during the await
    if (!this.connectionStates.has(connection)) {
      this.mux.detachSession(newSessionId, 'permanent')
      throw new ProtocolViolationError()
    }

    if (ctrl.sessionId) this.sessionStates.delete(ctrl.sessionId)
    this.sessionStates.set(newSessionId, sessionState)
    this.transport.setSessionId(connection, newSessionId)
    sessionState.drainActiveTransport = async () => {
      const pending = state.sendChain
      if (pending) await pending
    }
    sessionState.sendFin = () => this.send(connection, encodeCtrl({ t: 'fin' }))
    sessionState.finalizeUpgrade = finalizeUpgrade

    state.reconciling = false
    this.resetPingTimer(connection)
    return { sessionId: newSessionId }
  }

  /** Resume the session attached to a previous reconnect (when `ctrl.sessionId` is given)
   *  or create a fresh `SessionState` for a first-time reconcile. The mux owns the per-channel
   *  state; this side just carries the open-list snapshot and transport drain handlers. */
  private resumeOrCreateSessionState(prevSessionId: string | undefined): SessionState {
    if (prevSessionId) return this.getSessionStateOrThrow(prevSessionId)
    return { openList: [], drainActiveTransport: null, sendFin: null, finalizeUpgrade: null }
  }

  /** Snapshot the previous transport's drain + fin handlers, then return a finalizer the
   *  caller invokes after the new transport has emitted CtrlReconciled. The finalizer drains
   *  the old transport and sends its `fin` so the client can fully release it. */
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

  sendReconciled(connection: TConnection, sessionId: string): void | Promise<void> {
    const sessionState = this.getSessionStateOrThrow(sessionId)
    const pending = this.send(
      connection,
      encodeCtrl({
        t: 'reconciled',
        sessionId,
        open: sessionState.openList,
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

  private getSessionStateOrThrow(sessionId: string | undefined): SessionState {
    if (!sessionId) throw new ProtocolViolationError()
    const sessionState = this.sessionStates.get(sessionId)
    if (!sessionState) throw new ProtocolViolationError()
    return sessionState
  }

  private getOrCreateConnectionState(connection: TConnection): ConnectionState {
    let state = this.connectionStates.get(connection)
    if (!state) {
      state = { pingTimer: null, terminatePermanently: null, reconciling: false, sendChain: null }
      this.connectionStates.set(connection, state)
    }
    return state
  }

  private clearPingTimer(connection: TConnection): void {
    const state = this.connectionStates.get(connection)
    if (!state?.pingTimer) return
    clearTimeout(state.pingTimer)
    state.pingTimer = null
  }

  private resetPingTimer(connection: TConnection): void {
    const state = this.getOrCreateConnectionState(connection)
    this.clearPingTimer(connection)
    state.pingTimer = unrefTimer(
      setTimeout(() => {
        state.pingTimer = null
        if (state.reconciling) return
        this.transport.terminateConnection(connection)
        state.terminatePermanently = false
      }, this.options.pingDeadline),
    )
  }

  /** Single dispatch for an inbound wire frame. Data frames go to the mux for per-ix routing;
   *  ctrl frames branch by `t`. Returns a `ReconcileOutcome` only on reconcile (so the caller
   *  can decide when to send `reconciled`); null in every other case.
   *
   *  Any frame other than `reconcile` or `ping` arriving before this connection has reconciled
   *  is a protocol violation — the spec says the first frame must be `reconcile`. We throw
   *  `ProtocolViolationError`; the caller's `catch` terminates the connection. */
  private handleFrame(
    connection: TConnection,
    rawFrame: Uint8Array<ArrayBuffer>,
  ): null | Promise<ReconcileOutcome | null> {
    const frame = decode(rawFrame)

    // Pre-session ctrls: only `reconcile` (creates the session) and `ping` (heartbeat) are
    // legal before reconcile. Handle them first so the session-existence check below can
    // be unconditional for everything that follows.
    if (frame.tag === TAG.CTRL && frame.ctrl.t === 'reconcile') return this.reconcile(connection, frame.ctrl)
    if (frame.tag === TAG.CTRL && frame.ctrl.t === 'ping') {
      this.resetPingTimer(connection)
      this.send(connection, encodeCtrl({ t: 'pong' }))
      return null
    }

    // Everything else requires an established session.
    const sessionId = this.transport.getSessionId(connection)
    if (!sessionId) throw new ProtocolViolationError()

    if (frame.tag !== TAG.CTRL) {
      this.mux.handleClientFrame(sessionId, rawFrame)
      return null
    }

    // `pong`/`fin`/`reconciled` are server→client only; a client sending one is a protocol
    // violation. Everything else carries `ix` and goes to the mux for per-channel routing.
    if (!('ix' in frame.ctrl)) throw new ProtocolViolationError()
    this.mux.handleClientCtrl(sessionId, frame.ctrl)
    return null
  }
}
