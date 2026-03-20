export { ServerConnection, ProtocolViolationError }
export type { ServerTransport }

import { unrefTimer } from '../../utils/unrefTimer.js'
import { getGlobalObject } from '../../utils/getGlobalObject.js'
import { getServerConfig } from '../../node/server/serverConfig.js'
import type { ServerChannel } from './channel.js'
import { getChannelRegistry, onChannelCreated, setChannelDefaults } from './channel.js'
import { ReplayBuffer } from '../replay-buffer.js'
import { IndexedPeer } from './IndexedPeer.js'
import type { PeerSender } from './IndexedPeer.js'
import { TAG, decode, encodeCtrl } from '../shared-ws.js'
import type { CtrlMessage, CtrlReconcile } from '../shared-ws.js'
import { CHANNEL_PING_INTERVAL_MIN_MS, type ChannelTransports } from '../constants.js'

type DecodedFrame = ReturnType<typeof decode>

type MuxServerOptions = {
  reconnectTimeout: number
  idleTimeout: number
  pingInterval: number
  pingDeadline: number
  serverReplayBuffer: number
  clientReplayBuffer: number
  connectTtl: number
  bufferLimit: number
  sseFlushThrottle: number
  ssePostIdleFlushDelay: number
  replayMaxAge: number
  transports: ChannelTransports
}

type ChannelEntry = { channel: ServerChannel; lastClientSeq: number; replay: ReplayBuffer }
type SessionState = {
  ixMap: Map<number, ChannelEntry>
  /** Waits until the currently attached transport has fully drained its send chain. */
  drainActiveTransport: (() => Promise<void>) | null
  /** Sends a fin ctrl frame on the current transport. Set at end of each reconcile. */
  sendFin: (() => void | Promise<void>) | null
}
type ReconcileResult = { sessionId: string; finalizeUpgrade?: () => void }

const globalObject = getGlobalObject('wire-protocol/server/connection.ts', {
  sessionStates: new Map<string, SessionState>(),
  transportChannels: new Map<string, ChannelEntry>(),
})

class ProtocolViolationError extends Error {}

function resolveMuxServerOptions(): MuxServerOptions {
  const channelConfig = getServerConfig().channel
  const reconnectTimeout = channelConfig.reconnectTimeout
  const idleTimeout = channelConfig.idleTimeout
  const pingInterval = Math.max(channelConfig.pingInterval, CHANNEL_PING_INTERVAL_MIN_MS)
  const pingDeadline = pingInterval * 2
  const serverReplayBuffer = channelConfig.serverReplayBuffer
  const clientReplayBuffer = channelConfig.clientReplayBuffer
  const connectTtl = channelConfig.connectTtl
  const bufferLimit = channelConfig.bufferLimit
  const sseFlushThrottle = channelConfig.sseFlushThrottle
  const ssePostIdleFlushDelay = channelConfig.ssePostIdleFlushDelay
  const transports = channelConfig.transports
  return {
    reconnectTimeout,
    idleTimeout,
    pingInterval,
    pingDeadline,
    serverReplayBuffer,
    clientReplayBuffer,
    connectTtl,
    bufferLimit,
    sseFlushThrottle,
    ssePostIdleFlushDelay,
    replayMaxAge: pingDeadline + reconnectTimeout + 1_000,
    transports,
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
  private readonly options: MuxServerOptions
  private readonly transport: ServerTransport<TConnection>
  private readonly sessionStates = globalObject.sessionStates
  private readonly transportChannels = globalObject.transportChannels
  private readonly connectionStates = new Map<TConnection, ConnectionState>()

  constructor(transport: ServerTransport<TConnection>) {
    this.transport = transport
    const resolvedOptions = resolveMuxServerOptions()
    this.options = resolvedOptions
    setChannelDefaults({
      connectTtl: resolvedOptions.connectTtl,
      bufferLimit: resolvedOptions.bufferLimit,
    })
  }

  onConnectionOpen(connection: TConnection): void {
    this.getOrCreateConnectionState(connection)
    this.resetPingTimer(connection)
  }

  async onConnectionRawMessage(connection: TConnection, rawFrame: Uint8Array<ArrayBuffer>): Promise<void> {
    const state = this.getOrCreateConnectionState(connection)
    try {
      const pending = this.handleFrame(connection, decode(rawFrame), false)
      if (pending) await pending
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
      const result = this.handleFrame(connection, decode(rawFrame), true)
      if (result) return await result
      return result
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

  /** Connection-scoped outbound send gate.
   *
   *  Every server-to-client frame for one transport connection flows through here, whether it
   *  originates from mux control handling (`reconcile` replay, `reconciled`, `pong`) or from a
   *  `ServerChannel` via `IndexedPeer` and the `PeerSender` closure created during reconcile.
   *
   *  Its job is to preserve strict wire order across the whole connection while still keeping an
   *  idle fast path cheap:
   *  - if no prior send is in flight, it calls `_sendNow()` immediately;
   *  - if `_sendNow()` completes synchronously, this method returns `void` and the connection
   *    stays idle;
   *  - if `_sendNow()` returns a promise, that promise becomes the connection's active send chain;
   *  - while a send chain exists, later sends are appended behind it and therefore cannot overtake
   *    earlier frames on the wire.
   *
   *  This method knows nothing about per-channel pause/disconnect state; that is handled earlier
   *  by `ServerChannel._sendBinaryAwaitable()`. That is also where ws-side flow control is applied:
   *  client pause/resume signals toggle per-channel sendability before a frame reaches this layer.
   *
   *  `_sendNow()` is the transport-specific leaf:
   *  - ws sends synchronously once the channel has already been deemed sendable;
   *  - sse may return a promise here when stream backpressure requires waiting.
   */
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
    if (!sessionId) return
    const sessionState = this.sessionStates.get(sessionId)
    if (!sessionState) return

    sessionState.drainActiveTransport = null
    sessionState.sendFin = null

    if (permanent) {
      for (const entry of sessionState.ixMap.values()) {
        if (!entry.channel._didShutdown) entry.channel._onPeerClose()
      }
      sessionState.ixMap.clear()
      this.sessionStates.delete(sessionId)
      return
    }

    for (const entry of sessionState.ixMap.values()) {
      entry.channel._onPeerDisconnect(this.options.reconnectTimeout)
    }
  }

  private async handleCtrl(
    connection: TConnection,
    ctrl: CtrlMessage,
    deferReconciled: boolean,
  ): Promise<string | null> {
    if (ctrl.t === 'reconcile') {
      const { sessionId, finalizeUpgrade } = await this.reconcile(connection, ctrl)
      if (!deferReconciled) {
        this.sendReconciled(connection, sessionId)
        finalizeUpgrade?.()
        return null
      }
      return sessionId
    }
    if (ctrl.t === 'ping') {
      this.resetPingTimer(connection)
      this.send(connection, encodeCtrl({ t: 'pong' }))
      return null
    }
    const sessionState = this.getSessionStateOrThrow(this.transport.getSessionId(connection))
    switch (ctrl.t) {
      case 'close': {
        const entry = sessionState.ixMap.get(ctrl.ix)
        if (!entry) return null
        entry.channel._onPeerCloseRequest(ctrl.timeoutMs)
        return null
      }
      case 'close-ack': {
        const entry = sessionState.ixMap.get(ctrl.ix)
        if (!entry) return null
        entry.channel._onPeerCloseAck()
        return null
      }
      case 'pause': {
        const entry = sessionState.ixMap.get(ctrl.ix)
        if (entry) entry.channel._onPeerPause()
        return null
      }
      case 'resume': {
        const entry = sessionState.ixMap.get(ctrl.ix)
        if (entry) entry.channel._onPeerResume()
        return null
      }
    }
    return null
  }

  private async reconcile(connection: TConnection, ctrl: CtrlReconcile): Promise<ReconcileResult> {
    const state = this.getOrCreateConnectionState(connection)
    const prevSessionId = ctrl.sessionId
    const sessionState = prevSessionId
      ? this.getSessionStateOrThrow(prevSessionId)
      : { ixMap: new Map<number, ChannelEntry>(), drainActiveTransport: null, sendFin: null }
    state.reconciling = true
    this.resetPingTimer(connection)
    const registry = getChannelRegistry()
    const reconciledIxs = new Set<number>()
    const prevIxMap = new Map(sessionState.ixMap)
    sessionState.ixMap.clear()
    let finalizeUpgrade: (() => void) | undefined

    if (ctrl.upgrade) {
      const drainPreviousTransport = sessionState.drainActiveTransport
      const sendPreviousFin = sessionState.sendFin
      sessionState.drainActiveTransport = null
      sessionState.sendFin = null
      finalizeUpgrade = () => {
        void (async () => {
          if (drainPreviousTransport) await drainPreviousTransport()
          const finPending = sendPreviousFin?.()
          if (finPending) await finPending
        })()
      }
    }

    await Promise.all(
      ctrl.open
        .filter(({ id, defer }) => defer && (!registry.get(id) || registry.get(id)!._didShutdown))
        .map(
          ({ id }) =>
            new Promise<void>((resolve) => {
              onChannelCreated(id, resolve)
              setTimeout(resolve, this.options.connectTtl)
            }),
        ),
    )

    const sender: PeerSender = {
      send: (frame, onCommit) => this.send(connection, frame as Uint8Array<ArrayBuffer>, onCommit),
    }

    for (const { id, ix, lastSeq } of ctrl.open) {
      reconciledIxs.add(ix)
      const channel = registry.get(id)
      if (!channel || channel._didShutdown) continue

      const {
        replay = new ReplayBuffer(this.options.serverReplayBuffer, this.options.replayMaxAge),
        lastClientSeq = 0,
      } = this.transportChannels.get(id) ?? {}
      const entry: ChannelEntry = { channel, lastClientSeq, replay }

      for (const frame of replay.getAfter(lastSeq)) {
        const pending = this.send(connection, frame as Uint8Array<ArrayBuffer>)
        if (pending) await pending
      }

      sessionState.ixMap.set(ix, entry)
      this.transportChannels.set(id, entry)
      channel._onShutdown(() => {
        this.transportChannels.delete(id)
        replay.dispose()
      })

      channel.attachPeer(new IndexedPeer(sender, ix, replay))
    }

    for (const [ix, entry] of prevIxMap) {
      if (reconciledIxs.has(ix)) continue
      if (!entry.channel._didShutdown) entry.channel._onPeerRecoveryFailure()
    }

    const sessionId = crypto.randomUUID()
    if (prevSessionId) this.sessionStates.delete(prevSessionId)
    this.sessionStates.set(sessionId, sessionState)
    this.transport.setSessionId(connection, sessionId)
    sessionState.drainActiveTransport = async () => {
      const pending = state.sendChain
      if (pending) await pending
    }

    sessionState.sendFin = () => this.send(connection, encodeCtrl({ t: 'fin' }))
    state.reconciling = false
    this.resetPingTimer(connection)
    return { sessionId, finalizeUpgrade }
  }

  sendReconciled(connection: TConnection, sessionId: string): void | Promise<void> {
    const sessionState = this.getSessionStateOrThrow(sessionId)
    const open: CtrlReconcile['open'] = []
    for (const [ix, entry] of sessionState.ixMap) {
      open.push({ id: entry.channel.id, ix, lastSeq: entry.lastClientSeq })
    }
    return this.send(
      connection,
      encodeCtrl({
        t: 'reconciled',
        sessionId,
        open,
        reconnectTimeout: this.options.reconnectTimeout,
        idleTimeout: this.options.idleTimeout,
        pingInterval: this.options.pingInterval,
        clientReplayBuffer: this.options.clientReplayBuffer,
        sseFlushThrottle: this.options.sseFlushThrottle,
        ssePostIdleFlushDelay: this.options.ssePostIdleFlushDelay,
        transports: this.options.transports,
      }),
    )
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

  private handleFrame(
    connection: TConnection,
    frame: DecodedFrame,
    deferReconciled: boolean,
  ): null | Promise<string | null> {
    switch (frame.tag) {
      case TAG.CTRL:
        return this.handleCtrl(connection, frame.ctrl, deferReconciled)
      case TAG.TEXT: {
        const entry = this.getSessionStateOrThrow(this.transport.getSessionId(connection)).ixMap.get(frame.index)
        if (!entry) return null
        if (frame.seq && frame.seq <= entry.lastClientSeq) return null
        if (frame.seq) entry.lastClientSeq = frame.seq
        entry.channel._onPeerMessage(frame.text)
        return null
      }
      case TAG.TEXT_ACK_REQ: {
        const entry = this.getSessionStateOrThrow(this.transport.getSessionId(connection)).ixMap.get(frame.index)
        if (!entry) return null
        if (frame.seq && frame.seq <= entry.lastClientSeq) return null
        if (frame.seq) entry.lastClientSeq = frame.seq
        entry.channel._onPeerAckReqMessage(frame.text, frame.seq)
        return null
      }
      case TAG.BINARY: {
        const entry = this.getSessionStateOrThrow(this.transport.getSessionId(connection)).ixMap.get(frame.index)
        if (!entry) return null
        if (frame.seq && frame.seq <= entry.lastClientSeq) return null
        if (frame.seq) entry.lastClientSeq = frame.seq
        entry.channel._onPeerBinaryMessage(frame.data)
        return null
      }
      case TAG.ACK_RES: {
        const entry = this.getSessionStateOrThrow(this.transport.getSessionId(connection)).ixMap.get(frame.index)
        if (!entry) return null
        entry.channel._onPeerAckRes(frame.ackedSeq, frame.text, frame.status)
        return null
      }
    }
  }
}
