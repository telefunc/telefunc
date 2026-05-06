export { ClientConnection }
export type { MuxChannel, MuxConnection }

import { parse } from '@brillout/json-serializer/parse'
import { makeAbortError, makeBugError } from '../../client/remoteTelefunctionCall/errors.js'
import { ShieldValidationError } from '../../shared/ShieldValidationError.js'
import { assert } from '../../utils/assert.js'
import { ChannelClosedError, ChannelNetworkError } from '../channel-errors.js'
import {
  CHANNEL_CLIENT_REPLAY_BUFFER_BYTES,
  CHANNEL_CLIENT_REPLAY_BUFFER_BINARY_BYTES,
  CHANNEL_IDLE_TIMEOUT_MS,
  CHANNEL_PING_INTERVAL_MS,
  CHANNEL_RECONNECT_INITIAL_DELAY_MS,
  CHANNEL_RECONNECT_MAX_DELAY_MS,
  CHANNEL_RECONNECT_TIMEOUT_MS,
  SSE_FLUSH_THROTTLE_MS,
  SSE_POST_IDLE_FLUSH_DELAY_MS,
  SSE_RECONCILE_DEADLINE_MS,
  TELEFUNC_SESSION_HEADER,
  WS_PROBE_TIMEOUT_MS,
  STREAM_REQUEST_HANDSHAKE_TIMEOUT_MS,
  type ChannelTransports,
} from '../constants.js'
import { encodeU32, encodeLengthPrefixedFrames, textEncoder } from '../frame.js'
import { ReplayBuffer } from '../replay-buffer.js'
import { REQUEST_KIND, REQUEST_KIND_HEADER, getMarkedRequestUrl } from '../request-kind.js'
import { ClientBroadcast } from './channel.js'
import { encodeSseRequest, METADATA_REFRESH_ALIAS, type SseRouteChannel } from '../sse-request.js'
import { ACK_STATUS, TAG, decode, encode } from '../shared-ws.js'
import type {
  AckResultStatus,
  DecodedFrame,
  ReconcilePayload,
  ReconciledPayload,
  WirePublishInfo,
} from '../shared-ws.js'

type ServerCtrlTag =
  | typeof TAG.PONG
  | typeof TAG.CLOSE
  | typeof TAG.CLOSE_ACK
  | typeof TAG.ABORT
  | typeof TAG.ERROR
  | typeof TAG.WINDOW
  | typeof TAG.FIN
  | typeof TAG.RECONCILED
import { base64urlToUint8Array } from '../base64url.js'
import { DeadlineScheduler } from './deadlineScheduler.js'
import { CHANNEL_TRANSPORT, type ChannelTransport } from '../constants.js'
import { PushToPullStream } from '../push-to-pull-stream.js'

type PendingAck = {
  resolve: (result: unknown) => void
  reject: (err: Error) => void
}

type BufferedFrame = {
  frame: Uint8Array<ArrayBuffer>
  channelIx: number
  seq?: number
}

type OutboundFrameKind = 'reconcile' | 'control' | 'ack' | 'data' | 'heartbeat'

type OutboundFrame = {
  kind: OutboundFrameKind
  frame: Uint8Array<ArrayBuffer>
}

interface MuxChannel {
  readonly id: string
  readonly isClosed: boolean
  _onTransportOpen(): void
  _onTransportMessage(data: string): void
  _onTransportBinaryMessage(data: Uint8Array): void
  _onTransportAckReqMessage(data: string, seq: number): Promise<void>
  _onTransportBinaryAckReqMessage(data: Uint8Array, seq: number): Promise<void>
  _onPeerWindowUpdate(bytes: number): void
  _onTransportCloseRequest(timeoutMs: number): void
  _onTransportCloseAck(): void
  _onTransportClose(err?: Error): void
}

interface MuxConnection {
  send(channel: MuxChannel, data: string): void
  sendPublishAckReq(channel: MuxChannel, data: string): Promise<unknown>
  sendPublishBinaryAckReq(channel: MuxChannel, data: Uint8Array): Promise<unknown>
  sendTextAckReq(channel: MuxChannel, data: string): Promise<unknown>
  sendBinaryAckReq(channel: MuxChannel, data: Uint8Array): Promise<unknown>
  sendBinary(channel: MuxChannel, data: Uint8Array): void
  sendAckRes(channel: MuxChannel, ackedSeq: number, result: string, status?: AckResultStatus): void
  sendAbort(channel: MuxChannel): void
  sendCloseRequest(channel: MuxChannel, timeoutMs: number): void
  sendCloseAck(channel: MuxChannel): void
  sendWindowUpdate(channel: MuxChannel, bytes: number): void
  sendBroadcastSubscribe(channel: MuxChannel, binary: boolean): void
  sendBroadcastUnsubscribe(channel: MuxChannel, binary: boolean): void
  unregister(channel: MuxChannel, err?: Error): void
}

type ReconcileOutcome = {
  frames: OutboundFrame[]
  channelsToOpen: MuxChannel[]
  reconcileComplete: boolean
}

type ReconcileBatch = {
  reconcileFrame: OutboundFrame
  movedBufferedFrames: OutboundFrame[]
}

type ReconcileBufferedFramesMode = 'batch-on-reconcile' | 'release-after-reconciled'

type ClientConnectionOptions = {
  transports: ChannelTransports
  fetchImpl: typeof fetch
  sessionToken?: string
}

type ClientChannelTransport = {
  readonly type: ChannelTransport
  readonly reconnectTimeoutMessage: string
  readonly sendReconcileOnOpen: boolean
  /** Initial reconcile buffered frames mode for this transport. */
  readonly reconcileMode: ReconcileBufferedFramesMode
  /** Cluster-stable wire identifier the server can resolve via `locateConnection` to find
   *  the owning instance. Set on transports that multiplex client→server traffic across
   *  requests (SSE — needed for cross-instance routing); `null` on transports where every
   *  frame already lands on the owner (WS). */
  readonly connId: string | null
  probe(): Promise<(() => void) | null>
  start(): void
  hasActiveTransport(): boolean
  isConnecting(): boolean
  sendFrame(frame: OutboundFrame): void
  abandonActiveTransport(): void
  closeAbandonedTransport(): void
  applyReconciledSettings(ctrl: ReconciledPayload): void
  /** Push any post-reconcile routing metadata onto the transport's persistent client→server
   *  stream. Sibling of `applyReconciledSettings`: settings update internal state, this one
   *  emits the resulting routing table on the wire. No-op for transports that don't carry a
   *  persistent upstream channel (WS — every frame already lands on the owner). */
  pushReconciledRouting(): void
  /** Drain everything in flight on this transport before the upgrade orchestrator switches
   *  to the new one: outbox, in-flight batch POSTs, AND any persistent client→server stream
   *  (closed body → server returns 200). After this resolves the server has finished
   *  processing every frame the client ever pushed on this transport — symmetric to the
   *  server's `TAG.FIN` drain on the downstream. */
  prepareForUpgrade(): Promise<void>
  dispose(): void
}

type SseInitialBatchStage = {
  initialFrames: OutboundFrame[]
  movedOutboxFrames: Uint8Array<ArrayBuffer>[]
  movedOutboxDeadlines: number[]
  movedBufferedFrames: OutboundFrame[]
}

type UpgradeState = {
  active: boolean
  disabled: boolean
  probeAbort: (() => void) | null
  handoffTransport: ClientChannelTransport | null
  handoffBuffer: Uint8Array<ArrayBuffer>[] | null
}

class ClientConnection implements MuxConnection {
  private static cache = new Map<string, ClientConnection>()

  static getOrCreate(telefuncUrl: string, channel: MuxChannel, options: ClientConnectionOptions): ClientConnection {
    const key = `${options.transports.join(',')}:${telefuncUrl}`
    let connection = ClientConnection.cache.get(key)
    if (!connection || connection.closed) {
      connection = new ClientConnection(telefuncUrl, options, key)
      ClientConnection.cache.set(key, connection)
    }
    connection.register(channel)
    return connection
  }

  private readonly cacheKey: string
  private readonly telefuncUrl: string
  private readonly connectionOptions: ClientConnectionOptions
  private reconcileBufferedFramesMode: ReconcileBufferedFramesMode
  private transport: ClientChannelTransport

  private closed = false
  private connected = false
  private pingInterval: ReturnType<typeof setInterval> | null = null
  private pongTimer: ReturnType<typeof setTimeout> | null = null
  private ttl: ReturnType<typeof setTimeout> | null = null
  private reconnectTimer: ReturnType<typeof setTimeout> | null = null
  private reconnectAttempt = 0
  private reconnectStart = 0
  private readonly upgrade: UpgradeState = {
    active: false,
    disabled: false,
    probeAbort: null,
    handoffTransport: null,
    handoffBuffer: null,
  }

  // Protocol state
  private sessionId: string | null = null
  private nextIndex = 0
  private reconciling = false
  private reconcileIxes = new Set<number>()
  private channels = new Map<number, MuxChannel>()
  private channelIndex = new Map<MuxChannel, number>()
  /** Ixs that have not yet been confirmed by a `ReconciledPayload`. They're flagged
   *  `initial: true` in the next `reconcile` so the server waits up to `connectTtl` for
   *  late-creation. Cleared once the server confirms the ix in a `reconciled.open`. */
  private initialPendingIxes = new Set<number>()
  private sendBuffer: BufferedFrame[] = []
  private lastSeqByChannel = new Map<number, number>()
  private replayBuffers = new Map<number, ReplayBuffer>()
  private pendingAcks = new Map<string, PendingAck>()
  private reconnectTimeoutMs = CHANNEL_RECONNECT_TIMEOUT_MS
  private idleTimeoutMs = CHANNEL_IDLE_TIMEOUT_MS
  private clientReplayBufferBytes = CHANNEL_CLIENT_REPLAY_BUFFER_BYTES
  private clientReplayBufferBinaryBytes = CHANNEL_CLIENT_REPLAY_BUFFER_BINARY_BYTES
  private pingIntervalMs = CHANNEL_PING_INTERVAL_MS
  private pongTimeoutMs = CHANNEL_PING_INTERVAL_MS * 2
  private readonly dispatchFrame = (_raw: Uint8Array<ArrayBuffer>, frame: DecodedFrame): void => {
    switch (frame.tag) {
      case TAG.PONG:
      case TAG.CLOSE:
      case TAG.CLOSE_ACK:
      case TAG.ABORT:
      case TAG.ERROR:
      case TAG.WINDOW:
      case TAG.FIN:
      case TAG.RECONCILED:
        this.handleCtrl(frame)
        return
      case TAG.PING:
      case TAG.RECONCILE:
        // Server→client only; client never receives these.
        return
      case TAG.TEXT:
      case TAG.PUBLISH:
      case TAG.PUBLISH_ACK_REQ:
      case TAG.TEXT_ACK_REQ:
      case TAG.BINARY:
      case TAG.PUBLISH_BINARY:
      case TAG.PUBLISH_BINARY_ACK_REQ:
        this.handleDataFrame(frame)
        return
      case TAG.ACK_RES:
        this.handleAckRes(frame.index, frame.ackedSeq, frame.text, frame.status)
    }
  }
  private readonly bufferFrameDuringHandoff = (raw: Uint8Array<ArrayBuffer>, frame: DecodedFrame): void => {
    if (frame.tag === TAG.FIN) {
      this.handleHandoffFin()
      return
    }
    if (frame.tag === TAG.RECONCILED) {
      this.handleReconciled(frame.payload)
      return
    }
    const { handoffBuffer } = this.upgrade
    assert(handoffBuffer)
    handoffBuffer.push(raw)
  }
  private handleTransportFrame = this.dispatchFrame

  private constructor(telefuncUrl: string, options: ClientConnectionOptions, cacheKey: string) {
    this.cacheKey = cacheKey
    this.telefuncUrl = telefuncUrl
    this.connectionOptions = options
    this.transport = TRANSPORT_REGISTRY[options.transports[0]!](telefuncUrl, options, this)
    this.reconcileBufferedFramesMode = this.transport.reconcileMode
  }

  private canSendImmediately(): boolean {
    return this.connected && this.transport.hasActiveTransport() && !this.reconciling
  }

  private register(channel: MuxChannel): void {
    this.clearTimer('ttl')
    const ix = this.nextIndex++
    this.channels.set(ix, channel)
    this.channelIndex.set(channel, ix)
    this.initialPendingIxes.add(ix)
    this.replayBuffers.set(
      ix,
      new ReplayBuffer(this.clientReplayBufferBytes, this.reconnectTimeoutMs, this.clientReplayBufferBinaryBytes),
    )

    if (!this.transport.hasActiveTransport() && !this.transport.isConnecting()) {
      this.transport.start()
      return
    }
    if (this.connected && !this.reconciling) {
      const reconcileBatch = this.stageReconcileBatch()
      this.sendReconcileBatch(reconcileBatch)
    }
  }

  unregister(channel: MuxChannel, err = new ChannelClosedError()): void {
    const ix = this.channelIndex.get(channel)
    if (ix === undefined) return
    this.releaseChannel(ix, channel, err)
    this.startTtlIfIdle()
  }

  send(channel: MuxChannel, data: string): void {
    const ix = this.channelIndex.get(channel)
    if (ix === undefined) return
    const replay = this.replayBuffers.get(ix)!
    const seq = replay.nextSeq()
    const frame = encode.text(ix, data, seq)
    if (!this.canSendImmediately()) {
      this.sendBuffer.push({ frame, channelIx: ix, seq })
      return
    }
    replay.push(seq, frame)
    this.transport.sendFrame({ kind: 'data', frame })
  }

  sendPublishAckReq(channel: MuxChannel, data: string): Promise<unknown> {
    const ix = this.channelIndex.get(channel)
    if (ix === undefined) return Promise.reject(new ChannelClosedError())
    const replay = this.replayBuffers.get(ix)!
    const seq = replay.nextSeq()
    const promise = new Promise<unknown>((resolve, reject) => {
      this.pendingAcks.set(`${ix}:${seq}`, { resolve, reject })
    })
    const frame = encode.publishAckReq(ix, data, seq)
    if (!this.canSendImmediately()) {
      this.sendBuffer.push({ frame, channelIx: ix, seq })
      return promise
    }
    replay.push(seq, frame)
    this.transport.sendFrame({ kind: 'ack', frame })
    return promise
  }

  sendPublishBinaryAckReq(channel: MuxChannel, data: Uint8Array): Promise<unknown> {
    const ix = this.channelIndex.get(channel)
    if (ix === undefined) return Promise.reject(new ChannelClosedError())
    const replay = this.replayBuffers.get(ix)!
    const seq = replay.nextSeq()
    const promise = new Promise<unknown>((resolve, reject) => {
      this.pendingAcks.set(`${ix}:${seq}`, { resolve, reject })
    })
    const frame = encode.publishBinaryAckReq(ix, data, seq)
    if (!this.canSendImmediately()) {
      this.sendBuffer.push({ frame, channelIx: ix, seq })
      return promise
    }
    replay.push(seq, frame, true)
    this.transport.sendFrame({ kind: 'ack', frame })
    return promise
  }

  sendTextAckReq(channel: MuxChannel, data: string): Promise<unknown> {
    const ix = this.channelIndex.get(channel)
    if (ix === undefined) return Promise.reject(new ChannelClosedError())
    const replay = this.replayBuffers.get(ix)!
    const seq = replay.nextSeq()
    const promise = new Promise<unknown>((resolve, reject) => {
      this.pendingAcks.set(`${ix}:${seq}`, { resolve, reject })
    })
    const frame = encode.textAckReq(ix, data, seq)
    if (!this.canSendImmediately()) {
      this.sendBuffer.push({ frame, channelIx: ix, seq })
      return promise
    }
    replay.push(seq, frame)
    this.transport.sendFrame({ kind: 'ack', frame })
    return promise
  }

  sendBinaryAckReq(channel: MuxChannel, data: Uint8Array): Promise<unknown> {
    const ix = this.channelIndex.get(channel)
    if (ix === undefined) return Promise.reject(new ChannelClosedError())
    const replay = this.replayBuffers.get(ix)!
    const seq = replay.nextSeq()
    const promise = new Promise<unknown>((resolve, reject) => {
      this.pendingAcks.set(`${ix}:${seq}`, { resolve, reject })
    })
    const frame = encode.binaryAckReq(ix, data, seq)
    if (!this.canSendImmediately()) {
      this.sendBuffer.push({ frame, channelIx: ix, seq })
      return promise
    }
    replay.push(seq, frame, true)
    this.transport.sendFrame({ kind: 'ack', frame })
    return promise
  }

  sendBinary(channel: MuxChannel, data: Uint8Array): void {
    const ix = this.channelIndex.get(channel)
    if (ix === undefined) return
    const replay = this.replayBuffers.get(ix)!
    const seq = replay.nextSeq()
    const frame = encode.binary(ix, data, seq)
    if (!this.canSendImmediately()) {
      this.sendBuffer.push({ frame, channelIx: ix, seq })
      return
    }
    replay.push(seq, frame, true)
    this.transport.sendFrame({ kind: 'data', frame })
  }

  sendAckRes(channel: MuxChannel, ackedSeq: number, result: string, status: AckResultStatus = ACK_STATUS.OK): void {
    const ix = this.channelIndex.get(channel)
    if (ix === undefined) return
    const replay = this.replayBuffers.get(ix)!
    const seq = replay.nextSeq()
    const frame = encode.ackRes(ix, seq, ackedSeq, result, status)
    if (!this.canSendImmediately()) {
      this.sendBuffer.push({ frame, channelIx: ix, seq })
      return
    }
    replay.push(seq, frame)
    this.transport.sendFrame({ kind: 'ack', frame })
  }

  sendAbort(channel: MuxChannel): void {
    const ix = this.channelIndex.get(channel)
    if (ix === undefined) return
    const frame = encode.close(ix, 0)
    if (this.canSendImmediately()) {
      this.transport.sendFrame({ kind: 'control', frame })
    } else {
      this.sendBuffer.push({ frame, channelIx: ix, seq: undefined })
    }
  }

  sendCloseRequest(channel: MuxChannel, timeoutMs: number): void {
    const ix = this.channelIndex.get(channel)
    if (ix === undefined) return
    const frame = encode.close(ix, timeoutMs)
    if (!this.canSendImmediately()) {
      this.sendBuffer.push({ frame, channelIx: ix, seq: undefined })
      return
    }
    this.transport.sendFrame({ kind: 'control', frame })
  }

  sendCloseAck(channel: MuxChannel): void {
    const ix = this.channelIndex.get(channel)
    if (ix === undefined) return
    const frame = encode.closeAck(ix)
    if (!this.canSendImmediately()) {
      this.sendBuffer.push({ frame, channelIx: ix, seq: undefined })
      return
    }
    this.transport.sendFrame({ kind: 'control', frame })
  }

  sendWindowUpdate(channel: MuxChannel, bytes: number): void {
    const ix = this.channelIndex.get(channel)
    if (ix === undefined) return
    // Drop if transport isn't ready — window updates are ephemeral state.
    // Both sides reset to CREDIT_WINDOW_BYTES on reconnect.
    if (!this.canSendImmediately()) return
    this.transport.sendFrame({ kind: 'control', frame: encode.window(ix, bytes) })
  }

  sendBroadcastSubscribe(channel: MuxChannel, binary: boolean): void {
    const ix = this.channelIndex.get(channel)
    if (ix === undefined) return
    const frame = encode.broadcastSub(ix, binary)
    if (!this.canSendImmediately()) {
      this.sendBuffer.push({ frame, channelIx: ix, seq: undefined })
      return
    }
    this.transport.sendFrame({ kind: 'control', frame })
  }

  sendBroadcastUnsubscribe(channel: MuxChannel, binary: boolean): void {
    const ix = this.channelIndex.get(channel)
    if (ix === undefined) return
    const frame = encode.broadcastUnsub(ix, binary)
    if (!this.canSendImmediately()) {
      this.sendBuffer.push({ frame, channelIx: ix, seq: undefined })
      return
    }
    this.transport.sendFrame({ kind: 'control', frame })
  }

  _onTransportOpen(): void {
    if (this.closed) return
    this.connected = true
    this.reconnectAttempt = 0
    this.reconnectStart = 0
    if (this.transport.sendReconcileOnOpen) {
      const reconcileBatch = this.stageReconcileBatch()
      this.sendReconcileBatch(reconcileBatch)
      return
    }
    const drained = this.drainBufferedFrames(this.channels, undefined)
    for (const frame of drained) this.transport.sendFrame(frame)
  }

  _onTransportFrame(raw: Uint8Array<ArrayBuffer>): void {
    this.handleTransportFrame(raw, decode(raw))
  }

  private flushHandoffBuffer(): void {
    const buffer = this.upgrade.handoffBuffer
    this.upgrade.handoffBuffer = null
    this.handleTransportFrame = this.dispatchFrame
    if (buffer) for (const raw of buffer) this._onTransportFrame(raw)
  }

  private stopUpgradeProbe(): void {
    this.upgrade.active = false
    this.upgrade.probeAbort?.()
    this.upgrade.probeAbort = null
  }

  private disposeHandoffTransport(): void {
    const handoffTransport = this.upgrade.handoffTransport
    if (!handoffTransport) return
    handoffTransport.abandonActiveTransport()
    handoffTransport.dispose()
    this.upgrade.handoffTransport = null
  }

  private completeUpgradeHandoff(): void {
    this.disposeHandoffTransport()
    this.flushHandoffBuffer()
  }

  private handleHandoffFin(): void {
    if (!this.upgrade.handoffTransport) return
    this.completeUpgradeHandoff()
  }

  _onTransportClosed(transport: ClientChannelTransport, rejectedInitial = false): void {
    if (this.closed) return
    // Ignore close events from non-active transports (e.g. SSE closing while WS upgrade drains it)
    if (transport !== this.transport) {
      // SSE dropped without fin — clean up the pending reference
      if (transport === this.upgrade.handoffTransport)
        this.abortUpgradeAndReconnectSse(new ChannelNetworkError('Connection dropped'))
      return
    }
    if (this.upgrade.active) this.stopUpgradeProbe()
    const err = new ChannelNetworkError(
      rejectedInitial
        ? `Server rejected ${this.transport.type === CHANNEL_TRANSPORT.SSE ? 'SSE' : 'WebSocket'} connection`
        : 'Connection dropped',
    )
    this.handleTransportLoss(err, rejectedInitial)
  }

  private handleCtrl(frame: Extract<DecodedFrame, { tag: ServerCtrlTag }>): void {
    switch (frame.tag) {
      case TAG.PONG:
        this.resetPongTimer()
        return
      case TAG.CLOSE:
        this.channels.get(frame.index)?._onTransportCloseRequest(frame.timeoutMs)
        return
      case TAG.CLOSE_ACK:
        this.channels.get(frame.index)?._onTransportCloseAck()
        return
      case TAG.ABORT:
        this.closeRemoteChannel(frame.index, makeAbortError(parse(frame.abortValue)))
        this.startTtlIfIdle()
        return
      case TAG.ERROR:
        this.closeRemoteChannel(frame.index, makeBugError())
        this.startTtlIfIdle()
        return
      case TAG.WINDOW:
        this.channels.get(frame.index)?._onPeerWindowUpdate(frame.bytes)
        return
      case TAG.FIN:
        this.handleHandoffFin()
        return
      case TAG.RECONCILED:
        this.handleReconciled(frame.payload)
        return
    }
  }

  private handleReconciled(ctrl: ReconciledPayload): void {
    this.transport.applyReconciledSettings(ctrl)
    this.transport.pushReconciledRouting()
    const outcome = this.applyReconciled(ctrl)
    this.transport.closeAbandonedTransport()
    for (const frame of outcome.frames) this.transport.sendFrame(frame)
    for (const channel of outcome.channelsToOpen) channel._onTransportOpen()
    if (outcome.reconcileComplete) {
      this.startPing()
      this.startTtlIfIdle()
    }
    this.maybeStartUpgrade(ctrl)
  }

  // ── SSE→WS upgrade ──

  private maybeStartUpgrade(ctrl: ReconciledPayload): void {
    if (this.upgrade.disabled) return
    const nextTransport = UPGRADE_PATH[this.transport.type]
    if (!nextTransport || this.upgrade.active) return
    if (!this.isTransportUpgradeAllowed(nextTransport)) return
    if (!ctrl.transports.includes(nextTransport)) return
    this.upgrade.active = true
    void this.probeAndUpgrade(nextTransport)
  }

  private abortUpgradeAndReconnectSse(err: Error): void {
    this.disposeHandoffTransport()
    this.upgrade.handoffBuffer = null
    this.handleTransportFrame = this.dispatchFrame
    this.upgrade.disabled = true
    this.transport.abandonActiveTransport()
    this.transport.dispose()
    this.transport = TRANSPORT_REGISTRY[CHANNEL_TRANSPORT.SSE](this.telefuncUrl, this.connectionOptions, this)
    this.reconcileBufferedFramesMode = this.transport.reconcileMode
    this.handleTransportLoss(err)
  }

  private isTransportUpgradeAllowed(nextTransport: ChannelTransport): boolean {
    return this.connectionOptions.transports.includes(nextTransport)
  }

  private async probeAndUpgrade(targetTransport: ChannelTransport): Promise<void> {
    const from = this.transport
    const to = TRANSPORT_REGISTRY[targetTransport](this.telefuncUrl, this.connectionOptions, this)
    const closeProbe = await to.probe()
    if (!closeProbe || !this.upgrade.active) {
      closeProbe?.()
      this.upgrade.active = false
      return
    }

    this.upgrade.probeAbort = closeProbe
    await from.prepareForUpgrade()
    if (this.upgrade.probeAbort === closeProbe) this.upgrade.probeAbort = null
    if (!this.upgrade.active) {
      closeProbe()
      return
    }

    // Synchronous transport switch.
    this.upgrade.active = false
    this.transport = to
    this.reconcileBufferedFramesMode = to.reconcileMode
    this.upgrade.handoffTransport = from
    this.upgrade.handoffBuffer = []
    this.handleTransportFrame = this.bufferFrameDuringHandoff
    to.start()
  }

  // ── Ping ──

  private startPing(): void {
    this.resetPongTimer()
    if (this.pingInterval) return
    this.pingInterval = setInterval(() => {
      if (!this.canSendImmediately()) return
      this.transport.sendFrame({ kind: 'heartbeat', frame: encode.ping() })
    }, this.pingIntervalMs)
  }

  private stopPing(): void {
    if (this.pingInterval) {
      clearInterval(this.pingInterval)
      this.pingInterval = null
    }
    if (this.pongTimer) {
      clearTimeout(this.pongTimer)
      this.pongTimer = null
    }
  }

  private resetPongTimer(): void {
    if (!this.connected) return
    if (this.pongTimer) clearTimeout(this.pongTimer)
    this.pongTimer = setTimeout(() => {
      this.stopPing()
      this.connected = false
      this.transport.abandonActiveTransport()
      this.handleTransportLoss(new ChannelNetworkError(this.transport.reconnectTimeoutMessage))
    }, this.pongTimeoutMs)
  }

  private handleTransportLoss(err: Error, rejected = false): void {
    if (this.closed) return
    this.connected = false
    this.stopPing()
    this.reconciling = false
    this.reconcileIxes.clear()
    this.clearTimer('ttl')
    if (this.upgrade.handoffTransport) {
      this.abortUpgradeAndReconnectSse(err)
      return
    }

    if (rejected && this.reconnectAttempt === 0) {
      this.closeAll(err instanceof Error ? err : new ChannelNetworkError('Connection dropped'))
      this.dispose()
      return
    }
    if (this.channels.size === 0) {
      this.dispose()
      return
    }
    if (!this.reconnectStart) this.reconnectStart = Date.now()
    if (Date.now() - this.reconnectStart > this.reconnectTimeoutMs) {
      this.closeAll(err instanceof Error ? err : new ChannelNetworkError('Connection dropped'))
      this.dispose()
      return
    }
    if (this.reconnectTimer) return
    const delay = Math.min(
      CHANNEL_RECONNECT_INITIAL_DELAY_MS * 2 ** this.reconnectAttempt,
      CHANNEL_RECONNECT_MAX_DELAY_MS,
    )
    this.reconnectAttempt++
    this.reconnectTimer = setTimeout(() => {
      this.reconnectTimer = null
      this.transport.start()
    }, delay)
  }

  private startTtlIfIdle(): void {
    if (this.closed || this.channels.size > 0 || this.ttl) return
    this.ttl = setTimeout(() => {
      if (this.channels.size === 0) this.dispose()
    }, this.idleTimeoutMs)
  }

  private dispose(): void {
    if (this.closed) return
    this.closed = true
    this.connected = false
    this.clearTimer('ttl')
    this.clearTimer('reconnectTimer')
    this.stopPing()
    this.upgrade.probeAbort?.()
    this.upgrade.probeAbort = null
    this.disposeHandoffTransport()
    this.upgrade.handoffBuffer = null
    this.handleTransportFrame = this.dispatchFrame
    this.upgrade.active = false
    this.transport.dispose()
    for (const replayBuffer of this.replayBuffers.values()) replayBuffer.dispose()
    for (const [, pending] of this.pendingAcks) pending.reject(new ChannelNetworkError('Connection closed'))
    this.channels.clear()
    this.channelIndex.clear()
    this.sendBuffer = []
    this.lastSeqByChannel.clear()
    this.replayBuffers.clear()
    this.pendingAcks.clear()
    this.reconcileIxes.clear()
    this.reconciling = false
    ClientConnection.cache.delete(this.cacheKey)
  }

  private clearTimer(name: 'ttl' | 'reconnectTimer'): void {
    const timer = this[name]
    if (!timer) return
    clearTimeout(timer)
    this[name] = null
  }

  // ── Protocol internals ──

  buildReconcileFrame(): OutboundFrame {
    this.reconciling = true
    this.reconcileIxes = new Set()
    const open: ReconcilePayload['open'] = []
    for (const [ix, channel] of this.channels) {
      this.reconcileIxes.add(ix)
      const entry: ReconcilePayload['open'][number] = {
        id: channel.id,
        ix,
        lastSeq: this.lastSeqByChannel.get(ix) ?? 0,
      }
      if (this.initialPendingIxes.has(ix)) entry.initial = true
      open.push(entry)
    }
    const reconcile: ReconcilePayload = { open }
    if (this.sessionId) reconcile.sessionId = this.sessionId
    if (this.upgrade.handoffTransport) {
      reconcile.upgrade = true
      // Identify the previous wire so the server can find it even when this reconcile
      // lands on a different cluster instance than the one holding it. Null when the
      // previous transport has no cluster-stable id (WS).
      const handoffConnId = this.upgrade.handoffTransport.connId
      if (handoffConnId !== null) reconcile.prevConnId = handoffConnId
    }
    return { kind: 'reconcile', frame: encode.reconcile(reconcile) }
  }

  drainBufferedFramesForReconcile(): OutboundFrame[] {
    if (this.reconcileBufferedFramesMode !== 'batch-on-reconcile') return []
    return this.drainBufferedFrames(this.channels, undefined)
  }

  stageReconcileBatch(): ReconcileBatch {
    const reconcileFrame = this.buildReconcileFrame()
    const movedBufferedFrames = this.drainBufferedFramesForReconcile()
    return { reconcileFrame, movedBufferedFrames }
  }

  private sendReconcileBatch(reconcileBatch: ReconcileBatch): void {
    this.transport.sendFrame(reconcileBatch.reconcileFrame)
    for (const frame of reconcileBatch.movedBufferedFrames) this.transport.sendFrame(frame)
  }

  private appendReconcileBatch(target: OutboundFrame[], reconcileBatch: ReconcileBatch): void {
    target.push(reconcileBatch.reconcileFrame)
    for (const frame of reconcileBatch.movedBufferedFrames) target.push(frame)
  }

  private applyReconciled(ctrl: ReconciledPayload): ReconcileOutcome {
    this.sessionId = ctrl.sessionId
    if (ctrl.reconnectTimeout) this.reconnectTimeoutMs = ctrl.reconnectTimeout
    if (ctrl.idleTimeout) this.idleTimeoutMs = ctrl.idleTimeout
    if (ctrl.clientReplayBuffer) this.clientReplayBufferBytes = ctrl.clientReplayBuffer
    if (ctrl.clientReplayBufferBinary) this.clientReplayBufferBinaryBytes = ctrl.clientReplayBufferBinary
    if (ctrl.pingInterval) {
      this.pingIntervalMs = ctrl.pingInterval
      this.pongTimeoutMs = ctrl.pingInterval * 2
    }

    const serverMap = new Map<number, number>()
    for (const channel of ctrl.open) {
      serverMap.set(channel.ix, channel.lastSeq)
      // Server confirmed this ix — it now exists on the server side, so subsequent reconciles
      // shouldn't flag it as `initial`.
      this.initialPendingIxes.delete(channel.ix)
    }
    const reconcileIxes = this.reconcileIxes
    this.reconcileIxes = new Set()
    const releaseFrames: OutboundFrame[] = []
    const channelsToOpen: MuxChannel[] = []
    let hasNewChannels = false

    for (const [ix, channel] of this.channels) {
      if (!reconcileIxes.has(ix)) {
        if (!serverMap.has(ix)) hasNewChannels = true
        continue
      }
      if (!serverMap.has(ix)) {
        const err = new ChannelNetworkError('Channel not acknowledged by server after reconnect')
        this.releaseChannel(ix, channel, err)
        channel._onTransportClose(err)
        continue
      }
      const replay = this.replayBuffers.get(ix)
      if (replay)
        for (const frame of replay.getAfter(serverMap.get(ix)!)) releaseFrames.push({ kind: 'reconcile', frame })
      if (!channel.isClosed) channelsToOpen.push(channel)
    }

    for (const frame of this.drainBufferedFrames(serverMap, this.channels)) releaseFrames.push(frame)

    if (hasNewChannels) {
      const reconcileBatch = this.stageReconcileBatch()
      this.appendReconcileBatch(releaseFrames, reconcileBatch)
    } else {
      this.reconciling = false
    }

    return { frames: releaseFrames, channelsToOpen, reconcileComplete: !hasNewChannels }
  }

  private closeRemoteChannel(ix: number, err?: Error): void {
    const channel = this.channels.get(ix)
    if (!channel) return
    this.releaseChannel(ix, channel, err ?? new ChannelClosedError())
    channel._onTransportClose(err)
  }

  private handleAckRes(index: number, ackedSeq: number, text: string, status: AckResultStatus = ACK_STATUS.OK): void {
    const key = `${index}:${ackedSeq}`
    const pending = this.pendingAcks.get(key)
    if (!pending) return
    this.pendingAcks.delete(key)
    switch (status) {
      case ACK_STATUS.OK:
        pending.resolve(parse(text))
        return
      case ACK_STATUS.ABORT:
        pending.reject(makeAbortError(parse(text)))
        return
      case ACK_STATUS.ERROR:
        pending.reject(makeBugError(text || undefined))
        return
      case ACK_STATUS.SHIELD_ERROR:
        // Server-declared data shield rejected what this channel sent — same branded class as
        // every other shield-fail surface so user code can catch with `isShieldValidationError`.
        pending.reject(new ShieldValidationError(text))
    }
  }

  private handleDataFrame(frame: Extract<DecodedFrame, { index: number; seq: number }>): void {
    if (frame.seq && !this.trackSeq(frame.index, frame.seq)) return
    if (frame.tag === TAG.TEXT_ACK_REQ) {
      void this.channels.get(frame.index)?._onTransportAckReqMessage(frame.text!, frame.seq)
      return
    }
    if (frame.tag === TAG.PUBLISH) {
      const channel = this.channels.get(frame.index)
      if (channel && ClientBroadcast.isClientBroadcast(channel)) channel._onTransportPublish(frame.text!, frame.info!)
      return
    }
    if (frame.tag === TAG.PUBLISH_BINARY) {
      const channel = this.channels.get(frame.index)
      if (channel && ClientBroadcast.isClientBroadcast(channel))
        channel._onTransportPublishBinary(frame.data!, frame.info!)
      return
    }
    if (frame.tag === TAG.TEXT) {
      this.channels.get(frame.index)?._onTransportMessage(frame.text!)
      return
    }
    if (frame.tag === TAG.BINARY_ACK_REQ) {
      this.channels.get(frame.index)?._onTransportBinaryAckReqMessage(frame.data!, frame.seq)
      return
    }
    if (frame.tag === TAG.BINARY) this.channels.get(frame.index)?._onTransportBinaryMessage(frame.data!)
  }

  private closeAll(err: Error): void {
    for (const [ix, channel] of this.channels) {
      this.clearPendingAcks(ix, err)
      channel._onTransportClose(err)
    }
    this.dispose()
  }

  private trackSeq(ix: number, seq: number): boolean {
    const prev = this.lastSeqByChannel.get(ix) ?? 0
    if (seq <= prev) return false
    this.lastSeqByChannel.set(ix, seq)
    return true
  }

  private drainBufferedFrames(
    releasableChannels: Set<number> | Map<number, unknown>,
    retainedChannels: Set<number> | Map<number, unknown> | undefined,
  ): OutboundFrame[] {
    const frames: OutboundFrame[] = []
    const sendBuffer = this.sendBuffer
    let writeIx = 0
    for (let readIx = 0; readIx < sendBuffer.length; readIx++) {
      const entry = sendBuffer[readIx]!
      const frame = entry.frame
      const channelIx = entry.channelIx
      const seq = entry.seq
      if (!releasableChannels.has(channelIx)) {
        if (retainedChannels?.has(channelIx)) sendBuffer[writeIx++] = entry
        continue
      }
      if (seq !== undefined) {
        const tag = frame[0]
        const isBinary = tag === TAG.BINARY || tag === TAG.PUBLISH_BINARY || tag === TAG.PUBLISH_BINARY_ACK_REQ
        this.replayBuffers.get(channelIx)?.push(seq, frame, isBinary)
      }
      frames.push({ kind: 'reconcile', frame })
    }
    sendBuffer.length = writeIx
    return frames
  }

  private clearPendingAcks(ix: number, err: Error): void {
    const prefix = `${ix}:`
    for (const [key, pending] of this.pendingAcks) {
      if (!key.startsWith(prefix)) continue
      this.pendingAcks.delete(key)
      pending.reject(err)
    }
  }

  private releaseChannel(ix: number, channel: MuxChannel, err: Error): void {
    this.channels.delete(ix)
    this.channelIndex.delete(channel)
    this.lastSeqByChannel.delete(ix)
    this.initialPendingIxes.delete(ix)
    const replayBuffer = this.replayBuffers.get(ix)
    replayBuffer?.dispose()
    this.replayBuffers.delete(ix)
    this.clearPendingAcks(ix, err)
  }
}

class WsTransport implements ClientChannelTransport {
  readonly type = CHANNEL_TRANSPORT.WS
  readonly reconnectTimeoutMessage = 'WebSocket reconnect timed out'
  readonly sendReconcileOnOpen = true
  readonly reconcileMode = 'release-after-reconciled' as const
  readonly connId = null
  private probedWs: WebSocket | null = null
  private ws: WebSocket | null = null
  private abandonedWs: WebSocket | null = null
  private connecting = false
  private everOpened = false

  private readonly wsUrl: string

  constructor(
    telefuncUrl: string,
    private readonly owner: ClientConnection,
  ) {
    const base = typeof window === 'undefined' ? undefined : window.location.href
    const url = new URL(telefuncUrl, base)
    url.protocol = url.protocol === 'https:' ? 'wss:' : 'ws:'
    this.wsUrl = url.href
  }

  async probe(): Promise<(() => void) | null> {
    const ws = await new Promise<WebSocket | null>((resolve) => {
      let ws: WebSocket
      try {
        ws = new WebSocket(this.wsUrl)
      } catch {
        resolve(null)
        return
      }
      ws.binaryType = 'arraybuffer'
      const timer = setTimeout(() => {
        ws.onopen = ws.onmessage = ws.onclose = ws.onerror = null
        ws.close()
        resolve(null)
      }, WS_PROBE_TIMEOUT_MS)
      ws.onopen = () => ws.send(encode.ping())
      ws.onmessage = ({ data }: MessageEvent) => {
        const frame = decode(new Uint8Array(data as ArrayBuffer))
        if (frame.tag === TAG.PONG) {
          clearTimeout(timer)
          ws.onopen = ws.onmessage = ws.onclose = ws.onerror = null
          resolve(ws)
        }
      }
      ws.onclose = () => {
        clearTimeout(timer)
        resolve(null)
      }
      ws.onerror = () => {}
    })
    if (!ws) return null
    this.probedWs = ws
    return () => {
      if (this.probedWs === ws) this.probedWs = null
      try {
        ws.close()
      } catch {}
    }
  }

  prepareForUpgrade(): Promise<void> {
    return Promise.resolve()
  }

  start(): void {
    if (this.connecting || this.ws) return

    const wsProbed = this.probedWs
    if (wsProbed) {
      this.probedWs = null
      this.ws = wsProbed
      this.setupHandlers(wsProbed)
      this.handleOpen(wsProbed)
      return
    }

    this.connecting = true

    let ws: WebSocket
    try {
      ws = new WebSocket(this.wsUrl)
    } catch {
      this.connecting = false
      this.owner._onTransportClosed(this, false)
      return
    }

    this.ws = ws
    ws.binaryType = 'arraybuffer'

    ws.onopen = () => {
      if (this.ws !== ws) return
      this.handleOpen(ws)
    }

    this.setupHandlers(ws)
  }

  private handleOpen(ws: WebSocket): void {
    if (this.ws !== ws) return
    this.everOpened = true
    this.connecting = false
    this.owner._onTransportOpen()
  }

  private setupHandlers(ws: WebSocket): void {
    ws.onmessage = ({ data }: MessageEvent) => {
      this.owner._onTransportFrame(new Uint8Array(data as ArrayBuffer))
    }
    ws.onclose = () => {
      if (this.ws === ws) this.ws = null
      this.connecting = false
      this.owner._onTransportClosed(this, !this.everOpened)
    }
    ws.onerror = () => {}
  }

  hasActiveTransport(): boolean {
    return this.ws !== null
  }

  isConnecting(): boolean {
    return this.connecting
  }

  sendFrame(frame: OutboundFrame): void {
    const ws = this.ws
    assert(ws)
    ws.send(frame.frame)
  }

  abandonActiveTransport(): void {
    const ws = this.ws
    if (!ws) return
    this.ws = null
    this.closeAbandonedTransport()
    this.abandonedWs = ws
    ws.onopen = ws.onerror = ws.onclose = null
  }

  closeAbandonedTransport(): void {
    const ws = this.abandonedWs
    if (!ws) return
    this.abandonedWs = null
    ws.onmessage = ws.onclose = null
    try {
      ws.close()
    } catch {}
  }

  applyReconciledSettings(): void {}
  pushReconciledRouting(): void {}

  dispose(): void {
    this.connecting = false
    const wsProbed = this.probedWs
    this.probedWs = null
    if (wsProbed) {
      wsProbed.onopen = wsProbed.onmessage = wsProbed.onerror = wsProbed.onclose = null
      try {
        wsProbed.close(1000)
      } catch {}
    }
    const ws = this.ws
    this.ws = null
    if (ws) {
      ws.onopen = ws.onmessage = ws.onerror = ws.onclose = null
      try {
        ws.close(1000)
      } catch {}
    }
    this.closeAbandonedTransport()
  }
}

class SseTransport implements ClientChannelTransport {
  readonly type = CHANNEL_TRANSPORT.SSE
  readonly reconnectTimeoutMessage = 'SSE reconnect timed out'
  readonly sendReconcileOnOpen = false
  readonly reconcileMode = 'batch-on-reconcile' as const
  async probe(): Promise<(() => void) | null> {
    throw new Error('SSE transport does not implement probe()')
  }

  readonly connId = crypto.randomUUID()
  private connecting = false
  private startTimer: ReturnType<typeof setTimeout> | null = null
  /** Abort handle for the active transport's fetches (SSE downstream, streamRequest, batch
   *  POSTs, heartbeat pings). `null` means "no active transport" — set on `openStream`,
   *  cleared on any teardown (failOpen / IIFE end / abandon / dispose). Whether we also
   *  call `.abort()` depends on the path: failOpen and dispose abort to kill in-flight;
   *  IIFE end and abandon don't (in-flight batch POSTs are allowed to complete on their
   *  own; abandon's deferred abort is handled via `abandonedControllers`). */
  private transportAbort: AbortController | null = null
  private abandonedStream: AbortController | null = null
  private readonly abandonedControllers = new WeakSet<AbortController>()
  private outboxFrames: Uint8Array<ArrayBuffer>[] = []
  private outboxDeadlines: number[] = []
  private readonly flushScheduler = new DeadlineScheduler(() => {
    void this.flushOutbox()
  })
  private flushing = false
  private lastPostStartedAt = 0
  private flushThrottleMs = SSE_FLUSH_THROTTLE_MS
  private postIdleFlushDelayMs = SSE_POST_IDLE_FLUSH_DELAY_MS
  private heartbeatFlushDelayMs = Math.floor(CHANNEL_PING_INTERVAL_MS / 2)
  private drainCallbacks: Array<() => void> = []
  // Persistent client→server stream-request POST (half-duplex streaming request body).
  // When available, sendFrame() writes directly here instead of going through the outbox.
  // Falls back to outbox/flush if the browser/protocol doesn't support streaming request.
  /** Active client→server stream-request POST. `body` is the upstream wire (frames pushed
   *  here flow on the persistent half-duplex POST); `fetch` resolves when the body ends
   *  (server returns 200 after EOF) — `prepareForUpgrade` awaits it as the client→server
   *  directional fin; `metadataPushed` tracks whether the post-reconcile routing table has
   *  been emitted on the wire (it's pushed only after `RECONCILED` arrives, since
   *  `ownerInstance` and per-channel `home` aren't known until then). */
  private streamRequest: {
    body: PushToPullStream<Uint8Array<ArrayBuffer>>
    fetch: Promise<unknown>
    metadataPushed: boolean
  } | null = null
  /** Sticky cross-attempt flag — once a stream-request fetch fails (runtime can't stream,
   *  network drop, abort), all subsequent transport opens skip the streamRequest path and
   *  flow upstream frames through outbox+batch POSTs. */
  private streamRequestFailed = false
  // Routing table populated by `applyReconciledSettings`. The receiver builds aliases
  // straight from `channels[]` (alias N = `channels[N − 1]`); alias 0 routes to `ownerInstance`.
  private ownerInstance = ''
  private channels: SseRouteChannel[] = []
  private ixToAlias = new Map<number, number>()

  constructor(
    private readonly telefuncUrl: string,
    private readonly fetchImpl: typeof fetch,
    private readonly sessionToken: string | undefined,
    private readonly owner: ClientConnection,
  ) {}

  async prepareForUpgrade(): Promise<void> {
    // 1. Drain outbox + in-flight batch POST.
    if (this.flushing || this.outboxFrames.length > 0) {
      await new Promise<void>((resolve) => this.drainCallbacks.push(resolve))
    }
    // 2. Drain the persistent client→server stream-request: close body → server reads EOF
    //    → returns 200. After this resolves, every frame the client ever pushed has been
    //    server-acknowledged. Symmetric to the server's `TAG.FIN` drain on the downstream.
    if (this.streamRequest) {
      const fetch = this.streamRequest.fetch
      this.closeStreamRequest()
      try {
        await fetch
      } catch {}
    }
  }

  start(): void {
    if (this.connecting || this.hasActiveTransport()) return
    this.connecting = true
    // Match WS batching behavior: wait one reconcile window so startup code can
    // register channels and queue payload before we build the initial SSE batch.
    this.startTimer = setTimeout(() => {
      this.startTimer = null
      if (!this.connecting || this.hasActiveTransport()) return
      void this.openStream()
    }, SSE_RECONCILE_DEADLINE_MS)
  }

  hasActiveTransport(): boolean {
    return this.transportAbort !== null
  }

  isConnecting(): boolean {
    return this.connecting
  }

  sendFrame(frame: OutboundFrame): void {
    if (this.flushing && frame.kind === 'heartbeat') {
      this.schedulePingDuringFlush(frame)
      return
    }
    if (this.streamRequest) {
      const aliased = this.aliasPrepend(frame.frame)
      this.streamRequest.body.push(encodeU32(aliased.byteLength))
      this.streamRequest.body.push(aliased)
      return
    }
    const now = Date.now()
    const deadlineAt = this.getFrameDeadline(frame.kind, now)
    this.outboxFrames.push(frame.frame)
    this.outboxDeadlines.push(deadlineAt)
    this.scheduleFlush()
    if (deadlineAt <= now) void this.flushOutbox()
  }

  /** Compute the per-frame routing alias from the frame's tag/ix and the table populated
   *  by `applyReconciledSettings`. Alias 0 = connection-routed (owner); alias N (≥ 1) =
   *  `channels[N − 1]`. Unknown ix falls back to alias 0 — the owner re-injects the frame
   *  and its mux's session registry routes it correctly. */
  private aliasFor(frame: Uint8Array): number {
    const decoded = decode(frame)
    return 'index' in decoded ? (this.ixToAlias.get(decoded.index) ?? 0) : 0
  }

  private aliasPrepend(frame: Uint8Array<ArrayBuffer>): Uint8Array<ArrayBuffer> {
    const out = new Uint8Array(1 + frame.byteLength)
    out[0] = this.aliasFor(frame)
    out.set(frame, 1)
    return out
  }

  private async openStream(): Promise<void> {
    const abortController = new AbortController()
    this.transportAbort = abortController
    const stage = this.stageInitialBatch()

    // Fire stream-response (SSE downstream) + stream-request (client→server upstream)
    // POSTs in parallel to reduce connection time. Both carry the same connId. The
    // stream-request may fail (streaming POST not supported, or race with server creating
    // the connection) — that's fine, outbox/flush handles all upstream sends in that case.
    const ssePromise = this.fetchImpl(getMarkedRequestUrl(this.telefuncUrl, REQUEST_KIND.SSE), {
      method: 'POST',
      headers: {
        Accept: 'text/event-stream',
        'Content-Type': 'application/octet-stream',
        [REQUEST_KIND_HEADER]: REQUEST_KIND.SSE,
        ...(this.sessionToken ? { [TELEFUNC_SESSION_HEADER]: this.sessionToken } : undefined),
      },
      body: encodeSseRequest({
        connId: this.connId,
        streamResponse: true,
        batch: encodeLengthPrefixedFrames(stage.initialFrames, (entry) => entry.frame),
      }),
      signal: abortController.signal,
    })
    // Fire the stream-request POST. With browser fetch + `duplex:'half'` its response
    // Promise doesn't resolve until the body ends, and the body never ends — so we don't
    // await it. The body is wired up to `this.streamRequest` immediately so
    // `pushReconciledRouting` can write to it as soon as `RECONCILED` arrives via SSE;
    // that first push is what unblocks the server's `readMetadata()`. The fetch's
    // rejection (runtime can't stream, network drop, abort) is consumed eagerly by the
    // `fetchEndedP` IIFE — built up-front so a rejection is always handled even if
    // `openStream` exits early via the `await ssePromise` catch below. The handshake
    // race uses `fetchEndedP` to detect a dead wire and fall back to outbox+batch.
    let fetchEndedP: Promise<'fetch-ended'> | undefined
    if (!this.streamRequestFailed) {
      const body = new PushToPullStream<Uint8Array<ArrayBuffer>>()
      const fetch = this.openStreamRequest(body, abortController.signal)
      this.streamRequest = { body, fetch, metadataPushed: false }
      fetchEndedP = (async (): Promise<'fetch-ended'> => {
        try {
          await fetch
        } catch {}
        return 'fetch-ended'
      })()
    }

    const failOpen = (permanent: boolean): void => {
      this.rollbackInitialBatch(stage)
      this.closeStreamRequest()
      abortController.abort()
      this.transportAbort = null
      this.connecting = false
      this.owner._onTransportClosed(this, permanent)
    }

    let response: Response
    try {
      response = await ssePromise
    } catch {
      failOpen(false)
      return
    }

    if (!response.ok || !response.body) {
      failOpen(true)
      return
    }

    const reader = createSseEventStreamReader(response.body.getReader(), abortController)

    // Start the SSE event loop. It dispatches frames AND signals `handshakeOk` on
    // STREAM_REQUEST_OPEN_ACK. We start it before awaiting the handshake so RECONCILED
    // frames received during the wait are still dispatched (their `pushReconciledRouting`
    // writes the metadata into the streamRequest body, which is what unblocks the server's
    // `readMetadata()` and prompts it to emit the ack back).
    let resolveHandshakeOk!: () => void
    const handshakeOkP = new Promise<'ok'>((resolve) => {
      resolveHandshakeOk = () => resolve('ok')
    })
    ;(async () => {
      try {
        while (true) {
          const entry = await reader.readNextEntry()
          if (!entry) break
          if (!entry.frame) continue
          // Peek the tag without full decode — STREAM_REQUEST_OPEN_ACK is a bare ConnCtrl
          // frame with no payload, so we just read the tag byte and short-circuit.
          if (entry.frame[0] === TAG.STREAM_REQUEST_OPEN_ACK) {
            resolveHandshakeOk()
            continue
          }
          this.owner._onTransportFrame(entry.frame)
        }
      } catch {
        if (abortController.signal.aborted) return
      } finally {
        reader.cancel()
        this.closeStreamRequest()
        this.transportAbort = null
        // Skip notifying transport-closed if this controller was moved to the abandoned
        // pool by an SSE→WS upgrade — the new (WS) transport has taken over.
        if (!this.abandonedControllers.has(abortController)) this.owner._onTransportClosed(this, false)
      }
    })()

    // Three-way race for the upstream wire: handshake ack (ok), fetch ended (rejected =
    // streamRequest is dead), or timeout. Non-`ok` outcomes flip us to outbox+batch.
    if (fetchEndedP) {
      const timeoutP = new Promise<'timeout'>((resolve) =>
        setTimeout(() => resolve('timeout'), STREAM_REQUEST_HANDSHAKE_TIMEOUT_MS),
      )
      const result = await Promise.race([handshakeOkP, timeoutP, fetchEndedP])
      if (result !== 'ok') {
        this.closeStreamRequest()
        this.streamRequestFailed = true
      }
    }

    this.connecting = false
    this.owner._onTransportOpen()
    if (this.outboxFrames.length > 0) void this.flushOutbox()
  }

  private stageInitialBatch(): SseInitialBatchStage {
    const reconcileBatch = this.owner.stageReconcileBatch()
    const initialFrames: OutboundFrame[] = []
    initialFrames.push(reconcileBatch.reconcileFrame)
    for (const frame of reconcileBatch.movedBufferedFrames) initialFrames.push(frame)
    const movedBufferedFrames = reconcileBatch.movedBufferedFrames
    const movedOutboxFrames = this.outboxFrames
    const movedOutboxDeadlines = this.outboxDeadlines
    this.outboxFrames = []
    this.outboxDeadlines = []
    for (const frame of movedOutboxFrames) initialFrames.push({ kind: 'data', frame })
    return { initialFrames, movedOutboxFrames, movedOutboxDeadlines, movedBufferedFrames }
  }

  private rollbackInitialBatch(stage: SseInitialBatchStage): void {
    if (stage.movedOutboxFrames.length === 0 && stage.movedBufferedFrames.length === 0) return
    const now = Date.now()
    const movedBufferedOutboxFrames = stage.movedBufferedFrames.map((entry) => entry.frame)
    const movedBufferedDeadlines = stage.movedBufferedFrames.map((entry) => this.getFrameDeadline(entry.kind, now))
    this.outboxFrames = stage.movedOutboxFrames.concat(movedBufferedOutboxFrames, this.outboxFrames)
    this.outboxDeadlines = stage.movedOutboxDeadlines.concat(movedBufferedDeadlines, this.outboxDeadlines)
  }

  private async flushOutbox(): Promise<void> {
    if (!this.hasActiveTransport() || this.flushing || this.outboxFrames.length === 0) return
    // The `hasActiveTransport()` guard above proves `transportAbort` is non-null and not
    // closed, and there's no async hop until the fetch fires.
    assert(this.transportAbort)
    this.flushScheduler.cancel()
    this.flushing = true
    try {
      const now = Date.now()
      const queuedFrames = this.outboxFrames.splice(0, this.outboxFrames.length)
      const queuedDeadlines = this.outboxDeadlines.splice(0, this.outboxDeadlines.length)
      this.lastPostStartedAt = now

      try {
        const response = await this.fetchImpl(getMarkedRequestUrl(this.telefuncUrl, REQUEST_KIND.SSE), {
          method: 'POST',
          headers: {
            'Content-Type': 'application/octet-stream',
            [REQUEST_KIND_HEADER]: REQUEST_KIND.SSE,
            ...(this.sessionToken ? { [TELEFUNC_SESSION_HEADER]: this.sessionToken } : undefined),
          },
          body: encodeSseRequest({
            connId: this.connId,
            ownerInstance: this.ownerInstance,
            channels: this.channels,
            batch: encodeLengthPrefixedFrames(queuedFrames.map((f) => this.aliasPrepend(f))),
          }),
          signal: this.transportAbort.signal,
        })
        if (!response.ok) throw new Error('POST failed')
      } catch {
        this.outboxFrames = queuedFrames.concat(this.outboxFrames)
        this.outboxDeadlines = queuedDeadlines.concat(this.outboxDeadlines)
        this.abandonActiveTransport()
        this.owner._onTransportClosed(this, false)
        return
      }
    } finally {
      this.flushing = false
      if (this.outboxFrames.length > 0) {
        this.scheduleFlush()
      } else {
        const cbs = this.drainCallbacks.splice(0)
        for (const cb of cbs) cb()
      }
    }
  }

  /** Send a ping in a concurrent POST while a flush POST is in flight.
   *  Respects the heartbeat deadline before sending. */
  private schedulePingDuringFlush(frame: OutboundFrame): void {
    const delay = Math.max(0, this.getFrameDeadline(frame.kind) - Date.now())
    setTimeout(() => {
      if (this.flushing) {
        void this.sendConcurrentPost([frame.frame])
      } else {
        this.sendFrame(frame)
      }
    }, delay)
  }

  private async sendConcurrentPost(frames: Uint8Array<ArrayBuffer>[]): Promise<void> {
    // Called from a deferred `setTimeout` — the transport may have been torn down in the
    // meantime. Skip silently rather than firing a fetch on a dead wire (it's a heartbeat
    // ping; the next reconnect will recover).
    if (!this.hasActiveTransport()) return
    assert(this.transportAbort)
    try {
      await this.fetchImpl(getMarkedRequestUrl(this.telefuncUrl, REQUEST_KIND.SSE), {
        method: 'POST',
        headers: {
          'Content-Type': 'application/octet-stream',
          [REQUEST_KIND_HEADER]: REQUEST_KIND.SSE,
          ...(this.sessionToken ? { [TELEFUNC_SESSION_HEADER]: this.sessionToken } : undefined),
        },
        body: encodeSseRequest({
          connId: this.connId,
          ownerInstance: this.ownerInstance,
          channels: this.channels,
          batch: encodeLengthPrefixedFrames(frames.map((f) => this.aliasPrepend(f))),
        }),
        signal: this.transportAbort.signal,
      })
    } catch {
      // Best-effort — if this fails the connection will timeout and reconnect.
    }
  }

  private scheduleFlush(): void {
    if (this.outboxFrames.length === 0 || !this.hasActiveTransport()) return
    let earliest = Infinity
    for (const deadlineAt of this.outboxDeadlines) if (deadlineAt < earliest) earliest = deadlineAt
    this.flushScheduler.schedule(earliest)
  }

  private getFrameDeadline(kind: OutboundFrameKind, now = Date.now()): number {
    switch (kind) {
      case 'reconcile':
        return now + SSE_RECONCILE_DEADLINE_MS
      case 'control':
        return now
      case 'heartbeat':
        return now + this.heartbeatFlushDelayMs
      case 'ack':
      case 'data':
        return (
          now +
          (now - this.lastPostStartedAt >= this.flushThrottleMs ? this.postIdleFlushDelayMs : this.flushThrottleMs)
        )
    }
  }

  abandonActiveTransport(): void {
    const abortController = this.transportAbort
    if (!abortController) return
    this.transportAbort = null
    this.streamRequest = null
    this.closeAbandonedTransport()
    this.abandonedStream = abortController
    this.abandonedControllers.add(abortController)
    const cbs = this.drainCallbacks.splice(0)
    for (const cb of cbs) cb()
  }

  closeAbandonedTransport(): void {
    const abortController = this.abandonedStream
    if (!abortController) return
    this.abandonedStream = null
    abortController.abort()
  }

  applyReconciledSettings(ctrl: ReconciledPayload): void {
    if (ctrl.sseFlushThrottle) this.flushThrottleMs = ctrl.sseFlushThrottle
    if (ctrl.ssePostIdleFlushDelay) this.postIdleFlushDelayMs = ctrl.ssePostIdleFlushDelay
    this.heartbeatFlushDelayMs = Math.floor(ctrl.pingInterval / 2)
    // Refresh the routing table tagged onto every subsequent data POST. Every channel
    // the server attached gets an alias — `alias N` targets `channels[N − 1]`; alias 0
    // targets the owner. The wire push happens separately in `pushReconciledRouting`.
    this.ownerInstance = ctrl.ownerInstance
    this.channels = ctrl.open.map((entry) => ({ id: entry.id, home: entry.home }))
    this.ixToAlias = new Map(ctrl.open.map((entry, i) => [entry.ix, i + 1]))
  }

  /** Push the post-reconcile routing table onto the live stream-request body so the
   *  receiver routes subsequent frames against the new aliases. The stream-request POST
   *  stays open across reconciles: the first call emits metadata-at-start (`{ connId,
   *  ownerInstance, channels, streamRequest: true }`) consumed by the server's
   *  `parseSseRequestMetadata`; every later call emits an in-band refresh entry
   *  (`alias 0xFF` + JSON `channels[]`). `openStream` awaits the stream-request fetch
   *  before the SSE reader starts, so by the time this fires `this.streamRequest` is
   *  reliably set (or stays null if the stream-request POST failed). */
  pushReconciledRouting(): void {
    const sr = this.streamRequest
    if (!sr) return
    if (sr.metadataPushed) {
      const refresh = textEncoder.encode(JSON.stringify(this.channels))
      const entry = new Uint8Array(1 + refresh.byteLength)
      entry[0] = METADATA_REFRESH_ALIAS
      entry.set(refresh, 1)
      sr.body.push(encodeU32(entry.byteLength))
      sr.body.push(entry)
      return
    }
    // `streamRequest: true` tells the server this is a long-lived request body —
    // in-body reconciles get `reconciled` emitted inline (the body never ends, so we
    // can't defer to body-end like outbox batch POSTs do).
    const meta = textEncoder.encode(
      JSON.stringify({
        connId: this.connId,
        ownerInstance: this.ownerInstance,
        channels: this.channels,
        streamRequest: true,
      }),
    )
    sr.body.push(encodeU32(meta.byteLength))
    sr.body.push(meta)
    sr.metadataPushed = true
  }

  dispose(): void {
    this.connecting = false
    if (this.startTimer) {
      clearTimeout(this.startTimer)
      this.startTimer = null
    }
    this.flushScheduler.cancel()
    this.outboxFrames = []
    this.outboxDeadlines = []
    this.closeStreamRequest()
    this.transportAbort?.abort()
    this.transportAbort = null
    this.closeAbandonedTransport()
    const cbs = this.drainCallbacks.splice(0)
    for (const cb of cbs) cb()
  }

  // ── Persistent client→server stream-request POST (half-duplex streaming body) ──

  /** Fire the half-duplex stream-request POST. Pure: no shared-state mutation, no error
   *  handling — caller owns the body and decides what to do with the returned promise.
   *  Resolves on body-end (closed), rejects on fetch error. With browser fetch +
   *  `duplex:'half'` it never resolves while the body stays open. */
  private openStreamRequest(body: PushToPullStream<Uint8Array<ArrayBuffer>>, signal: AbortSignal): Promise<unknown> {
    return this.fetchImpl(getMarkedRequestUrl(this.telefuncUrl, REQUEST_KIND.SSE), {
      method: 'POST',
      headers: {
        'Content-Type': 'application/octet-stream',
        [REQUEST_KIND_HEADER]: REQUEST_KIND.SSE,
        ...(this.sessionToken ? { [TELEFUNC_SESSION_HEADER]: this.sessionToken } : undefined),
      },
      body: body.readable,
      signal,
      // @ts-ignore duplex is not yet in TypeScript's RequestInit
      duplex: 'half',
    })
  }

  private closeStreamRequest(): void {
    if (!this.streamRequest) return
    this.streamRequest.body.close()
    this.streamRequest = null
  }
}

// ── Transport registry ──

/** Maps each ChannelTransport to a factory that creates the corresponding ClientChannelTransport. */
const TRANSPORT_REGISTRY: Record<
  ChannelTransport,
  (telefuncUrl: string, options: ClientConnectionOptions, owner: ClientConnection) => ClientChannelTransport
> = {
  [CHANNEL_TRANSPORT.WS]: (telefuncUrl, _options, owner) => new WsTransport(telefuncUrl, owner),
  [CHANNEL_TRANSPORT.SSE]: (telefuncUrl, options, owner) =>
    new SseTransport(telefuncUrl, options.fetchImpl, options.sessionToken, owner),
}

/** Defines which transport can upgrade to which. */
const UPGRADE_PATH: Partial<Record<ChannelTransport, ChannelTransport>> = {
  [CHANNEL_TRANSPORT.SSE]: CHANNEL_TRANSPORT.WS,
}

function createSseEventStreamReader(
  reader: ReadableStreamDefaultReader<Uint8Array<ArrayBuffer>>,
  abortController: AbortController,
): {
  cancel: () => void
  readNextEntry: () => Promise<{ comment?: string; frame?: Uint8Array<ArrayBuffer> } | null>
} {
  const decoder = new TextDecoder()
  let lineBuf = ''
  let pendingComment: string | null = null
  let pendingData = ''
  let readyComment: string | null = null
  let readyFrame: Uint8Array<ArrayBuffer> | null = null
  let cancelled = false

  const cancel = () => {
    if (cancelled) return
    cancelled = true
    reader.cancel().catch(() => {})
  }

  abortController.signal.addEventListener('abort', cancel, { once: true })

  const processBufferedLines = () => {
    const lines = lineBuf.split('\n')
    if (lines.length === 1) return
    lineBuf = lines.pop()!

    for (let index = 0; index < lines.length; index++) {
      const line = lines[index]!
      if (line.startsWith(':')) {
        pendingComment = line
        continue
      }
      if (line.startsWith('data: ')) {
        pendingData = line.slice(6)
        continue
      }
      if (line !== '') continue

      if (pendingComment !== null) {
        readyComment = pendingComment
        pendingComment = null
      }
      if (pendingData !== '') {
        readyFrame = base64urlToUint8Array(pendingData)
        pendingData = ''
      }
      if (readyComment === null && readyFrame === null) continue

      const remainingLines = lines.slice(index + 1)
      if (remainingLines.length > 0) {
        lineBuf = `${remainingLines.join('\n')}\n${lineBuf}`
      }
      return
    }
  }

  const readNextEntry = async (): Promise<{ comment?: string; frame?: Uint8Array<ArrayBuffer> } | null> => {
    while (true) {
      if (readyComment !== null) {
        const comment = readyComment
        readyComment = null
        return { comment }
      }
      if (readyFrame !== null) {
        const frame = readyFrame
        readyFrame = null
        return { frame }
      }

      processBufferedLines()
      if (readyComment !== null || readyFrame !== null) continue

      let done: boolean
      let value: Uint8Array<ArrayBuffer> | undefined
      let readError: unknown
      try {
        ;({ done, value } = await reader.read())
      } catch (err) {
        readError = err
        done = true
      }
      if (done) {
        if (abortController.signal.aborted || cancelled) return null
        throw readError ?? new Error('Connection lost before all SSE frames were received.')
      }
      lineBuf += decoder.decode(value!, { stream: true })
    }
  }

  return { cancel, readNextEntry }
}
