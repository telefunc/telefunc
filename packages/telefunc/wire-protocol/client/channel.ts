export { ClientChannel, ClientBroadcast }

import type {
  ChannelAck,
  ClientChannel as ClientChannelType,
  ChannelCloseCallback,
  ChannelCloseOptions,
  ChannelCloseResult,
  ChannelData,
  ChannelListener,
  ChannelBinaryListener,
  ChannelPublishAck,
  BroadcastBinaryListener,
  BroadcastListener,
} from '../channel.js'
import type { TELEFUNC_SHIELDS } from '../../node/shared/transformer/generateShield/shield-key.js'
import { makePublishInfo } from '../channel.js'
import { parse } from '@brillout/json-serializer/parse'
import { stringify } from '@brillout/json-serializer/stringify'
import { resolveClientConfig } from '../../client/clientConfig.js'
import { createAbortError, isAbort } from '../../shared/Abort.js'
import {
  ACK_STATUS,
  TAG,
  isChannelCtrlTag,
  type AckResultStatus,
  type ChannelCtrlFrame,
  type ChannelDataFrame,
  type ChannelFrame,
  type WirePublishInfo,
} from '../shared-ws.js'
import { assert } from '../../utils/assert.js'
import { makeAbortError, makeBugError } from '../../client/remoteTelefunctionCall/errors.js'
import { ShieldValidationError } from '../../shared/ShieldValidationError.js'
import { ClientConnection } from './connection.js'
import { appendSessionParam, getSessionToken } from './session-registry.js'
import { CHANNEL_CLOSE_TIMEOUT_MS, type ChannelTransports } from '../constants.js'
import { FlowControl } from '../flow-control/flow-control.js'
import type { MuxChannel, MuxConnection } from './connection.js'
import { ChannelClosedError } from '../channel-errors.js'
import { isPromise } from '../../utils/isPromise.js'
import { hasProp } from '../../utils/hasProp.js'

const CLIENT_BROADCAST_BRAND = Symbol.for('ClientBroadcast')

class ClientChannel<ClientToServer = unknown, ServerToClient = unknown>
  implements ClientChannelType<ClientToServer, ServerToClient>, MuxChannel
{
  /** @see ChannelShield in ../channel.ts — `data` validates C2S; `ack` validates ack of own C2S send. */
  declare readonly [TELEFUNC_SHIELDS]: {
    data: ChannelData<ClientToServer>
    ack: ChannelAck<ServerToClient>
  }
  readonly id: string
  readonly ack: boolean
  readonly key: string | undefined
  protected _connection: MuxConnection
  private _listeners: Array<ChannelListener<ServerToClient>> = []
  private _binaryListeners: Array<ChannelBinaryListener> = []
  private _openCallbacks: Array<() => void> = []
  private _closeCallbacks: Array<ChannelCloseCallback> = []
  private _closeError: Error | undefined
  protected _isClosed = false
  private _didTerminate = false
  private _didFireClose = false
  private _didFireOpen = false
  private _closePromise: Promise<ChannelCloseResult> | null = null
  private _closeDeadline = 0
  private _closeWaiters: Array<() => void> = []
  private _didReceiveCloseAck = false
  private _expectCloseAck = false
  private _pendingCloseCallbacks = 0
  protected _inflightAcks = 0
  /** Outstanding ack-bearing sends, keyed by the seq the server will echo on `ACK_RES`.
   *  Lives on the channel (mirrors server-side `_pendingAcks`) so per-channel state
   *  stays encapsulated; connection just routes ACK_RES frames in. `resolve` is typed
   *  `any` because the channel hosts ack-bearing sends with several distinct resolution
   *  types (`ChannelAck<C2S>`, `unknown` for binary, `ChannelPublishAck` for broadcast
   *  publishes); the stored resolver is always invoked with `parse(text)` at the boundary. */
  protected _pendingAcks = new Map<number, { resolve: (v: any) => void; reject: (err: Error) => void }>()
  /** Owns sender-side credit, receiver-side consumption tracking, BDP estimator,
   *  and the queue of senders blocked on credit refresh. Credit governs fire-and-
   *  forget TEXT/BINARY only — see `constants.ts`. */
  private _flow: FlowControl

  constructor({
    channelId,
    ack = false,
    key,
    transports,
    connectionKey,
    headers,
    telefuncUrl,
    idleTimeout,
  }: {
    channelId: string
    ack?: boolean
    key?: string
    transports: ChannelTransports
    connectionKey?: string
    headers?: Record<string, string>
    telefuncUrl: string
    idleTimeout?: number
  }) {
    this.id = channelId
    this.ack = ack
    this.key = key
    this._flow = new FlowControl({
      byteWindowUpdate: (bytes) => this._connection.sendByteWindowUpdate(this, bytes),
      msgWindowUpdate: (count) => this._connection.sendMsgWindowUpdate(this, count),
      bdpPing: () => this._connection.sendBdpPing(this),
    })
    const config = resolveClientConfig()
    // Read the latest stored session token for this URL. `makeHttpRequest` writes the freshest
    // server-issued token to the registry before this constructor runs (on the response side),
    // and the request side reads whatever was stored from the prior round-trip. Either way,
    // querying here keeps the channel's transport in sync with whatever HTTP is using.
    const sessionToken = getSessionToken(telefuncUrl)
    const url = sessionToken ? appendSessionParam(telefuncUrl, sessionToken) : telefuncUrl
    this._connection = ClientConnection.getOrCreate(url, this, {
      transports,
      fetchImpl: (config.fetch ?? globalThis.fetch).bind(globalThis),
      sessionToken,
      connectionKey,
      headers,
      idleTimeout,
    })
  }

  get isClosed(): boolean {
    return this._isClosed
  }

  send(data: ChannelData<ClientToServer>): Promise<void>
  send(data: ChannelData<ClientToServer>, opts: { ack: true }): Promise<ChannelAck<ClientToServer>>
  send(data: ChannelData<ClientToServer>, opts: { ack: false }): Promise<void>
  send(
    data: ChannelData<ClientToServer>,
    opts?: { ack?: boolean },
  ): Promise<ChannelAck<ClientToServer>> | Promise<void> {
    const t0 = performance.now()
    try {
      const ret = this._send(data, opts) ?? Promise.resolve()
      ret.catch(() => {})
      return ret
    } finally {
      this._flow._recordSelfTime(performance.now() - t0)
    }
  }

  _send(
    data: ChannelData<ClientToServer>,
    opts?: { ack?: boolean },
  ): void | Promise<ChannelAck<ClientToServer>> | Promise<void> {
    if (this._isClosed) throw new ChannelClosedError()
    const needsAck = opts?.ack !== false && (opts?.ack === true || this.ack === true)
    const serialized = stringify(data)
    // Ack-bearing sends bypass credit accounting — the caller's `await` on the ack
    // Promise already serializes the next send, so credit would add nothing.
    if (needsAck) {
      return this._trackAck(
        new Promise<ChannelAck<ClientToServer>>((resolve, reject) => {
          this._connection.sendTextAckReq(this, serialized, (seq) => {
            this._pendingAcks.set(seq, { resolve, reject })
          })
        }),
      )
    }
    return this._waitForWindow(this._connection.send(this, serialized))
  }

  sendBinary(data: Uint8Array): Promise<void>
  sendBinary(data: Uint8Array, opts: { ack: true }): Promise<unknown>
  sendBinary(data: Uint8Array, opts: { ack: false }): Promise<void>
  sendBinary(data: Uint8Array, opts?: { ack?: boolean }): Promise<unknown> | Promise<void> {
    const t0 = performance.now()
    try {
      const ret = this._sendBinary(data, opts) ?? Promise.resolve()
      ret.catch(() => {})
      return ret
    } finally {
      this._flow._recordSelfTime(performance.now() - t0)
    }
  }

  _sendBinary(data: Uint8Array, opts?: { ack?: boolean }): void | Promise<unknown> | Promise<void> {
    if (this._isClosed) throw new ChannelClosedError()
    // Ack-bearing path bypasses credit; see `_send` for rationale.
    if (opts?.ack === true) {
      return this._trackAck(
        new Promise<unknown>((resolve, reject) => {
          this._connection.sendBinaryAckReq(this, data, (seq) => {
            this._pendingAcks.set(seq, { resolve, reject })
          })
        }),
      )
    }
    this._connection.sendBinary(this, data)
    return this._waitForWindow(data.byteLength)
  }

  /** Decrement credit by `bytes`; return void if credit remains, otherwise a Promise
   *  that resolves when credit refreshes (so the caller's next `await` blocks).
   *
   *  Cooperative model: the send already fired in `connection.send(...)`; this only
   *  gates the return value. Awaiting throttles the caller's next send; not awaiting
   *  bypasses (matches the user-facing "skip await for max bandwidth" escape hatch). */
  private _waitForWindow(bytes: number): void | Promise<void> {
    return this._flow.decrement(bytes)
  }

  listen(callback: ChannelListener<ServerToClient>): () => void {
    this._listeners.push(callback)
    return () => {
      const i = this._listeners.indexOf(callback)
      if (i >= 0) this._listeners.splice(i, 1)
    }
  }

  listenBinary(callback: ChannelBinaryListener): () => void {
    this._binaryListeners.push(callback)
    return () => {
      const i = this._binaryListeners.indexOf(callback)
      if (i >= 0) this._binaryListeners.splice(i, 1)
    }
  }

  onClose(callback: ChannelCloseCallback): void {
    if (this._didFireClose) {
      this._invokeCloseCallback(callback, this._closeError, false)
      return
    }
    this._closeCallbacks.push(callback)
  }

  onOpen(callback: () => void): void {
    if (this._didFireOpen) {
      callback()
      return
    }
    this._openCallbacks.push(callback)
  }

  close(opts?: ChannelCloseOptions): Promise<ChannelCloseResult> {
    if (this._closePromise) return this._closePromise
    if (this._didTerminate) return Promise.resolve(this._didReceiveCloseAck ? 0 : 1)
    const timeout = normalizeCloseTimeout(opts?.timeout)
    this._closeDeadline = Date.now() + timeout
    this._expectCloseAck = true
    this._isClosed = true
    this._connection.sendCloseRequest(this, timeout)
    this._closePromise = this._runFinalizationLoop()
    return this._closePromise
  }

  abort(): void
  abort(abortValue: unknown, message?: string): void
  abort(abortValue?: unknown, message?: string): void {
    if (this._didTerminate || this._isClosed) return
    this._isClosed = true
    const abortError = createAbortError(abortValue, message)
    this._connection.sendAbort(this)
    this._finalizeClose(abortError)
  }

  // ── Called by transport connection ──

  _onTransportOpen(batched: boolean): void {
    if (this._isClosed) return
    this._flow.reset()
    if (batched) this._flow.useBatchTransportInitial()
    this._fireOpen()
  }

  _onTransportMessage(data: string, bytes: number): void {
    const t0 = performance.now()
    try {
      this._flow.onReceived(bytes)
      const parsed = parse(data) as ChannelData<ServerToClient>
      const pending: Promise<unknown>[] = []
      for (const cb of this._listeners) {
        try {
          const result = cb(parsed)
          if (isPromise(result)) {
            pending.push(result.catch((err: unknown) => this._handleCallbackError(err)))
          }
        } catch (err) {
          if (this._handleCallbackError(err)) return
        }
      }
      if (pending.length > 0) {
        Promise.all(pending).finally(() => this._flow.onConsumed(bytes))
      } else {
        this._flow.onConsumed(bytes)
      }
    } finally {
      this._flow._recordSelfTime(performance.now() - t0)
    }
  }

  _onTransportAckReqMessage(data: string, seq: number): Promise<void> {
    return this._trackAck(this._dispatchAckReq(data, seq))
  }

  _onTransportBinaryAckReqMessage(data: Uint8Array, seq: number): Promise<void> {
    return this._trackAck(this._dispatchBinaryAckReq(data, seq))
  }

  _onTransportBinaryMessage(data: Uint8Array<ArrayBuffer>): void {
    const t0 = performance.now()
    const bytes = data.byteLength
    try {
      this._flow.onReceived(bytes)
      const pending: Promise<unknown>[] = []
      for (const cb of this._binaryListeners) {
        try {
          const result = cb(data)
          if (isPromise(result)) {
            pending.push(result.catch((err: unknown) => this._handleCallbackError(err)))
          }
        } catch (err) {
          if (this._handleCallbackError(err)) return
        }
      }
      if (pending.length > 0) {
        Promise.all(pending).finally(() => this._flow.onConsumed(bytes))
      } else {
        this._flow.onConsumed(bytes)
      }
    } finally {
      this._flow._recordSelfTime(performance.now() - t0)
    }
  }

  /** @internal — Entry point for every per-channel wire frame. The connection routes
   *  here for any frame carrying an index; this method then splits ctrl vs data. */
  _dispatchFrame(frame: ChannelFrame): void {
    if (isChannelCtrlTag(frame.tag)) {
      this._dispatchCtrl(frame as ChannelCtrlFrame)
      return
    }
    this._dispatchDataFrame(frame as ChannelDataFrame)
  }

  /** @internal — Tag-keyed data-frame switch. `ClientBroadcast` overrides to add the
   *  PUBLISH variants and falls back to `super` for the common cases. Receive-time
   *  BDP accounting happens inside each credit-counted `_onTransport*` handler. */
  protected _dispatchDataFrame(frame: ChannelDataFrame): void {
    switch (frame.tag) {
      case TAG.TEXT:
        this._onTransportMessage(frame.text, frame.bytes)
        return
      case TAG.TEXT_ACK_REQ:
        void this._onTransportAckReqMessage(frame.text, frame.seq)
        return
      case TAG.BINARY:
        this._onTransportBinaryMessage(frame.data as Uint8Array<ArrayBuffer>)
        return
      case TAG.BINARY_ACK_REQ:
        void this._onTransportBinaryAckReqMessage(frame.data, frame.seq)
        return
      case TAG.ACK_RES:
        this._onPeerAckRes(frame.ackedSeq, frame.text, frame.status)
        return
      case TAG.PUBLISH:
      case TAG.PUBLISH_ACK_REQ:
      case TAG.PUBLISH_BINARY:
      case TAG.PUBLISH_BINARY_ACK_REQ:
        // PUBLISH variants land here only on `ClientBroadcast`; non-broadcast channels drop them.
        return
    }
  }

  /** @internal — Per-channel ctrl-message switch. Connection-level ctrls (PING/PONG/
   *  FIN/RECONCILED) and channel-termination ctrls (ABORT/ERROR — which involve
   *  connection-side cleanup) are handled by the connection and never reach here. */
  protected _dispatchCtrl(frame: ChannelCtrlFrame): void {
    switch (frame.tag) {
      case TAG.CLOSE:
        this._onTransportCloseRequest(frame.timeoutMs)
        return
      case TAG.CLOSE_ACK:
        this._onTransportCloseAck()
        return
      case TAG.WINDOW:
        this._flow.onPeerByteWindow(frame.bytes)
        return
      case TAG.MSG_WINDOW:
        this._flow.onPeerMessageWindow(frame.count)
        return
      case TAG.BDP_PING:
        this._connection.sendBdpPingAck(this)
        return
      case TAG.BDP_PING_ACK:
        this._flow.onPingAck()
        return
      // BROADCAST_SUB/UNSUB are server-side only; ABORT/ERROR handled by connection.
    }
  }

  /** Settle an outstanding ack-bearing send. Mirrors server-side `_onPeerAckRes`. */
  protected _onPeerAckRes(ackedSeq: number, text: string, status: AckResultStatus): void {
    const pending = this._pendingAcks.get(ackedSeq)
    if (!pending) return
    this._pendingAcks.delete(ackedSeq)
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
        pending.reject(new ShieldValidationError(text))
    }
  }

  _onTransportCloseRequest(timeoutMs: number): void {
    if (this._didTerminate) return
    const peerDeadline = Date.now() + normalizeCloseTimeout(timeoutMs)
    if (!this._closeDeadline || peerDeadline < this._closeDeadline) this._closeDeadline = peerDeadline
    this._connection.sendCloseAck(this)
    if (this._isClosed) {
      this._notifyCloseProgress()
    } else {
      this._isClosed = true
      void this._runFinalizationLoop()
    }
  }

  _onTransportCloseAck(): void {
    if (this._didTerminate) return
    this._didReceiveCloseAck = true
    this._expectCloseAck = false
    this._notifyCloseProgress()
  }

  _onTransportClose(err?: Error): void {
    this._isClosed = true
    this._finalizeClose(err)
  }

  // ── Private ──

  private _fireOpen(): void {
    if (this._didFireOpen) return
    this._didFireOpen = true
    for (const cb of this._openCallbacks) {
      try {
        cb()
      } catch (err) {
        if (this._handleCallbackError(err)) return
      }
    }
    this._openCallbacks.length = 0
  }

  private _fireClose(err?: Error): void {
    if (this._didFireClose) return
    this._didFireClose = true
    for (const cb of this._closeCallbacks) {
      this._invokeCloseCallback(cb, err, true)
    }
    this._closeCallbacks.length = 0
    this._openCallbacks.length = 0
    this._notifyCloseProgress()
  }

  private async _runFinalizationLoop(): Promise<ChannelCloseResult> {
    while (!this._didTerminate) {
      // Fire onClose only once the close roundtrip is settled and no inbound work is in flight,
      // so listeners see "closed" only after the channel can no longer receive frames.
      if (this._isCloseRoundtripDone()) this._fireClose()
      if (this._didFireClose && this._pendingCloseCallbacks === 0) {
        this._finalizeClose()
        break
      }
      const remaining = this._closeDeadline - Date.now()
      if (remaining <= 0) {
        this._finalizeClose(new ChannelClosedError('Channel close timed out'))
        break
      }
      await this._waitForCloseProgress(remaining)
    }
    return this._didReceiveCloseAck ? 0 : 1
  }

  private _isCloseRoundtripDone(): boolean {
    return this._inflightAcks === 0 && (!this._expectCloseAck || this._didReceiveCloseAck)
  }

  private _waitForCloseProgress(timeoutMs: number): Promise<void> {
    return new Promise<void>((resolve) => {
      let settled = false
      const timer = setTimeout(() => {
        if (settled) return
        settled = true
        const index = this._closeWaiters.indexOf(wake)
        if (index >= 0) this._closeWaiters.splice(index, 1)
        resolve()
      }, timeoutMs)
      const wake = () => {
        if (settled) return
        settled = true
        clearTimeout(timer)
        resolve()
      }
      this._closeWaiters.push(wake)
    })
  }

  private _notifyCloseProgress(): void {
    const waiters = this._closeWaiters.splice(0)
    for (const waiter of waiters) waiter()
  }

  private async _dispatchAckReq(data: string, seq: number): Promise<void> {
    if (this._listeners.length === 0) {
      this._connection.sendAckRes(this, seq, 'No listener registered for ack request', ACK_STATUS.ERROR)
      return
    }
    const parsed = parse(data) as ChannelData<ServerToClient>
    let lastResult: unknown
    for (const cb of this._listeners) {
      try {
        lastResult = await cb(parsed)
      } catch (err) {
        if (this._handleCallbackError(err)) return
        this._connection.sendAckRes(this, seq, 'Internal client channel error — see client logs', ACK_STATUS.ERROR)
        return
      }
    }
    this._connection.sendAckRes(this, seq, stringify(lastResult))
  }

  private async _dispatchBinaryAckReq(data: Uint8Array, seq: number): Promise<void> {
    if (this._binaryListeners.length === 0) {
      this._connection.sendAckRes(this, seq, 'No listener registered for ack request', ACK_STATUS.ERROR)
      return
    }
    let lastResult: unknown
    for (const cb of this._binaryListeners) {
      try {
        lastResult = await cb(data)
      } catch (err) {
        if (this._handleCallbackError(err)) return
        this._connection.sendAckRes(this, seq, 'Internal client channel error — see client logs', ACK_STATUS.ERROR)
        return
      }
    }
    this._connection.sendAckRes(this, seq, stringify(lastResult))
  }

  protected _trackAck<T>(promise: Promise<T>): Promise<T> {
    this._inflightAcks++
    return promise.finally(() => {
      this._inflightAcks--
      this._notifyCloseProgress()
    })
  }

  private _finalizeClose(err?: Error): void {
    if (this._didTerminate) return
    this._didTerminate = true
    this._closeError = err
    this._flow.shutdown()
    const ackErr = err ?? new ChannelClosedError()
    for (const { reject } of this._pendingAcks.values()) reject(ackErr)
    this._pendingAcks.clear()
    this._connection.unregister(this, ackErr)
    this._fireClose(err)
    this._notifyCloseProgress()
  }

  private _invokeCloseCallback(callback: ChannelCloseCallback, err: Error | undefined, track: boolean): void {
    try {
      const pending = callback(err)
      if (!isPromise(pending)) return
      if (track) {
        this._pendingCloseCallbacks++
        void pending.catch(reportChannelError).finally(() => {
          this._pendingCloseCallbacks--
          this._notifyCloseProgress()
        })
      } else {
        void pending.catch(reportChannelError)
      }
    } catch (callbackErr) {
      reportChannelError(callbackErr)
    }
  }

  protected _handleCallbackError(err: unknown): boolean {
    if (isAbort(err)) {
      const abortError = err
      this.abort(abortError.abortValue, abortError.message)
      return true
    }
    reportChannelError(err)
    return false
  }
}

class ClientBroadcast<T = unknown> extends ClientChannel {
  readonly [CLIENT_BROADCAST_BRAND] = true
  private _broadcastListeners: Array<BroadcastListener<T>> = []
  private _broadcastBinaryListeners: Array<BroadcastBinaryListener> = []

  static isClientBroadcast(value: unknown): value is ClientBroadcast {
    return hasProp(value, CLIENT_BROADCAST_BRAND)
  }

  publish(data: ChannelData<T>): Promise<ChannelPublishAck> {
    if (this._isClosed) throw new ChannelClosedError()
    const serialized = stringify(data)
    const ret = this._trackAck(
      new Promise<ChannelPublishAck>((resolve, reject) => {
        this._connection.sendPublishAckReq(this, serialized, (seq) => {
          this._pendingAcks.set(seq, { resolve, reject })
        })
      }),
    )
    ret.catch(reportChannelError)
    return ret
  }

  subscribe(callback: BroadcastListener<T>): () => void {
    if (this._broadcastListeners.length === 0) {
      this._connection.sendBroadcastSubscribe(this, false)
    }
    this._broadcastListeners.push(callback)
    return () => {
      const index = this._broadcastListeners.indexOf(callback)
      if (index >= 0) this._broadcastListeners.splice(index, 1)
      if (this._broadcastListeners.length === 0) {
        this._connection.sendBroadcastUnsubscribe(this, false)
      }
    }
  }

  publishBinary(data: Uint8Array): Promise<ChannelPublishAck> {
    if (this._isClosed) throw new ChannelClosedError()
    const ret = this._trackAck(
      new Promise<ChannelPublishAck>((resolve, reject) => {
        this._connection.sendPublishBinaryAckReq(this, data, (seq) => {
          this._pendingAcks.set(seq, { resolve, reject })
        })
      }),
    )
    ret.catch(reportChannelError)
    return ret
  }

  subscribeBinary(callback: BroadcastBinaryListener): () => void {
    if (this._broadcastBinaryListeners.length === 0) {
      this._connection.sendBroadcastSubscribe(this, true)
    }
    this._broadcastBinaryListeners.push(callback)
    return () => {
      const index = this._broadcastBinaryListeners.indexOf(callback)
      if (index >= 0) this._broadcastBinaryListeners.splice(index, 1)
      if (this._broadcastBinaryListeners.length === 0) {
        this._connection.sendBroadcastUnsubscribe(this, true)
      }
    }
  }

  override _dispatchDataFrame(frame: ChannelDataFrame): void {
    if (frame.tag === TAG.PUBLISH) {
      this._onTransportPublish(frame.text, frame.info)
      return
    }
    if (frame.tag === TAG.PUBLISH_BINARY) {
      this._onTransportPublishBinary(frame.data as Uint8Array<ArrayBuffer>, frame.info)
      return
    }
    super._dispatchDataFrame(frame)
  }

  _onTransportPublish(data: string, wireInfo: WirePublishInfo): void {
    const parsed = parse(data) as ChannelData<T>
    const info = makePublishInfo(this.key!, wireInfo.seq, wireInfo.ts)
    for (const cb of this._broadcastListeners) {
      try {
        cb(parsed, info)
      } catch (err) {
        if (this._handleCallbackError(err)) return
      }
    }
  }

  _onTransportPublishBinary(data: Uint8Array, wireInfo: WirePublishInfo): void {
    const info = makePublishInfo(this.key!, wireInfo.seq, wireInfo.ts)
    for (const cb of this._broadcastBinaryListeners) {
      try {
        cb(data, info)
      } catch (err) {
        if (this._handleCallbackError(err)) return
      }
    }
  }
}

function reportChannelError(err: unknown): void {
  console.error('[telefunc:channel-error]', err instanceof Error ? err : new Error(String(err)))
}

function normalizeCloseTimeout(timeout: number | undefined): number {
  if (timeout === undefined) return CHANNEL_CLOSE_TIMEOUT_MS
  if (!Number.isFinite(timeout) || timeout < 0)
    throw new Error('Channel close timeout must be a non-negative finite number')
  return timeout
}
