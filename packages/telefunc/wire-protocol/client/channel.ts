export { ClientChannel, ClientPubSub }

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
  PubSubBinaryListener,
  PubSubListener,
} from '../channel.js'
import { makePublishInfo } from '../channel.js'
import { parse } from '@brillout/json-serializer/parse'
import { stringify } from '@brillout/json-serializer/stringify'
import { resolveClientConfig } from '../../client/clientConfig.js'
import { createAbortError, isAbort } from '../../shared/Abort.js'
import type { WirePublishInfo } from '../shared-ws.js'
import { ClientConnection } from './connection.js'
import {
  CHANNEL_CLOSE_TIMEOUT_MS,
  CREDIT_WINDOW_BYTES,
  WINDOW_UPDATE_THRESHOLD_BYTES,
  type ChannelTransports,
} from '../constants.js'
import type { MuxChannel, MuxConnection } from './connection.js'
import { ChannelClosedError } from '../channel-errors.js'
import { utf8ByteLength } from '../../utils/utf8ByteLength.js'
import { isPromise } from '../../utils/isPromise.js'
import { hasProp } from '../../utils/hasProp.js'

const CLIENT_PUBSUB_BRAND = Symbol.for('ClientPubSub')

class ClientChannel<ClientToServer = unknown, ServerToClient = unknown>
  implements ClientChannelType<ClientToServer, ServerToClient>, MuxChannel
{
  readonly id: string
  readonly ack: boolean
  readonly defer: boolean
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
  private _peerWindow: number = CREDIT_WINDOW_BYTES
  private _consumedBytes = 0
  private _sendWaiters: Array<() => void> = []

  constructor({
    channelId,
    ack = false,
    key,
    transports,
    sessionToken,
    defer = false,
  }: {
    channelId: string
    ack?: boolean
    key?: string
    transports: ChannelTransports
    sessionToken?: string
    defer?: boolean
  }) {
    this.id = channelId
    this.ack = ack
    this.key = key
    this.defer = defer
    const config = resolveClientConfig()
    const url = sessionToken ? appendSessionParam(config.telefuncUrl, sessionToken) : config.telefuncUrl
    this._connection = ClientConnection.getOrCreate(url, this, {
      transports,
      fetchImpl: (config.fetch ?? globalThis.fetch).bind(globalThis),
      sessionToken,
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
    const ret = this._send(data, opts) ?? Promise.resolve()
    ret.catch(() => {})
    return ret
  }

  _send(
    data: ChannelData<ClientToServer>,
    opts?: { ack?: boolean },
  ): void | Promise<ChannelAck<ClientToServer>> | Promise<void> {
    if (this._isClosed) throw new ChannelClosedError()
    const needsAck = opts?.ack !== false && (opts?.ack === true || this.ack === true)
    const serialized = stringify(data, { forbidReactElements: false })
    if (needsAck) {
      return this._trackAck(this._connection.sendTextAckReq(this, serialized) as Promise<ChannelAck<ClientToServer>>)
    }
    this._connection.send(this, serialized)
    return this._waitForWindow(utf8ByteLength(serialized))
  }

  sendBinary(data: Uint8Array): Promise<void>
  sendBinary(data: Uint8Array, opts: { ack: true }): Promise<unknown>
  sendBinary(data: Uint8Array, opts: { ack: false }): Promise<void>
  sendBinary(data: Uint8Array, opts?: { ack?: boolean }): Promise<unknown> | Promise<void> {
    const ret = this._sendBinary(data, opts) ?? Promise.resolve()
    ret.catch(() => {})
    return ret
  }

  _sendBinary(data: Uint8Array, opts?: { ack?: boolean }): void | Promise<unknown> | Promise<void> {
    if (this._isClosed) throw new ChannelClosedError()
    if (opts?.ack === true) {
      return this._trackAck(this._connection.sendBinaryAckReq(this, data) as Promise<unknown>)
    }
    this._connection.sendBinary(this, data)
    return this._waitForWindow(data.byteLength)
  }

  private _waitForWindow(bytes: number): void | Promise<void> {
    this._peerWindow -= bytes
    if (this._peerWindow > 0) return
    return new Promise<void>((resolve) => {
      this._sendWaiters.push(resolve)
    })
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
    this._startClose()
    this._connection.sendCloseRequest(this, timeout)
    this._closePromise = this._runFinalizationLoop()
    return this._closePromise
  }

  abort(): void {
    this._abortWithValue()
  }

  _abortWithValue(abortValue?: unknown, message?: string): void {
    if (this._didTerminate || this._isClosed) return
    this._isClosed = true
    const abortError = createAbortError(abortValue, message)
    this._connection.sendAbort(this)
    this._finalizeClose(abortError)
  }

  _sendWindowUpdate(bytes: number): void {
    this._connection.sendWindowUpdate(this, bytes)
  }

  // ── Called by transport connection ──

  _onTransportOpen(): void {
    if (this._isClosed) return
    this._peerWindow = CREDIT_WINDOW_BYTES
    this._notifySendReady()
    this._fireOpen()
  }

  _onTransportMessage(data: string): void {
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
    const bytes = utf8ByteLength(data)
    if (pending.length > 0) {
      Promise.all(pending).finally(() => this._trackConsumption(bytes))
    } else {
      this._trackConsumption(bytes)
    }
  }

  _onTransportAckReqMessage(data: string, seq: number): Promise<void> {
    return this._trackAck(this._dispatchAckReq(data, seq))
  }

  _onTransportBinaryAckReqMessage(data: Uint8Array, seq: number): Promise<void> {
    return this._trackAck(this._dispatchBinaryAckReq(data, seq))
  }

  _onTransportBinaryMessage(data: Uint8Array<ArrayBuffer>): void {
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
    const bytes = data.byteLength
    if (pending.length > 0) {
      Promise.all(pending).finally(() => this._trackConsumption(bytes))
    } else {
      this._trackConsumption(bytes)
    }
  }

  _onPeerWindowUpdate(bytes: number): void {
    this._peerWindow = bytes
    this._notifySendReady()
  }

  private _notifySendReady(): void {
    const waiters = this._sendWaiters.splice(0)
    for (const waiter of waiters) waiter()
  }

  private _trackConsumption(bytes: number): void {
    this._consumedBytes += bytes
    if (this._consumedBytes >= WINDOW_UPDATE_THRESHOLD_BYTES) {
      this._consumedBytes = 0
      this._sendWindowUpdate(CREDIT_WINDOW_BYTES)
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
      this._startClose()
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
    const sendWaiters = this._sendWaiters.splice(0)
    for (const waiter of sendWaiters) waiter()
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

  private _startClose(err?: Error): void {
    if (this._isClosed) return
    this._isClosed = true
    this._fireClose(err)
  }

  private async _runFinalizationLoop(): Promise<ChannelCloseResult> {
    while (!this._didTerminate) {
      if (this._isCloseWorkComplete()) {
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
    try {
      if (this._listeners.length === 0) {
        this._connection.sendAckRes(this, seq, 'No listener registered for ack request', 'error')
        return
      }
      const parsed = parse(data) as ChannelData<ServerToClient>
      let lastResult: unknown
      for (const cb of this._listeners) {
        try {
          lastResult = await cb(parsed)
        } catch (err) {
          if (this._handleCallbackError(err)) return
          this._connection.sendAckRes(this, seq, 'Internal client channel error — see client logs', 'error')
          return
        }
      }
      this._connection.sendAckRes(this, seq, stringify(lastResult, { forbidReactElements: false }))
    } finally {
      this._trackConsumption(utf8ByteLength(data))
    }
  }

  private async _dispatchBinaryAckReq(data: Uint8Array, seq: number): Promise<void> {
    try {
      if (this._binaryListeners.length === 0) {
        this._connection.sendAckRes(this, seq, 'No listener registered for ack request', 'error')
        return
      }
      let lastResult: unknown
      for (const cb of this._binaryListeners) {
        try {
          lastResult = await cb(data)
        } catch (err) {
          if (this._handleCallbackError(err)) return
          this._connection.sendAckRes(this, seq, 'Internal client channel error — see client logs', 'error')
          return
        }
      }
      this._connection.sendAckRes(this, seq, stringify(lastResult, { forbidReactElements: false }))
    } finally {
      this._trackConsumption(data.byteLength)
    }
  }

  protected _trackAck<T>(promise: Promise<T>): Promise<T> {
    this._inflightAcks++
    return promise.finally(() => {
      this._inflightAcks--
      this._notifyCloseProgress()
    })
  }

  private _isCloseWorkComplete(): boolean {
    return (
      this._inflightAcks === 0 &&
      this._pendingCloseCallbacks === 0 &&
      (!this._expectCloseAck || this._didReceiveCloseAck)
    )
  }

  private _finalizeClose(err?: Error): void {
    if (this._didTerminate) return
    this._didTerminate = true
    this._closeError = err
    this._connection.unregister(this, err ?? new ChannelClosedError())
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
      this._abortWithValue(abortError.abortValue, abortError.message)
      return true
    }
    reportChannelError(err)
    return false
  }
}

class ClientPubSub<T = unknown> extends ClientChannel {
  readonly [CLIENT_PUBSUB_BRAND] = true
  private _pubSubListeners: Array<PubSubListener<T>> = []
  private _pubSubBinaryListeners: Array<PubSubBinaryListener> = []

  static isClientPubSub(value: unknown): value is ClientPubSub {
    return hasProp(value, CLIENT_PUBSUB_BRAND)
  }

  publish(data: ChannelData<T>): Promise<ChannelPublishAck> {
    if (this._isClosed) throw new ChannelClosedError()
    const serialized = stringify(data, { forbidReactElements: false })
    const ret = this._trackAck(this._connection.sendPublishAckReq(this, serialized) as Promise<ChannelPublishAck>)
    ret.catch(reportChannelError)
    return ret
  }

  subscribe(callback: PubSubListener<T>): () => void {
    if (this._pubSubListeners.length === 0) {
      this._connection.sendPubSubSubscribe(this, false)
    }
    this._pubSubListeners.push(callback)
    return () => {
      const index = this._pubSubListeners.indexOf(callback)
      if (index >= 0) this._pubSubListeners.splice(index, 1)
      if (this._pubSubListeners.length === 0) {
        this._connection.sendPubSubUnsubscribe(this, false)
      }
    }
  }

  publishBinary(data: Uint8Array): Promise<ChannelPublishAck> {
    if (this._isClosed) throw new ChannelClosedError()
    const ret = this._trackAck(this._connection.sendPublishBinaryAckReq(this, data) as Promise<ChannelPublishAck>)
    ret.catch(reportChannelError)
    return ret
  }

  subscribeBinary(callback: PubSubBinaryListener): () => void {
    if (this._pubSubBinaryListeners.length === 0) {
      this._connection.sendPubSubSubscribe(this, true)
    }
    this._pubSubBinaryListeners.push(callback)
    return () => {
      const index = this._pubSubBinaryListeners.indexOf(callback)
      if (index >= 0) this._pubSubBinaryListeners.splice(index, 1)
      if (this._pubSubBinaryListeners.length === 0) {
        this._connection.sendPubSubUnsubscribe(this, true)
      }
    }
  }

  _onTransportPublish(data: string, wireInfo: WirePublishInfo): void {
    const parsed = parse(data) as ChannelData<T>
    const info = makePublishInfo(this.key!, wireInfo.seq, wireInfo.ts)
    for (const cb of this._pubSubListeners) {
      try {
        cb(parsed, info)
      } catch (err) {
        if (this._handleCallbackError(err)) return
      }
    }
  }

  _onTransportPublishBinary(data: Uint8Array, wireInfo: WirePublishInfo): void {
    const info = makePublishInfo(this.key!, wireInfo.seq, wireInfo.ts)
    for (const cb of this._pubSubBinaryListeners) {
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

function appendSessionParam(url: string, token: string): string {
  return url.includes('?') ? `${url}&session=${token}` : `${url}?session=${token}`
}
