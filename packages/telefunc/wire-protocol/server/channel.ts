export { channel, getChannelRegistry, onChannelCreated, setChannelDefaults, ServerChannel, SERVER_CHANNEL_BRAND }
export { ChannelClosedError, ChannelNetworkError, ChannelOverflowError } from '../channel-errors.js'

const SERVER_CHANNEL_BRAND = Symbol.for('ServerChannel')

import type {
  Channel,
  ChannelAck,
  ClientChannel,
  ChannelCloseCallback,
  ChannelCloseOptions,
  ChannelCloseResult,
  ChannelData,
  ChannelListener,
} from '../channel.js'
import type { IndexedPeer } from './IndexedPeer.js'
import { stringify } from '@brillout/json-serializer/stringify'
import { parse } from '@brillout/json-serializer/parse'
import { getGlobalObject } from '../../utils/getGlobalObject.js'
import { hasProp } from '../../utils/hasProp.js'
import { unrefTimer } from '../../utils/unrefTimer.js'
import { assertUsage } from '../../utils/assert.js'
import { isAbort } from '../../node/server/Abort.js'
import { createAbortError, type AbortError } from '../../shared/Abort.js'
import { handleTelefunctionBug } from '../../node/server/runTelefunc/validateTelefunctionError.js'
import { ChannelClosedError, ChannelNetworkError } from '../channel-errors.js'
import { isPromise } from '../../utils/isPromise.js'
import {
  CHANNEL_BUFFER_LIMIT_BYTES,
  CHANNEL_CLOSE_TIMEOUT_MS,
  CHANNEL_CONNECT_TTL_MS,
  CREDIT_WINDOW_BYTES,
} from '../constants.js'
import { STATUS_BODY_INTERNAL_SERVER_ERROR } from '../../shared/constants.js'
import { ServerChannelBuffer } from './ServerChannelBuffer.js'
import type { AckResultStatus } from '../shared-ws.js'

const globalObject = getGlobalObject('channel.ts', {
  channelRegistry: new Map<string, ServerChannel<unknown, unknown>>(),
  creationHooks: new Map<string, () => void>(),
  connectTtlMs: CHANNEL_CONNECT_TTL_MS,
  bufferLimit: CHANNEL_BUFFER_LIMIT_BYTES,
})

function setChannelDefaults(opts: { connectTtl: number; bufferLimit: number }): void {
  globalObject.connectTtlMs = opts.connectTtl
  globalObject.bufferLimit = opts.bufferLimit
}

function getChannelRegistry(): Map<string, ServerChannel<unknown, unknown>> {
  return globalObject.channelRegistry
}

function onChannelCreated(id: string, cb: () => void): void {
  globalObject.creationHooks.set(id, cb)
}

type UntypedChannelHandler = (data: unknown) => unknown

function channel<ClientToServer = UntypedChannelHandler, ServerToClient = UntypedChannelHandler>(opts?: {
  ack?: false
}): Channel<ServerToClient, ClientToServer>
function channel<ClientToServer = UntypedChannelHandler, ServerToClient = UntypedChannelHandler>(opts: {
  ack: true
}): Channel<ServerToClient, ClientToServer, true>
function channel<ClientToServer = UntypedChannelHandler, ServerToClient = UntypedChannelHandler>(opts?: {
  ack?: boolean
}): Channel<ServerToClient, ClientToServer, false>
function channel(opts?: { ack?: boolean }): any {
  return new ServerChannel({
    ackMode: opts?.ack === true,
  })
}

class ServerChannel<ServerToClient = unknown, ClientToServer = unknown>
  implements Channel<ServerToClient, ClientToServer>
{
  readonly [SERVER_CHANNEL_BRAND] = true
  readonly id: string
  readonly ackMode: boolean

  get client(): ClientChannel<ClientToServer, ServerToClient> {
    return this as unknown as ClientChannel<ClientToServer, ServerToClient>
  }

  static isServerChannel(value: unknown): value is ServerChannel {
    return hasProp(value, SERVER_CHANNEL_BRAND)
  }

  protected _isClosed = false
  /** @internal */ _didShutdown = false
  private _didRegister = false
  protected _peer: IndexedPeer | null = null
  private _listeners: Array<ChannelListener<ClientToServer>> = []
  private _binaryListeners: Array<(data: Uint8Array) => void | Promise<void>> = []
  protected _prePeerBuffer: ServerChannelBuffer<ChannelAck<ServerToClient>>
  protected _pendingAcks = new Map<
    number,
    { resolve: (result: ChannelAck<ServerToClient>) => void; reject: (err: Error) => void }
  >()
  private _closeCallbacks: Array<ChannelCloseCallback> = []
  private _openCallbacks: Array<() => void> = []
  private _closeError: Error | undefined
  private _didFireClose = false
  private _didFireOpen = false
  private _closePromise: Promise<ChannelCloseResult> | null = null
  private _closeDeadline = 0
  private _closeWaiters: Array<() => void> = []
  private _didReceiveCloseAck = false
  private _awaitingCloseAck = false
  private _pendingCloseCallbacks = 0
  protected _inflightAcks = 0
  private _ttlTimer: ReturnType<typeof setTimeout> | null = null
  protected _peerWindow: number = CREDIT_WINDOW_BYTES
  private _awaitableBinaryBuffer: Uint8Array[] = []
  private _awaitableBinaryBufferBytes = 0
  private _sendWaiters: Array<() => void> = []
  private _reconnectTimer: ReturnType<typeof setTimeout> | null = null
  private _responseAbort: ((abortValue?: unknown) => void) | null = null
  private _shutdownCallback: (() => void) | null = null
  private _pendingCloseAck = false

  constructor({
    ackMode = false,
    id,
    bufferLimit,
  }: {
    ackMode?: boolean
    id?: string
    bufferLimit?: number
  } = {}) {
    this.ackMode = ackMode
    this.id = id ?? crypto.randomUUID()
    this._prePeerBuffer = new ServerChannelBuffer<ChannelAck<ServerToClient>>(bufferLimit ?? globalObject.bufferLimit)
  }

  get isClosed(): boolean {
    return this._isClosed
  }

  /** @internal — Register a one-shot callback that fires when the transport shuts down.
   *  Replaces any previously registered callback (does not accumulate). */
  _onShutdown(cb: () => void): void {
    this._shutdownCallback = cb
  }

  send(data: ChannelData<ServerToClient>): void
  send(data: ChannelData<ServerToClient>, opts: { ack: true }): Promise<ChannelAck<ServerToClient>>
  send(data: ChannelData<ServerToClient>, opts: { ack: false }): void
  send(data: ChannelData<ServerToClient>, opts?: { ack?: boolean }): Promise<ChannelAck<ServerToClient>> | void {
    if (this._isClosed) throw new ChannelClosedError()
    const needsAck = opts?.ack !== false && (opts?.ack === true || this.ackMode === true)
    const serialized = stringify(data, { forbidReactElements: false })
    if (!needsAck) {
      if (this._peer) {
        this._peer.sendText(serialized)
      } else {
        this._prePeerBuffer.pushText(serialized)
      }
      return
    }
    if (this._peer) {
      return this._trackAck(
        new Promise<ChannelAck<ServerToClient>>((resolve, reject) => {
          this._peer!.sendTextAckReq(serialized, (seq) => {
            this._pendingAcks.set(seq, { resolve, reject })
          })
        }),
      )
    }
    return this._trackAck(
      new Promise<ChannelAck<ServerToClient>>((resolve, reject) => {
        this._prePeerBuffer.pushTextAck(serialized, resolve, reject)
      }),
    )
  }

  sendBinary(data: Uint8Array): void {
    if (this._isClosed) throw new ChannelClosedError()
    if (this._peer) {
      void this._peer.sendBinary(data)
      return
    }
    this._prePeerBuffer.pushBinary(data)
  }

  listen(callback: ChannelListener<ClientToServer>): void {
    this._listeners.push(callback)
  }

  listenBinary(callback: (data: Uint8Array) => void | Promise<void>): void {
    this._binaryListeners.push(callback)
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

  _setResponseAbort(abortResponse: (abortValue?: unknown) => void): void {
    this._responseAbort = abortResponse
  }

  abort(abortValue?: unknown): void {
    if (this._didShutdown || this._isClosed) return
    this._isClosed = true
    const serializedAbortValue = stringify(abortValue, { forbidReactElements: false })
    if (this._peer) {
      this._peer.sendAbort(serializedAbortValue)
      this._peer = null
    }
    this._shutdown(createAbortError(abortValue))
  }

  close(opts?: ChannelCloseOptions): Promise<ChannelCloseResult> {
    if (this._closePromise) return this._closePromise
    if (this._didShutdown) return Promise.resolve(this._didReceiveCloseAck ? 0 : 1)
    const timeout = normalizeCloseTimeout(opts?.timeout)
    this._closeDeadline = Date.now() + timeout
    this._awaitingCloseAck = true
    this._startClose()
    if (this._peer) this._peer.sendCloseRequest(timeout)
    this._closePromise = this._runFinalizationLoop()
    return this._closePromise
  }

  _registerChannel(): void {
    if (this._didShutdown || this._peer || this._didRegister) return
    this._didRegister = true
    getChannelRegistry().set(this.id, this as ServerChannel<unknown, unknown>)
    const hook = globalObject.creationHooks.get(this.id)
    if (hook) {
      globalObject.creationHooks.delete(this.id)
      hook()
    }
    this._clearTimer('_ttlTimer')
    this._ttlTimer = unrefTimer(
      setTimeout(() => {
        this._ttlTimer = null
        this._shutdown(
          new ChannelNetworkError('Channel timed out: no client connected within TTL after response was sent'),
        )
      }, globalObject.connectTtlMs),
    )
  }

  _attachPeer(peer: IndexedPeer): void {
    if (this._didShutdown) return
    this._clearTimer('_ttlTimer')
    this._clearTimer('_reconnectTimer')
    this._peerWindow = CREDIT_WINDOW_BYTES
    this._peer = peer
    this._prePeerBuffer.flush(
      (msg) => peer.sendText(msg),
      (msg) => peer.sendPublish(msg),
      (msg) => peer.sendBinary(msg),
      (data, ackEntry) => {
        const seq = peer.sendTextAckReq(data)
        this._pendingAcks.set(seq, ackEntry)
      },
      (msg) => peer.sendPublishBinary(msg),
    )
    this._flushAwaitableBinaryBuffer()
    if (this._pendingCloseAck) peer.sendCloseAck()
    if (this._awaitingCloseAck) peer.sendCloseRequest(Math.max(0, this._closeDeadline - Date.now()))
    if (this._isClosed) {
      this._notifyCloseProgress()
      return
    }
    this._fireOpen()
    this._notifySendReady()
  }

  _sendBinaryAwaitable(data: Uint8Array): void | Promise<void> {
    if (this._isClosed) throw new ChannelClosedError()
    if (!this._peer) {
      // Pre-peer: buffer up to CREDIT_WINDOW_BYTES, block when full
      if (this._awaitableBinaryBufferBytes + data.byteLength > CREDIT_WINDOW_BYTES) {
        return this._sendBinaryWhenReady(data)
      }
      this._awaitableBinaryBuffer.push(data)
      this._awaitableBinaryBufferBytes += data.byteLength
      return
    }
    if (this._peerWindow <= 0) return this._sendBinaryWhenReady(data)
    this._peerWindow -= data.byteLength
    return this._peer.sendBinary(data)
  }

  private _flushAwaitableBinaryBuffer(): void {
    if (!this._peer || this._awaitableBinaryBuffer.length === 0) return
    for (const chunk of this._awaitableBinaryBuffer) {
      this._peerWindow -= chunk.byteLength
      this._peer.sendBinary(chunk)
    }
    this._awaitableBinaryBuffer.length = 0
    this._awaitableBinaryBufferBytes = 0
    this._notifySendReady()
  }

  private async _sendBinaryWhenReady(data: Uint8Array): Promise<void> {
    while ((!this._peer || this._peerWindow <= 0) && !this._isClosed) {
      await this._waitUntilSendReady()
    }
    if (this._isClosed) throw new ChannelClosedError()
    this._peerWindow -= data.byteLength
    return this._peer!.sendBinary(data)
  }

  _onPeerMessage(text: string): void {
    const data = parse(text) as ChannelData<ClientToServer>
    for (const cb of this._listeners) {
      try {
        cb(data)
      } catch (err) {
        if (this._handleCallbackError(err)) return
      }
    }
  }

  _onPeerAckReqMessage(text: string, seq: number): Promise<void> {
    return this._trackAck(this._dispatchAckReq(text, seq))
  }

  _onPeerBinaryMessage(data: Uint8Array): void {
    for (const cb of this._binaryListeners) {
      try {
        cb(data)
      } catch (err) {
        if (this._handleCallbackError(err)) return
      }
    }
  }

  _onPeerAckRes(ackedSeq: number, resultText: string, status: AckResultStatus = 'ok'): void {
    const pending = this._pendingAcks.get(ackedSeq)
    if (!pending) return
    this._pendingAcks.delete(ackedSeq)
    switch (status) {
      case 'ok':
        pending.resolve(parse(resultText) as ChannelAck<ServerToClient>)
        return
      case 'abort':
        pending.reject(createAbortError(parse(resultText)))
        return
      case 'error':
        pending.reject(new Error(resultText || 'Internal client channel error — see client logs'))
    }
  }

  _onPeerCloseRequest(timeoutMs: number): void {
    if (this._didShutdown) return
    const peerDeadline = Date.now() + normalizeCloseTimeout(timeoutMs)
    if (!this._closeDeadline || peerDeadline < this._closeDeadline) this._closeDeadline = peerDeadline
    this._pendingCloseAck = true
    if (this._peer) this._peer.sendCloseAck()
    if (this._isClosed) {
      this._notifyCloseProgress()
      return
    }
    this._startClose()
    void this._runFinalizationLoop()
  }

  _onPeerCloseAck(): void {
    if (this._didShutdown) return
    this._didReceiveCloseAck = true
    this._awaitingCloseAck = false
    this._notifyCloseProgress()
  }

  _onPeerDisconnect(reconnectTimeout: number): void {
    if (this._didShutdown || !this._peer) return
    this._peer = null
    this._reconnectTimer = unrefTimer(
      setTimeout(() => {
        this._reconnectTimer = null
        this._shutdown(new ChannelNetworkError('Channel timed out: client did not reconnect within grace period'))
      }, reconnectTimeout),
    )
  }

  _onPeerRecoveryFailure(): void {
    if (this._didShutdown) return
    this._peer = null
    this._shutdown(new ChannelNetworkError('Channel not acknowledged by client after reconnect'))
  }

  _onPeerClose(): void {
    if (this._didShutdown) return
    this._peer = null
    this._shutdown()
  }

  _onPeerWindowUpdate(bytes: number): void {
    this._peerWindow = bytes
    this._notifySendReady()
  }

  /** Advertise free buffer space to the peer so it can unblock. */
  _sendWindowUpdate(bytes: number): void {
    this._peer?.sendWindowUpdate(bytes)
  }

  private _startClose(err?: Error): void {
    if (this._isClosed) return
    this._isClosed = true
    this._fireClose(err)
  }

  private async _runFinalizationLoop(): Promise<ChannelCloseResult> {
    while (!this._didShutdown) {
      if (this._isCloseWorkComplete()) {
        this._shutdown()
        break
      }
      const remaining = this._closeDeadline - Date.now()
      if (remaining <= 0) {
        this._shutdown(new ChannelClosedError('Channel close timed out'))
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

  private async _dispatchAckReq(text: string, seq: number): Promise<void> {
    const data = parse(text) as ChannelData<ClientToServer>
    for (const cb of this._listeners) {
      try {
        const result = await cb(data)
        this._peer?.sendAckRes(seq, stringify(result, { forbidReactElements: false }))
      } catch (err) {
        if (this._handleCallbackError(err)) return
        this._peer?.sendAckRes(seq, `${STATUS_BODY_INTERNAL_SERVER_ERROR} — see server logs`, 'error')
      }
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
      (!this._awaitingCloseAck || this._didReceiveCloseAck)
    )
  }

  protected _shutdown(err?: Error): void {
    if (this._didShutdown) return
    this._didShutdown = true
    this._isClosed = true
    this._closeError = err
    this._peer = null
    this._pendingCloseAck = false
    this._awaitingCloseAck = false
    this._clearTimer('_ttlTimer')
    this._clearTimer('_reconnectTimer')
    getChannelRegistry().delete(this.id)
    const shutdownCb = this._shutdownCallback
    this._shutdownCallback = null
    shutdownCb?.()
    this._fireClose(err)
    this._notifySendReady()
    this._notifyCloseProgress()
    const ackErr = err ?? new ChannelClosedError()
    for (const { reject } of this._pendingAcks.values()) reject(ackErr)
    this._pendingAcks.clear()
    this._prePeerBuffer.clear(ackErr)
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

  private _invokeCloseCallback(callback: ChannelCloseCallback, err: Error | undefined, track: boolean): void {
    try {
      const pending = callback(err)
      if (!isPromise(pending)) return
      if (track) {
        this._pendingCloseCallbacks++
        void pending
          .catch((e) => reportServerChannelError(e))
          .finally(() => {
            this._pendingCloseCallbacks--
            this._notifyCloseProgress()
          })
      } else {
        void pending.catch((e) => reportServerChannelError(e))
      }
    } catch (callbackErr) {
      reportServerChannelError(callbackErr)
    }
  }

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

  protected _handleCallbackError(err: unknown): boolean {
    if (isAbort(err)) {
      const abortError: AbortError = err
      if (this._responseAbort) {
        this._responseAbort(abortError.abortValue)
      } else {
        this.abort(abortError.abortValue)
      }
      return true
    }
    reportServerChannelError(err)
    return false
  }

  private _clearTimer(name: '_ttlTimer' | '_reconnectTimer'): void {
    const timer = this[name]
    if (!timer) return
    clearTimeout(timer)
    if (name === '_ttlTimer') {
      this._ttlTimer = null
      return
    }
    this._reconnectTimer = null
  }

  private _waitUntilSendReady(): Promise<void> {
    return new Promise<void>((resolve) => {
      this._sendWaiters.push(resolve)
    })
  }

  private _notifySendReady(): void {
    const waiters = this._sendWaiters.splice(0)
    for (const waiter of waiters) waiter()
  }
}

function reportServerChannelError(err: unknown): void {
  handleTelefunctionBug(err instanceof Error ? err : new Error(String(err)))
}

function normalizeCloseTimeout(timeout: number | undefined): number {
  if (timeout === undefined) return CHANNEL_CLOSE_TIMEOUT_MS
  assertUsage(Number.isFinite(timeout) && timeout >= 0, 'Channel close timeout must be a non-negative finite number')
  return timeout
}
