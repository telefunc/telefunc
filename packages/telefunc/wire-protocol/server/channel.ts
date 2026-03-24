export { createChannel, getChannelRegistry, onChannelCreated, setChannelDefaults, ServerChannel, SERVER_CHANNEL_BRAND }
export { ChannelClosedError, ChannelNetworkError, ChannelOverflowError } from '../channel-errors.js'

const SERVER_CHANNEL_BRAND = Symbol.for('ServerChannel')

import type {
  Channel,
  ChannelAck,
  ChannelClient,
  ChannelCloseCallback,
  ChannelCloseOptions,
  ChannelCloseResult,
  ChannelData,
  ChannelListener,
  ChannelPublishAck,
} from '../channel.js'
import type { IndexedPeer } from './IndexedPeer.js'
import { stringify } from '@brillout/json-serializer/stringify'
import { parse } from '@brillout/json-serializer/parse'
import { getGlobalObject } from '../../utils/getGlobalObject.js'
import { hasProp } from '../../utils/hasProp.js'
import { unrefTimer } from '../../utils/unrefTimer.js'
import { assert, assertUsage } from '../../utils/assert.js'
import { isAbort } from '../../node/server/Abort.js'
import { createAbortError, type AbortError } from '../../shared/Abort.js'
import { handleTelefunctionBug } from '../../node/server/runTelefunc/validateTelefunctionError.js'
import { ChannelClosedError, ChannelNetworkError } from '../channel-errors.js'
import { isPromise } from '../../utils/isPromise.js'
import { CHANNEL_BUFFER_LIMIT_BYTES, CHANNEL_CLOSE_TIMEOUT_MS, CHANNEL_CONNECT_TTL_MS } from '../constants.js'
import { STATUS_BODY_INTERNAL_SERVER_ERROR } from '../../shared/constants.js'
import { ServerChannelBuffer } from './ServerChannelBuffer.js'
import type { AckResultStatus } from '../shared-ws.js'
import { getPubSubTransport } from './pubsub.js'
import type { PubSubSubscription, PubSubTransport } from './pubsub.js'

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

function createChannel<ClientToServer = UntypedChannelHandler, ServerToClient = UntypedChannelHandler>(opts?: {
  ack?: false
  selfDelivery?: boolean
}): Channel<ServerToClient, ClientToServer>
function createChannel<ClientToServer = UntypedChannelHandler, ServerToClient = UntypedChannelHandler>(opts: {
  ack?: false
  key: string
  selfDelivery?: boolean
}): Channel<ServerToClient, ClientToServer, false, true>
function createChannel<ClientToServer = UntypedChannelHandler, ServerToClient = UntypedChannelHandler>(opts: {
  ack: true
  selfDelivery?: boolean
}): Channel<ServerToClient, ClientToServer, true>
function createChannel<ClientToServer = UntypedChannelHandler, ServerToClient = UntypedChannelHandler>(opts: {
  ack: true
  key: string
  selfDelivery?: boolean
}): Channel<ServerToClient, ClientToServer, true, true>
function createChannel<ClientToServer = UntypedChannelHandler, ServerToClient = UntypedChannelHandler>(opts?: {
  ack?: boolean
  key?: string
  selfDelivery?: boolean
}): Channel<ServerToClient, ClientToServer, false>
function createChannel(opts?: { ack?: boolean; key?: string; selfDelivery?: boolean }): any {
  return new ServerChannel({
    ackMode: opts?.ack === true,
    key: opts?.key,
    selfDelivery: opts?.selfDelivery,
  })
}

const DEFAULT_TTL_MS = 5 * 60_000

class ServerChannel<ServerToClient = unknown, ClientToServer = unknown>
  implements Channel<ServerToClient, ClientToServer>
{
  readonly [SERVER_CHANNEL_BRAND] = true
  readonly id: string
  readonly ackMode: boolean
  readonly key: string | undefined
  readonly selfDelivery: boolean

  get client(): ChannelClient<ClientToServer, ServerToClient> {
    return this as unknown as ChannelClient<ClientToServer, ServerToClient>
  }

  static isServerChannel(value: unknown): value is ServerChannel {
    return hasProp(value, SERVER_CHANNEL_BRAND)
  }

  private _isClosed = false
  /** @internal */ _didShutdown = false
  private _didRegister = false
  private _peer: IndexedPeer | null = null
  private _listeners: Array<ChannelListener<ClientToServer>> = []
  private _pubSubListeners: Array<ChannelListener<ClientToServer>> = []
  private _binaryListeners: Array<(data: Uint8Array) => void> = []
  private _prePeerBuffer: ServerChannelBuffer<ChannelAck<ServerToClient>>
  private _pendingAcks = new Map<
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
  private _inflightAcks = 0
  private _ttlTimer: ReturnType<typeof setTimeout> | null = null
  private _disconnected = false
  private _sendPaused = false
  private _sendWaiters: Array<() => void> = []
  private _reconnectTimer: ReturnType<typeof setTimeout> | null = null
  private _responseAbort: ((abortValue?: unknown) => void) | null = null
  private _shutdownCallback: (() => void) | null = null
  private _pendingCloseAck = false
  private _didRegisterPubSub = false
  private _pubSubTransport: PubSubTransport | null = null
  private _pubSubSubscription: PubSubSubscription | null = null

  constructor({
    ackMode = false,
    key,
    selfDelivery = true,
    id,
    bufferLimit,
  }: {
    ackMode?: boolean
    key?: string
    selfDelivery?: boolean
    id?: string
    bufferLimit?: number
  } = {}) {
    this.ackMode = ackMode
    this.key = key
    this.selfDelivery = selfDelivery
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

  publish(data: ChannelData<ServerToClient>): Promise<ChannelPublishAck> {
    assertUsage(this.key, 'Channel.publish() requires createChannel({ key })')
    this._registerPubSub()
    if (!this._pubSubSubscription) throw new ChannelClosedError()
    const serialized = stringify(data, { forbidReactElements: false })
    const ret = this._trackAck(Promise.resolve(this._publishPubSub(serialized)))
    ret.catch((e) => reportServerChannelError(e))
    return ret
  }

  listen(callback: ChannelListener<ClientToServer>): void {
    this._listeners.push(callback)
  }

  subscribe(callback: ChannelListener<ClientToServer>): () => void {
    assertUsage(this.key, 'Channel.subscribe() requires createChannel({ key })')
    // Pure server-side keyed pub/sub registers lazily here; Cloudflare needs async request context on this path.
    this._registerPubSub()
    this._pubSubListeners.push(callback)
    return () => {
      const index = this._pubSubListeners.indexOf(callback)
      if (index >= 0) this._pubSubListeners.splice(index, 1)
    }
  }

  listenBinary(callback: (data: Uint8Array) => void): void {
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
    // Returned keyed channels register pub/sub here while serialization has restored sync request context.
    this._registerPubSub()
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

  /** @internal */
  _onPeerPublishAckReqMessage(text: string, seq: number): Promise<void> {
    return this._trackAck(this._dispatchPublishAckReq(text, seq))
  }

  /** @internal */
  _deliverPubSubMessage(serialized: string, _sourceChannelId: string): void {
    for (const cb of this._pubSubListeners) {
      try {
        cb(parse(serialized) as ChannelData<ClientToServer>)
      } catch (err) {
        if (this._handleCallbackError(err)) return
      }
    }
    if (this._peer) {
      this._peer.sendPublish(serialized)
      return
    }
    this._prePeerBuffer.pushPublish(serialized)
  }

  _attachPeer(peer: IndexedPeer): void {
    if (this._didShutdown) return
    this._clearTimer('_ttlTimer')
    this._clearTimer('_reconnectTimer')
    this._disconnected = false
    this._sendPaused = false
    this._peer = peer
    this._prePeerBuffer.flush(
      (msg) => peer.sendText(msg),
      (msg) => peer.sendPublish(msg),
      (msg) => peer.sendBinary(msg),
      (data, ackEntry) => {
        const seq = peer.sendTextAckReq(data)
        this._pendingAcks.set(seq, ackEntry)
      },
    )
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
    if (this._sendPaused || this._disconnected) return this._sendBinaryWhenReady(data)
    return this._sendBinaryNow(data)
  }

  private _sendBinaryNow(data: Uint8Array): void | Promise<void> {
    if (this._peer) return this._peer.sendBinary(data)
    this._prePeerBuffer.pushBinary(data)
  }

  private async _sendBinaryWhenReady(data: Uint8Array): Promise<void> {
    while ((this._sendPaused || this._disconnected) && !this._isClosed) {
      await this._waitUntilSendReady()
    }
    if (this._isClosed) throw new ChannelClosedError()
    const pending = this._sendBinaryNow(data)
    if (pending) await pending
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
    if (this._didShutdown || this._disconnected) return
    this._peer = null
    this._disconnected = true
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

  _onPeerPause(): void {
    this._sendPaused = true
  }

  _onPeerResume(): void {
    this._sendPaused = false
    this._notifySendReady()
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

  private async _dispatchPublishAckReq(serialized: string, seq: number): Promise<void> {
    assert(this.key)
    try {
      this._registerPubSub()
      const result = await this._publishPubSub(serialized)
      this._peer?.sendAckRes(seq, stringify(result, { forbidReactElements: false }))
    } catch (err) {
      if (this._handleCallbackError(err)) return
      this._peer?.sendAckRes(seq, `${STATUS_BODY_INTERNAL_SERVER_ERROR} — see server logs`, 'error')
    }
  }

  private _trackAck<T>(promise: Promise<T>): Promise<T> {
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

  private _shutdown(err?: Error): void {
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
    this._unregisterPubSub()
    this._notifySendReady()
    this._notifyCloseProgress()
    const ackErr = err ?? new ChannelClosedError()
    for (const { reject } of this._pendingAcks.values()) reject(ackErr)
    this._pendingAcks.clear()
    this._prePeerBuffer.clear(ackErr)
  }

  private _registerPubSub(): void {
    if (!this.key || this._didRegisterPubSub) return
    this._didRegisterPubSub = true
    this._pubSubTransport = getPubSubTransport()
    this._pubSubSubscription = {
      id: this.id,
      key: this.key,
      selfDelivery: this.selfDelivery,
      onMessage: (serialized, sourceChannelId) => {
        this._deliverPubSubMessage(serialized, sourceChannelId)
      },
    }
    void this._pubSubTransport.subscribe(this._pubSubSubscription)
  }

  private _unregisterPubSub(): void {
    if (!this.key || !this._didRegisterPubSub) return
    this._didRegisterPubSub = false
    assert(this._pubSubTransport)
    assert(this._pubSubSubscription)
    void this._pubSubTransport.unsubscribe(this._pubSubSubscription)
    this._pubSubSubscription = null
  }

  private _publishPubSub(serialized: string): ChannelPublishAck | Promise<ChannelPublishAck> {
    assert(this.key)
    assert(this._pubSubTransport)
    assert(this._pubSubSubscription)
    return this._pubSubTransport.publish(this._pubSubSubscription, serialized)
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

  private _handleCallbackError(err: unknown): boolean {
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
