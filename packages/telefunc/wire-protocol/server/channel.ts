export { createChannel, getChannelRegistry, onChannelCreated, setChannelDefaults, ServerChannel, SERVER_CHANNEL_BRAND }
export { ChannelClosedError, ChannelNetworkError, ChannelOverflowError } from '../channel-errors.js'

const SERVER_CHANNEL_BRAND = Symbol.for('ServerChannel')

import type { Channel, ChannelClient, ChannelData, ChannelAck, ChannelListener } from '../channel.js'
import type { IndexedPeer } from './IndexedPeer.js'
import { stringify } from '@brillout/json-serializer/stringify'
import { parse } from '@brillout/json-serializer/parse'
import { getGlobalObject } from '../../utils/getGlobalObject.js'
import { hasProp } from '../../utils/hasProp.js'
import { unrefTimer } from '../../utils/unrefTimer.js'
import { isAbort } from '../../node/server/Abort.js'
import { createAbortError, type AbortError } from '../../shared/Abort.js'
import { handleTelefunctionBug } from '../../node/server/runTelefunc/validateTelefunctionError.js'
import { ChannelClosedError, ChannelNetworkError } from '../channel-errors.js'
import { type ChannelTransport, CHANNEL_BUFFER_LIMIT_BYTES, CHANNEL_CONNECT_TTL_MS } from '../constants.js'
import { STATUS_BODY_INTERNAL_SERVER_ERROR } from '../../shared/constants.js'
import { ServerChannelBuffer } from './ServerChannelBuffer.js'
import type { AckResultStatus } from '../shared-ws.js'

const globalObject = getGlobalObject('channel.ts', {
  channelRegistry: new Map<string, ServerChannel<unknown, unknown>>(),
  creationHooks: new Map<string, () => void>(),
  connectTtlMs: CHANNEL_CONNECT_TTL_MS,
  bufferLimit: CHANNEL_BUFFER_LIMIT_BYTES,
})

/**
 * @internal — Called by the shared channel transport server to propagate global defaults.
 * Must be called before any `createChannel()` call to take effect on those channels.
 */
function setChannelDefaults(opts: { connectTtl: number; bufferLimit: number }): void {
  globalObject.connectTtlMs = opts.connectTtl
  globalObject.bufferLimit = opts.bufferLimit
}

function getChannelRegistry(): Map<string, ServerChannel<unknown, unknown>> {
  return globalObject.channelRegistry
}

/**
 * Register a one-shot callback that fires when a `ServerChannel` with the given `id` is created.
 * The callback fires synchronously inside the constructor, before any client connection.
 */
function onChannelCreated(id: string, cb: () => void): void {
  globalObject.creationHooks.set(id, cb)
}

/**
 * Create a bidirectional channel for real-time communication with the client.
 *
 * Returns a `ServerChannel` — use it directly for `send()`/`listen()`.
 * Return `channel.client` to the client — it serializes to a `ClientChannel`
 * on the other side.
 *
 * @example
 * ```ts
 * import { createChannel } from 'telefunc'
 *
 * export async function onChat() {
 *   type ClientToServer = (msg: string) => void
 *   type ServerToClient = (msg: string) => void
 *   const channel = createChannel<ClientToServer, ServerToClient>()
 *   channel.listen((msg) => console.log('from client:', msg))
 *   channel.send('hello from server')
 *   channel.onClose(() => console.log('channel closed'))
 *   return { channel: channel.client }
 * }
 * ```
 */
type UntypedChannelHandler = (data: unknown) => unknown

function createChannel<ClientToServer = UntypedChannelHandler, ServerToClient = UntypedChannelHandler>(opts?: {
  ack?: false
}): Channel<ServerToClient, ClientToServer>
function createChannel<ClientToServer = UntypedChannelHandler, ServerToClient = UntypedChannelHandler>(opts: {
  ack: true
}): Channel<ServerToClient, ClientToServer, true>
function createChannel<ClientToServer = UntypedChannelHandler, ServerToClient = UntypedChannelHandler>(opts?: {
  ack?: boolean
}): Channel<ServerToClient, ClientToServer, false>
function createChannel(opts?: { ack?: boolean }): any {
  return new ServerChannel({
    ackMode: opts?.ack === true,
  })
}

/**
 * Safety TTL: if a channel is created but never serialized into a response
 * (i.e. `_registerChannel` is never called), close it after this many ms.
 * Should be large enough to cover any reasonable telefunction run time.
 */
const DEFAULT_TTL_MS = 5 * 60_000
class ServerChannel<ServerToClient = unknown, ClientToServer = unknown>
  implements Channel<ServerToClient, ClientToServer>
{
  readonly [SERVER_CHANNEL_BRAND] = true
  readonly id: string
  /** Whether ack is on by default. Set via `createChannel({ ack: true })`. */
  readonly ackMode: boolean
  readonly channelTransport?: ChannelTransport

  /** Return this to the client — it serializes to a `ClientChannel` on the other side. */
  get client(): ChannelClient<ClientToServer, ServerToClient> {
    return this as unknown as ChannelClient<ClientToServer, ServerToClient>
  }

  static isServerChannel(value: unknown): value is ServerChannel {
    // Vite dev server can cause multiple instances of this module to be loaded,
    // so instanceof checks don't work — use a unique brand property instead.
    return hasProp(value, SERVER_CHANNEL_BRAND)
  }

  private _isClosed = false
  private _peer: IndexedPeer | null = null
  private _listeners: Array<ChannelListener<ClientToServer>> = []
  private _binaryListeners: Array<(data: Uint8Array) => void> = []
  /** Hard-capped buffer for outgoing messages while no client is connected. */
  private _prePeerBuffer: ServerChannelBuffer<ChannelAck<ServerToClient>>
  /** Pending ack promises keyed by the seq of the outgoing frame. */
  private _pendingAcks = new Map<
    number,
    { resolve: (result: ChannelAck<ServerToClient>) => void; reject: (err: Error) => void }
  >()
  private _closeCallbacks: Array<(err?: Error) => void> = []
  private _openCallbacks: Array<() => void> = []
  private _closeError: Error | undefined
  private _didFireClose = false
  private _didFireOpen = false
  private _ttlTimer: ReturnType<typeof setTimeout> | null = null
  /** True when the client is gone but we're waiting for reconnection. */
  private _disconnected = false
  private _sendPaused = false
  private _sendWaiters: Array<() => void> = []
  private _reconnectTimer: ReturnType<typeof setTimeout> | null = null
  private _responseAbort: ((abortValue?: unknown) => void) | null = null

  constructor({
    ackMode = false,
    id,
    bufferLimit,
    channelTransport,
  }: {
    ackMode?: boolean
    id?: string
    bufferLimit?: number
    channelTransport?: ChannelTransport
  } = {}) {
    this.ackMode = ackMode
    this.id = id ?? crypto.randomUUID()
    this.channelTransport = channelTransport
    this._prePeerBuffer = new ServerChannelBuffer<ChannelAck<ServerToClient>>(bufferLimit ?? globalObject.bufferLimit)
    // Safety TTL: if this channel is never serialized into a response, shut it down
    // to prevent memory leaks. The registry entry is added in _registerChannel().
    this._ttlTimer = unrefTimer(
      setTimeout(() => {
        this._ttlTimer = null
        this._shutdown(new ChannelNetworkError('Channel timed out: no client connected within TTL'))
      }, DEFAULT_TTL_MS),
    )
  }

  get isClosed(): boolean {
    return this._isClosed
  }

  /** Send a message to the client.
   *  @param opts.ack - When `true`, returns a Promise resolved with the client's ack value.
   *                    Always `true` (and the overload returns Promise automatically) when the
   *                    channel was created with `createChannel({ ack: true })`.
   *  @throws {ChannelClosedError} if the channel is closed. */
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
      return new Promise<ChannelAck<ServerToClient>>((resolve, reject) => {
        this._peer!.sendTextAckReq(serialized, (seq) => {
          this._pendingAcks.set(seq, { resolve, reject })
        })
      })
    } else {
      return new Promise<ChannelAck<ServerToClient>>((resolve, reject) => {
        this._prePeerBuffer.pushTextAck(serialized, resolve, reject)
      })
    }
  }

  /** Send a binary message to the client.
   *  @throws {ChannelClosedError} if the channel is closed. */
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

  listenBinary(callback: (data: Uint8Array) => void): void {
    this._binaryListeners.push(callback)
  }

  /** Register a callback that fires when the channel closes for any reason. Fires exactly once.
   *  `err` is set when the close was caused by a network/timeout error; `undefined` for clean closes. */
  onClose(callback: (err?: Error) => void): void {
    if (this._didFireClose) {
      callback(this._closeError)
      return
    }
    this._closeCallbacks.push(callback)
  }

  /** Register a callback that fires when the client connects for the first time. Fires exactly once. */
  onOpen(callback: () => void): void {
    if (this._didFireOpen) {
      callback()
      return
    }
    this._openCallbacks.push(callback)
  }

  /** @internal — Bind this channel to the telefunc response-wide abort path. */
  _setResponseAbort(abortResponse: (abortValue?: unknown) => void): void {
    this._responseAbort = abortResponse
  }

  /** Close the channel from the server side with an abort value.
   *  The client's `onClose` callback will receive an `Abort` error carrying `abortValue`. */
  abort(abortValue?: unknown): void {
    if (this._isClosed) return
    if (this._peer) {
      this._peer.abort(stringify(abortValue, { forbidReactElements: false }))
      this._peer = null
    }
    this._shutdown()
  }

  /** Close the channel from the server side. */
  close(): void {
    if (this._isClosed) return
    if (this._peer) {
      this._peer.close()
      this._peer = null
    }
    this._shutdown()
  }

  /** @internal — Called by the response serializer when this channel is included in a
   * telefunc response. Registers the channel in the registry (so reconcile can find it)
   * and starts the short connect TTL from this moment.
   */
  _registerChannel(): void {
    if (this._isClosed || this._peer) return
    getChannelRegistry().set(this.id, this as ServerChannel<unknown, unknown>)
    // Fire any waiter that was registered before this channel entered the registry.
    // Must happen after the registry write so reconcile can immediately do registry.get(id).
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

  attachPeer(peer: IndexedPeer): void {
    this._clearTimer('_ttlTimer')
    this._clearTimer('_reconnectTimer')
    this._disconnected = false
    this._sendPaused = false
    this._peer = peer
    this._prePeerBuffer.flush(
      (msg) => peer.sendText(msg),
      (msg) => peer.sendBinary(msg),
      (data, { resolve, reject }) => {
        const seq = peer.sendTextAckReq(data)
        this._pendingAcks.set(seq, { resolve, reject })
      },
    )
    this._fireOpen()
    this._notifySendReady()
  }

  /** @internal — Channel-scoped binary send gate used by streaming response pumps.
   *
   *  This method is responsible only for readiness rules that are local to this
   *  `ServerChannel`, before the frame reaches the shared transport connection:
   *  - if the channel is paused by the client, it waits for `_onPeerResume()`;
   *  - if the channel is temporarily disconnected, it waits for `attachPeer()` on reconnect;
   *  - if no peer is attached yet but the channel is otherwise sendable, it buffers into
   *    `_prePeerBuffer` and returns synchronously.
   *
   *  Once the channel is ready to hand the frame to its peer, the returned value from
   *  `IndexedPeer.sendBinary()` is propagated unchanged:
   *  - `void` means the send completed synchronously all the way down;
   *  - `Promise<void>` means the underlying transport or connection-wide send gate needs waiting.
   *
   *  Ordering and transport backpressure after the frame leaves this channel are handled by
   *  `MuxServer.send()`. This method does not serialize different channels against each other;
   *  it only enforces this channel's paused/disconnected readiness rules.
   */
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

  /** @internal — Called by ws.ts when a TEXT frame arrives (no ack expected). */
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

  /** @internal — Called by ws.ts when a TEXT_ACK_REQ frame arrives. Dispatches the
   *  message and sends ACK_RES with the listener's resolved value. */
  async _onPeerAckReqMessage(text: string, seq: number): Promise<void> {
    const data = parse(text) as ChannelData<ClientToServer>
    for (const cb of this._listeners) {
      try {
        const result = await cb(data)
        this._peer?.sendAckRes(seq, stringify(result, { forbidReactElements: false }))
      } catch (err) {
        if (this._handleCallbackError(err)) return
        this._peer?.sendAckRes(seq, `${STATUS_BODY_INTERNAL_SERVER_ERROR} — see server logs`, 'error')
        return
      }
    }
  }

  /** @internal */
  _onPeerBinaryMessage(data: Uint8Array): void {
    for (const cb of this._binaryListeners) {
      try {
        cb(data)
      } catch (err) {
        if (this._handleCallbackError(err)) return
      }
    }
  }

  /** @internal — Client sent ACK_RES for a message we sent. */
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

  /** @internal — Connection dropped, keep channel alive for reconnection. */
  _onPeerDisconnect(reconnectTimeout: number): void {
    if (this._isClosed || this._disconnected) return
    this._peer = null
    this._disconnected = true
    this._reconnectTimer = unrefTimer(
      setTimeout(() => {
        this._reconnectTimer = null
        this._shutdown(new ChannelNetworkError('Channel timed out: client did not reconnect within grace period'))
      }, reconnectTimeout),
    )
  }

  /** @internal — Reconnect failed because the client no longer acknowledged this channel. */
  _onPeerRecoveryFailure(): void {
    if (this._isClosed) return
    this._peer = null
    this._shutdown(new ChannelNetworkError('Channel not acknowledged by client after reconnect'))
  }

  /** @internal — Permanent close from the client side. */
  _onPeerClose(): void {
    if (this._isClosed) return
    this._peer = null
    this._shutdown()
  }

  /** @internal */
  _onPeerPause(): void {
    this._sendPaused = true
  }

  /** @internal */
  _onPeerResume(): void {
    this._sendPaused = false
    this._notifySendReady()
  }

  // ── Private ──

  /** Single close path — every close route funnels here. */
  private _shutdown(err?: Error): void {
    if (this._isClosed) return
    this._isClosed = true
    this._closeError = err
    this._clearTimer('_ttlTimer')
    this._clearTimer('_reconnectTimer')
    getChannelRegistry().delete(this.id)
    this._fireClose(err)
    this._notifySendReady()
    const ackErr = new ChannelClosedError()
    for (const { reject } of this._pendingAcks.values()) reject(ackErr)
    this._pendingAcks.clear()
    this._prePeerBuffer.clear(ackErr)
  }

  private _fireClose(err?: Error): void {
    if (this._didFireClose) return
    this._didFireClose = true
    for (const cb of this._closeCallbacks) {
      try {
        cb(err)
      } catch (callbackErr) {
        this._reportCallbackError(callbackErr)
      }
    }
    this._closeCallbacks.length = 0
    this._openCallbacks.length = 0
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
    this._reportCallbackError(err)
    return false
  }

  private _reportCallbackError(err: unknown): void {
    handleTelefunctionBug(err instanceof Error ? err : new Error(String(err)))
  }

  private _clearTimer(name: '_ttlTimer' | '_reconnectTimer'): void {
    const timer = this[name]
    if (timer) {
      clearTimeout(timer)
      ;(this as any)[name] = null
    }
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
