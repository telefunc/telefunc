export { createChannel, getChannelRegistry, onChannelCreated, setChannelDefaults, ServerChannel, SERVER_CHANNEL_BRAND }
export { ChannelClosedError, ChannelNetworkError } from '../channel-errors.js'

const SERVER_CHANNEL_BRAND = Symbol.for('ServerChannel')

import type { Channel } from '../channel.js'
import type { IndexedPeer } from './IndexedPeer.js'
import { stringify } from '@brillout/json-serializer/stringify'
import { parse } from '@brillout/json-serializer/parse'
import { getGlobalObject } from '../../utils/getGlobalObject.js'
import { hasProp } from '../../utils/hasProp.js'
import { unrefTimer } from '../../utils/unrefTimer.js'
import { isAbort } from '../../node/server/Abort.js'
import { ChannelClosedError, ChannelNetworkError } from '../channel-errors.js'
import { WS_CHANNEL_SEND_BUFFER, WS_CHANNEL_CONNECT_TTL_MS } from '../constants.js'
import { ServerChannelBuffer } from './ServerChannelBuffer.js'

const globalObject = getGlobalObject('channel.ts', {
  channelRegistry: new Map<string, ServerChannel<unknown, unknown>>(),
  creationHooks: new Map<string, () => void>(),
  channelConnectTtlMs: WS_CHANNEL_CONNECT_TTL_MS,
  channelSendBufferBytes: WS_CHANNEL_SEND_BUFFER,
})

/**
 * @internal — Called by `getTelefuncChannelHooks` to propagate WS-level defaults to channels.
 * Must be called before any `createChannel()` call to take effect on those channels.
 */
function setChannelDefaults(opts: { connectTtlMs: number; sendBufferBytes: number }): void {
  globalObject.channelConnectTtlMs = opts.connectTtlMs
  globalObject.channelSendBufferBytes = opts.sendBufferBytes
}

function getChannelRegistry(): Map<string, ServerChannel<unknown, unknown>> {
  return globalObject.channelRegistry
}

/**
 * Register a one-shot callback that fires when a `ServerChannel` with the given `id` is created.
 * The callback fires synchronously inside the constructor, before any peer attachment.
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
 *   const channel = createChannel<string, string>()
 *   channel.listen((msg) => console.log('from client:', msg))
 *   channel.send('hello from server')
 *   channel.onClose(() => console.log('channel closed'))
 *   return { channel: channel.client }
 * }
 * ```
 */
type CreateChannelOpts = {
  /**
   * Override the global send-buffer byte cap for this specific channel.
   * Defaults to the value set via `getTelefuncChannelHooks` options (or 256 KB).
   */
  sendBufferBytes?: number
}
function createChannel<TSend = unknown, TReceive = unknown>(opts?: CreateChannelOpts): Channel<TSend, TReceive>
function createChannel<TSend, TReceive>(
  opts: CreateChannelOpts & { ack: true },
): Channel<TSend, TReceive, unknown, unknown, true>
function createChannel<TSend, TReceive, TAckSend, TAckReceive>(
  opts: CreateChannelOpts & {
    ack: true
  },
): Channel<TSend, TReceive, TAckSend, TAckReceive, true>
function createChannel<TSend, TReceive, TAckSend, TAckReceive>(
  opts?: CreateChannelOpts,
): Channel<TSend, TReceive, TAckSend, TAckReceive, false>
function createChannel(opts?: CreateChannelOpts & { ack?: true }): any {
  return new ServerChannel(opts?.ack === true, undefined, opts?.sendBufferBytes)
}

/**
 * Safety TTL: if a channel is created but never serialized into a response
 * (i.e. `_registerChannel` is never called), close it after this many ms.
 * Should be large enough to cover any reasonable telefunction run time.
 */
const DEFAULT_TTL_MS = 5 * 60_000
class ServerChannel<TSend = unknown, TReceive = unknown, TAckSend = unknown, TAckReceive = unknown>
  implements Channel<TSend, TReceive, TAckSend, TAckReceive>
{
  readonly [SERVER_CHANNEL_BRAND] = true
  readonly id: string
  /** Whether ack is on by default. Set via `createChannel({ ack: true })`. */
  readonly ackMode: boolean

  /** Return this to the client — it serializes to a `ClientChannel` on the other side. */
  get client(): Channel<TReceive, TSend, TAckReceive, TAckSend> {
    return this as unknown as Channel<TReceive, TSend, TAckReceive, TAckSend>
  }

  static isServerChannel(value: unknown): value is ServerChannel {
    // Vite dev server can cause multiple instances of this module to be loaded,
    // so instanceof checks don't work — use a unique brand property instead.
    return hasProp(value, SERVER_CHANNEL_BRAND)
  }

  private _isClosed = false
  private _peer: IndexedPeer | null = null
  private _listeners: Array<(data: TReceive) => TAckReceive | Promise<TAckReceive> | void> = []
  private _binaryListeners: Array<(data: Uint8Array) => void> = []
  private _pauseCallbacks: Array<() => void> = []
  private _resumeCallbacks: Array<() => void> = []
  /** Drop-oldest ring buffer for messages sent before a peer connects. */
  private _prePeerBuffer: ServerChannelBuffer
  /** Buffered ack sends when peer not yet connected: { serialized, resolve, reject }[]. */
  private _ackSendBuffer: Array<{ data: string; resolve: (v: TAckSend) => void; reject: (err: Error) => void }> = []
  /** Pending ack promises keyed by the seq of the outgoing frame. */
  private _pendingAcks = new Map<number, { resolve: (result: TAckSend) => void; reject: (err: Error) => void }>()
  private _closeCallbacks: Array<(err?: Error) => void> = []
  private _openCallbacks: Array<() => void> = []
  private _closeError: Error | undefined
  private _didFireClose = false
  private _didFireOpen = false
  private _ttlTimer: ReturnType<typeof setTimeout> | null = null
  /** True when peer is gone but we're waiting for reconnection. */
  private _disconnected = false
  private _reconnectTimer: ReturnType<typeof setTimeout> | null = null

  constructor(ackMode = false, id?: string, maxSendBufferBytes?: number) {
    this.ackMode = ackMode
    this.id = id ?? crypto.randomUUID()
    this._prePeerBuffer = new ServerChannelBuffer(maxSendBufferBytes ?? globalObject.channelSendBufferBytes)
    // Safety TTL: if this channel is never serialized into a response, shut it down
    // to prevent memory leaks. The registry entry is added in _registerChannel().
    this._ttlTimer = unrefTimer(
      setTimeout(() => {
        this._ttlTimer = null
        this._shutdown(new ChannelNetworkError('Channel timed out: no peer connected within TTL'))
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
  send(data: TSend): void
  send(data: TSend, opts: { ack: true }): Promise<TAckSend>
  send(data: TSend, opts: { ack: false }): void
  send(data: TSend, opts?: { ack?: boolean }): Promise<TAckSend> | void {
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
      const seq = this._peer.sendTextAckReq(serialized)
      return new Promise<TAckSend>((resolve, reject) => this._pendingAcks.set(seq, { resolve, reject }))
    } else {
      // Ack-buffered sends don't count against the byte cap (they await a response and are rare).
      return new Promise<TAckSend>((resolve, reject) => {
        this._ackSendBuffer.push({ data: serialized, resolve, reject })
      })
    }
  }

  /** Send a binary message to the client.
   *  @throws {ChannelClosedError} if the channel is closed. */
  sendBinary(data: Uint8Array): void {
    if (this._isClosed) throw new ChannelClosedError()
    if (this._peer) {
      this._peer.sendBinary(data)
    } else {
      this._prePeerBuffer.pushBinary(data)
    }
  }

  listen(callback: (data: TReceive) => TAckReceive | Promise<TAckReceive> | void): void {
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

  /** Register a callback that fires when a peer connects for the first time. Fires exactly once. */
  onOpen(callback: () => void): void {
    if (this._didFireOpen) {
      callback()
      return
    }
    this._openCallbacks.push(callback)
  }

  /** @internal — Register a callback for backpressure pause from the transport layer. */
  _onPause(callback: () => void): void {
    this._pauseCallbacks.push(callback)
  }

  /** @internal — Register a callback for backpressure resume from the transport layer. */
  _onResume(callback: () => void): void {
    this._resumeCallbacks.push(callback)
  }

  /** Close the channel from the server side with an abort value.
   *  The client's `onClose` callback will receive an error with `isAbort: true` and `abortValue`. */
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
          new ChannelNetworkError('Channel timed out: no peer connected within TTL after response was sent'),
        )
      }, globalObject.channelConnectTtlMs),
    )
  }

  attachPeer(peer: IndexedPeer): void {
    this._clearTimer('_ttlTimer')
    this._clearTimer('_reconnectTimer')
    const wasDisconnected = this._disconnected
    this._disconnected = false
    this._peer = peer
    this._prePeerBuffer.flush(
      (msg) => peer.sendText(msg),
      (msg) => peer.sendBinary(msg),
    )
    // Flush buffered ack sends
    for (const { data, resolve, reject } of this._ackSendBuffer) {
      const seq = peer.sendTextAckReq(data)
      this._pendingAcks.set(seq, { resolve, reject })
    }
    this._ackSendBuffer = []
    this._fireOpen()
    if (wasDisconnected) {
      for (const cb of this._resumeCallbacks) cb()
    }
  }

  /** @internal — Called by ws.ts when a TEXT frame arrives (no ack expected). */
  _onPeerMessage(text: string): void {
    const data = parse(text) as TReceive
    for (const cb of this._listeners) {
      try {
        cb(data)
      } catch (err) {
        this._handleCallbackError(err)
        return
      }
    }
  }

  /** @internal — Called by ws.ts when a TEXT_ACK_REQ frame arrives. Dispatches the
   *  message and sends ACK_RES with the listener's resolved value. */
  async _onPeerAckReqMessage(text: string, seq: number): Promise<void> {
    const data = parse(text) as TReceive
    for (const cb of this._listeners) {
      try {
        const result = await cb(data)
        this._peer?.sendAckRes(seq, stringify(result, { forbidReactElements: false }))
      } catch (err) {
        this._handleCallbackError(err)
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
        this._handleCallbackError(err)
        return
      }
    }
  }

  /** @internal — Client sent ACK_RES for a message we sent. */
  _onPeerAckRes(ackedSeq: number, resultText: string): void {
    const pending = this._pendingAcks.get(ackedSeq)
    if (!pending) return
    this._pendingAcks.delete(ackedSeq)
    pending.resolve(parse(resultText) as TAckSend)
  }

  /** @internal — Connection dropped, keep channel alive for reconnection. */
  _onPeerDisconnect(reconnectTimeout: number): void {
    if (this._isClosed || this._disconnected) return
    this._peer = null
    this._disconnected = true
    for (const cb of this._pauseCallbacks) cb()
    this._reconnectTimer = unrefTimer(
      setTimeout(() => {
        this._reconnectTimer = null
        this._shutdown(new ChannelNetworkError('Channel timed out: peer did not reconnect within grace period'))
      }, reconnectTimeout),
    )
  }

  /** @internal — Permanent close from peer side. */
  _onPeerClose(): void {
    if (this._isClosed) return
    this._peer = null
    this._shutdown()
  }

  /** @internal */
  _onPeerPause(): void {
    for (const cb of this._pauseCallbacks) cb()
  }

  /** @internal */
  _onPeerResume(): void {
    for (const cb of this._resumeCallbacks) cb()
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
    const ackErr = new ChannelClosedError()
    for (const { reject } of this._pendingAcks.values()) reject(ackErr)
    this._pendingAcks.clear()
    for (const { reject } of this._ackSendBuffer) reject(ackErr)
    this._ackSendBuffer = []
  }

  private _fireClose(err?: Error): void {
    if (this._didFireClose) return
    this._didFireClose = true
    for (const cb of this._closeCallbacks) {
      try {
        cb(err)
      } catch {
        /* swallow */
      }
    }
    this._closeCallbacks.length = 0
    this._openCallbacks.length = 0
    this._pauseCallbacks.length = 0
    this._resumeCallbacks.length = 0
  }

  private _fireOpen(): void {
    if (this._didFireOpen) return
    this._didFireOpen = true
    for (const cb of this._openCallbacks) {
      try {
        cb()
      } catch (err) {
        this._handleCallbackError(err)
        return
      }
    }
    this._openCallbacks.length = 0
  }

  private _handleCallbackError(err: unknown): void {
    if (isAbort(err)) {
      this.abort(err.abortValue)
    } else {
      // Non-Abort error — send bug signal to client (no details leaked) then close
      if (this._peer) {
        this._peer.error()
        this._peer = null
      }
      this._shutdown()
    }
  }

  private _clearTimer(name: '_ttlTimer' | '_reconnectTimer'): void {
    const timer = this[name]
    if (timer) {
      clearTimeout(timer)
      ;(this as any)[name] = null
    }
  }
}
