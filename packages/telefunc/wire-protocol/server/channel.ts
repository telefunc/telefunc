export { createChannel, getChannelRegistry, ServerChannel, ChannelClosedError }

import type { Channel } from '../channel.js'
import { stringify } from '@brillout/json-serializer/stringify'
import { parse } from '@brillout/json-serializer/parse'
import { getGlobalObject } from '../../utils/getGlobalObject.js'
import { hasProp } from '../../utils/hasProp.js'

const globalObject = getGlobalObject('channel.ts', {
  channelRegistry: new Map<string, ServerChannel<unknown, unknown>>(),
})

function getChannelRegistry(): Map<string, ServerChannel<unknown, unknown>> {
  return globalObject.channelRegistry
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
function createChannel<TSend = unknown, TReceive = unknown>(): Channel<TSend, TReceive> {
  return new ServerChannel<TSend, TReceive>()
}

const DEFAULT_TTL_MS = 30_000
const RECONNECT_GRACE_MS = 60_000

/** Thrown by sendBinary/send when the channel has been closed. */
class ChannelClosedError extends Error {
  constructor() {
    super('Channel is closed')
    this.name = 'ChannelClosedError'
  }
}
class ServerChannel<TSend = unknown, TReceive = unknown> implements Channel<TSend, TReceive> {
  readonly id: string

  /** Return this to the client — it serializes to a `ClientChannel` on the other side. */
  get client(): Channel<TReceive, TSend> {
    return this as unknown as Channel<TReceive, TSend>
  }

  private _isClosed = false
  private _peer: { sendText(data: string): void; sendBinary(data: Uint8Array): void; close(): void } | null = null
  private _listeners: Array<(data: TReceive) => void> = []
  private _binaryListeners: Array<(data: Uint8Array) => void> = []
  private _pauseCallbacks: Array<() => void> = []
  private _resumeCallbacks: Array<() => void> = []
  private _sendBuffer: string[] = []
  private _binarySendBuffer: Uint8Array[] = []
  private _closeCallbacks: Array<() => void> = []
  private _openCallbacks: Array<() => void> = []
  private _didFireClose = false
  private _didFireOpen = false
  private _ttlTimer: ReturnType<typeof setTimeout> | null = null
  /** True when peer is gone but we're waiting for reconnection. */
  private _disconnected = false
  private _reconnectTimer: ReturnType<typeof setTimeout> | null = null

  constructor() {
    this.id = crypto.randomUUID()
    getChannelRegistry().set(this.id, this as ServerChannel<unknown, unknown>)
    // If no peer connects within TTL, close automatically.
    this._ttlTimer = this._unrefTimer(
      setTimeout(() => {
        this._ttlTimer = null
        this._shutdown()
      }, DEFAULT_TTL_MS),
    )
  }

  get isClosed(): boolean {
    return this._isClosed
  }

  /** Send a message to the client.
   *  @throws {ChannelClosedError} if the channel is closed. */
  send(data: TSend): void {
    if (this._isClosed) throw new ChannelClosedError()
    const serialized = stringify(data, { forbidReactElements: false })
    if (this._peer) {
      this._peer.sendText(serialized)
    } else {
      this._sendBuffer.push(serialized)
    }
  }

  /** Send a binary message to the client.
   *  @throws {ChannelClosedError} if the channel is closed. */
  sendBinary(data: Uint8Array): void {
    if (this._isClosed) throw new ChannelClosedError()
    if (this._peer) {
      this._peer.sendBinary(data)
    } else {
      this._binarySendBuffer.push(data)
    }
  }

  listen(callback: (data: TReceive) => void): void {
    this._listeners.push(callback)
  }

  listenBinary(callback: (data: Uint8Array) => void): void {
    this._binaryListeners.push(callback)
  }

  /** Register a callback that fires when the channel closes for any reason. Fires exactly once. */
  onClose(callback: () => void): void {
    if (this._didFireClose) {
      callback()
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

  /** Close the channel from the server side. */
  close(): void {
    if (this._isClosed) return
    if (this._peer) {
      this._peer.close()
      this._peer = null
    }
    this._shutdown()
  }

  /** @internal — Called by WS hooks when a peer connects (or reconnects). */
  attachPeer(peer: { sendText(data: string): void; sendBinary(data: Uint8Array): void; close(): void }): void {
    this._clearTimer('_ttlTimer')
    this._clearTimer('_reconnectTimer')
    const wasDisconnected = this._disconnected
    this._disconnected = false
    this._peer = peer
    for (const msg of this._sendBuffer) peer.sendText(msg)
    this._sendBuffer = []
    for (const msg of this._binarySendBuffer) peer.sendBinary(msg)
    this._binarySendBuffer = []
    this._fireOpen()
    if (wasDisconnected) {
      for (const cb of this._resumeCallbacks) cb()
    }
  }

  /** @internal */
  _onPeerMessage(text: string): void {
    const data = parse(text) as TReceive
    for (const cb of this._listeners) cb(data)
  }

  /** @internal */
  _onPeerBinaryMessage(data: Uint8Array): void {
    for (const cb of this._binaryListeners) cb(data)
  }

  /** @internal — Connection dropped, keep channel alive for reconnection. */
  _onPeerDisconnect(): void {
    if (this._isClosed || this._disconnected) return
    this._peer = null
    this._disconnected = true
    for (const cb of this._pauseCallbacks) cb()
    this._reconnectTimer = this._unrefTimer(
      setTimeout(() => {
        this._reconnectTimer = null
        this._shutdown()
      }, RECONNECT_GRACE_MS),
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
  private _shutdown(): void {
    if (this._isClosed) return
    this._isClosed = true
    this._clearTimer('_ttlTimer')
    this._clearTimer('_reconnectTimer')
    getChannelRegistry().delete(this.id)
    this._fireClose()
  }

  private _fireClose(): void {
    if (this._didFireClose) return
    this._didFireClose = true
    for (const cb of this._closeCallbacks) {
      try {
        cb()
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
      } catch {
        /* swallow */
      }
    }
    this._openCallbacks.length = 0
  }

  private _clearTimer(name: '_ttlTimer' | '_reconnectTimer'): void {
    const timer = this[name]
    if (timer) {
      clearTimeout(timer)
      ;(this as any)[name] = null
    }
  }

  private _unrefTimer(timer: ReturnType<typeof setTimeout>): ReturnType<typeof setTimeout> {
    if (hasProp(timer, 'unref', 'function')) timer.unref()
    return timer
  }
}
