export { ClientChannel }

import type { Channel } from '../channel.js'
import { parse } from '@brillout/json-serializer/parse'
import { stringify } from '@brillout/json-serializer/stringify'
import { resolveClientConfig } from '../../client/clientConfig.js'
import { WsConnection } from './ws.js'

/**
 * Client-side channel for bidirectional communication with the server.
 *
 * Created automatically when a telefunction returns a `createChannel()` value.
 *
 * Public API:
 *  - `send(data)` / `sendBinary(data)` — send to the server.
 *  - `listen(cb)` / `listenBinary(cb)` — receive from the server.
 *  - `onOpen(cb)` — fires once, when the server acknowledges this channel.
 *  - `onClose(cb)` — fires once, when the channel closes for any reason.
 *  - `close()` — close from the client side.
 *  - `isClosed` — whether the channel has been closed.
 *
 * Implements the shared `Channel` interface (see wire-protocol/channel.ts).
 */
class ClientChannel<TSend = unknown, TReceive = unknown> implements Channel<TSend, TReceive> {
  readonly id: string
  private _connection: WsConnection
  private _listeners: Array<(data: TReceive) => void> = []
  private _binaryListeners: Array<(data: Uint8Array) => void> = []
  private _openCallbacks: Array<() => void> = []
  private _closeCallbacks: Array<() => void> = []
  private _isClosed = false
  private _didFireClose = false
  private _didFireOpen = false

  get client(): Channel<TReceive, TSend> {
    return this as unknown as Channel<TReceive, TSend>
  }

  constructor(channelId: string) {
    this.id = channelId
    const config = resolveClientConfig()
    this._connection = WsConnection.getOrCreate(config.telefuncUrl, this)
  }

  get isClosed(): boolean {
    return this._isClosed
  }

  send(data: TSend): void {
    if (this._isClosed) return
    this._connection.send(this, stringify(data, { forbidReactElements: false }))
  }

  sendBinary(data: Uint8Array): void {
    if (this._isClosed) return
    this._connection.sendBinary(this, data)
  }

  listen(callback: (data: TReceive) => void): void {
    this._listeners.push(callback)
  }

  listenBinary(callback: (data: Uint8Array) => void): void {
    this._binaryListeners.push(callback)
  }

  onClose(callback: () => void): void {
    if (this._didFireClose) {
      callback()
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

  close(): void {
    if (this._isClosed) return
    this._isClosed = true
    this._connection.unregister(this)
    this._fireClose()
  }

  /** @internal */
  _pause(): void {
    this._connection.sendPause(this)
  }

  /** @internal */
  _resume(): void {
    this._connection.sendResume(this)
  }

  // ── Called by WsConnection ──

  /** @internal */
  _onWsOpen(): void {
    this._fireOpen()
  }

  /** @internal */
  _onWsMessage(data: string): void {
    const parsed = parse(data) as TReceive
    for (const cb of this._listeners) cb(parsed)
  }

  /** @internal */
  _onWsBinaryMessage(data: Uint8Array): void {
    for (const cb of this._binaryListeners) cb(data)
  }

  /** @internal */
  _onWsClose(): void {
    this._isClosed = true
    this._fireClose()
  }

  // ── Private ──

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
  }
}
