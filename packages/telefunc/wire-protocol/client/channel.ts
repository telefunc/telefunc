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
 *  - `onClose(cb)` — fires once, when the channel closes; `err` is set on network/timeout errors, `undefined` for clean closes.
 *  - `close()` — close from the client side.
 *  - `isClosed` — whether the channel has been closed.
 *
 * Implements the shared `Channel` interface (see wire-protocol/channel.ts).
 */
class ClientChannel<TSend = unknown, TReceive = unknown> implements Channel<TSend, TReceive> {
  readonly id: string
  readonly ackMode: boolean
  private _connection: WsConnection
  private _listeners: Array<(data: TReceive) => Promise<unknown> | void> = []
  private _binaryListeners: Array<(data: Uint8Array) => void> = []
  private _openCallbacks: Array<() => void> = []
  private _closeCallbacks: Array<(err?: Error) => void> = []
  private _closeError: Error | undefined
  private _isClosed = false
  private _didFireClose = false
  private _didFireOpen = false

  get client(): Channel<TReceive, TSend> {
    return this as unknown as Channel<TReceive, TSend>
  }

  constructor(channelId: string, ackMode = false, shard?: string) {
    this.id = channelId
    this.ackMode = ackMode
    const config = resolveClientConfig()
    const wsUrl = shard ? appendShardParam(config.telefuncUrl, shard) : config.telefuncUrl
    this._connection = WsConnection.getOrCreate(wsUrl, this)
  }

  get isClosed(): boolean {
    return this._isClosed
  }

  send(data: TSend, opts?: { ack?: boolean }): Promise<unknown> | void {
    if (this._isClosed) return
    const needsAck = opts?.ack !== false && (opts?.ack === true || this.ackMode === true)
    const serialized = stringify(data, { forbidReactElements: false })
    if (needsAck) {
      return this._connection.sendTextAckReq(this, serialized)
    } else {
      this._connection.send(this, serialized)
    }
  }

  sendBinary(data: Uint8Array): void {
    if (this._isClosed) return
    this._connection.sendBinary(this, data)
  }

  listen(callback: (data: TReceive) => Promise<unknown> | void): void {
    this._listeners.push(callback)
  }

  listenBinary(callback: (data: Uint8Array) => void): void {
    this._binaryListeners.push(callback)
  }

  onClose(callback: (err?: Error) => void): void {
    if (this._didFireClose) {
      callback(this._closeError)
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
    this._connection.sendClose(this)
    this._connection.unregister(this)
    this._fireClose()
  }

  abort(abortValue?: unknown): void {
    if (this._isClosed) return
    this._isClosed = true
    this._connection.sendAbort(this, stringify(abortValue, { forbidReactElements: false }))
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
    for (const cb of this._listeners) {
      try {
        cb(parsed)
      } catch (err) {
        this._handleCallbackError(err)
        return
      }
    }
  }

  /** @internal — Server sent TEXT_ACK_REQ; dispatch and send ACK_RES back. */
  async _onWsAckReqMessage(data: string, seq: number): Promise<void> {
    const parsed = parse(data) as TReceive
    for (const cb of this._listeners) {
      try {
        const result = await cb(parsed)
        this._connection.sendAckRes(this, seq, stringify(result, { forbidReactElements: false }))
      } catch (err) {
        this._handleCallbackError(err)
        return
      }
    }
  }

  /** @internal */
  _onWsBinaryMessage(data: Uint8Array): void {
    for (const cb of this._binaryListeners) {
      try {
        cb(data)
      } catch (err) {
        this._handleCallbackError(err)
        return
      }
    }
  }

  /** @internal */
  _onWsClose(err?: Error): void {
    this._isClosed = true
    this._closeError = err
    this._fireClose(err)
  }

  // ── Private ──

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
  }

  private _handleCallbackError(err: unknown): void {
    if (this._isClosed) return
    this._isClosed = true
    this._connection.sendClose(this)
    this._connection.unregister(this)
    this._fireClose(err instanceof Error ? err : new Error(String(err)))
  }
}

/** Append ?shard=N (or &shard=N) to a URL string. */
function appendShardParam(url: string, shard: string): string {
  return url.includes('?') ? `${url}&shard=${shard}` : `${url}?shard=${shard}`
}
