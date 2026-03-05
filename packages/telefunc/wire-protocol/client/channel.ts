export { ClientChannel }

import type { Channel } from '../channel.js'
import { parse } from '@brillout/json-serializer/parse'
import { stringify } from '@brillout/json-serializer/stringify'
import { resolveClientConfig } from '../../client/clientConfig.js'
import { WsConnection } from './ws.js'
import type { ChannelCallbacks, ChannelHandle } from './ws.js'

// Connection cache — keyed by wsUrl. Allows future channelPerConnection option
// to bypass the cache and create a dedicated WsConnection per channel.
const connectionCache = new Map<string, WsConnection>()

function getOrCreateConnection(wsUrl: string, channelId: string, callbacks: ChannelCallbacks): ChannelHandle {
  let connection = connectionCache.get(wsUrl)
  if (!connection || connection.closed) {
    connection = new WsConnection(wsUrl, channelId, callbacks, () => connectionCache.delete(wsUrl))
    connectionCache.set(wsUrl, connection)
    return connection.primaryHandle
  }
  return connection.register(channelId, callbacks)
}

/**
 * Client-side channel for bidirectional communication with the server.
 *
 * Created automatically when a telefunction returns a `createChannel()` value.
 * All channels multiplex over a single WebSocket connection per server URL.
 */
class ClientChannel<TSend = unknown, TReceive = unknown> implements Channel<TSend, TReceive> {
  readonly id: string
  private _handle: ChannelHandle
  private _listeners: Array<(data: TReceive) => void> = []
  private _binaryListeners: Array<(data: Uint8Array) => void> = []
  private _openCallbacks: Array<() => void> = []
  private _closeCallbacks: Array<() => void> = []
  private _isOpen = true
  private _closed = false
  private _opened = false

  /** The other end of the channel with flipped types — on the client this just returns itself. */
  get client(): Channel<TReceive, TSend> {
    return this as unknown as Channel<TReceive, TSend>
  }

  constructor(channelId: string) {
    this.id = channelId
    const config = resolveClientConfig()
    const wsUrl = deriveWsUrl(channelId, config.telefuncUrl)

    const callbacks: ChannelCallbacks = {
      onOpen: () => this._fireOpen(),
      onMessage: (data: string) => {
        const parsed = parse(data) as TReceive
        for (const cb of this._listeners) cb(parsed)
      },
      onBinaryMessage: (data: Uint8Array) => {
        for (const cb of this._binaryListeners) cb(data)
      },
      onClose: () => {
        this._isOpen = false
        this._fireClose()
      },
    }

    this._handle = getOrCreateConnection(wsUrl, channelId, callbacks)
  }

  get isOpen(): boolean {
    return this._isOpen
  }

  send(data: TSend): void {
    if (!this._isOpen) return
    this._handle.send(stringify(data, { forbidReactElements: false }))
  }

  sendBinary(data: Uint8Array): void {
    if (!this._isOpen) return
    this._handle.sendBinary(data)
  }

  listen(callback: (data: TReceive) => void): void {
    this._listeners.push(callback)
  }

  listenBinary(callback: (data: Uint8Array) => void): void {
    this._binaryListeners.push(callback)
  }

  onClose(callback: () => void): void {
    if (this._closed) {
      callback()
      return
    }
    this._closeCallbacks.push(callback)
  }

  onOpen(callback: () => void): void {
    if (this._opened) {
      callback()
      return
    }
    this._openCallbacks.push(callback)
  }

  /** @internal */
  _pause(): void {
    this._handle.pause()
  }

  /** @internal */
  _resume(): void {
    this._handle.resume()
  }

  close(): void {
    if (!this._isOpen) return
    this._isOpen = false
    this._handle.close()
    this._fireClose()
  }

  private _fireOpen(): void {
    if (this._opened) return
    this._opened = true
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
    if (this._closed) return
    this._closed = true
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

function deriveWsUrl(channelId: string, telefuncUrl: string): string {
  const base = telefuncUrl.startsWith('http') ? telefuncUrl : location.origin + telefuncUrl
  const url = new URL(base)
  url.protocol = url.protocol === 'https:' ? 'wss:' : 'ws:'
  url.searchParams.set('channelId', channelId)
  return url.toString()
}
