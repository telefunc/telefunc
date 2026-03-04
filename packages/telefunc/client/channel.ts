export { ClientChannel }

import { stringify } from '@brillout/json-serializer/stringify'
import { parse } from '@brillout/json-serializer/parse'
import { resolveClientConfig } from './clientConfig.js'

/**
 * Client-side channel for bidirectional WebSocket communication with the server.
 *
 * Created automatically when a telefunction returns a `createChannel()` value.
 * The WebSocket connection is opened lazily when the channel is deserialized.
 */
class ClientChannel<TSend = unknown, TReceive = unknown> {
  private _ws: WebSocket | null = null
  private _listeners: Array<(data: TReceive) => void> = []
  private _binaryListeners: Array<(data: Uint8Array) => void> = []
  private _sendBuffer: string[] = []
  private _binarySendBuffer: Uint8Array[] = []
  private _isOpen = true

  constructor(private _channelId: string) {
    this._connect()
  }

  /** Whether the channel is open */
  get isOpen(): boolean {
    return this._isOpen
  }

  /** Send a message to the server */
  send(data: TSend): void {
    if (!this._isOpen) return
    const serialized = stringify(data, { forbidReactElements: false })
    if (this._ws && this._ws.readyState === WebSocket.OPEN) {
      this._ws.send(serialized)
    } else {
      this._sendBuffer.push(serialized)
    }
  }

  /** Send a binary message to the server */
  sendBinary(data: Uint8Array<ArrayBuffer>): void {
    if (!this._isOpen) return
    if (this._ws && this._ws.readyState === WebSocket.OPEN) {
      this._ws.send(data)
    } else {
      this._binarySendBuffer.push(data)
    }
  }

  /** Listen for messages from the server */
  listen(callback: (data: TReceive) => void): void {
    this._listeners.push(callback)
  }

  /** Listen for binary messages from the server */
  listenBinary(callback: (data: Uint8Array) => void): void {
    this._binaryListeners.push(callback)
  }

  /** Close the channel */
  close(): void {
    if (!this._isOpen) return
    this._isOpen = false
    if (this._ws) {
      this._ws.close()
      this._ws = null
    }
    this._sendBuffer = []
    this._binarySendBuffer = []
  }

  private _connect(): void {
    const wsUrl = deriveWsUrl(this._channelId)
    try {
      this._ws = new WebSocket(wsUrl)
    } catch {
      // WebSocket unavailable — degrade gracefully
      this._isOpen = false
      return
    }

    this._ws.binaryType = 'arraybuffer'

    this._ws.onopen = () => {
      // Flush buffered text sends
      for (const msg of this._sendBuffer) {
        this._ws!.send(msg)
      }
      this._sendBuffer = []
      // Flush buffered binary sends
      for (const msg of this._binarySendBuffer) {
        this._ws!.send(msg as Uint8Array<ArrayBuffer>)
      }
      this._binarySendBuffer = []
    }

    this._ws.onmessage = (event: MessageEvent) => {
      if (event.data instanceof ArrayBuffer) {
        const data = new Uint8Array(event.data)
        for (const cb of this._binaryListeners) {
          cb(data)
        }
      } else {
        const data = parse(event.data as string) as TReceive
        for (const cb of this._listeners) {
          cb(data)
        }
      }
    }

    this._ws.onclose = () => {
      this._isOpen = false
      this._ws = null
    }

    this._ws.onerror = () => {
      // Error will be followed by onclose
    }
  }
}

function deriveWsUrl(channelId: string): string {
  const { telefuncUrl } = resolveClientConfig()
  const base = telefuncUrl.startsWith('http') ? telefuncUrl : location.origin + telefuncUrl
  const url = new URL(base)
  url.protocol = url.protocol === 'https:' ? 'wss:' : 'ws:'
  url.searchParams.set('channelId', channelId)
  return url.toString()
}
