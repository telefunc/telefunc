export { createChannel, getChannelRegistry, ServerChannel, ChannelClosedError }
export type { Channel }

import { stringify } from '@brillout/json-serializer/stringify'
import { parse } from '@brillout/json-serializer/parse'
import { getGlobalObject } from '../../utils/getGlobalObject.js'
import { assertUsage } from '../../utils/assert.js'
import { objectAssign } from '../../utils/objectAssign.js'

/**
 * Bidirectional channel interface.
 *
 * `Channel<TSend, TReceive>` means "I send TSend, I receive TReceive".
 * Both server and client use the same interface, with generics flipped.
 */
interface Channel<TSend = unknown, TReceive = unknown> {
  send(data: TSend): void
  listen(callback: (data: TReceive) => void): void
  close(): void
  readonly isOpen: boolean
  /** Register a callback that fires when the channel closes for any reason
   *  (server close, peer disconnect, or TTL). Fires exactly once. */
  onClose(callback: () => void): void
  /** Register a callback that fires only when the peer disconnects unexpectedly
   *  (not when the server calls `close()`). Does NOT fire on normal close. */
  onAbort(callback: () => void): void
}

type ChannelOptions = {}

const SERVER_CHANNEL_BRAND = Symbol.for('telefunc.ServerChannel')

const globalObject = getGlobalObject('channel.ts', {
  channelRegistry: new Map<string, ServerChannel<unknown, unknown>>(),
})

function getChannelRegistry(): Map<string, ServerChannel<unknown, unknown>> {
  return globalObject.channelRegistry
}

/**
 * Create a bidirectional channel for real-time communication with the client.
 *
 * Use the returned channel directly for server-side `send()`/`listen()`.
 * Return `channel.client` to the client — it has the same underlying connection
 * but with flipped types.
 *
 * @example
 * ```ts
 * import { createChannel } from 'telefunc'
 *
 * export async function onChat() {
 *   const channel = createChannel<string, string>({
 *     onClose: () => console.log('client disconnected'),
 *   })
 *   channel.listen((msg) => console.log('from client:', msg))
 *   channel.send('hello from server')
 *   channel.onClose(() => console.log('channel closed'))
 *   channel.onAbort(() => console.log('client disconnected'))
 *   return { channel: channel.client }
 * }
 * ```
 */
function createChannel<TSend = unknown, TReceive = unknown>(
  options?: ChannelOptions,
): Channel<TSend, TReceive> & { client: Channel<TReceive, TSend> } {
  const channel = new ServerChannel<TSend, TReceive>(options)
  const client = new Proxy(
    {},
    {
      get(_target, prop: string | symbol) {
        if (prop === SERVER_CHANNEL_BRAND) return true
        if (prop === 'id') return channel.id
        if (
          prop === 'send' ||
          prop === 'listen' ||
          prop === 'close' ||
          prop === 'isOpen' ||
          prop === 'onClose' ||
          prop === 'onAbort'
        ) {
          assertUsage(
            false,
            '`channel.client` is not meant to be used on the server. Use `channel` directly instead. `channel.client` is only meant to be returned to the client.',
          )
        }
        return undefined
      },
      has(_target, prop: string | symbol) {
        if (prop === SERVER_CHANNEL_BRAND) return true
        return false
      },
    },
  ) as unknown as Channel<TReceive, TSend>
  objectAssign(channel, { client })
  return channel
}

const DEFAULT_TTL_MS = 30_000

/** Thrown by sendBinary/send when the channel has been closed. */
class ChannelClosedError extends Error {
  constructor() {
    super('Channel is closed')
    this.name = 'ChannelClosedError'
  }
}

class ServerChannel<TSend = unknown, TReceive = unknown> implements Channel<TSend, TReceive> {
  readonly id: string
  readonly [SERVER_CHANNEL_BRAND] = true

  /** Resolves when a WebSocket peer connects to this channel. */
  readonly peerConnected: Promise<void>

  private _isOpen = true
  private _peer: Peer | null = null
  private _listeners: Array<(data: TReceive) => void> = []
  private _binaryListeners: Array<(data: Uint8Array) => void> = []
  private _sendBuffer: string[] = []
  private _binarySendBuffer: Uint8Array[] = []
  private _closeCallbacks: Array<() => void> = []
  private _abortCallbacks: Array<() => void> = []
  private _closed = false
  private _aborted = false
  private _ttlTimer: ReturnType<typeof setTimeout> | null = null
  private _resolvePeerConnected!: () => void
  private _rejectPeerConnected!: (err: Error) => void

  constructor(_options?: ChannelOptions) {
    this.id = crypto.randomUUID()
    this.peerConnected = new Promise<void>((resolve, reject) => {
      this._resolvePeerConnected = resolve
      this._rejectPeerConnected = reject
    })
    getChannelRegistry().set(this.id, this as ServerChannel<unknown, unknown>)
    this._startTtl()
  }

  /** Whether the channel is open */
  get isOpen(): boolean {
    return this._isOpen
  }

  /** Send a message to the client.
   *  @throws {ChannelClosedError} if the channel is closed. */
  send(data: TSend): void {
    if (!this._isOpen) throw new ChannelClosedError()
    const serialized = stringify(data, { forbidReactElements: false })
    if (this._peer) {
      this._peer.send(serialized)
    } else {
      this._sendBuffer.push(serialized)
    }
  }

  /** Send a binary message to the client.
   *  @throws {ChannelClosedError} if the channel is closed. */
  sendBinary(data: Uint8Array): void {
    if (!this._isOpen) throw new ChannelClosedError()
    if (this._peer) {
      this._peer.send(data.buffer as ArrayBuffer)
    } else {
      this._binarySendBuffer.push(data)
    }
  }

  /** Listen for messages from the client */
  listen(callback: (data: TReceive) => void): void {
    this._listeners.push(callback)
  }

  /** Listen for binary messages from the client */
  listenBinary(callback: (data: Uint8Array) => void): void {
    this._binaryListeners.push(callback)
  }

  /** Register a callback that fires when the channel closes for any reason. */
  onClose(callback: () => void): void {
    if (this._closed) {
      callback()
      return
    }
    this._closeCallbacks.push(callback)
  }

  /** Register a callback that fires only when the peer disconnects unexpectedly. */
  onAbort(callback: () => void): void {
    if (this._closed && this._aborted) {
      callback()
      return
    }
    if (this._closed) return
    this._abortCallbacks.push(callback)
  }

  /** Close the channel from the server side (normal close, not abort). */
  close(): void {
    if (!this._isOpen) return
    this._isOpen = false
    this._clearTtl()
    this._rejectPeerConnected(new ChannelClosedError())
    if (this._peer) {
      this._peer.close()
      this._peer = null
    }
    getChannelRegistry().delete(this.id)
    this._fireClose()
  }

  /** @internal — Called by crossws hooks when a peer connects */
  attachPeer(peer: Peer): void {
    this._clearTtl()
    this._peer = peer
    // Flush buffered text messages
    for (const msg of this._sendBuffer) {
      peer.send(msg)
    }
    this._sendBuffer = []
    // Flush buffered binary messages
    for (const msg of this._binarySendBuffer) {
      peer.send(msg.buffer as ArrayBuffer)
    }
    this._binarySendBuffer = []
    this._resolvePeerConnected()
  }

  /** @internal — Called by crossws hooks when a message arrives */
  _onPeerMessage(text: string): void {
    const data = parse(text) as TReceive
    for (const cb of this._listeners) {
      cb(data)
    }
  }

  /** @internal — Called by crossws hooks when a binary message arrives */
  _onPeerBinaryMessage(data: Uint8Array): void {
    for (const cb of this._binaryListeners) {
      cb(data)
    }
  }

  /** @internal — Called by crossws hooks when the peer disconnects or errors */
  _onPeerClose(): void {
    if (!this._isOpen) return
    this._isOpen = false
    this._peer = null
    this._clearTtl()
    this._rejectPeerConnected(new ChannelClosedError())
    getChannelRegistry().delete(this.id)
    this._fireAbort()
  }

  private _fireAbort(): void {
    if (this._closed) return
    this._aborted = true
    for (const cb of this._abortCallbacks) {
      try {
        cb()
      } catch {
        /* swallow */
      }
    }
    this._abortCallbacks.length = 0
    this._fireClose()
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
    this._abortCallbacks.length = 0
  }

  private _startTtl(): void {
    this._ttlTimer = setTimeout(() => {
      if (!this._peer && this._isOpen) {
        // No WebSocket connected within TTL — clean up
        this._isOpen = false
        this._rejectPeerConnected(new ChannelClosedError())
        getChannelRegistry().delete(this.id)
        this._fireAbort()
      }
    }, DEFAULT_TTL_MS)
    // Don't prevent Node.js from exiting
    if (this._ttlTimer && typeof this._ttlTimer === 'object' && 'unref' in this._ttlTimer) {
      this._ttlTimer.unref()
    }
  }

  private _clearTtl(): void {
    if (this._ttlTimer) {
      clearTimeout(this._ttlTimer)
      this._ttlTimer = null
    }
  }

  /** @internal — Brand check for cross-module detection */
  static isServerChannel(value: unknown): value is ServerChannel {
    return (
      typeof value === 'object' &&
      value !== null &&
      (value instanceof ServerChannel ||
        (SERVER_CHANNEL_BRAND in value && (value as any)[SERVER_CHANNEL_BRAND] === true))
    )
  }
}

/** Minimal interface for the crossws Peer that ServerChannel needs */
type Peer = {
  send(data: string | ArrayBuffer): void
  close(code?: number, reason?: string): void
}
