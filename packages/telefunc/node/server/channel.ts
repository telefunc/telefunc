export { createChannel, getChannelRegistry, ServerChannel }
export type { Channel }

import { randomUUID } from 'node:crypto'
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
}

type ChannelOptions = {
  onClose?: () => void
}

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
        if (prop === 'send' || prop === 'listen' || prop === 'close' || prop === 'isOpen') {
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

class ServerChannel<TSend = unknown, TReceive = unknown> implements Channel<TSend, TReceive> {
  readonly id: string
  readonly [SERVER_CHANNEL_BRAND] = true

  private _isOpen = true
  private _peer: Peer | null = null
  private _listeners: Array<(data: TReceive) => void> = []
  private _sendBuffer: string[] = []
  private _onClose: (() => void) | undefined
  private _ttlTimer: ReturnType<typeof setTimeout> | null = null

  constructor(options?: ChannelOptions) {
    this.id = randomUUID()
    this._onClose = options?.onClose
    getChannelRegistry().set(this.id, this as ServerChannel<unknown, unknown>)
    this._startTtl()
  }

  /** Whether the channel is open */
  get isOpen(): boolean {
    return this._isOpen
  }

  /** Send a message to the client */
  send(data: TSend): void {
    if (!this._isOpen) return
    const serialized = stringify(data, { forbidReactElements: false })
    if (this._peer) {
      this._peer.send(serialized)
    } else {
      this._sendBuffer.push(serialized)
    }
  }

  /** Listen for messages from the client */
  listen(callback: (data: TReceive) => void): void {
    this._listeners.push(callback)
  }

  /** Close the channel from the server side */
  close(): void {
    if (!this._isOpen) return
    this._isOpen = false
    this._clearTtl()
    if (this._peer) {
      this._peer.close()
      this._peer = null
    }
    getChannelRegistry().delete(this.id)
    this._fireOnClose()
  }

  /** @internal — Called by crossws hooks when a peer connects */
  attachPeer(peer: Peer): void {
    this._clearTtl()
    this._peer = peer
    // Flush buffered messages
    for (const msg of this._sendBuffer) {
      peer.send(msg)
    }
    this._sendBuffer = []
  }

  /** @internal — Called by crossws hooks when a message arrives */
  _onPeerMessage(text: string): void {
    const data = parse(text) as TReceive
    for (const cb of this._listeners) {
      cb(data)
    }
  }

  /** @internal — Called by crossws hooks when the peer disconnects or errors */
  _onPeerClose(): void {
    if (!this._isOpen) return
    this._isOpen = false
    this._peer = null
    this._clearTtl()
    getChannelRegistry().delete(this.id)
    this._fireOnClose()
  }

  private _fireOnClose(): void {
    if (this._onClose) {
      try {
        this._onClose()
      } catch {
        // User callback errors are silently swallowed
      }
    }
  }

  private _startTtl(): void {
    this._ttlTimer = setTimeout(() => {
      if (!this._peer && this._isOpen) {
        // No WebSocket connected within TTL — clean up
        this._isOpen = false
        getChannelRegistry().delete(this.id)
        this._fireOnClose()
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
