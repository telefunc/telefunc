export { ClientChannel }

import type { Channel, ChannelClient, ChannelData, ChannelAck, ChannelListener } from '../channel.js'
import { parse } from '@brillout/json-serializer/parse'
import { stringify } from '@brillout/json-serializer/stringify'
import { getChannelTransport, resolveClientConfig } from '../../client/clientConfig.js'
import { createAbortError, isAbort } from '../../shared/Abort.js'
import { assert } from '../../utils/assert.js'
import { ClientConnection } from './connection.js'
import { CHANNEL_TRANSPORT, type ChannelTransport } from '../constants.js'
import type { MuxChannel, MuxConnection } from './connection.js'
import { ChannelClosedError } from '../channel-errors.js'

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
class ClientChannel<ClientToServer = unknown, ServerToClient = unknown>
  implements ChannelClient<ClientToServer, ServerToClient>, MuxChannel
{
  readonly id: string
  readonly ackMode: boolean
  readonly defer: boolean
  private _connection: MuxConnection
  private _listeners: Array<ChannelListener<ServerToClient>> = []
  private _binaryListeners: Array<(data: Uint8Array<ArrayBuffer>) => void> = []
  private _openCallbacks: Array<() => void> = []
  private _closeCallbacks: Array<(err?: Error) => void> = []
  private _closeError: Error | undefined
  private _isClosed = false
  private _didFireClose = false
  private _didFireOpen = false

  get client(): Channel<ServerToClient, ClientToServer> {
    return this as unknown as Channel<ServerToClient, ClientToServer>
  }

  constructor({
    channelId,
    ackMode = false,
    channelTransport,
    shard,
    defer = false,
  }: {
    channelId: string
    ackMode?: boolean
    channelTransport?: ChannelTransport
    shard?: string
    defer?: boolean
  }) {
    this.id = channelId
    this.ackMode = ackMode
    this.defer = defer
    const config = resolveClientConfig()
    const url = shard ? appendShardParam(config.telefuncUrl, shard) : config.telefuncUrl
    const resolvedTransport = channelTransport ?? getChannelTransport(config)
    assert(
      resolvedTransport === CHANNEL_TRANSPORT.SSE || resolvedTransport === CHANNEL_TRANSPORT.WS,
      `Unknown channel transport: ${String(resolvedTransport)}`,
    )
    this._connection = ClientConnection.getOrCreate(url, this, {
      transport: resolvedTransport,
      fetchImpl: config.fetch ?? globalThis.fetch,
    })
  }

  get isClosed(): boolean {
    return this._isClosed
  }

  send(data: ChannelData<ClientToServer>): void
  send(data: ChannelData<ClientToServer>, opts: { ack: true }): Promise<ChannelAck<ClientToServer>>
  send(data: ChannelData<ClientToServer>, opts: { ack: false }): void
  send(data: ChannelData<ClientToServer>, opts?: { ack?: boolean }): Promise<ChannelAck<ClientToServer>> | void {
    if (this._isClosed) throw new ChannelClosedError()
    const needsAck = opts?.ack !== false && (opts?.ack === true || this.ackMode === true)
    const serialized = stringify(data, { forbidReactElements: false })
    if (needsAck) {
      return this._connection.sendTextAckReq(this, serialized) as Promise<ChannelAck<ClientToServer>>
    } else {
      this._connection.send(this, serialized)
    }
  }

  sendBinary(data: Uint8Array): void {
    if (this._isClosed) throw new ChannelClosedError()
    this._connection.sendBinary(this, data)
  }

  listen(callback: ChannelListener<ServerToClient>): void {
    this._listeners.push(callback)
  }

  listenBinary(callback: (data: Uint8Array<ArrayBuffer>) => void): void {
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
    this._fireClose()
  }

  abort(): void {
    if (this._isClosed) return
    this._isClosed = true
    const abortError = createAbortError()
    this._connection.sendClose(this, abortError)
    this._fireClose(abortError)
  }

  /** @internal */
  _abortLocally(abortValue?: unknown, message?: string): void {
    if (this._isClosed) return
    this._isClosed = true
    const abortError = createAbortError(abortValue, message)
    this._connection.sendClose(this, abortError)
    this._fireClose(abortError)
  }

  /** @internal */
  _pause(): void {
    this._connection.sendPause(this)
  }

  /** @internal */
  _resume(): void {
    this._connection.sendResume(this)
  }

  // ── Called by transport connection ──

  /** @internal */
  _onTransportOpen(): void {
    this._fireOpen()
  }

  /** @internal */
  _onTransportMessage(data: string): void {
    const parsed = parse(data) as ChannelData<ServerToClient>
    for (const cb of this._listeners) {
      try {
        cb(parsed)
      } catch (err) {
        if (this._handleCallbackError(err)) return
      }
    }
  }

  /** @internal — Server sent TEXT_ACK_REQ; dispatch and send ACK_RES back. */
  async _onTransportAckReqMessage(data: string, seq: number): Promise<void> {
    const parsed = parse(data) as ChannelData<ServerToClient>
    for (const cb of this._listeners) {
      try {
        const result = await cb(parsed)
        this._connection.sendAckRes(this, seq, stringify(result, { forbidReactElements: false }))
      } catch (err) {
        if (this._handleCallbackError(err)) return
        this._connection.sendAckRes(this, seq, 'Internal client channel error — see client logs', 'error')
        return
      }
    }
  }

  /** @internal */
  _onTransportBinaryMessage(data: Uint8Array<ArrayBuffer>): void {
    for (const cb of this._binaryListeners) {
      try {
        cb(data)
      } catch (err) {
        if (this._handleCallbackError(err)) return
      }
    }
  }

  /** @internal */
  _onTransportClose(err?: Error): void {
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
        if (this._handleCallbackError(err)) return
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
      } catch (callbackErr) {
        reportLocalChannelCallbackError(callbackErr)
      }
    }
    this._closeCallbacks.length = 0
    this._openCallbacks.length = 0
  }

  private _handleCallbackError(err: unknown): boolean {
    if (isAbort(err)) {
      const abortError = err
      this._abortLocally(abortError.abortValue, abortError.message)
      return true
    }
    reportLocalChannelCallbackError(err)
    return false
  }
}

function reportLocalChannelCallbackError(err: unknown): void {
  console.error('[telefunc:channel-callback-error]', normalizeCallbackError(err))
}

function normalizeCallbackError(err: unknown): Error {
  return err instanceof Error ? err : new Error(String(err))
}

/** Append ?shard=N (or &shard=N) to a URL string. */
function appendShardParam(url: string, shard: string): string {
  return url.includes('?') ? `${url}&shard=${shard}` : `${url}?shard=${shard}`
}
