export { ClientChannel }

import type {
  ChannelAck,
  ChannelClient,
  ChannelCloseCallback,
  ChannelCloseOptions,
  ChannelCloseResult,
  ChannelData,
  ChannelListener,
} from '../channel.js'
import { parse } from '@brillout/json-serializer/parse'
import { stringify } from '@brillout/json-serializer/stringify'
import { getChannelTransport, resolveClientConfig } from '../../client/clientConfig.js'
import { createAbortError, isAbort } from '../../shared/Abort.js'
import { assert } from '../../utils/assert.js'
import { ClientConnection } from './connection.js'
import { CHANNEL_CLOSE_TIMEOUT_MS, CHANNEL_TRANSPORT, type ChannelTransport } from '../constants.js'
import type { MuxChannel, MuxConnection } from './connection.js'
import { ChannelClosedError, ChannelNetworkError } from '../channel-errors.js'
import { isPromise } from '../../utils/isPromise.js'

/**
 * Client-side channel for bidirectional communication with the server.
 *
 * Created automatically when a telefunction returns a `createChannel()` value.
 *
 * Public API:
 *  - `send(data)` / `sendBinary(data)` — send to the server.
 *  - `listen(cb)` / `listenBinary(cb)` — receive from the server.
 *  - `onOpen(cb)` — fires once, when the server acknowledges this channel.
 *  - `onClose(cb)` — fires once, immediately when closing starts; async callbacks are awaited before final teardown.
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
  private _closeCallbacks: Array<ChannelCloseCallback> = []
  private _closeError: Error | undefined
  private _isClosed = false
  private _didTerminate = false
  private _didFireClose = false
  private _didFireOpen = false
  private _closePromise: Promise<ChannelCloseResult> | null = null
  private _closeDeadline = 0
  private _closeWaiters: Array<() => void> = []
  private _didReceiveCloseAck = false
  private _expectCloseAck = false
  private _pendingCloseCallbacks = 0
  private _inflightAcks = 0

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
      return this._trackAck(this._connection.sendTextAckReq(this, serialized) as Promise<ChannelAck<ClientToServer>>)
    }
    this._connection.send(this, serialized)
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

  onClose(callback: ChannelCloseCallback): void {
    if (this._didFireClose) {
      this._invokeCloseCallback(callback, this._closeError, false)
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

  close(opts?: ChannelCloseOptions): Promise<ChannelCloseResult> {
    if (this._closePromise) return this._closePromise
    if (this._didTerminate) return Promise.resolve(this._didReceiveCloseAck ? 0 : 1)
    const timeout = normalizeCloseTimeout(opts?.timeout)
    this._closeDeadline = Date.now() + timeout
    this._expectCloseAck = true
    this._startClose()
    this._connection.sendCloseRequest(this, timeout)
    this._closePromise = this._runFinalizationLoop()
    return this._closePromise
  }

  abort(): void {
    if (this._didTerminate || this._isClosed) return
    this._isClosed = true
    const abortError = createAbortError()
    this._connection.sendAbort(this)
    this._finalizeClose(abortError)
  }

  /** @internal */
  _abortLocally(abortValue?: unknown, message?: string): void {
    if (this._didTerminate || this._isClosed) return
    this._isClosed = true
    const abortError = createAbortError(abortValue, message)
    this._connection.sendAbort(this)
    this._finalizeClose(abortError)
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
    if (this._isClosed) return
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
  _onTransportAckReqMessage(data: string, seq: number): Promise<void> {
    return this._trackAck(this._dispatchAckReq(data, seq))
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
  _onTransportCloseRequest(timeoutMs: number): void {
    if (this._didTerminate) return
    const peerDeadline = Date.now() + normalizeCloseTimeout(timeoutMs)
    if (!this._closeDeadline || peerDeadline < this._closeDeadline) this._closeDeadline = peerDeadline
    if (this._isClosed) {
      this._notifyCloseProgress()
    } else {
      this._startClose()
      void this._runFinalizationLoop()
    }
    this._connection.sendCloseAck(this)
  }

  /** @internal */
  _onTransportCloseAck(): void {
    if (this._didTerminate) return
    this._didReceiveCloseAck = true
    this._expectCloseAck = false
    this._notifyCloseProgress()
  }

  /** @internal */
  _onTransportClose(err?: Error): void {
    this._isClosed = true
    this._finalizeClose(err)
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
      this._invokeCloseCallback(cb, err, true)
    }
    this._closeCallbacks.length = 0
    this._openCallbacks.length = 0
    this._notifyCloseProgress()
  }

  private _startClose(err?: Error): void {
    if (this._isClosed) return
    this._isClosed = true
    this._fireClose(err)
  }

  private async _runFinalizationLoop(): Promise<ChannelCloseResult> {
    while (!this._didTerminate) {
      if (this._isCloseWorkComplete()) {
        this._finalizeClose()
        break
      }
      const remaining = this._closeDeadline - Date.now()
      if (remaining <= 0) {
        this._finalizeClose(new ChannelClosedError('Channel close timed out'))
        break
      }
      await this._waitForCloseProgress(remaining)
    }
    return this._didReceiveCloseAck ? 0 : 1
  }

  private _waitForCloseProgress(timeoutMs: number): Promise<void> {
    return new Promise<void>((resolve) => {
      let settled = false
      const timer = setTimeout(() => {
        if (settled) return
        settled = true
        const index = this._closeWaiters.indexOf(wake)
        if (index >= 0) this._closeWaiters.splice(index, 1)
        resolve()
      }, timeoutMs)
      const wake = () => {
        if (settled) return
        settled = true
        clearTimeout(timer)
        resolve()
      }
      this._closeWaiters.push(wake)
    })
  }

  private _notifyCloseProgress(): void {
    const waiters = this._closeWaiters.splice(0)
    for (const waiter of waiters) waiter()
  }

  private async _dispatchAckReq(data: string, seq: number): Promise<void> {
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

  private _trackAck<T>(promise: Promise<T>): Promise<T> {
    this._inflightAcks++
    return promise.finally(() => {
      this._inflightAcks--
      this._notifyCloseProgress()
    })
  }

  private _isCloseWorkComplete(): boolean {
    return (
      this._inflightAcks === 0 &&
      this._pendingCloseCallbacks === 0 &&
      (!this._expectCloseAck || this._didReceiveCloseAck)
    )
  }

  private _finalizeClose(err?: Error): void {
    if (this._didTerminate) return
    this._didTerminate = true
    this._closeError = err
    this._connection.unregister(this, err ?? new ChannelClosedError())
    this._fireClose(err)
    this._notifyCloseProgress()
  }

  private _invokeCloseCallback(callback: ChannelCloseCallback, err: Error | undefined, track: boolean): void {
    try {
      const pending = callback(err)
      if (!isPromise(pending)) return
      if (track) {
        this._pendingCloseCallbacks++
        void pending.catch(reportLocalChannelCallbackError).finally(() => {
          this._pendingCloseCallbacks--
          this._notifyCloseProgress()
        })
      } else {
        void pending.catch(reportLocalChannelCallbackError)
      }
    } catch (callbackErr) {
      reportLocalChannelCallbackError(callbackErr)
    }
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
  console.error('[telefunc:channel-callback-error]', err instanceof Error ? err : new Error(String(err)))
}

function normalizeCloseTimeout(timeout: number | undefined): number {
  if (timeout === undefined) return CHANNEL_CLOSE_TIMEOUT_MS
  if (!Number.isFinite(timeout) || timeout < 0)
    throw new Error('Channel close timeout must be a non-negative finite number')
  return timeout
}

/** Append ?shard=N (or &shard=N) to a URL string. */
function appendShardParam(url: string, shard: string): string {
  return url.includes('?') ? `${url}&shard=${shard}` : `${url}?shard=${shard}`
}
