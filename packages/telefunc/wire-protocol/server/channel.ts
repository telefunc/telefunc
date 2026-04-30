export { channel, setChannelDefaults, ServerChannel, SERVER_CHANNEL_BRAND }
export { ChannelClosedError, ChannelNetworkError, ChannelOverflowError } from '../channel-errors.js'

const SERVER_CHANNEL_BRAND = Symbol.for('ServerChannel')

import type {
  Channel,
  ChannelAck,
  ClientChannel,
  ChannelCloseCallback,
  ChannelCloseOptions,
  ChannelCloseResult,
  ChannelData,
  ChannelListener,
  ChannelBinaryListener,
} from '../channel.js'
import type { IndexedPeer } from './IndexedPeer.js'
import { stringify } from '@brillout/json-serializer/stringify'
import { parse } from '@brillout/json-serializer/parse'
import { getGlobalObject } from '../../utils/getGlobalObject.js'
import { hasProp } from '../../utils/hasProp.js'
import { unrefTimer } from '../../utils/unrefTimer.js'
import { assertUsage } from '../../utils/assert.js'
import { isAbort } from '../../node/server/Abort.js'
import type { ShieldValidators } from '../../node/server/shield.js'
import { createAbortError, type AbortError } from '../../shared/Abort.js'
import { ShieldValidationError } from '../../shared/ShieldValidationError.js'
import { handleTelefunctionBug } from '../../node/server/runTelefunc/validateTelefunctionError.js'
import { ChannelClosedError, ChannelNetworkError } from '../channel-errors.js'
import { isPromise } from '../../utils/isPromise.js'
import { utf8ByteLength } from '../../utils/utf8ByteLength.js'
import {
  CHANNEL_BUFFER_LIMIT_BYTES,
  CHANNEL_BUFFER_LIMIT_BINARY_BYTES,
  CHANNEL_CLOSE_TIMEOUT_MS,
  CHANNEL_CONNECT_TTL_MS,
  CHANNEL_PING_INTERVAL_MIN_MS,
  CREDIT_WINDOW_BYTES,
  WINDOW_UPDATE_THRESHOLD_BYTES,
} from '../constants.js'
import { STATUS_BODY_INTERNAL_SERVER_ERROR } from '../../shared/constants.js'
import { ServerChannelBuffer } from './ServerChannelBuffer.js'
import { ReplayBuffer } from '../replay-buffer.js'
import { getServerConfig } from '../../node/server/serverConfig.js'
import { assert } from '../../utils/assert.js'
import { TAG } from '../shared-ws.js'
import type { AckResultStatus, CtrlMessage, DecodedFrame } from '../shared-ws.js'

const globalObject = getGlobalObject('channel.ts', {
  connectTtlMs: CHANNEL_CONNECT_TTL_MS,
  bufferLimit: CHANNEL_BUFFER_LIMIT_BYTES,
  bufferLimitBinary: CHANNEL_BUFFER_LIMIT_BINARY_BYTES,
})

function setChannelDefaults(opts: { connectTtl: number; bufferLimit: number; bufferLimitBinary: number }): void {
  globalObject.connectTtlMs = opts.connectTtl
  globalObject.bufferLimit = opts.bufferLimit
  globalObject.bufferLimitBinary = opts.bufferLimitBinary
}

type UntypedChannelHandler = (data: unknown) => unknown

function channel<ClientToServer = UntypedChannelHandler, ServerToClient = UntypedChannelHandler>(opts?: {
  ack?: false
}): Channel<ServerToClient, ClientToServer>
function channel<ClientToServer = UntypedChannelHandler, ServerToClient = UntypedChannelHandler>(opts: {
  ack: true
}): Channel<ServerToClient, ClientToServer, true>
function channel<ClientToServer = UntypedChannelHandler, ServerToClient = UntypedChannelHandler>(opts?: {
  ack?: boolean
}): Channel<ServerToClient, ClientToServer, false>
function channel(opts?: { ack?: boolean }): any {
  return new ServerChannel({
    ack: opts?.ack === true,
  })
}

class ServerChannel<ServerToClient = unknown, ClientToServer = unknown>
  implements Channel<ServerToClient, ClientToServer>
{
  readonly [SERVER_CHANNEL_BRAND] = true
  /** @see __DEFINE_TELEFUNC_SHIELDS on ChannelBase — server's TOut/TIn are ServerToClient/ClientToServer. */
  declare readonly __DEFINE_TELEFUNC_SHIELDS: {
    data: ChannelData<ServerToClient>
    ack: ChannelAck<ClientToServer>
  }
  readonly id: string
  readonly ack: boolean

  get client(): ClientChannel<ClientToServer, ServerToClient> {
    return this as unknown as ClientChannel<ClientToServer, ServerToClient>
  }

  static isServerChannel(value: unknown): value is ServerChannel {
    return hasProp(value, SERVER_CHANNEL_BRAND)
  }

  protected _isClosed = false
  /** @internal */ _didShutdown = false
  private _didRegister = false
  protected _peer: IndexedPeer | null = null
  private _listeners: Array<ChannelListener<ClientToServer>> = []
  private _binaryListeners: Array<ChannelBinaryListener> = []
  protected _prePeerBuffer: ServerChannelBuffer<ChannelAck<ServerToClient>>
  protected _pendingAcks = new Map<
    number,
    { resolve: (result: ChannelAck<ServerToClient>) => void; reject: (err: Error) => void }
  >()
  private _closeCallbacks: Array<ChannelCloseCallback> = []
  private _openCallbacks: Array<() => void> = []
  private _closeError: Error | undefined
  private _didFireClose = false
  private _didFireOpen = false
  private _closePromise: Promise<ChannelCloseResult> | null = null
  private _closeDeadline = 0
  private _closeWaiters: Array<() => void> = []
  private _didReceiveCloseAck = false
  private _awaitingCloseAck = false
  private _pendingCloseCallbacks = 0
  protected _inflightAcks = 0
  private _ttlTimer: ReturnType<typeof setTimeout> | null = null
  protected _peerWindow: number = CREDIT_WINDOW_BYTES
  protected _consumedBytes = 0
  private _sendWaiters: Array<() => void> = []
  private _reconnectTimer: ReturnType<typeof setTimeout> | null = null
  private _responseAbort: ((abortValue?: unknown) => void) | null = null
  private _pendingAckRes: Array<{ ackedSeq: number; result: string; status: AckResultStatus }> = []
  private _shutdownCallback: (() => void) | null = null
  private _pendingCloseAck = false

  // ── Wire state — channel-owned, persistent across attach-mode transitions ────
  //
  // Lives on the channel itself so the same buffer/seq counter serves a client
  // whether the wire is local (`ServerConnection`'s `IndexedPeer`) or substrate-
  // mediated (the runtime's substrate-backed `IndexedPeer`). Both paths reuse
  // these fields rather than maintaining parallel external maps.
  /** Buffer of outgoing wire frames, used to replay missed frames on reconnect.
   *  Allocated in `_registerChannel`; disposed in `_shutdown`. Null only between
   *  construction and registration (no peer can attach before registration). */
  /** @internal */ _replayBuffer: ReplayBuffer | null = null
  /** Highest client→server seq the channel has received and dispatched. Used
   *  for both local-frame and substrate-frame deduplication, and reported back
   *  to the client in `CtrlReconciled.open[].lastSeq`. */
  /** @internal */ _lastClientSeq = 0

  /** Shield validators keyed by name (see __DEFINE_TELEFUNC_SHIELDS on ChannelBase).
   *  - `data`: validates incoming client data (_onPeerMessage / _dispatchAckReq)
   *  - `ack`: validates client ack responses (_onPeerAckRes)
   *  Each returns `true` on success or an error string — callers decide the action (drop, throw). */
  _validators: ShieldValidators = new Map()

  constructor({
    ack = false,
    id,
    bufferLimit,
  }: {
    ack?: boolean
    id?: string
    bufferLimit?: number
  } = {}) {
    this.ack = ack
    this.id = id ?? crypto.randomUUID()
    this._prePeerBuffer = new ServerChannelBuffer<ChannelAck<ServerToClient>>(
      bufferLimit ?? globalObject.bufferLimit,
      globalObject.bufferLimitBinary,
    )
  }

  get isClosed(): boolean {
    return this._isClosed
  }

  /** @internal — Register a one-shot callback that fires when the transport shuts down.
   *  Replaces any previously registered callback (does not accumulate). */
  _onShutdown(cb: () => void): void {
    this._shutdownCallback = cb
  }

  send(data: ChannelData<ServerToClient>): Promise<void>
  send(data: ChannelData<ServerToClient>, opts: { ack: true }): Promise<ChannelAck<ServerToClient>>
  send(data: ChannelData<ServerToClient>, opts: { ack: false }): Promise<void>
  send(
    data: ChannelData<ServerToClient>,
    opts?: { ack?: boolean },
  ): Promise<ChannelAck<ServerToClient>> | Promise<void> {
    const ret = this._send(data, opts) ?? Promise.resolve()
    ret.catch(() => {})
    return ret
  }

  _send(
    data: ChannelData<ServerToClient>,
    opts?: { ack?: boolean },
  ): void | Promise<ChannelAck<ServerToClient>> | Promise<void> {
    if (this._isClosed) throw new ChannelClosedError()
    const needsAck = opts?.ack !== false && (opts?.ack === true || this.ack === true)
    const serialized = stringify(data, { forbidReactElements: false })
    if (!this._peer) {
      if (needsAck) {
        return this._trackAck(
          new Promise<ChannelAck<ServerToClient>>((resolve, reject) => {
            this._prePeerBuffer.pushTextAck(serialized, resolve, reject)
          }),
        )
      }
      return new Promise<void>((resolve, reject) => {
        this._prePeerBuffer.pushText(serialized, resolve, reject)
      })
    }
    if (needsAck) {
      return this._trackAck(
        new Promise<ChannelAck<ServerToClient>>((resolve, reject) => {
          this._peer!.sendTextAckReq(serialized, (seq) => {
            this._pendingAcks.set(seq, { resolve, reject })
          })
        }),
      )
    }
    return this._sendToPeerWithWindow(this._peer.sendText(serialized), utf8ByteLength(serialized))
  }

  private _sendToPeerWithWindow(transport: void | Promise<void>, bytes: number): void | Promise<void> {
    this._peerWindow -= bytes
    if (this._peerWindow > 0) return transport ?? undefined
    const window = new Promise<void>((resolve) => {
      this._sendWaiters.push(resolve)
    })
    return transport ? transport.then(() => window) : window
  }

  sendBinary(data: Uint8Array): Promise<void>
  sendBinary(data: Uint8Array, opts: { ack: true }): Promise<unknown>
  sendBinary(data: Uint8Array, opts: { ack: false }): Promise<void>
  sendBinary(data: Uint8Array, opts?: { ack?: boolean }): Promise<unknown> | Promise<void> {
    const ret = this._sendBinary(data, opts) ?? Promise.resolve()
    ret.catch(() => {})
    return ret
  }

  _sendBinary(data: Uint8Array, opts?: { ack?: boolean }): void | Promise<unknown> | Promise<void> {
    if (this._isClosed) throw new ChannelClosedError()
    const needsAck = opts?.ack === true
    if (!this._peer) {
      if (needsAck) {
        return new Promise<unknown>((resolve, reject) => {
          this._prePeerBuffer.pushBinaryAck(data, resolve, reject)
        })
      }
      return new Promise<void>((resolve, reject) => {
        this._prePeerBuffer.pushBinary(data, resolve, reject)
      })
    }
    if (needsAck) {
      return this._trackAck(
        new Promise<unknown>((resolve, reject) => {
          this._peer!.sendBinaryAckReq(data, (seq) => {
            this._pendingAcks.set(seq, { resolve, reject })
          })
        }),
      )
    }
    return this._sendToPeerWithWindow(this._peer.sendBinary(data), data.byteLength)
  }

  listen(callback: ChannelListener<ClientToServer>): () => void {
    this._listeners.push(callback)
    return () => {
      const i = this._listeners.indexOf(callback)
      if (i >= 0) this._listeners.splice(i, 1)
    }
  }

  listenBinary(callback: ChannelBinaryListener): () => void {
    this._binaryListeners.push(callback)
    return () => {
      const i = this._binaryListeners.indexOf(callback)
      if (i >= 0) this._binaryListeners.splice(i, 1)
    }
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

  _setResponseAbort(abortResponse: (abortValue?: unknown) => void): void {
    this._responseAbort = abortResponse
  }

  abort(): void
  abort(abortValue: unknown, message?: string): void
  abort(abortValue?: unknown, message?: string): void {
    if (this._didShutdown || this._isClosed) return
    this._isClosed = true
    const serializedAbortValue = stringify(abortValue, { forbidReactElements: false })
    if (this._peer) {
      this._peer.sendAbort(serializedAbortValue)
      this._peer = null
    }
    this._shutdown(createAbortError(abortValue, message))
  }

  close(opts?: ChannelCloseOptions): Promise<ChannelCloseResult> {
    if (this._closePromise) return this._closePromise
    if (this._didShutdown) return Promise.resolve(this._didReceiveCloseAck ? 0 : 1)
    const timeout = normalizeCloseTimeout(opts?.timeout)
    this._closeDeadline = Date.now() + timeout
    this._awaitingCloseAck = true
    this._startClose()
    if (this._peer) this._peer.sendCloseRequest(timeout)
    this._closePromise = this._runFinalizationLoop()
    return this._closePromise
  }

  /** @internal — Prepare channel state so the mux can register it. Called by
   *  `ChannelMux.registerChannel`; external code should call that instead. */
  _registerChannel(): void {
    if (this._didShutdown || this._peer || this._didRegister) return
    this._didRegister = true
    // Allocate the replay buffer up-front: registration is the moment the channel
    // becomes addressable on the wire, so a peer can attach immediately after this
    // returns. Its TTL covers a full ping-deadline + reconnect-timeout window.
    const c = getServerConfig().channel
    const pingDeadline = Math.max(c.pingInterval, CHANNEL_PING_INTERVAL_MIN_MS) * 2
    this._replayBuffer = new ReplayBuffer(
      c.serverReplayBuffer,
      pingDeadline + c.reconnectTimeout + 1_000,
      c.serverReplayBufferBinary,
    )
    this._clearTimer('_ttlTimer')
    this._ttlTimer = unrefTimer(
      setTimeout(() => {
        this._ttlTimer = null
        this._shutdown(
          new ChannelNetworkError('Channel timed out: no client connected within TTL after response was sent'),
        )
      }, globalObject.connectTtlMs),
    )
  }

  _attachPeer(peer: IndexedPeer): void {
    if (this._didShutdown) return
    this._clearTimer('_ttlTimer')
    this._clearTimer('_reconnectTimer')
    this._peerWindow = CREDIT_WINDOW_BYTES
    this._peer = peer
    this._prePeerBuffer.flush({
      sendText: (msg) => peer.sendText(msg),
      sendPublish: (msg) => peer.sendPublish(msg),
      sendBinary: (msg) => peer.sendBinary(msg),
      sendTextAck: (data, cb) => {
        const seq = peer.sendTextAckReq(data)
        this._pendingAcks.set(seq, cb)
      },
      sendBinaryAck: (data, cb) => {
        const seq = peer.sendBinaryAckReq(data)
        this._pendingAcks.set(seq, cb)
      },
      sendPublishBinary: (msg) => peer.sendPublishBinary(msg),
    })
    for (const ack of this._pendingAckRes) {
      peer.sendAckRes(ack.ackedSeq, ack.result, ack.status)
    }
    this._pendingAckRes.length = 0
    if (this._pendingCloseAck) peer.sendCloseAck()
    if (this._awaitingCloseAck) peer.sendCloseRequest(Math.max(0, this._closeDeadline - Date.now()))
    if (this._isClosed) {
      this._notifyCloseProgress()
      return
    }
    this._fireOpen()
  }

  /** @internal — Entry point for an incoming wire frame. Both the local connection
   *  (`ServerConnection.handleFrame`) and the cross-instance substrate runtime
   *  (`ChannelSubstrateRuntime.dispatchHomeFrame`) call this. Handles ctrl routing,
   *  client→server seq dedup, and delegation to `_dispatchDataFrame`. */
  _dispatchFrame(frame: DecodedFrame): void {
    if (frame.tag === TAG.CTRL) {
      this._dispatchCtrl(frame.ctrl)
      return
    }
    if (frame.seq) {
      if (frame.seq <= this._lastClientSeq) return
      this._lastClientSeq = frame.seq
    }
    this._dispatchDataFrame(frame)
  }

  /** @internal — Tag-keyed data-frame switch. Subclasses (`ServerPubSub`) override
   *  to handle their extra tags and fall back to `super` for the common cases. */
  protected _dispatchDataFrame(frame: Exclude<DecodedFrame, { tag: typeof TAG.CTRL }>): void {
    switch (frame.tag) {
      case TAG.TEXT:
        this._onPeerMessage(frame.text)
        return
      case TAG.TEXT_ACK_REQ:
        void this._onPeerAckReqMessage(frame.text, frame.seq)
        return
      case TAG.BINARY:
        this._onPeerBinaryMessage(frame.data)
        return
      case TAG.BINARY_ACK_REQ:
        void this._onPeerBinaryAckReqMessage(frame.data, frame.seq)
        return
      case TAG.ACK_RES:
        this._onPeerAckRes(frame.ackedSeq, frame.text, frame.status)
        return
      case TAG.PUBLISH:
      case TAG.PUBLISH_BINARY:
        assert(false, `Server received unexpected ${frame.tag} frame from peer`)
    }
  }

  /** @internal — Per-channel ctrl-message switch. Connection-level ctrls (ping,
   *  reconcile, fin) never reach here — they're handled in `ServerConnection`. */
  _dispatchCtrl(ctrl: CtrlMessage): void {
    switch (ctrl.t) {
      case 'close':
        this._onPeerCloseRequest(ctrl.timeoutMs)
        return
      case 'close-ack':
        this._onPeerCloseAck()
        return
      case 'window':
        this._onPeerWindowUpdate(ctrl.bytes)
        return
      // pubsub-sub / pubsub-unsub: dropped on plain channels; ServerPubSub overrides.
    }
  }

  _onPeerMessage(text: string): void {
    const data = parse(text) as ChannelData<ClientToServer>
    const validateData = this._validators.get('data')
    // Shield fail on a no-ack message: silent drop (validator auto-logs). The client doesn't
    // await a response, so there's no `ShieldValidationError` to surface — listeners simply
    // never see the bad value. Ack-bearing sends go through `_dispatchAckReq` and *do*
    // reject the sender's promise via the `shield-error` wire status.
    if (validateData && validateData(data) !== true) return
    const pending: Promise<unknown>[] = []
    for (const cb of this._listeners) {
      try {
        const result = cb(data)
        if (isPromise(result)) {
          pending.push(result.catch((err: unknown) => this._handleCallbackError(err)))
        }
      } catch (err) {
        if (this._handleCallbackError(err)) return
      }
    }
    const bytes = utf8ByteLength(text)
    if (pending.length > 0) {
      Promise.all(pending).finally(() => this._trackConsumption(bytes))
    } else {
      this._trackConsumption(bytes)
    }
  }

  _onPeerAckReqMessage(text: string, seq: number): Promise<void> {
    return this._trackAck(this._dispatchAckReq(text, seq))
  }

  _onPeerBinaryAckReqMessage(data: Uint8Array, seq: number): Promise<void> {
    return this._trackAck(this._dispatchBinaryAckReq(data, seq))
  }

  _onPeerBinaryMessage(data: Uint8Array): void {
    const pending: Promise<unknown>[] = []
    for (const cb of this._binaryListeners) {
      try {
        const result = cb(data)
        if (isPromise(result)) {
          pending.push(result.catch((err: unknown) => this._handleCallbackError(err)))
        }
      } catch (err) {
        if (this._handleCallbackError(err)) return
      }
    }
    const bytes = data.byteLength
    if (pending.length > 0) {
      Promise.all(pending).finally(() => this._trackConsumption(bytes))
    } else {
      this._trackConsumption(bytes)
    }
  }

  _onPeerAckRes(ackedSeq: number, resultText: string, status: AckResultStatus = 'ok'): void {
    const pending = this._pendingAcks.get(ackedSeq)
    if (!pending) return
    this._pendingAcks.delete(ackedSeq)
    switch (status) {
      case 'ok': {
        const parsed = parse(resultText) as ChannelAck<ServerToClient>
        const validateAck = this._validators.get('ack')
        if (validateAck) {
          const result = validateAck(parsed)
          // Server-declared ack shield rejected the peer's response — same class as every
          // other shield-fail surface, so user code can catch with `isShieldValidationError`.
          if (result !== true) {
            pending.reject(new ShieldValidationError(result))
            return
          }
        }
        pending.resolve(parsed)
        return
      }
      case 'abort':
        pending.reject(createAbortError(parse(resultText)))
        return
      case 'error':
        pending.reject(new Error(resultText || 'Internal client channel error — see client logs'))
        return
      case 'shield-error':
        pending.reject(new ShieldValidationError(resultText))
    }
  }

  _onPeerCloseRequest(timeoutMs: number): void {
    if (this._didShutdown) return
    const peerDeadline = Date.now() + normalizeCloseTimeout(timeoutMs)
    if (!this._closeDeadline || peerDeadline < this._closeDeadline) this._closeDeadline = peerDeadline
    this._pendingCloseAck = true
    if (this._peer) this._peer.sendCloseAck()
    if (this._isClosed) {
      this._notifyCloseProgress()
      return
    }
    this._startClose()
    void this._runFinalizationLoop()
  }

  _onPeerCloseAck(): void {
    if (this._didShutdown) return
    this._didReceiveCloseAck = true
    this._awaitingCloseAck = false
    this._notifyCloseProgress()
  }

  _onPeerDisconnect(reconnectTimeout: number): void {
    if (this._didShutdown || !this._peer) return
    this._peer = null
    this._reconnectTimer = unrefTimer(
      setTimeout(() => {
        this._reconnectTimer = null
        this._shutdown(new ChannelNetworkError('Channel timed out: client did not reconnect within grace period'))
      }, reconnectTimeout),
    )
  }

  _onPeerRecoveryFailure(): void {
    if (this._didShutdown) return
    this._peer = null
    this._shutdown(new ChannelNetworkError('Channel not acknowledged by client after reconnect'))
  }

  _onPeerClose(): void {
    if (this._didShutdown) return
    this._peer = null
    this._shutdown()
  }

  _onPeerWindowUpdate(bytes: number): void {
    this._peerWindow = bytes
    const waiters = this._sendWaiters.splice(0)
    for (const waiter of waiters) waiter()
  }

  /** Send an ack response, buffering it if the peer is currently disconnected. */
  protected _sendAckRes(ackedSeq: number, result: string, status: AckResultStatus = 'ok'): void {
    if (this._peer) {
      this._peer.sendAckRes(ackedSeq, result, status)
    } else {
      this._pendingAckRes.push({ ackedSeq, result, status })
    }
  }

  /** Advertise free buffer space to the peer so it can unblock. */
  _sendWindowUpdate(bytes: number): void {
    this._peer?.sendWindowUpdate(bytes)
  }

  protected _trackConsumption(bytes: number): void {
    this._consumedBytes += bytes
    if (this._consumedBytes >= WINDOW_UPDATE_THRESHOLD_BYTES) {
      this._consumedBytes = 0
      this._sendWindowUpdate(CREDIT_WINDOW_BYTES)
    }
  }

  private _startClose(err?: Error): void {
    if (this._isClosed) return
    this._isClosed = true
    this._fireClose(err)
  }

  private async _runFinalizationLoop(): Promise<ChannelCloseResult> {
    while (!this._didShutdown) {
      if (this._isCloseWorkComplete()) {
        this._shutdown()
        break
      }
      const remaining = this._closeDeadline - Date.now()
      if (remaining <= 0) {
        this._shutdown(new ChannelClosedError('Channel close timed out'))
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

  private async _dispatchAckReq(text: string, seq: number): Promise<void> {
    try {
      if (this._listeners.length === 0) {
        this._sendAckRes(seq, 'No listener registered for ack request', 'error')
        return
      }
      const data = parse(text) as ChannelData<ClientToServer>
      const validateData = this._validators.get('data')
      if (validateData) {
        const result = validateData(data)
        // `shield-error` status lets the client reject its `send()` promise with a branded
        // ShieldValidationError — same identity every other shield-fail surface produces.
        if (result !== true) {
          this._sendAckRes(seq, result, 'shield-error')
          return
        }
      }
      let lastResult: unknown
      for (const cb of this._listeners) {
        try {
          lastResult = await cb(data)
        } catch (err) {
          if (this._handleCallbackError(err)) return
          this._sendAckRes(seq, `${STATUS_BODY_INTERNAL_SERVER_ERROR} — see server logs`, 'error')
          return
        }
      }
      this._sendAckRes(seq, stringify(lastResult, { forbidReactElements: false }))
    } finally {
      this._trackConsumption(utf8ByteLength(text))
    }
  }

  private async _dispatchBinaryAckReq(data: Uint8Array, seq: number): Promise<void> {
    try {
      if (this._binaryListeners.length === 0) {
        this._sendAckRes(seq, 'No listener registered for ack request', 'error')
        return
      }
      let lastResult: unknown
      for (const cb of this._binaryListeners) {
        try {
          lastResult = await cb(data)
        } catch (err) {
          if (this._handleCallbackError(err)) return
          this._sendAckRes(seq, `${STATUS_BODY_INTERNAL_SERVER_ERROR} — see server logs`, 'error')
          return
        }
      }
      this._sendAckRes(seq, stringify(lastResult, { forbidReactElements: false }))
    } finally {
      this._trackConsumption(data.byteLength)
    }
  }

  protected _trackAck<T>(promise: Promise<T>): Promise<T> {
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
      (!this._awaitingCloseAck || this._didReceiveCloseAck)
    )
  }

  protected _shutdown(err?: Error): void {
    if (this._didShutdown) return
    this._didShutdown = true
    this._isClosed = true
    this._closeError = err
    this._peer = null
    this._pendingCloseAck = false
    this._awaitingCloseAck = false
    this._clearTimer('_ttlTimer')
    this._clearTimer('_reconnectTimer')
    if (this._replayBuffer) {
      this._replayBuffer.dispose()
      this._replayBuffer = null
    }
    // The mux subscribes to this callback in `registerChannel` to evict its bookkeeping —
    // keeping the channel agnostic of who's listening.
    const shutdownCb = this._shutdownCallback
    this._shutdownCallback = null
    shutdownCb?.()
    this._fireClose(err)
    const waiters = this._sendWaiters.splice(0)
    for (const waiter of waiters) waiter()
    this._notifyCloseProgress()
    this._pendingAckRes.length = 0
    const ackErr = err ?? new ChannelClosedError()
    for (const { reject } of this._pendingAcks.values()) reject(ackErr)
    this._pendingAcks.clear()
    this._prePeerBuffer.clear(ackErr)
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

  private _invokeCloseCallback(callback: ChannelCloseCallback, err: Error | undefined, track: boolean): void {
    try {
      const pending = callback(err)
      if (!isPromise(pending)) return
      if (track) {
        this._pendingCloseCallbacks++
        void pending
          .catch((e) => reportServerChannelError(e))
          .finally(() => {
            this._pendingCloseCallbacks--
            this._notifyCloseProgress()
          })
      } else {
        void pending.catch((e) => reportServerChannelError(e))
      }
    } catch (callbackErr) {
      reportServerChannelError(callbackErr)
    }
  }

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

  protected _handleCallbackError(err: unknown): boolean {
    if (isAbort(err)) {
      const abortError: AbortError = err
      if (this._responseAbort) {
        this._responseAbort(abortError.abortValue)
      } else {
        this.abort(abortError.abortValue)
      }
      return true
    }
    reportServerChannelError(err)
    return false
  }

  private _clearTimer(name: '_ttlTimer' | '_reconnectTimer'): void {
    const timer = this[name]
    if (!timer) return
    clearTimeout(timer)
    if (name === '_ttlTimer') {
      this._ttlTimer = null
      return
    }
    this._reconnectTimer = null
  }
}

function reportServerChannelError(err: unknown): void {
  handleTelefunctionBug(err instanceof Error ? err : new Error(String(err)))
}

function normalizeCloseTimeout(timeout: number | undefined): number {
  if (timeout === undefined) return CHANNEL_CLOSE_TIMEOUT_MS
  assertUsage(Number.isFinite(timeout) && timeout >= 0, 'Channel close timeout must be a non-negative finite number')
  return timeout
}
