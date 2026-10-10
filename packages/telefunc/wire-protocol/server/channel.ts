export { Channel, ServerChannel, SERVER_CHANNEL_BRAND, reconnectWindow }
export { ChannelClosedError, ChannelOverflowError } from '../channel-errors.js'
export { NetworkError } from '../../shared/NetworkError.js'

const SERVER_CHANNEL_BRAND = Symbol.for('telefunc.ServerChannel')

import type {
  ChannelBase,
  ChannelShield,
  ChannelAck,
  ClientChannel,
  ChannelCloseCallback,
  ChannelCloseOptions,
  ChannelCloseResult,
  ChannelData,
  ChannelListener,
  ChannelBinaryListener,
} from '../channel.js'
import type { TELEFUNC_SHIELDS } from '../../node/shared/transformer/generateShield/shield-key.js'

import type { IndexedPeer } from './IndexedPeer.js'
import { stringify } from '@brillout/json-serializer/stringify'
import { parse } from '@brillout/json-serializer/parse'
import { hasProp } from '../../utils/hasProp.js'
import { unrefTimer } from '../../utils/unrefTimer.js'
import { assertUsage } from '../../utils/assert.js'
import { isAbort } from '../../node/server/Abort.js'
import type { ShieldValidators } from '../../node/server/shield.js'
import { createAbortError, type AbortError } from '../../shared/Abort.js'
import { ShieldValidationError } from '../../shared/ShieldValidationError.js'
import { handleTelefunctionBug } from '../../node/server/runTelefunc/validateTelefunctionError.js'
import { ChannelClosedError, ChannelOverflowError, replayLossError } from '../channel-errors.js'
import { NetworkError } from '../../shared/NetworkError.js'
import { isPromise } from '../../utils/isPromise.js'
import { TIMER_DELAY_MAX_MS, CHANNEL_CLOSE_TIMEOUT_MS, CREDIT_WINDOW_MAX_BYTES } from '../constants.js'
import { FlowControl, replayWindow } from '../flow-control/flow-control.js'
import { STATUS_BODY_INTERNAL_SERVER_ERROR } from '../../shared/constants.js'
import { ServerChannelBuffer } from './ServerChannelBuffer.js'
import { ReplayBuffer } from '../replay-buffer.js'
import { getServerConfig, pingDeadlineOf } from '../../node/server/serverConfig.js'
import { assert } from '../../utils/assert.js'
import { Listeners } from '../../utils/Listeners.js'
import {
  ACK_STATUS,
  ERROR_REASON,
  ProtocolViolationError,
  TAG,
  assertProtocol,
  countsCredit,
  isChannelCtrlTag,
  isSequencedFrame,
  seqNear,
  seqThrough,
} from '../shared-ws.js'
import type {
  AckResultStatus,
  ChannelCtrlFrame,
  ChannelDataFrame,
  ChannelFrame,
  ErrorReason,
  ReattachState,
} from '../shared-ws.js'

/** Peer-authored JSON: a parse failure is the peer's, so it surfaces as a protocol violation. */
function parsePeerText(text: string): unknown {
  try {
    return parse(text)
  } catch {
    throw new ProtocolViolationError('peer payload is not parsable')
  }
}

/** The closing frames made while no peer was attached, sent to the next. */
type PendingEnd = { closeAck: boolean; closeRequest: boolean; abort: string | null; error: ErrorReason | null }
const NO_PENDING_END: PendingEnd = Object.freeze({ closeAck: false, closeRequest: false, abort: null, error: null })

class ServerChannel<ClientToServer = unknown, ServerToClient = unknown>
  implements Channel<ClientToServer, ServerToClient>
{
  readonly [SERVER_CHANNEL_BRAND] = true
  /** @see ChannelShield in ../channel.ts — `data` validates incoming C2S, `ack` validates ack of own S2C sends. */
  declare readonly [TELEFUNC_SHIELDS]: {
    data: ChannelData<ClientToServer>
    ack: ChannelAck<ServerToClient>
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
  private readonly _listeners = new Listeners<ChannelListener<ClientToServer>>()
  private readonly _binaryListeners = new Listeners<ChannelBinaryListener>()
  protected _prePeerBuffer: ServerChannelBuffer<ChannelAck<ServerToClient>>
  protected _pendingAcks = new Map<
    number,
    { resolve: (result: ChannelAck<ServerToClient>) => void; reject: (err: Error) => void; bytes: number }
  >()
  /** Payload bytes of the ack requests in `_pendingAcks`. */
  private _pendingAckBytes = 0
  protected readonly _bufferLimit: number
  protected readonly _bufferLimitBinary: number
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
  /** Owns sender-side credit, receiver-side consumption tracking, BDP estimator,
   *  and the queue of senders blocked on credit refresh. Credit governs fire-and-
   *  forget TEXT/BINARY, and PUBLISH in bytes — see `constants.ts`. */
  protected _flow: FlowControl
  private _reconnectTimer: ReturnType<typeof setTimeout> | null = null
  private _responseAbort: ((abortValue?: unknown) => void) | null = null
  private _pendingAckRes: Array<{ ackedSeq: number; result: string; status: AckResultStatus }> = []
  private _shutdownCallback: ((keep: boolean, pageAttached: boolean) => void) | null = null
  private _pendingEnd: PendingEnd = NO_PENDING_END
  private _closeRequestSeq = 0
  /** How far the page is known to have what this channel sent it. */
  private _pageLastSeq = 0
  /** Its page closed its end, and takes nothing more of it. */
  private _pageClosed = false

  // ── Wire state — channel-owned, persistent across attach-mode transitions ────
  /** Buffer of outgoing wire frames, used to replay missed frames on reconnect.
   *  Allocated in `_registerChannel`; disposed in `_release`. Null only between
   *  construction and registration (no peer can attach before registration). */
  /** @internal */ _replayBuffer: ReplayBuffer | null = null
  /** Highest client→server seq the channel has received and dispatched. Used for
   *  frame deduplication and reported back to the client in `CtrlReconciled.open[].lastSeq`. */
  /** @internal */ _lastClientSeq = 0

  /** Shield validators keyed by name (see `ChannelShield` in ../channel.ts).
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
    this._flow = new FlowControl(
      {
        byteWindowUpdate: (limit) => this._peer?.sendByteWindowUpdate(limit, this._lastClientSeq),
        msgWindowUpdate: (limit) => this._peer?.sendMsgWindowUpdate(limit),
        bdpPing: (probe) => this._peer?.sendBdpPing(probe),
      },
      () => this._peer?.sender.bufferedAmount(),
    )
    const c = getServerConfig().channel
    this._flow.fitReplays(
      replayWindow(c.clientReplayBuffer, c.clientReplayBufferBinary),
      replayWindow(c.serverReplayBuffer, c.serverReplayBufferBinary),
    )
    this._bufferLimit = bufferLimit ?? c.bufferLimit
    this._bufferLimitBinary = c.bufferLimitBinary
    this._prePeerBuffer = new ServerChannelBuffer<ChannelAck<ServerToClient>>(
      this._bufferLimit,
      this._bufferLimitBinary,
    )
  }

  get isClosed(): boolean {
    return this._isClosed
  }

  /** @internal — Register a one-shot callback that fires when the transport shuts down, with whether the channel keeps
   *  what its page may still lack of it (see `_release`), and whether its page is attached. Replaces any previously
   *  registered callback. */
  _onShutdown(cb: (keep: boolean, pageAttached: boolean) => void): void {
    this._shutdownCallback = cb
  }

  send(data: ChannelData<ServerToClient>): Promise<void>
  send(data: ChannelData<ServerToClient>, opts: { ack: true }): Promise<ChannelAck<ServerToClient>>
  send(data: ChannelData<ServerToClient>, opts: { ack: false }): Promise<void>
  send(
    data: ChannelData<ServerToClient>,
    opts?: { ack?: boolean },
  ): Promise<ChannelAck<ServerToClient>> | Promise<void> {
    const t0 = performance.now()
    try {
      const ret = this._send(data, opts) ?? Promise.resolve()
      ret.catch(() => {})
      return ret
    } finally {
      this._flow._recordSelfTime(performance.now() - t0)
    }
  }

  _send(
    data: ChannelData<ServerToClient>,
    opts?: { ack?: boolean },
  ): void | Promise<ChannelAck<ServerToClient>> | Promise<void> {
    if (this._isClosed) throw new ChannelClosedError()
    const needsAck = opts?.ack !== false && (opts?.ack === true || this.ack === true)
    const serialized = stringify(data)
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
    // Ack-bearing sends bypass credit accounting — the caller's `await` on the ack
    // Promise already serializes the next send, so credit would add nothing.
    if (needsAck) {
      if (this._isPeerBehind()) return rejectOverflow()
      return this._trackAck(
        new Promise<ChannelAck<ServerToClient>>((resolve, reject) => {
          this._peer!.sendTextAckReq(serialized, (seq, bytes) => this._addPendingAck(seq, bytes, { resolve, reject }))
        }),
      )
    }
    // Cooperative credit model: the send already fired; `decrement` only gates the return
    // value. Awaiting throttles the caller's next send; not awaiting bypasses credit, until the peer is behind.
    if (this._flow.isPastByteCredit && this._isPeerBehind()) return rejectOverflow()
    return this._flow.decrement(this._peer.sendText(serialized))
  }

  sendBinary(data: Uint8Array): Promise<void>
  sendBinary(data: Uint8Array, opts: { ack: true }): Promise<unknown>
  sendBinary(data: Uint8Array, opts: { ack: false }): Promise<void>
  sendBinary(data: Uint8Array, opts?: { ack?: boolean }): Promise<unknown> | Promise<void> {
    const t0 = performance.now()
    try {
      const ret = this._sendBinary(data, opts) ?? Promise.resolve()
      ret.catch(() => {})
      return ret
    } finally {
      this._flow._recordSelfTime(performance.now() - t0)
    }
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
    // Ack-bearing path bypasses credit; see `_send` for rationale.
    if (needsAck) {
      if (this._isPeerBehind()) return rejectOverflow()
      return this._trackAck(
        new Promise<unknown>((resolve, reject) => {
          this._peer!.sendBinaryAckReq(data, (seq, bytes) => this._addPendingAck(seq, bytes, { resolve, reject }))
        }),
      )
    }
    // Cooperative credit model; see `_send`.
    if (this._flow.isPastByteCredit && this._isPeerBehind()) return rejectOverflow()
    this._peer.sendBinary(data)
    return this._flow.decrement(data.byteLength)
  }

  /** @internal The most this channel's flow control lets wait on its wire for a page that reads: its credit, which a
   *  page never grants past `CREDIT_WINDOW_MAX_BYTES`, what `_pastCreditAllowance` lets past it, and what it buffered
   *  while the page was offline, `bufferLimit` of text and `bufferLimitBinary` of binary, which an attach sends at once
   *  whatever the credit. What a reattach replays is what the first two let go out. */
  _sendAllowance(): number {
    return CREDIT_WINDOW_MAX_BYTES + this._pastCreditAllowance() + this._bufferLimit + this._bufferLimitBinary
  }

  /** How far past its credit the peer can be sent while it reads: a burst up to the largest window a page grants,
   *  however small the window this one granted. */
  protected _pastCreditAllowance(): number {
    return CREDIT_WINDOW_MAX_BYTES
  }

  /** What this channel sent once past its credit, and the ack requests the peer hasn't answered, as far as its wire
   *  still holds them, or all of them where the runtime can't tell. Past `_pastCreditAllowance`, the peer is behind. */
  protected _isPeerBehind(): boolean {
    const allowance = this._pastCreditAllowance()
    const behind = this._flow.bytesSentPastCredit + this._pendingAckBytes
    if (behind < allowance) return false
    const buffered = this._peer!.sender.bufferedAmount()
    return (buffered === undefined ? behind : Math.min(behind, buffered)) >= allowance
  }

  private _addPendingAck(
    seq: number,
    bytes: number,
    settle: { resolve: (result: ChannelAck<ServerToClient>) => void; reject: (err: Error) => void },
  ): void {
    this._pendingAcks.set(seq, { ...settle, bytes })
    this._pendingAckBytes += bytes
  }

  listen(callback: ChannelListener<ClientToServer>): () => void {
    return this._listeners.add(callback)
  }

  listenBinary(callback: ChannelBinaryListener): () => void {
    return this._binaryListeners.add(callback)
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
    const serializedAbortValue = stringify(abortValue)
    if (this._peer) this._peer.sendAbort(serializedAbortValue)
    else this._pendingEnd = { ...this._pendingEnd, abort: serializedAbortValue }
    this._shutdown(createAbortError(abortValue, message))
  }

  close(opts?: ChannelCloseOptions): Promise<ChannelCloseResult> {
    if (this._closePromise) return this._closePromise
    if (this._didShutdown) return Promise.resolve(this._didReceiveCloseAck ? 0 : 1)
    const timeout = normalizeCloseTimeout(opts?.timeout)
    this._closeDeadline = Date.now() + timeout
    this._awaitingCloseAck = true
    this._startClose()
    if (this._peer) this._closeRequestSeq = this._peer.sendCloseRequest(timeout)
    else this._pendingEnd = { ...this._pendingEnd, closeRequest: true }
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
    // returns.
    const c = getServerConfig().channel
    this._replayBuffer = new ReplayBuffer(c.serverReplayBuffer, c.serverReplayBufferBinary)
    this._clearTimer('_ttlTimer')
    this._ttlTimer = unrefTimer(
      setTimeout(() => {
        this._ttlTimer = null
        this._shutdown(
          new NetworkError('Channel timed out: no client connected within TTL after response was sent', true),
          { pageGone: true },
        )
      }, c.connectTtl),
    )
  }

  /** The peer's RECONCILE declarations apply before `onOpen` fires, through the same hooks as its frames. An ended
   *  channel sends only what it made while no peer was attached: its messages, its answers and its end. */
  _attachPeer(peer: IndexedPeer, state?: ReattachState): void {
    if (this._didShutdown) {
      this._flushPrePeerBuffer(peer)
      this._sendPendingAckRes(peer)
      this._sendPendingEnd(peer)
      return
    }
    this._clearTimer('_ttlTimer')
    this._clearTimer('_reconnectTimer')
    // The wire of the last peer lost nothing to repair, and still answers the probe in flight.
    const rewired = this._peer?.sender !== peer.sender
    this._peer = peer
    if (rewired) this._flow.reattach()
    this._flushPrePeerBuffer(peer)
    this._sendPendingAckRes(peer)
    if (state?.broadcast) {
      this._onPeerSubscription('text', state.broadcast.text)
      this._onPeerSubscription('binary', state.broadcast.binary)
    }
    this._sendPendingEnd(peer)
    if (this._isClosed) {
      this._notifyCloseProgress()
      return
    }
    this._fireOpen()
  }

  /** @internal — Entry point from the mux for an incoming wire frame. Handles ctrl routing,
   *  client→server seq dedup, and delegation to `_dispatchDataFrame`. */
  _dispatchFrame(frame: ChannelFrame): void {
    // The page's closing frames are sequenced with its data, so a replay repeats none of them. The wire carries a seq's
    // low 32 bits, read here as the whole from the next one this channel expects.
    if (isSequencedFrame(frame)) {
      frame.seq = seqNear(frame.seq, this._lastClientSeq + 1)
      if (frame.seq) {
        if (frame.seq <= this._lastClientSeq) return
        this._lastClientSeq = frame.seq
      }
    }
    // An ended channel keeps only how far the page's frames reached it.
    if (this._didShutdown) return
    if (isChannelCtrlTag(frame.tag)) {
      this._dispatchCtrl(frame as ChannelCtrlFrame)
      return
    }
    const data = frame as ChannelDataFrame
    if (!countsCredit(data.tag)) this._flow.onReceivedUncounted(data.bytes)
    this._dispatchDataFrame(data)
  }

  /** @internal — Tag-keyed data-frame switch. Subclasses (`ServerBroadcast`) override
   *  to handle their extra tags and fall back to `super` for the common cases. */
  protected _dispatchDataFrame(frame: ChannelDataFrame): void {
    switch (frame.tag) {
      case TAG.TEXT:
        this._onPeerMessage(frame.text, frame.bytes)
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
   *  reconcile, fin) never reach here — the mux's `handleFrame` handles them. */
  _dispatchCtrl(frame: ChannelCtrlFrame): void {
    switch (frame.tag) {
      case TAG.CLOSE:
        this._onPeerCloseRequest(frame.timeoutMs)
        return
      case TAG.CLOSE_ACK:
        this._onPeerCloseAck()
        return
      case TAG.ABORT:
        this._shutdown(createAbortError(parsePeerText(frame.abortValue)))
        return
      case TAG.ERROR:
        assertProtocol(frame.reason === ERROR_REASON.LOST, `ERROR reason ${frame.reason} from a page`)
        this._shutdown(replayLossError('client'))
        return
      case TAG.WINDOW:
        this._flow.onPeerByteWindow(frame.bytes)
        this._onPageHas(seqThrough(frame.lastSeq, this._replayBuffer?.seq ?? 0))
        return
      case TAG.MSG_WINDOW:
        this._flow.onPeerMessageWindow(frame.count)
        return
      case TAG.BDP_PING:
        // It has no attach of its own to probe the path with.
        this._peer?.sendBdpPingAck(frame.probe, this._flow.onPing(), Infinity)
        return
      case TAG.BDP_PING_ACK:
        this._flow.onPingAck(frame.probe, frame.starved, frame.pathRtt)
        return
      case TAG.BROADCAST_SUB:
      case TAG.BROADCAST_UNSUB:
        this._onPeerSubscription(frame.binary ? 'binary' : 'text', frame.tag === TAG.BROADCAST_SUB)
    }
  }

  // A broadcast takes subscriptions; a plain channel drops them.
  _onPeerSubscription(_kind: 'text' | 'binary', _on: boolean): void {}

  _onPeerMessage(text: string, bytes: number): void {
    const t0 = performance.now()
    try {
      this._flow.onReceived(bytes)
      const data = parsePeerText(text) as ChannelData<ClientToServer>
      const validateData = this._validators.get('data')
      // Shield fail on a no-ack message: silent drop (validator auto-logs). The client doesn't
      // await a response, so there's no `ShieldValidationError` to surface — listeners simply
      // never see the bad value. Ack-bearing sends go through `_dispatchAckReq` and *do*
      // reject the sender's promise via the `shield-error` wire status.
      // Dropping still consumes: window refreshes are consumption-driven, so skipping
      // `onConsumed` would leak receive credit and eventually stall the client's sends.
      if (validateData && validateData(data) !== true) {
        this._flow.onConsumed(bytes)
        return
      }
      const pending: Promise<unknown>[] = []
      for (const cb of this._listeners.list()) {
        try {
          const result = cb(data)
          if (isPromise(result)) {
            pending.push(result.catch((err: unknown) => this._handleCallbackError(err)))
          }
        } catch (err) {
          if (this._handleCallbackError(err)) return
        }
      }
      if (pending.length > 0) {
        Promise.all(pending).finally(() => this._flow.onConsumed(bytes))
      } else {
        this._flow.onConsumed(bytes)
      }
    } finally {
      this._flow._recordSelfTime(performance.now() - t0)
    }
  }

  _onPeerAckReqMessage(text: string, seq: number): Promise<void> {
    // Parsed here rather than inside the async dispatch so a malformed payload throws in the recv
    // turn, where the violation still names the wire that sent it.
    return this._trackAck(this._dispatchAckReq(parsePeerText(text) as ChannelData<ClientToServer>, seq))
  }

  _onPeerBinaryAckReqMessage(data: Uint8Array, seq: number): Promise<void> {
    return this._trackAck(this._dispatchBinaryAckReq(data, seq))
  }

  _onPeerBinaryMessage(data: Uint8Array): void {
    const t0 = performance.now()
    const bytes = data.byteLength
    try {
      this._flow.onReceived(bytes)
      const pending: Promise<unknown>[] = []
      for (const cb of this._binaryListeners.list()) {
        try {
          const result = cb(data)
          if (isPromise(result)) {
            pending.push(result.catch((err: unknown) => this._handleCallbackError(err)))
          }
        } catch (err) {
          if (this._handleCallbackError(err)) return
        }
      }
      if (pending.length > 0) {
        Promise.all(pending).finally(() => this._flow.onConsumed(bytes))
      } else {
        this._flow.onConsumed(bytes)
      }
    } finally {
      this._flow._recordSelfTime(performance.now() - t0)
    }
  }

  _onPeerAckRes(ackedSeq: number, resultText: string, status: AckResultStatus = ACK_STATUS.OK): void {
    const pending = this._pendingAcks.get(ackedSeq)
    if (!pending) return
    try {
      switch (status) {
        case ACK_STATUS.OK: {
          const parsed = parsePeerText(resultText) as ChannelAck<ServerToClient>
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
        case ACK_STATUS.ABORT:
          pending.reject(createAbortError(parsePeerText(resultText)))
          return
        case ACK_STATUS.ERROR:
          pending.reject(new Error(resultText || 'Internal client channel error — see client logs'))
          return
        case ACK_STATUS.SHIELD_ERROR:
          pending.reject(new ShieldValidationError(resultText))
          return
        default:
          throw new ProtocolViolationError(`ACK_RES unknown status ${status}`)
      }
    } catch (err) {
      // Settle the caller AND rethrow: the awaiting `send()` must not hang, and a malformed ack is
      // still a wire-level violation for the recv turn to act on.
      pending.reject(err instanceof Error ? err : new Error(String(err)))
      throw err
    } finally {
      this._pendingAcks.delete(ackedSeq)
      this._pendingAckBytes -= pending.bytes
    }
  }

  _onPeerCloseRequest(timeoutMs: number): void {
    if (this._didShutdown) return
    assertProtocol(timeoutMs <= TIMER_DELAY_MAX_MS, `CLOSE timeout ${timeoutMs}`)
    const peerDeadline = Date.now() + timeoutMs
    if (!this._closeDeadline || peerDeadline < this._closeDeadline) this._closeDeadline = peerDeadline
    if (this._peer) this._peer.sendCloseAck()
    else this._pendingEnd = { ...this._pendingEnd, closeAck: true }
    if (this._isClosed) {
      this._notifyCloseProgress()
      return
    }
    this._startClose()
    void this._runFinalizationLoop()
  }

  _onPeerCloseAck(): void {
    if (this._didShutdown) return
    // The page acknowledges the close request as it gets it, so it has all sent before it.
    this._onPageHas(this._closeRequestSeq)
    this._didReceiveCloseAck = true
    this._awaitingCloseAck = false
    this._notifyCloseProgress()
  }

  /** @internal `peer`'s wire went away. A channel a later reconcile moved to another wire keeps that one. */
  _onPeerDisconnect(peer: IndexedPeer, reconnectTimeout: number): void {
    if (this._didShutdown || this._peer?.sender !== peer.sender) return
    this._peer = null
    this._reconnectTimer = unrefTimer(
      setTimeout(() => {
        this._reconnectTimer = null
        this._shutdown(new NetworkError('Channel timed out: client did not reconnect within grace period', true), {
          pageGone: true,
        })
      }, reconnectTimeout),
    )
  }

  _onPeerRecoveryFailure(): void {
    if (this._didShutdown) return
    this._peer = null
    this._shutdown(new NetworkError('Channel not acknowledged by client after reconnect', true), { pageGone: true })
  }

  _onPeerClose(): void {
    if (this._didShutdown) return
    this._peer = null
    this._shutdown(undefined, { pageGone: true })
  }

  /** @internal Its replay no longer holds what its page lacks of it: the channel ends on both ends, `peer` telling its
   *  page so in place of the replay. What it made while no peer was attached is dropped, as it would reach the page
   *  past the hole. */
  _onReplayLost(peer: IndexedPeer): void {
    const err = replayLossError('server')
    this._dropPending(err)
    peer.sendError(ERROR_REASON.LOST)
    this._shutdown(err)
  }

  /** @internal At each of its page's heartbeats: see `FlowControl.acknowledge`. */
  _acknowledge(): void {
    this._flow.acknowledge()
  }

  /** @internal The page has what this channel sent it through `lastSeq`, which its replay lets go. */
  _onPageHas(lastSeq: number): void {
    if (lastSeq > this._pageLastSeq) this._pageLastSeq = lastSeq
    this._replayBuffer?.acknowledge(lastSeq)
  }

  /** @internal Its page closed its end, having what this channel sent it through `lastSeq`. */
  _onPageClosed(lastSeq: number): void {
    this._pageClosed = true
    this._onPageHas(lastSeq)
  }

  /** @internal Its page takes nothing more of it: the page closed its end, or has all this channel sent it while it has
   *  nothing more to send. */
  _pageNeedsNothing(): boolean {
    return (
      this._replayBuffer !== null &&
      (this._pageClosed ||
        (this._pageLastSeq >= this._replayBuffer.seq &&
          this._prePeerBuffer.size === 0 &&
          this._pendingAckRes.length === 0 &&
          this._pendingEnd === NO_PENDING_END))
    )
  }

  /** What was sent while no peer was attached goes to `peer`. An ended channel's ack requests were rejected as it
   *  ended, and take no answer. */
  private _flushPrePeerBuffer(peer: IndexedPeer): void {
    this._prePeerBuffer.flush({
      sendText: (msg) => this._flow.countSent(peer.sendText(msg)),
      sendPublish: (msg) => this._flow.countSentBytes(peer.sendPublish(msg)),
      sendBinary: (msg) => {
        peer.sendBinary(msg)
        this._flow.countSent(msg.byteLength)
      },
      sendTextAck: (data, cb) =>
        peer.sendTextAckReq(data, (seq, bytes) => {
          if (cb) this._addPendingAck(seq, bytes, cb)
        }),
      sendBinaryAck: (data, cb) =>
        peer.sendBinaryAckReq(data, (seq, bytes) => {
          if (cb) this._addPendingAck(seq, bytes, cb)
        }),
      sendPublishBinary: (msg) => this._flow.countSentBytes(peer.sendPublishBinary(msg)),
    })
  }

  private _sendPendingAckRes(peer: IndexedPeer): void {
    for (const ack of this._pendingAckRes) peer.sendAckRes(ack.ackedSeq, ack.result, ack.status)
    this._pendingAckRes.length = 0
  }

  private _sendPendingEnd(peer: IndexedPeer): void {
    const { closeAck, closeRequest, abort, error } = this._pendingEnd
    if (closeAck) peer.sendCloseAck()
    if (closeRequest) this._closeRequestSeq = peer.sendCloseRequest(Math.max(0, this._closeDeadline - Date.now()))
    if (abort !== null) peer.sendAbort(abort)
    if (error !== null) peer.sendError(error)
    this._pendingEnd = NO_PENDING_END
  }

  /** Ends the channel on both ends with an ERROR of `reason`, which a page not attached gets at its next attach, in place
   *  of what the channel held for it, past what it lost. */
  protected _endWithError(reason: ErrorReason, err: Error): void {
    if (this._didShutdown) return
    this._prePeerBuffer.clear(err)
    if (this._peer) this._peer.sendError(reason)
    else this._pendingEnd = { ...this._pendingEnd, error: reason }
    this._shutdown(err)
  }

  /** Send an ack response, buffering it if the peer is currently disconnected. */
  protected _sendAckRes(ackedSeq: number, result: string, status: AckResultStatus = ACK_STATUS.OK): void {
    if (this._peer) {
      this._peer.sendAckRes(ackedSeq, result, status)
    } else {
      this._pendingAckRes.push({ ackedSeq, result, status })
    }
  }

  private _startClose(): void {
    this._isClosed = true
  }

  private async _runFinalizationLoop(): Promise<ChannelCloseResult> {
    while (!this._didShutdown) {
      // Fire onClose only once the close roundtrip is settled and no inbound work is in flight,
      // so listeners see "closed" only after the channel can no longer receive frames.
      if (this._isCloseRoundtripDone()) this._fireClose()
      if (this._didFireClose && this._pendingCloseCallbacks === 0) {
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

  private _isCloseRoundtripDone(): boolean {
    return this._inflightAcks === 0 && (!this._awaitingCloseAck || this._didReceiveCloseAck)
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

  private async _dispatchAckReq(data: ChannelData<ClientToServer>, seq: number): Promise<void> {
    if (this._listeners.size === 0) {
      this._sendAckRes(seq, 'No listener registered for ack request', ACK_STATUS.ERROR)
      return
    }
    const validateData = this._validators.get('data')
    if (validateData) {
      const result = validateData(data)
      // `shield-error` status lets the client reject its `send()` promise with a branded
      // ShieldValidationError — same identity every other shield-fail surface produces.
      if (result !== true) {
        this._sendAckRes(seq, result, ACK_STATUS.SHIELD_ERROR)
        return
      }
    }
    let lastResult: unknown
    for (const cb of this._listeners.list()) {
      try {
        lastResult = await cb(data)
      } catch (err) {
        if (this._handleCallbackError(err)) return
        this._sendAckRes(seq, `${STATUS_BODY_INTERNAL_SERVER_ERROR} — see server logs`, ACK_STATUS.ERROR)
        return
      }
    }
    this._sendAckRes(seq, stringify(lastResult))
  }

  private async _dispatchBinaryAckReq(data: Uint8Array, seq: number): Promise<void> {
    if (this._binaryListeners.size === 0) {
      this._sendAckRes(seq, 'No listener registered for ack request', ACK_STATUS.ERROR)
      return
    }
    let lastResult: unknown
    for (const cb of this._binaryListeners.list()) {
      try {
        lastResult = await cb(data)
      } catch (err) {
        if (this._handleCallbackError(err)) return
        this._sendAckRes(seq, `${STATUS_BODY_INTERNAL_SERVER_ERROR} — see server logs`, ACK_STATUS.ERROR)
        return
      }
    }
    this._sendAckRes(seq, stringify(lastResult))
  }

  protected _trackAck<T>(promise: Promise<T>): Promise<T> {
    this._inflightAcks++
    return promise.finally(() => {
      this._inflightAcks--
      this._notifyCloseProgress()
    })
  }

  /** `pageGone`: its page left it, or never came, so nothing it lacks of the channel can reach it. */
  protected _shutdown(err?: Error, { pageGone = false } = {}): void {
    if (this._didShutdown) return
    this._didShutdown = true
    this._isClosed = true
    this._closeError = err
    const pageAttached = this._peer !== null
    this._peer = null
    this._awaitingCloseAck = false
    this._clearTimer('_ttlTimer')
    this._clearTimer('_reconnectTimer')
    // The mux subscribes to this callback in `registerChannel` to evict its bookkeeping —
    // keeping the channel agnostic of who's listening.
    const shutdownCb = this._shutdownCallback
    this._shutdownCallback = null
    const keep = !pageGone && shutdownCb !== null && !this._pageNeedsNothing()
    if (!keep) this._release()
    shutdownCb?.(keep, pageAttached)
    this._fireClose(err)
    this._flow.shutdown()
    this._notifyCloseProgress()
    const ackErr = err ?? new ChannelClosedError()
    for (const { reject } of this._pendingAcks.values()) reject(ackErr)
    this._pendingAcks.clear()
    this._pendingAckBytes = 0
    // What it keeps for its page takes no answer.
    this._prePeerBuffer.rejectAcks(ackErr)
  }

  /** @internal Drops what the channel keeps for its page: its replay, and what it made while no peer was attached. */
  _release(): void {
    this._replayBuffer?.dispose()
    this._replayBuffer = null
    this._dropPending(this._closeError ?? new ChannelClosedError())
  }

  /** What was sent while no peer was attached rejects with `err`. */
  private _dropPending(err: Error): void {
    this._prePeerBuffer.clear(err)
    this._pendingAckRes.length = 0
    this._pendingEnd = NO_PENDING_END
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

function rejectOverflow(): Promise<never> {
  return Promise.reject(new ChannelOverflowError())
}

function reportServerChannelError(err: unknown): void {
  handleTelefunctionBug(err instanceof Error ? err : new Error(String(err)))
}

/** How long a gone client is still held: until its drop is noticed at the ping deadline, then for `reconnectTimeout`. */
function reconnectWindow(): number {
  const c = getServerConfig().channel
  return Math.min(TIMER_DELAY_MAX_MS, pingDeadlineOf(c) + c.reconnectTimeout)
}

function normalizeCloseTimeout(timeout: number | undefined): number {
  if (timeout === undefined) return CHANNEL_CLOSE_TIMEOUT_MS
  assertUsage(
    Number.isFinite(timeout) && timeout >= 0 && timeout <= TIMER_DELAY_MAX_MS,
    `Channel close timeout must be a non-negative number of milliseconds, at most ${TIMER_DELAY_MAX_MS}`,
  )
  return timeout
}

/** Server-side channel. `ClientToServer` = messages the server receives; `ServerToClient` = messages the server sends. */
type Channel<ClientToServer = unknown, ServerToClient = unknown, TDefault extends boolean = false> = ChannelBase<
  ServerToClient,
  ClientToServer,
  TDefault
> &
  ChannelShield<ClientToServer, ServerToClient> & {
    /** The client-side end of the channel — return this from a telefunction. */
    readonly client: ClientChannel<ClientToServer, ServerToClient, TDefault>
  }
// Public `Channel` constructor — proxies to the `ServerChannel` impl, but typed against the
// `Channel` interface so internal `_method` members are hidden, and so the third generic
// `TDefault` is inferred from `opts.ack` (gives `send()` the right ack-aware return type).
const Channel = ServerChannel as unknown as {
  new <ClientToServer = unknown, ServerToClient = unknown>(opts?: {
    ack?: false
  }): Channel<ClientToServer, ServerToClient, false>
  new <ClientToServer = unknown, ServerToClient = unknown>(opts: {
    ack: true
  }): Channel<ClientToServer, ServerToClient, true>
  new <ClientToServer = unknown, ServerToClient = unknown>(opts?: {
    ack?: boolean
  }): Channel<ClientToServer, ServerToClient, false>
}
