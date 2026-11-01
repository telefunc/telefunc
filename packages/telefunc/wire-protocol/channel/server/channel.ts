export { Channel, ServerChannel, SERVER_CHANNEL_BRAND, reportServerChannelError }
export { ChannelClosedError, ChannelOverflowError } from '../channelErrors.js'
export { NetworkError } from '../../../shared/NetworkError.js'

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
import type { TELEFUNC_SHIELDS } from '../../../node/shared/transformer/generateShield/shieldKey.js'

import type { IndexedPeer } from './IndexedPeer.js'
import type { ChannelMux } from './mux.js'
import { stringify } from '@brillout/json-serializer/stringify'
import { parse } from '@brillout/json-serializer/parse'
import { hasProp } from '../../../utils/hasProp.js'
import { unrefTimer } from '../../../utils/unrefTimer.js'
import type { ShieldValidator, ShieldValidators } from '../../../node/server/shield.js'
import { createAbortError, type AbortError } from '../../../shared/Abort.js'
import { ShieldValidationError } from '../../../shared/ShieldValidationError.js'
import { handleTelefunctionBug } from '../../../node/server/runTelefunc/validateTelefunctionError.js'
import { ChannelClosedError, ChannelOverflowError, replayLossError } from '../channelErrors.js'
import { NetworkError } from '../../../shared/NetworkError.js'
import { isPromise } from '../../../utils/isPromise.js'
import { TIMER_DELAY_MAX_MS, CREDIT_WINDOW_MAX_BYTES } from '../../constants.js'
import { normalizeCloseTimeout } from '../normalizeCloseTimeout.js'
import { ChannelLifecycle, type ChannelLifecycleOwner } from '../ChannelLifecycle.js'
import { replayWindow, type CreditOwner, type Urgency } from '../../flowControl/credit.js'
import { ReceiveCredit } from '../../flowControl/ReceiveCredit.js'
import { SelfTime } from '../../flowControl/SelfTime.js'
import { SendCredit } from '../../flowControl/SendCredit.js'
import { STATUS_BODY_INTERNAL_SERVER_ERROR } from '../../../shared/constants.js'
import { ServerChannelBuffer, type BufferedSender, type EntryCallback } from './channelBuffer.js'
import { ReplayBuffer } from '../ReplayBuffer.js'
import { getServerConfig } from '../../../node/server/serverConfig.js'
import { assert } from '../../../utils/assert.js'
import { Listeners } from '../../../utils/Listeners.js'
import { ACK_STATUS, ERROR_REASON, encode, isSequencedFrame } from '../../wire/frame.js'
import { TAG, countsCredit, isChannelCtrlTag } from '../../wire/tags.js'
import { seqNear, seqThrough } from '../../wire/seq.js'
import { ProtocolViolationError, assertProtocol } from '../../wire/protocolViolation.js'
import type {
  AckResultStatus,
  ChannelCtrlFrame,
  ChannelDataFrame,
  ChannelFrame,
  ErrorReason,
} from '../../wire/frame.js'
import type { AttachState } from '../../wire/payloads.js'

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

class ServerChannel<ClientToServer = unknown, ServerToClient = unknown>
  implements Channel<ClientToServer, ServerToClient>, CreditOwner, ChannelLifecycleOwner, BufferedSender
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

  /** @internal `closing` once `close`, `abort` or the page's CLOSE started its end; `ended` once `shutdown` ran. */
  _phase: 'open' | 'closing' | 'ended' = 'open'
  private didRegister = false
  protected peer: IndexedPeer | null = null
  /** @internal The request that opened the wire the page last attached on. */
  _wireRequest: Request | undefined = undefined
  private readonly listeners = new Listeners<ChannelListener<ClientToServer>>()
  private readonly binaryListeners = new Listeners<ChannelBinaryListener>()
  protected offlineBuffer: ServerChannelBuffer<ChannelAck<ServerToClient>>
  protected pendingAcks = new Map<
    number,
    { resolve: (result: ChannelAck<ServerToClient>) => void; reject: (err: Error) => void; bytes: number }
  >()
  /** Payload bytes of the ack requests in `pendingAcks`. */
  private pendingAckBytes = 0
  protected readonly bufferLimit: number
  protected readonly bufferLimitBinary: number
  protected readonly lifecycle = new ChannelLifecycle({ owner: this })
  private ttlTimer: ReturnType<typeof setTimeout> | null = null
  /** Credit governs fire-and-forget TEXT/BINARY, and PUBLISH in bytes — see `constants.ts`. */
  protected selfTime = new SelfTime()
  protected sendCredit: SendCredit
  protected receiveCredit: ReceiveCredit
  private reconnectTimer: ReturnType<typeof setTimeout> | null = null
  private responseAbort: ((abortValue?: unknown) => void) | null = null
  private pendingAckRes: Array<{ ackedSeq: number; result: string; status: AckResultStatus }> = []
  /** The mux it is registered with, which keeps or forgets it once it ends. */
  private mux: ChannelMux | null = null
  private pendingEnd: PendingEnd | null = null
  private closeRequestSeq = 0
  /** How far the page is known to have what this channel sent it. */
  private pageLastSeq = 0
  /** Its page closed its end, and takes nothing more of it. */
  private pageClosed = false

  // ── Wire state — channel-owned, persistent across attach-mode transitions ────
  /** Buffer of outgoing wire frames, used to replay missed frames on reconnect.
   *  Allocated in `_registerChannel`; disposed in `_release`. Null only between
   *  construction and registration (no peer can attach before registration). */
  /** @internal */ _replayBuffer: ReplayBuffer | null = null
  /** Highest client→server seq the channel has received and dispatched. Used for
   *  frame deduplication and reported back to the client in `CtrlReconciled.open[].lastSeq`. */
  /** @internal */ _lastClientSeq = 0

  /** Shield validators keyed by name (see `ChannelShield` in ../channel.ts).
   *  - `data`: validates incoming client data (_onPeerMessage / answerAckReq)
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
    this.sendCredit = new SendCredit({ owner: this, selfTime: this.selfTime })
    this.receiveCredit = new ReceiveCredit({ owner: this, selfTime: this.selfTime })
    const c = getServerConfig().channel
    this.receiveCredit.fitReplay(replayWindow(c.clientReplayBuffer, c.clientReplayBufferBinary))
    this.sendCredit.fitReplay(replayWindow(c.serverReplayBuffer, c.serverReplayBufferBinary))
    this.bufferLimit = bufferLimit ?? c.bufferLimit
    this.bufferLimitBinary = c.bufferLimitBinary
    this.offlineBuffer = new ServerChannelBuffer<ChannelAck<ServerToClient>>(this.bufferLimit, this.bufferLimitBinary)
  }

  get isClosed(): boolean {
    return this._phase !== 'open'
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
      const ret = this.sendData(data, opts) ?? Promise.resolve()
      ret.catch(() => {})
      return ret
    } finally {
      this.selfTime.recordSince(t0)
    }
  }

  private sendData(
    data: ChannelData<ServerToClient>,
    opts?: { ack?: boolean },
  ): void | Promise<ChannelAck<ServerToClient>> | Promise<void> {
    if (this._phase !== 'open') throw new ChannelClosedError()
    const needsAck = opts?.ack !== false && (opts?.ack === true || this.ack === true)
    const serialized = stringify(data)
    if (!this.peer) {
      if (needsAck) {
        return this.lifecycle.trackAck(
          new Promise<ChannelAck<ServerToClient>>((resolve, reject) => {
            this.offlineBuffer.pushTextAck(serialized, resolve, reject)
          }),
        )
      }
      return new Promise<void>((resolve, reject) => {
        this.offlineBuffer.pushText(serialized, resolve, reject)
      })
    }
    // Ack-bearing sends bypass credit accounting — the caller's `await` on the ack
    // Promise already serializes the next send, so credit would add nothing.
    if (needsAck) {
      if (this.isPeerBehind()) return rejectOverflow()
      return this.lifecycle.trackAck(
        new Promise<ChannelAck<ServerToClient>>((resolve, reject) => {
          this.peer!.sendSequenced(encode.textAckReq, serialized, (seq, bytes) =>
            this.addPendingAck(seq, bytes, { resolve, reject }),
          )
        }),
      )
    }
    // Cooperative credit model: the send already fired; `decrement` only gates the return
    // value. Awaiting throttles the caller's next send; not awaiting bypasses credit, until the peer is behind.
    if (this.sendCredit.isPastByteCredit && this.isPeerBehind()) return rejectOverflow()
    return this.sendCredit.decrement(this.peer.sendSequenced(encode.text, serialized))
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
      this.selfTime.recordSince(t0)
    }
  }

  _sendBinary(data: Uint8Array, opts?: { ack?: boolean }): void | Promise<unknown> | Promise<void> {
    if (this._phase !== 'open') throw new ChannelClosedError()
    const needsAck = opts?.ack === true
    if (!this.peer) {
      if (needsAck) {
        return this.lifecycle.trackAck(
          new Promise<unknown>((resolve, reject) => {
            this.offlineBuffer.pushBinaryAck(data, resolve, reject)
          }),
        )
      }
      return new Promise<void>((resolve, reject) => {
        this.offlineBuffer.pushBinary(data, resolve, reject)
      })
    }
    // Ack-bearing path bypasses credit; see `sendData` for rationale.
    if (needsAck) {
      if (this.isPeerBehind()) return rejectOverflow()
      return this.lifecycle.trackAck(
        new Promise<unknown>((resolve, reject) => {
          this.peer!.sendSequenced(encode.binaryAckReq, data, (seq, bytes) =>
            this.addPendingAck(seq, bytes, { resolve, reject }),
          )
        }),
      )
    }
    // Cooperative credit model; see `sendData`.
    if (this.sendCredit.isPastByteCredit && this.isPeerBehind()) return rejectOverflow()
    return this.sendCredit.decrement(this.peer.sendSequenced(encode.binary, data))
  }

  /** @internal The most that may wait on its wire for a reading page: credit, the allowance past it, offline buffers. */
  _sendAllowance(): number {
    return CREDIT_WINDOW_MAX_BYTES + this.pastCreditAllowance() + this.bufferLimit + this.bufferLimitBinary
  }

  /** How far past its credit the peer may be sent while it reads: the largest window a page grants. */
  protected pastCreditAllowance(): number {
    return CREDIT_WINDOW_MAX_BYTES
  }

  /** Whether its sends past credit and unanswered ack requests, as far as its wire holds them, reach the allowance. */
  protected isPeerBehind(): boolean {
    const allowance = this.pastCreditAllowance()
    const behind = this.sendCredit.bytesSentPastCredit + this.pendingAckBytes
    if (behind < allowance) return false
    const buffered = this.peer!.sender.bufferedAmount()
    return (buffered === undefined ? behind : Math.min(behind, buffered)) >= allowance
  }

  private addPendingAck(
    seq: number,
    bytes: number,
    settle: { resolve: (result: ChannelAck<ServerToClient>) => void; reject: (err: Error) => void },
  ): void {
    this.pendingAcks.set(seq, { resolve: settle.resolve, reject: settle.reject, bytes })
    this.pendingAckBytes += bytes
  }

  listen(callback: ChannelListener<ClientToServer>): () => void {
    return this.listeners.add(callback)
  }

  listenBinary(callback: ChannelBinaryListener): () => void {
    return this.binaryListeners.add(callback)
  }

  onClose(callback: ChannelCloseCallback): void {
    this.lifecycle.onClose(callback)
  }

  onOpen(callback: () => void): void {
    this.lifecycle.onOpen(callback)
  }

  _setResponseAbort(abortResponse: (abortValue?: unknown) => void): void {
    this.responseAbort = abortResponse
  }

  abort(): void
  abort(abortValue: unknown, message?: string): void
  abort(abortValue?: unknown, message?: string): void {
    if (this._phase !== 'open') return
    this._phase = 'closing'
    const serializedAbortValue = stringify(abortValue)
    if (this.peer) this.peer.sendAbort(serializedAbortValue)
    else this.ensurePendingEnd().abort = serializedAbortValue
    this.shutdown(createAbortError(abortValue, message))
  }

  close(opts?: ChannelCloseOptions): Promise<ChannelCloseResult> {
    if (this.lifecycle.closing) return this.lifecycle.closing
    if (this._phase === 'ended') return Promise.resolve(this.lifecycle.result)
    const timeout = normalizeCloseTimeout(opts?.timeout)
    // A channel no response carried yet has no page to acknowledge its close.
    this.lifecycle.startClose(timeout, this.didRegister)
    this._phase = 'closing'
    if (this.peer) this.closeRequestSeq = this.peer.sendCloseRequest(timeout)
    else this.ensurePendingEnd().closeRequest = true
    return this.lifecycle.runClose()
  }

  /** @internal — Prepare channel state so the mux can register it. Called by
   *  `ChannelMux.registerChannel`; external code should call that instead. */
  _registerChannel(mux?: ChannelMux): void {
    this.mux = mux ?? null
    if (this.peer || this.didRegister) return
    this.didRegister = true
    // Allocate the replay buffer up-front: registration is the moment the channel
    // becomes addressable on the wire, so a peer can attach immediately after this
    // returns.
    const c = getServerConfig().channel
    this._replayBuffer = new ReplayBuffer({
      maxBytes: c.serverReplayBuffer,
      binaryMaxBytes: c.serverReplayBufferBinary,
    })
    if (this._phase === 'ended') return // the mux lets an ended channel go
    if (this.ttlTimer) clearTimeout(this.ttlTimer)
    this.ttlTimer = unrefTimer(
      setTimeout(() => {
        this.ttlTimer = null
        this.shutdown(
          new NetworkError('Channel timed out: no client connected within TTL after response was sent', true),
          { pageGone: true },
        )
      }, c.connectTtl),
    )
  }

  /** RECONCILE declarations apply before `onOpen`; an ended channel sends only what it made with no peer attached. */
  _attachPeer(peer: IndexedPeer, state?: AttachState): void {
    if (this._phase === 'ended') {
      this.flushOfflineBuffer(peer)
      this.sendPendingAckRes(peer)
      this.sendPendingEnd(peer)
      return
    }
    if (this.ttlTimer) clearTimeout(this.ttlTimer)
    this.ttlTimer = null
    if (this.reconnectTimer) clearTimeout(this.reconnectTimer)
    this.reconnectTimer = null
    this.peer = peer
    this.receiveCredit.attach(peer.sender.sendWait())
    this.flushOfflineBuffer(peer)
    this.sendPendingAckRes(peer)
    if (state?.broadcast) {
      this._onPeerSubscription({ binary: false, subscribed: state.broadcast.text })
      this._onPeerSubscription({ binary: true, subscribed: state.broadcast.binary })
    }
    this.sendPendingEnd(peer)
    if (this._phase === 'closing') return
    this.lifecycle.fireOpen()
  }

  /** @internal — Entry point from the mux for an incoming wire frame. Handles ctrl routing,
   *  client→server seq dedup, and delegation to `dispatchDataFrame`. */
  _dispatchFrame(frame: ChannelFrame): void {
    // The wire carries a seq's low 32 bits, widened here from the next one this channel expects.
    if (isSequencedFrame(frame)) {
      frame.seq = seqNear(frame.seq, this._lastClientSeq + 1)
      if (frame.seq !== 0 && frame.seq <= this._lastClientSeq) return
      if (frame.seq > this._lastClientSeq) this._lastClientSeq = frame.seq
    }
    // An ended channel keeps only how far the page's frames reached it.
    if (this._phase === 'ended') return
    if (isChannelCtrlTag(frame.tag)) {
      this.dispatchCtrl(frame as ChannelCtrlFrame)
      return
    }
    const data = frame as ChannelDataFrame
    if (!countsCredit(data.tag)) this.receiveCredit.onReceivedUncounted(data.bytes)
    this.dispatchDataFrame(data)
  }

  /** @internal — Tag-keyed data-frame switch. Subclasses (`ServerBroadcast`) override
   *  to handle their extra tags and fall back to `super` for the common cases. */
  protected dispatchDataFrame(frame: ChannelDataFrame): void {
    switch (frame.tag) {
      case TAG.TEXT:
        this._onPeerMessage(frame.text, frame.bytes)
        return
      case TAG.TEXT_ACK_REQ:
        void this._onPeerAckReqMessage(frame.text, frame.seq)
        return
      case TAG.BINARY:
        this._onPeerBinaryMessage(frame.data, frame.bytes)
        return
      case TAG.BINARY_ACK_REQ:
        void this._onPeerBinaryAckReqMessage(frame.data, frame.seq)
        return
      case TAG.ACK_RES:
        this._onPageHas(frame.ackedSeq)
        this._onPeerAckRes(frame.ackedSeq, frame.text, frame.status)
        return
      case TAG.PUBLISH:
      case TAG.PUBLISH_BINARY:
        assert(false, `Server received unexpected ${frame.tag} frame from peer`)
    }
  }

  /** @internal — Per-channel ctrl-message switch. Connection-level ctrls (ping,
   *  reconcile, fin) never reach here — the mux's `handleFrame` handles them. */
  private dispatchCtrl(frame: ChannelCtrlFrame): void {
    switch (frame.tag) {
      case TAG.CLOSE:
        this._onPeerCloseRequest(frame.timeoutMs)
        return
      case TAG.CLOSE_ACK:
        this._onPeerCloseAck()
        return
      case TAG.ABORT:
        this.shutdown(createAbortError(parsePeerText(frame.abortValue)), { pageGone: true })
        return
      case TAG.ERROR:
        assertProtocol(frame.reason === ERROR_REASON.LOST, `ERROR reason ${frame.reason} from a page`)
        this.shutdown(replayLossError('client'), { pageGone: true })
        return
      case TAG.WINDOW:
        this.sendCredit.onPeerByteWindow(frame.bytes)
        this._onPageHas(seqThrough(frame.lastSeq, this._replayBuffer?.seq ?? 0))
        return
      case TAG.MSG_WINDOW:
        this.sendCredit.onPeerMessageWindow(frame.count)
        return
      case TAG.BDP_PING:
        // It has no attach of its own to probe the path with.
        this.peer?.sendBdpPingAck(frame.probe, this.sendCredit.onPing(), Infinity)
        return
      case TAG.BDP_PING_ACK:
        this.receiveCredit.onPingAck(frame, (ms) => this.peer?.sender.deliveredBeforeFrame(ms) ?? 0)
        return
      case TAG.BROADCAST_SUB:
      case TAG.BROADCAST_UNSUB:
        this._onPeerSubscription({ binary: frame.binary, subscribed: frame.tag === TAG.BROADCAST_SUB })
    }
  }

  // ── Called by its credit (see `CreditOwner`) ──

  /** @internal */
  _sendByteLimit(limit: number, urgency: Urgency): void {
    this.peer?.sendByteLimit(limit, this._lastClientSeq, urgency)
  }

  /** @internal */
  _sendMessageLimit(limit: number, urgency: Urgency): void {
    this.peer?.sendMessageLimit(limit, urgency)
  }

  /** @internal */
  _sendBdpPing(probe: number): void {
    this.peer?.sendBdpPing(probe)
  }

  /** @internal */
  _backlogBytes(): number | undefined {
    return this.peer?.sender.bufferedAmount()
  }

  /** A broadcast takes subscriptions; a plain channel drops them. */
  _onPeerSubscription(_subscription: { binary: boolean; subscribed: boolean }): void {}

  _onPeerMessage(text: string, bytes: number): void {
    const t0 = performance.now()
    try {
      this.receiveCredit.onReceived(bytes)
      const data = parsePeerText(text) as ChannelData<ClientToServer>
      const validateData = this._validators.get('data')
      // Shield fail on a no-ack message: silent drop (validator auto-logs). The client doesn't
      // await a response, so there's no `ShieldValidationError` to surface — listeners simply
      // never see the bad value. Ack-bearing sends go through `answerAckReq` and *do*
      // reject the sender's promise via the `shield-error` wire status.
      // Dropping still consumes: window refreshes are consumption-driven, so skipping
      // `onConsumed` would leak receive credit and eventually stall the client's sends.
      if (validateData && validateData(data) !== true) {
        this.receiveCredit.onConsumed(bytes)
        return
      }
      this.callListeners(this.listeners, data, bytes)
    } finally {
      this.selfTime.recordSince(t0)
    }
  }

  _onPeerAckReqMessage(text: string, seq: number): Promise<void> {
    // Parsed here rather than inside the async dispatch so a malformed payload throws in the recv
    // turn, where the violation still names the wire that sent it.
    const data = parsePeerText(text) as ChannelData<ClientToServer>
    return this.lifecycle.trackAck(this.answerAckReq(this.listeners, data, seq, this._validators.get('data')))
  }

  _onPeerBinaryAckReqMessage(data: Uint8Array, seq: number): Promise<void> {
    return this.lifecycle.trackAck(this.answerAckReq(this.binaryListeners, data, seq))
  }

  _onPeerBinaryMessage(data: Uint8Array, bytes: number): void {
    const t0 = performance.now()
    try {
      this.receiveCredit.onReceived(bytes)
      this.callListeners(this.binaryListeners, data, bytes)
    } finally {
      this.selfTime.recordSince(t0)
    }
  }

  /** Calls each of `listeners` with `value`; its `bytes` are consumed once the promises they return settle. A listener
   *  that throws `Abort` stops the rest. */
  private callListeners<T>(listeners: Listeners<(value: T) => unknown>, value: T, bytes: number): void {
    const pending: Promise<unknown>[] = []
    for (const cb of listeners.list()) {
      try {
        const result = cb(value)
        if (isPromise(result)) {
          pending.push(result.catch((err: unknown) => this.lifecycle.handleCallbackError(err)))
        }
      } catch (err) {
        if (this.lifecycle.handleCallbackError(err)) return
      }
    }
    if (pending.length > 0) {
      Promise.all(pending).finally(() => this.receiveCredit.onConsumed(bytes))
    } else {
      this.receiveCredit.onConsumed(bytes)
    }
  }

  _onPeerAckRes(ackedSeq: number, resultText: string, status: AckResultStatus = ACK_STATUS.OK): void {
    const pending = this.pendingAcks.get(ackedSeq)
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
      this.pendingAcks.delete(ackedSeq)
      this.pendingAckBytes -= pending.bytes
    }
  }

  _onPeerCloseRequest(timeoutMs: number): void {
    if (this._phase === 'ended') return
    assertProtocol(timeoutMs <= TIMER_DELAY_MAX_MS, `CLOSE timeout ${timeoutMs}`)
    this.lifecycle.shortenDeadline(timeoutMs)
    if (this.peer) this.peer.sendCloseAck()
    else this.ensurePendingEnd().closeAck = true
    if (this._phase === 'closing') return
    this._phase = 'closing'
    void this.lifecycle.finish()
  }

  _onPeerCloseAck(): void {
    if (this._phase === 'ended') return
    // The page acknowledges the close request as it gets it, so it has all sent before it.
    this._onPageHas(this.closeRequestSeq)
    this.lifecycle.onCloseAck()
  }

  /** @internal `peer`'s wire went away. A channel a later reconcile moved to another wire keeps that one. */
  _onPeerDisconnect(peer: IndexedPeer, reconnectTimeout: number): void {
    if (this._phase === 'ended' || this.peer?.sender !== peer.sender) return
    this.peer = null
    this.receiveCredit.detach()
    this.reconnectTimer = unrefTimer(
      setTimeout(() => {
        this.reconnectTimer = null
        this.shutdown(new NetworkError('Channel timed out: client did not reconnect within grace period', true), {
          pageGone: true,
        })
      }, reconnectTimeout),
    )
  }

  _onPeerRecoveryFailure(): void {
    if (this._phase === 'ended') return
    this.peer = null
    this.shutdown(new NetworkError('Channel not acknowledged by client after reconnect', true), { pageGone: true })
  }

  _onPeerClose(): void {
    if (this._phase === 'ended') return
    this.peer = null
    this.shutdown(undefined, { pageGone: true })
  }

  /** @internal Its replay lost what its page lacks: drops what it made offline and ends, `peer` telling the page. */
  _onReplayLost(peer: IndexedPeer): void {
    const err = replayLossError('server')
    this.dropPending(err)
    peer.sendError(ERROR_REASON.LOST)
    this.shutdown(err)
  }

  /** @internal At each of its page's heartbeats: see `ReceiveCredit.acknowledge`. */
  _acknowledge(): void {
    this.receiveCredit.acknowledge()
  }

  /** @internal The page has what this channel sent it through `lastSeq`, which its replay lets go. */
  _onPageHas(lastSeq: number): void {
    if (lastSeq > this.pageLastSeq) this.pageLastSeq = lastSeq
    this._replayBuffer?.acknowledge(lastSeq)
  }

  /** @internal Its page closed its end, having what this channel sent it through `lastSeq`. */
  _onPageClosed(lastSeq: number): void {
    this.pageClosed = true
    this._onPageHas(lastSeq)
  }

  /** @internal Its page takes nothing more of it: it closed its end, or has all this channel sent and nothing waits. */
  private pageNeedsNothing(): boolean {
    if (this._replayBuffer === null) return false
    if (this.pageClosed) return true
    return (
      this.pageLastSeq >= this._replayBuffer.seq &&
      this.offlineBuffer.size === 0 &&
      this.pendingAckRes.length === 0 &&
      this.pendingEnd === null
    )
  }

  private ensurePendingEnd(): PendingEnd {
    return (this.pendingEnd ??= { closeAck: false, closeRequest: false, abort: null, error: null })
  }

  private flushOfflineBuffer(peer: IndexedPeer): void {
    this.offlineBuffer.flush(this, peer)
  }

  /** @internal `BufferedSender`: what was sent while no peer was attached goes to `peer`, as it would have then; an ack
   *  request rejected at the end takes no answer. */
  _sendBuffered(peer: IndexedPeer, tag: number, data: string | Uint8Array, callback: EntryCallback | null): void {
    switch (tag) {
      case TAG.TEXT:
        assert(typeof data === 'string' && callback)
        this.sendCredit.countSent(peer.sendSequenced(encode.text, data))
        callback.resolve()
        return
      case TAG.TEXT_ACK_REQ:
        assert(typeof data === 'string')
        peer.sendSequenced(encode.textAckReq, data, (seq, bytes) => {
          if (callback) this.addPendingAck(seq, bytes, callback)
        })
        return
      case TAG.PUBLISH:
        assert(typeof data === 'string')
        this.sendCredit.countPublishSent(peer.sendSequenced(encode.publish, data))
        return
      case TAG.BINARY:
        assert(data instanceof Uint8Array && callback)
        this.sendCredit.countSent(peer.sendSequenced(encode.binary, data))
        callback.resolve()
        return
      case TAG.BINARY_ACK_REQ:
        assert(data instanceof Uint8Array)
        peer.sendSequenced(encode.binaryAckReq, data, (seq, bytes) => {
          if (callback) this.addPendingAck(seq, bytes, callback)
        })
        return
      case TAG.PUBLISH_BINARY:
        assert(data instanceof Uint8Array)
        this.sendCredit.countPublishSent(peer.sendSequenced(encode.publishBinary, data))
        return
    }
  }

  private sendPendingAckRes(peer: IndexedPeer): void {
    for (const ack of this.pendingAckRes) peer.sendAckRes(ack.ackedSeq, ack.result, ack.status)
    this.pendingAckRes.length = 0
  }

  private sendPendingEnd(peer: IndexedPeer): void {
    if (this.pendingEnd === null) return
    const { closeAck, closeRequest, abort, error } = this.pendingEnd
    if (closeAck) peer.sendCloseAck()
    if (closeRequest) this.closeRequestSeq = peer.sendCloseRequest(this.lifecycle.timeLeft())
    if (abort !== null) peer.sendAbort(abort)
    if (error !== null) peer.sendError(error)
    this.pendingEnd = null
  }

  /** Ends the channel on both ends with an ERROR of `reason`, which a page not attached gets at its next attach. */
  protected endWithError(reason: ErrorReason, err: Error): void {
    if (this._phase === 'ended') return
    this.offlineBuffer.clear(err)
    if (this.peer) this.peer.sendError(reason)
    else this.ensurePendingEnd().error = reason
    this.shutdown(err)
  }

  /** Send an ack response, buffering it if the peer is currently disconnected. */
  protected sendAckRes(ackedSeq: number, result: string, status: AckResultStatus = ACK_STATUS.OK): void {
    if (this.peer) {
      this.peer.sendAckRes(ackedSeq, result, status)
      if (ackedSeq === this._lastClientSeq) this.receiveCredit.answered()
    } else {
      this.pendingAckRes.push({ ackedSeq, result, status })
    }
  }

  private async answerAckReq<T>(
    listeners: Listeners<(data: T) => unknown>,
    data: T,
    seq: number,
    validate?: ShieldValidator,
  ): Promise<void> {
    if (listeners.size === 0) {
      this.sendAckRes(seq, 'No listener registered for ack request', ACK_STATUS.ERROR)
      return
    }
    const verdict = validate ? validate(data) : true
    // `shield-error` status lets the client reject its `send()` promise with a branded
    // ShieldValidationError — same identity every other shield-fail surface produces.
    if (verdict !== true) {
      this.sendAckRes(seq, verdict, ACK_STATUS.SHIELD_ERROR)
      return
    }
    let answer: string
    try {
      let lastResult: unknown
      for (const cb of listeners.list()) lastResult = await cb(data)
      answer = stringify(lastResult)
    } catch (err) {
      if (this.lifecycle.handleCallbackError(err)) return
      this.sendAckRes(seq, `${STATUS_BODY_INTERNAL_SERVER_ERROR} — see server logs`, ACK_STATUS.ERROR)
      return
    }
    this.sendAckRes(seq, answer)
  }

  /** @internal */
  _end(err?: Error): void {
    this.shutdown(err)
  }

  /** @internal An `Abort` a callback threw ends its response where it has one. */
  _abortFromCallback(err: AbortError): void {
    if (this.responseAbort) {
      this.responseAbort(err.abortValue)
    } else {
      this.abort(err.abortValue)
    }
  }

  /** @internal */
  _reportCallbackError(err: unknown): void {
    reportServerChannelError(err)
  }

  /** `pageGone`: its page left it, or never came, so nothing it lacks of the channel can reach it. */
  protected shutdown(err?: Error, { pageGone = false } = {}): void {
    if (this._phase === 'ended') return
    this._phase = 'ended'
    this.lifecycle.end(err)
    const pageAttached = this.peer !== null
    this.peer = null
    this.receiveCredit.detach()
    if (this.ttlTimer) clearTimeout(this.ttlTimer)
    this.ttlTimer = null
    if (this.reconnectTimer) clearTimeout(this.reconnectTimer)
    this.reconnectTimer = null
    // Before any response carried it, it keeps its end for the page a later one hands it to.
    const keep = !pageGone && !this.pageNeedsNothing()
    if (!keep) this._release()
    this.mux?.onChannelEnded(this, { keep, pageAttached })
    this.lifecycle.fireClose(err)
    this.sendCredit.shutdown()
    // Building an error captures its stack: one is built only where an answer is awaited.
    if (this.pendingAcks.size > 0 || this.offlineBuffer.size > 0) {
      const ackErr = err ?? new ChannelClosedError()
      for (const { reject } of this.pendingAcks.values()) reject(ackErr)
      // What it keeps for its page takes no answer.
      this.offlineBuffer.rejectAcks(ackErr)
    }
    this.pendingAcks.clear()
    this.pendingAckBytes = 0
  }

  /** @internal Drops what the channel keeps for its page: its replay, and what it made while no peer was attached. */
  _release(): void {
    this._replayBuffer?.dispose()
    this._replayBuffer = null
    this.dropPending(
      this.offlineBuffer.size === 0 ? undefined : (this.lifecycle.closeError ?? new ChannelClosedError()),
    )
  }

  /** What was sent while no peer was attached rejects with `err`. */
  private dropPending(err?: Error): void {
    this.offlineBuffer.clear(err)
    this.pendingAckRes.length = 0
    this.pendingEnd = null
  }
}

function rejectOverflow(): Promise<never> {
  return Promise.reject(new ChannelOverflowError())
}

function reportServerChannelError(err: unknown): void {
  handleTelefunctionBug(err instanceof Error ? err : new Error(String(err)))
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
