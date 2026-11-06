export { Channel, ServerChannel, SERVER_CHANNEL_BRAND }
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
import type { ChannelMux } from './ChannelMux.js'
import { stringify } from '@brillout/json-serializer/stringify'
import { parse } from '@brillout/json-serializer/parse'
import { utf8ByteLength } from '../../../utils/utf8ByteLength.js'
import { hasProp } from '../../../utils/hasProp.js'
import { unrefTimer } from '../../../utils/unrefTimer.js'
import type { ShieldValidator, ShieldValidators } from '../../../node/server/shield.js'
import { createAbortError, type AbortError } from '../../../shared/Abort.js'
import { ShieldValidationError } from '../../../shared/ShieldValidationError.js'
import { handleCallbackBug } from '../../../node/server/runTelefunc/validateTelefunctionError.js'
import { ChannelClosedError, ChannelOverflowError, replayLossError } from '../channelErrors.js'
import { NetworkError } from '../../../shared/NetworkError.js'
import { channelMessages } from '../channelMessages.js'
import { forListener, listenerAnswer } from '../listenerAnswer.js'
import { isPromise } from '../../../utils/isPromise.js'
import { TIMER_DELAY_MAX_MS, CREDIT_WINDOW_MAX_BYTES } from '../../constants.js'
import { normalizeCloseTimeout } from '../normalizeCloseTimeout.js'
import { ChannelLifecycle, type ChannelLifecycleOwner } from '../ChannelLifecycle.js'
import { replayWindow, type CreditOwner } from '../../flowControl/credit.js'
import { ReceiveCredit } from '../../flowControl/ReceiveCredit.js'
import { SelfTime } from '../../flowControl/SelfTime.js'
import { SendCredit } from '../../flowControl/SendCredit.js'
import { SendsOutrun } from '../../flowControl/server/SendsOutrun.js'
import { STATUS_BODY_INTERNAL_SERVER_ERROR } from '../../../shared/constants.js'
import {
  ServerChannelBuffer,
  HELD_SEND_OVERHEAD_BYTES,
  type BufferedSender,
  type EntryCallback,
} from './ServerChannelBuffer.js'
import { ReplayBuffer } from '../ReplayBuffer.js'
import { getServerConfig } from '../../../node/server/serverConfig.js'
import { assert, assertWarning } from '../../../utils/assert.js'
import { Listeners } from '../../../utils/Listeners.js'
import { ACK_STATUS, ERROR_REASON, encode, isSequencedFrame } from '../../wire/frame.js'
import { HEADER } from '../../wire/header.js'
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

/** Closing frames the channel produced while no page was attached; the next attach sends them. */
type PendingEnd = { closeAck: boolean; closeRequest: boolean; abort: string | null; error: ErrorReason | null }
/** A `sendAck` awaiting the peer's ACK_RES; `bytes` is its payload's share of `pendingAckBytes`. */
type PendingAck<T> = { resolve: (result: ChannelAck<T>) => void; reject: (err: Error) => void; bytes: number }

/** Whether `data` takes at most `bytes`. A UTF-16 unit takes at most 3 UTF-8 bytes, so only text near the limit is counted. */
function fitsIn(data: string | Uint8Array, bytes: number): boolean {
  if (typeof data !== 'string') return data.byteLength <= bytes
  return data.length * 3 <= bytes || utf8ByteLength(data) <= bytes
}

class ServerChannel<ClientToServer = unknown, ServerToClient = unknown>
  implements Channel<ClientToServer, ServerToClient>, ChannelLifecycleOwner, BufferedSender
{
  readonly [SERVER_CHANNEL_BRAND] = true
  /** @see ChannelShield in ../channel.ts — `data` checks what the page sends, `ack` the page's answers to this
   *  channel's ack requests. */
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

  /** @internal `closing` once `close`, `abort` or the page's CLOSE starts the close; `ended` once `shutdown` ran. */
  _phase: 'open' | 'closing' | 'ended' = 'open'
  private didRegister = false
  protected peer: IndexedPeer | null = null
  /** The request that opened the wire the page last attached on. */
  protected wireRequest: Request | undefined = undefined
  private readonly listeners = new Listeners<ChannelListener<ClientToServer>>()
  private readonly binaryListeners = new Listeners<ChannelBinaryListener>()
  protected offlineBuffer: ServerChannelBuffer<ChannelAck<ServerToClient>>
  protected pendingAcks = new Map<number, PendingAck<ServerToClient>>()
  /** Payload bytes of the ack requests in `pendingAcks`. */
  private pendingAckBytes = 0
  private readonly maxPendingBytes: number
  private readonly maxPendingAcks: number
  protected readonly bufferLimit: number
  protected readonly bufferLimitBinary: number
  protected readonly lifecycle = new ChannelLifecycle({ owner: this, reportError: handleCallbackBug })
  private ttlTimer: ReturnType<typeof setTimeout> | null = null
  protected selfTime = new SelfTime()
  /** Paces fire-and-forget TEXT, BINARY and PUBLISH sends to what the page grants; see `constants.ts`. */
  protected sendCredit: SendCredit
  /** Whether its sends outrun the page; null once it warned they do. */
  private outrun: SendsOutrun | null = new SendsOutrun()
  /** A send nobody awaits can't show its rejection, so the first refusal warns. */
  private refusalWarned = false
  protected receiveCredit: ReceiveCredit
  private reconnectTimer: ReturnType<typeof setTimeout> | null = null
  private responseAbort: ((abortValue?: unknown) => void) | null = null
  private pendingAckRes: Array<{ ackedSeq: number; result: string; status: AckResultStatus }> = []
  /** The mux that registered this channel; when the channel ends, the mux keeps it or forgets it. */
  private mux: ChannelMux | null = null
  private pendingEnd: PendingEnd | null = null
  private closeRequestSeq = 0
  /** The highest seq the page is known to have received from this channel. */
  private pageLastSeq = 0
  /** The page closed its end, so it takes nothing more from this channel. */
  private pageClosed = false

  // ── Wire state — channel-owned, kept across the page's attaches ────
  /** @internal Buffer of outgoing wire frames, used to replay missed frames on reconnect. Allocated in
   *  `_registerChannel` and disposed in `_release`: null before registration, when no peer can attach, and after. */
  _replayBuffer: ReplayBuffer | null = null
  /** @internal Highest client→server seq the channel has received and dispatched. Used for frame deduplication and
   *  reported back to the client in `ReconciledPayload.open[].lastSeq`. */
  _lastClientSeq = 0

  /** Shield validators keyed by name (see `ChannelShield` in ../channel.ts).
   *  - `data`: validates incoming client data (onPeerMessage / answerAckReq)
   *  - `ack`: validates client ack responses (onPeerAckRes)
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
    // Its credits send to the page attached, nothing while none is, and read that page's wire.
    const owner: CreditOwner = {
      // The limit carries the last seq the channel has, which acknowledges what arrived.
      sendByteLimit: (limit, urgency) => this.peer?.sendByteLimit(limit, this._lastClientSeq, urgency),
      sendMessageLimit: (limit, urgency) => this.peer?.sendMessageLimit(limit, urgency),
      sendBdpPing: (probe) => this.peer?.sendBdpPing(probe),
      backlogBytes: () => this.peer?.sender.bufferedAmount(),
      deliveredBefore: (ms) => this.peer?.sender.deliveredBefore(ms) ?? 0,
    }
    this.sendCredit = new SendCredit({ owner, selfTime: this.selfTime })
    this.receiveCredit = new ReceiveCredit({ owner, selfTime: this.selfTime })
    const c = getServerConfig().channel
    this.receiveCredit.fitReplay(replayWindow(c.clientReplayBuffer, c.clientReplayBufferBinary))
    this.sendCredit.fitReplay(replayWindow(c.serverReplayBuffer, c.serverReplayBufferBinary))
    this.maxPendingBytes = c.maxPendingBytes
    this.maxPendingAcks = c.maxPendingAcks
    this.bufferLimit = bufferLimit ?? c.bufferLimit
    this.bufferLimitBinary = c.bufferLimitBinary
    this.offlineBuffer = new ServerChannelBuffer<ChannelAck<ServerToClient>>({
      maxBytes: this.bufferLimit,
      binaryMaxBytes: this.bufferLimitBinary,
    })
  }

  get opened(): Promise<void> {
    return this.lifecycle.opened
  }

  get closed(): Promise<void> {
    return this.lifecycle.closed
  }

  messages(options?: { signal?: AbortSignal }) {
    return channelMessages<ChannelData<ClientToServer>>(this, options)
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
    const refusal = this.pendingRefusal(serialized, needsAck)
    if (refusal) return Promise.reject<void>(refusal)
    // Ack-bearing sends bypass credit accounting — the caller's `await` on the ack
    // Promise already serializes the next send, so credit would add nothing.
    if (needsAck) {
      return this.lifecycle.trackAck(
        new Promise<ChannelAck<ServerToClient>>((resolve, reject) => {
          this.peer!.sendSequenced(encode.textAckReq, serialized, (seq, bytes) =>
            this.addPendingAck(seq, bytes, { resolve, reject }),
          )
        }),
      )
    }
    // Cooperative credit model: the send already fired; `decrement` only gates the return
    // value. Awaiting throttles the caller's next send; not awaiting bypasses credit.
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
    const refusal = this.pendingRefusal(data, needsAck)
    if (refusal) return Promise.reject<void>(refusal)
    // Ack-bearing path bypasses credit; see `sendData` for rationale.
    if (needsAck) {
      return this.lifecycle.trackAck(
        new Promise<unknown>((resolve, reject) => {
          this.peer!.sendSequenced(encode.binaryAckReq, data, (seq, bytes) =>
            this.addPendingAck(seq, bytes, { resolve, reject }),
          )
        }),
      )
    }
    // Cooperative credit model; see `sendData`.
    return this.sendCredit.decrement(this.peer.sendSequenced(encode.binary, data))
  }

  /** Why a send of `data` can't go, where it would pass `maxPendingAcks` or `maxPendingBytes`. It holds nothing and takes
   *  no seq, so the page sees no gap on the wire, and later sends that fit go through. */
  private pendingRefusal(data: string | Uint8Array, ack: boolean): ChannelOverflowError | null {
    if (ack && this.pendingAcks.size >= this.maxPendingAcks) return this.refuseSend('maxPendingAcks')
    const room = this.maxPendingBytes - this.bytesAhead()
    // Only a send that waits, for its answer or behind sends waiting for credit, is held with its promise.
    const held = ack || this.sendCredit.waitingCount > 0
    const fits = held ? fitsIn(data, room - HEADER - HELD_SEND_OVERHEAD_BYTES) : room >= 0
    return fits ? null : this.refuseSend('maxPendingBytes')
  }

  private refuseSend(setting: 'maxPendingAcks' | 'maxPendingBytes'): ChannelOverflowError {
    if (!this.refusalWarned) {
      this.refusalWarned = true
      assertWarning(
        false,
        `A channel refused a \`send()\`: config.channel.${setting} (${this[setting]}) is exceeded, so that message isn't delivered and its promise rejects with \`ChannelOverflowError\`. Later sends that fit still go. Await each \`channel.send()\` or \`channel.sendBinary()\` before the next, or raise the setting.`,
        { onlyOnce: false },
      )
    }
    return new ChannelOverflowError(
      `Send refused: config.channel.${setting} exceeded; await each send or raise the setting`,
    )
  }

  /** What its sends put ahead of the page: past the page's credit, and the ack requests it hasn't answered. */
  private bytesAhead(): number {
    return (
      this.sendCredit.bytesSentPastCredit +
      this.sendCredit.waitingCount * HELD_SEND_OVERHEAD_BYTES +
      this.pendingAckBytes +
      this.pendingAcks.size * HELD_SEND_OVERHEAD_BYTES
    )
  }

  /** The page took `bytes`; warns, once, where the sends nobody awaits keep outrunning it (see `SendsOutrun`). */
  private onDrained(bytes: number): void {
    if (this.outrun === null) return
    const ahead = this.bytesAhead()
    if (!this.outrun.drained(bytes, ahead, performance.now())) return
    const mibPerS = (this.outrun.rate * 1000) / 2 ** 20
    this.outrun = null
    assertWarning(
      false,
      `A channel's \`send()\` calls outrun its client: ${(ahead / 2 ** 20).toFixed(1)} MiB wait for it and keep growing, while it takes ${mibPerS.toFixed(1)} MiB/s. Await each \`channel.send()\` or \`channel.sendBinary()\` before the next, or send slower: past \`config.channel.maxPendingBytes\` (${(this.maxPendingBytes / 2 ** 20).toFixed(0)} MiB) or \`maxPendingAcks\` (${this.maxPendingAcks}), sends are refused with \`ChannelOverflowError\`.`,
      { onlyOnce: false },
    )
  }

  /** @internal The most bytes that may wait for a reading page: the credit window, the allowance past it, and both
   *  offline buffers. */
  _sendAllowance(): number {
    return CREDIT_WINDOW_MAX_BYTES + this.pastCreditAllowance() + this.bufferLimit + this.bufferLimitBinary
  }

  /** What the wire cap lets a page hold past its credit: the largest window a page grants, and all that this channel's
   *  sends put ahead of the page, which only the server code's `await` holds back. */
  protected pastCreditAllowance(): number {
    return CREDIT_WINDOW_MAX_BYTES + Math.min(this.maxPendingBytes, this.bytesAhead())
  }

  private addPendingAck(
    seq: number,
    bytes: number,
    settle: { resolve: (result: ChannelAck<ServerToClient>) => void; reject: (err: Error) => void },
  ): void {
    this.pendingAcks.set(seq, { resolve: settle.resolve, reject: settle.reject, bytes })
    this.pendingAckBytes += bytes
  }

  listen(callback: ChannelListener<ClientToServer>, options?: { signal?: AbortSignal }): () => void {
    return this.listeners.add(callback, options)
  }

  listenBinary(callback: ChannelBinaryListener, options?: { signal?: AbortSignal }): () => void {
    return this.binaryListeners.add(callback, options)
  }

  onClose(callback: ChannelCloseCallback, options?: { signal?: AbortSignal }): void {
    this.lifecycle.onClose(callback, options)
  }

  onOpen(callback: () => void, options?: { signal?: AbortSignal }): void {
    this.lifecycle.onOpen(callback, options)
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
    return this.lifecycle.localCloseRequested({
      timeout,
      peer: this.didRegister ? 'expected' : 'absent',
      send: () => {
        this._phase = 'closing'
        if (this.peer) this.closeRequestSeq = this.peer.sendCloseRequest(timeout)
        else this.ensurePendingEnd().closeRequest = true
      },
    })
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

  /** @internal Attaches the page's wire. An ended channel only sends what it produced while no page was attached;
   *  an open one applies the RECONCILE's broadcast subscriptions before `onOpen` fires. */
  _attachPeer(peer: IndexedPeer, state?: AttachState): void {
    if (this._phase === 'ended') {
      this.offlineBuffer.flush(this, peer)
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
    this.offlineBuffer.flush(this, peer)
    this.sendPendingAckRes(peer)
    if (state?.broadcast) {
      this.onPeerSubscription({ binary: false, subscribed: state.broadcast.text })
      this.onPeerSubscription({ binary: true, subscribed: state.broadcast.binary })
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
    // An ended channel only tracks the page's last seq; it acts on no frame.
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
        this.onPeerMessage(frame.text, frame.bytes)
        return
      case TAG.TEXT_ACK_REQ:
        void this.onPeerAckReqMessage(frame.text, frame.seq)
        return
      case TAG.BINARY:
        this.onPeerBinaryMessage(frame.data, frame.bytes)
        return
      case TAG.BINARY_ACK_REQ:
        void this.onPeerBinaryAckReqMessage(frame.data, frame.seq)
        return
      case TAG.ACK_RES:
        this.onPageHas(frame.ackedSeq)
        this.onPeerAckRes(frame.ackedSeq, frame.text, frame.status)
        return
      case TAG.PUBLISH:
      case TAG.PUBLISH_BINARY:
        assert(false, `Server received unexpected ${frame.tag} frame from peer`)
    }
  }

  /** @internal — Per-channel ctrl-message switch. Wire-level ctrls (ping,
   *  reconcile, fin) never reach here — the mux's `handleFrame` handles them. */
  private dispatchCtrl(frame: ChannelCtrlFrame): void {
    switch (frame.tag) {
      case TAG.CLOSE:
        this.onPeerCloseRequest(frame.timeoutMs)
        return
      case TAG.CLOSE_ACK:
        this.onPeerCloseAck()
        return
      case TAG.ABORT:
        this.shutdown(createAbortError(parsePeerText(frame.abortValue)), { pageGone: true })
        return
      case TAG.ERROR:
        assertProtocol(frame.reason === ERROR_REASON.LOST, `ERROR reason ${frame.reason} from a page`)
        this.shutdown(replayLossError('client'), { pageGone: true })
        return
      case TAG.WINDOW:
        this.onDrained(this.sendCredit.onPeerByteWindow(frame.bytes))
        this.onPageHas(seqThrough(frame.lastSeq, this._replayBuffer?.seq ?? 0))
        return
      case TAG.MSG_WINDOW:
        this.sendCredit.onPeerMessageWindow(frame.count)
        return
      case TAG.BDP_PING:
        // A channel measures no path RTT of its own, so `pathRtt` is `Infinity`.
        this.peer?.sendBdpPingAck(frame.probe, this.sendCredit.onPing(), Infinity)
        return
      case TAG.BDP_PING_ACK:
        this.receiveCredit.onPingAck(frame)
        return
      case TAG.BROADCAST_SUB:
      case TAG.BROADCAST_UNSUB:
        this.onPeerSubscription({ binary: frame.binary, subscribed: frame.tag === TAG.BROADCAST_SUB })
    }
  }

  /** `ServerBroadcast` overrides it; a plain channel ignores subscriptions. */
  protected onPeerSubscription(_subscription: { binary: boolean; subscribed: boolean }): void {}

  protected onPeerMessage(text: string, bytes: number): void {
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

  protected onPeerAckReqMessage(text: string, seq: number): Promise<void> {
    // Parsed here rather than inside the async dispatch so a malformed payload throws in the recv
    // turn, where the violation still names the wire that sent it.
    const data = parsePeerText(text) as ChannelData<ClientToServer>
    return this.lifecycle.trackAck(this.answerAckReq(this.listeners, data, seq, this._validators.get('data')))
  }

  protected onPeerBinaryAckReqMessage(data: Uint8Array, seq: number): Promise<void> {
    return this.lifecycle.trackAck(this.answerAckReq(this.binaryListeners, data, seq))
  }

  protected onPeerBinaryMessage(data: Uint8Array, bytes: number): void {
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
    const list = listeners.list()
    for (const [i, cb] of list.entries()) {
      try {
        const result = cb(forListener(value, list, i))
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

  protected onPeerAckRes(ackedSeq: number, resultText: string, status: AckResultStatus = ACK_STATUS.OK): void {
    const pending = this.pendingAcks.get(ackedSeq)
    if (!pending) return
    try {
      switch (status) {
        case ACK_STATUS.OK:
          this.settleAck(pending, parsePeerText(resultText) as ChannelAck<ServerToClient>)
          return
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
      this.onDrained(pending.bytes)
    }
  }

  /** Resolves `pending` with the peer's ack, unless the server-declared ack shield rejects it. */
  private settleAck(pending: PendingAck<ServerToClient>, parsed: ChannelAck<ServerToClient>): void {
    const validateAck = this._validators.get('ack')
    const result = validateAck ? validateAck(parsed) : true
    // Same class as every other shield-fail surface, so user code can catch with `isShieldValidationError`.
    if (result !== true) pending.reject(new ShieldValidationError(result))
    else pending.resolve(parsed)
  }

  protected onPeerCloseRequest(timeoutMs: number): void {
    if (this._phase === 'ended') return
    assertProtocol(timeoutMs <= TIMER_DELAY_MAX_MS, `CLOSE timeout ${timeoutMs}`)
    this.lifecycle.peerCloseReceived(timeoutMs, () => {
      this._phase = 'closing'
      if (this.peer) this.peer.sendCloseAck()
      else this.ensurePendingEnd().closeAck = true
    })
  }

  protected onPeerCloseAck(): void {
    if (this._phase === 'ended') return
    // The page sends CLOSE_ACK when the close request arrives, so it has received everything sent before it.
    this.onPageHas(this.closeRequestSeq)
    this.lifecycle.closeAckReceived()
  }

  /** @internal The wire of `peer` went away. A channel a later RECONCILE already moved to another wire stays there. */
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

  /** @internal The page reattached but never acknowledged this channel: it ends. */
  _onPeerRecoveryFailure(): void {
    if (this._phase === 'ended') return
    this.peer = null
    this.shutdown(new NetworkError('Channel not acknowledged by client after reconnect', true), { pageGone: true })
  }

  /** @internal The page's wire ended for good: the channel ends with it. */
  _onPeerGone(): void {
    if (this._phase === 'ended') return
    this.peer = null
    this.shutdown(undefined, { pageGone: true })
  }

  /** @internal The replay buffer no longer holds what the page lacks: drops what was buffered offline, sends the page
   *  a LOST error on `peer`, and ends. */
  _onReplayLost(peer: IndexedPeer): void {
    const err = replayLossError('server')
    this.dropPending(err)
    peer.sendError(ERROR_REASON.LOST)
    this.shutdown(err)
  }

  /** @internal Called on each of the page's heartbeats; see `ReceiveCredit.acknowledge`. */
  _acknowledge(): void {
    this.receiveCredit.acknowledge()
  }

  /** @internal The page attaches on the wire `request` opened, having received this channel's frames through
   *  `lastSeq`. */
  _onPageAttach(lastSeq: number, request: Request | undefined): void {
    this.onPageHas(lastSeq)
    this.wireRequest = request
  }

  /** @internal The page closed its end after receiving this channel's frames through `lastSeq`, the low 32 bits a PING
   *  carries. */
  _onPageClosed(lastSeq: number): void {
    this.pageClosed = true
    this.onPageHas(seqThrough(lastSeq, this._replayBuffer?.seq ?? 0))
  }

  /** The page received this channel's frames through `lastSeq`, so the replay buffer lets them go. */
  private onPageHas(lastSeq: number): void {
    if (lastSeq > this.pageLastSeq) this.pageLastSeq = lastSeq
    this._replayBuffer?.acknowledge(lastSeq)
  }

  /** True once the page needs nothing more: it closed its end, or it received every frame and nothing is pending. */
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

  /** As for a send made now, its `await` throttles the sender by the page's credit. */
  private settleBuffered(bytes: number, callback: EntryCallback): void {
    const wait = this.sendCredit.decrement(bytes)
    if (wait) wait.then(callback.resolve, callback.reject)
    else callback.resolve()
  }

  /** @internal `BufferedSender`: sends `peer` a frame buffered while no page was attached, as it would have gone then,
   *  within the same pending limits. An ack request already rejected at shutdown has no callback left to answer. */
  _sendBuffered(peer: IndexedPeer, tag: number, data: string | Uint8Array, callback: EntryCallback | null): void {
    if (this._phase !== 'ended' && tag !== TAG.PUBLISH && tag !== TAG.PUBLISH_BINARY) {
      const refusal = this.pendingRefusal(data, tag === TAG.TEXT_ACK_REQ || tag === TAG.BINARY_ACK_REQ)
      if (refusal) return callback?.reject(refusal)
    }
    switch (tag) {
      case TAG.TEXT:
        assert(typeof data === 'string' && callback)
        this.settleBuffered(peer.sendSequenced(encode.text, data), callback)
        break
      case TAG.TEXT_ACK_REQ:
        assert(typeof data === 'string')
        peer.sendSequenced(encode.textAckReq, data, (seq, bytes) => {
          if (callback) this.addPendingAck(seq, bytes, callback)
        })
        break
      case TAG.PUBLISH:
        assert(typeof data === 'string')
        peer.sendSequenced(encode.publish, data)
        break
      case TAG.BINARY:
        assert(data instanceof Uint8Array && callback)
        this.settleBuffered(peer.sendSequenced(encode.binary, data), callback)
        break
      case TAG.BINARY_ACK_REQ:
        assert(data instanceof Uint8Array)
        peer.sendSequenced(encode.binaryAckReq, data, (seq, bytes) => {
          if (callback) this.addPendingAck(seq, bytes, callback)
        })
        break
      case TAG.PUBLISH_BINARY:
        assert(data instanceof Uint8Array)
        peer.sendSequenced(encode.publishBinary, data)
        break
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

  /** Ends the channel on both sides with an ERROR of `reason`; a page not attached receives it on its next attach. */
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
    let answer: string | null
    try {
      answer = await listenerAnswer(listeners.list(), data)
    } catch (err) {
      if (this.lifecycle.handleCallbackError(err)) return
      this.sendAckRes(seq, `${STATUS_BODY_INTERNAL_SERVER_ERROR} — see server logs`, ACK_STATUS.ERROR)
      return
    }
    if (answer === null) {
      this.sendAckRes(seq, 'Only one channel listener may return an acknowledgement value', ACK_STATUS.ERROR)
      return
    }
    this.sendAckRes(seq, answer)
  }

  /** @internal */
  _end(err?: Error): void {
    this.shutdown(err)
  }

  /** @internal A callback threw `Abort`: aborts the response carrying the channel if there is one, else the channel. */
  _abortFromCallback(err: AbortError): void {
    if (this.responseAbort) {
      this.responseAbort(err.abortValue)
    } else {
      this.abort(err.abortValue)
    }
  }

  /** `pageGone`: the page left or never attached, so nothing the channel holds for it can reach it. */
  protected shutdown(err?: Error, { pageGone = false } = {}): void {
    if (this._phase === 'ended') return
    this._phase = 'ended'
    this.lifecycle.forcedTermination(err, () => {
      const pageAttached = this.peer !== null
      this.peer = null
      this.receiveCredit.detach()
      if (this.ttlTimer) clearTimeout(this.ttlTimer)
      this.ttlTimer = null
      if (this.reconnectTimer) clearTimeout(this.reconnectTimer)
      this.reconnectTimer = null
      // Kept for a page that may still attach and lacks something, or for a response that has yet to carry it.
      const keep = !pageGone && !this.pageNeedsNothing()
      if (!keep) this._release()
      this.mux?.onChannelEnded(this, { keep, pageAttached })
      this.listeners.clear()
      this.binaryListeners.clear()
      this.sendCredit.shutdown(err)
      // Building an error captures a stack trace, so one is built only while an ack is awaited or a frame is buffered.
      if (this.pendingAcks.size > 0 || this.offlineBuffer.size > 0) {
        const ackErr = err ?? new ChannelClosedError()
        for (const { reject } of this.pendingAcks.values()) reject(ackErr)
        // The buffered ack requests stay for the page, but their callbacks reject now.
        this.offlineBuffer.rejectAcks(ackErr)
      }
      this.pendingAcks.clear()
      this.pendingAckBytes = 0
    })
  }

  /** @internal Drops what the channel holds for its page: the replay buffer and all it buffered while detached. */
  _release(): void {
    this._replayBuffer?.dispose()
    this._replayBuffer = null
    this.dropPending(
      this.offlineBuffer.size === 0 ? undefined : (this.lifecycle.closeError ?? new ChannelClosedError()),
    )
  }

  /** Drops all buffered while no page was attached; buffered sends reject with `err` when it is given. */
  private dropPending(err?: Error): void {
    this.offlineBuffer.clear(err)
    this.pendingAckRes.length = 0
    this.pendingEnd = null
  }
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
