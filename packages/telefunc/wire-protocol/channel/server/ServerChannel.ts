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
  ChannelJson,
  ChannelListener,
  ChannelBinaryListener,
} from '../channel.js'
import { LOSSY_WITH_ACK_TEXT, stringifyMessage } from '../channel.js'
import type { TELEFUNC_SHIELDS } from '../../../node/shared/transformer/generateShield/shieldKey.js'

import type { IndexedPeer } from './IndexedPeer.js'
import type { ChannelMux } from './ChannelMux.js'
import { stringify } from '@brillout/json-serializer/stringify'
import { parse } from '@brillout/json-serializer/parse'
import { utf8ByteLength } from '../../../utils/utf8ByteLength.js'
import {
  HELD_SEND_OVERHEAD_BYTES,
  admit,
  fits,
  hold,
  holdsEnough,
  letGo,
  maxPendingBytesOf,
  releaseHolder,
  type MemoryHolder,
} from './channelMemory.js'
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
import { TIMER_DELAY_MAX_MS } from '../../constants.js'
import { normalizeCloseTimeout } from '../normalizeCloseTimeout.js'
import { ChannelLifecycle, type ChannelLifecycleOwner } from '../ChannelLifecycle.js'
import { creditWindowOf, type CreditOwner } from '../../flowControl/credit.js'
import { ReceiveCredit } from '../../flowControl/ReceiveCredit.js'
import { SelfTime } from '../../flowControl/SelfTime.js'
import { SendCredit } from '../../flowControl/SendCredit.js'
import { STATUS_BODY_INTERNAL_SERVER_ERROR } from '../../../shared/constants.js'
import {
  ServerChannelBuffer,
  type BufferedSender,
  type Entry,
  type EntryCallback,
  type EntryData,
} from './ServerChannelBuffer.js'
import { ServerReplay } from './ServerReplay.js'
import { LossyDelivery, type LossyHost } from './LossyDelivery.js'
import { getChannelConfig } from '../../../node/server/serverConfig.js'
import { assert, assertUsage, assertWarning } from '../../../utils/assert.js'
import { Listeners } from '../../../utils/Listeners.js'
import {
  ACK_STATUS,
  ERROR_REASON,
  MESSAGE_FRAME_OVERHEAD_MAX_BYTES,
  encode,
  isSequencedFrame,
} from '../../wire/frame.js'
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
  SequencedEncoder,
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

/** What an answer of `result` holds in a replay, or while its page is away. */
function heldAnswerBytes(result: string): number {
  return utf8ByteLength(result) + MESSAGE_FRAME_OVERHEAD_MAX_BYTES + HELD_SEND_OVERHEAD_BYTES
}
/** What a refusal holds: every ack request being answered holds at least this, so its refusal can take its place. */
const REFUSAL_BYTES = heldAnswerBytes('')
const TOO_MANY_ANSWERS_TEXT =
  'Channel closed: more `{ ack: true }` sends and `publish()` calls wait for their answers than the server can hold even refusals for, see config.channel.maxMemory'

/** Closing frames the channel produced while no page was attached; the next attach sends them. */
type PendingEnd = { closeAck: boolean; closeRequest: boolean; abort: string | null; error: ErrorReason | null }
/** A `sendAck` awaiting the peer's ACK_RES. */
type PendingAck<T> = { resolve: (result: ChannelAck<T>) => void; reject: (err: Error) => void }
/** A `send(data, { lossy: true })` to an attached page, text or a copy of its bytes once held; `held` is what it holds
 *  while it waits. */
type LossySend = { data: EntryData; held: number }

class ServerChannel<ClientToServer = unknown, ServerToClient = unknown>
  implements Channel<ClientToServer, ServerToClient>, ChannelLifecycleOwner, BufferedSender, MemoryHolder
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
  protected offlineBuffer = new ServerChannelBuffer<ChannelAck<ServerToClient>>(this)
  /** What it holds for its page. */
  heldBytes = 0
  released = false
  protected pendingAcks = new Map<number, PendingAck<ServerToClient>>()
  protected readonly lifecycle = new ChannelLifecycle({ owner: this, reportError: handleCallbackBug })
  private ttlTimer: ReturnType<typeof setTimeout> | null = null
  protected selfTime = new SelfTime()
  /** Paces fire-and-forget TEXT, BINARY and PUBLISH sends to what the page grants; see `constants.ts`. */
  protected sendCredit: SendCredit
  /** A send nobody awaits can't show its rejection, so the first refusal warns. */
  private refusalWarned = false
  protected receiveCredit: ReceiveCredit
  private reconnectTimer: ReturnType<typeof setTimeout> | null = null
  private responseAbort: ((abortValue?: unknown) => void) | null = null
  private pendingAckRes: Array<{ ackedSeq: number; result: string; status: AckResultStatus }> = []
  /** What each ack request being answered holds, by its seq: its answer takes that place. */
  private readonly answering = new Map<number, number>()
  /** The mux that registered this channel; when the channel ends, the mux keeps it or forgets it. */
  private mux: ChannelMux | null = null
  private pendingEnd: PendingEnd | null = null
  private closeRequestSeq = 0
  /** The highest seq the page is known to have received from this channel. */
  private pageLastSeq = 0
  /** The page closed its end, so it takes nothing more from this channel. */
  private pageClosed = false
  /** Its lossy sends while its page is attached, from the first one on. */
  private lossySends: LossyDelivery<LossySend> | null = null

  // ── Wire state — channel-owned, kept across the page's attaches ────
  /** @internal Buffer of outgoing wire frames, used to replay missed frames on reconnect. Allocated in
   *  `_registerChannel` and disposed in `_release`: null before registration, when no peer can attach, and after. */
  _replayBuffer: ServerReplay | null = null
  /** @internal Highest client→server seq the channel has received and dispatched. Used for frame deduplication and
   *  reported back to the client in `ReconciledPayload.open[].lastSeq`. */
  _lastClientSeq = 0

  /** Shield validators keyed by name (see `ChannelShield` in ../channel.ts).
   *  - `data`: validates incoming client data (onPeerMessage / answerAckReq)
   *  - `ack`: validates client ack responses (onPeerAckRes)
   *  Each returns `true` on success or an error string — callers decide the action (drop, throw). */
  _validators: ShieldValidators = new Map()

  constructor({ ack = false, id }: { ack?: boolean; id?: string } = {}) {
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
      holdsEnough,
    }
    this.sendCredit = new SendCredit({ owner, selfTime: this.selfTime })
    this.receiveCredit = new ReceiveCredit({ owner, selfTime: this.selfTime })
    const c = getChannelConfig()
    this.receiveCredit.capWindow(creditWindowOf(c.maxMemory, c.maxPendingBytes))
    this.sendCredit.capWindow(creditWindowOf(c.maxMemory, c.maxPendingBytes))
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

  send<D>(data: ChannelJson<ServerToClient, D, 'send'>): Promise<void>
  send<D>(data: ChannelJson<ServerToClient, D, 'send'>, opts: { ack: true }): Promise<ChannelAck<ServerToClient>>
  send<D>(data: ChannelJson<ServerToClient, D, 'send'>, opts: { ack: false }): Promise<void>
  send<D>(data: ChannelJson<ServerToClient, D, 'send'>, opts: { lossy: true; ack?: false }): Promise<void>
  send(data: unknown, opts?: { ack?: boolean; lossy?: boolean }): Promise<ChannelAck<ServerToClient>> | Promise<void> {
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
    data: unknown,
    opts?: { ack?: boolean; lossy?: boolean },
  ): void | Promise<ChannelAck<ServerToClient>> | Promise<void> {
    if (this._phase !== 'open') throw new ChannelClosedError()
    if (opts?.lossy) assertUsage(opts.ack !== true, LOSSY_WITH_ACK_TEXT)
    const needsAck = opts?.ack !== false && (opts?.ack === true || this.ack === true)
    const serialized = stringifyMessage(data, 'send')
    if (opts?.lossy) return this.sendLossy(TAG.TEXT, serialized, utf8ByteLength(serialized))
    const refusal = this.textRefusal(serialized)
    if (refusal) return Promise.reject<void>(refusal)
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
    this.lossySends?.flush()
    // Ack-bearing sends bypass credit accounting — the caller's `await` on the ack
    // Promise already serializes the next send, so credit would add nothing.
    if (needsAck) return this.sendAckRequest(encode.textAckReq, serialized)
    // Cooperative credit model: the send already fired; `decrement` only gates the return
    // value. Awaiting throttles the caller's next send; not awaiting bypasses credit.
    return this.sendCredit.decrement(this.peer.sendSequenced(encode.text, serialized))
  }

  sendBinary(data: Uint8Array): Promise<void>
  sendBinary(data: Uint8Array, opts: { ack: true }): Promise<unknown>
  sendBinary(data: Uint8Array, opts: { ack: false }): Promise<void>
  sendBinary(data: Uint8Array, opts: { lossy: true; ack?: false }): Promise<void>
  sendBinary(data: Uint8Array, opts?: { ack?: boolean; lossy?: boolean }): Promise<unknown> | Promise<void> {
    const t0 = performance.now()
    try {
      const ret = this._sendBinary(data, opts) ?? Promise.resolve()
      ret.catch(() => {})
      return ret
    } finally {
      this.selfTime.recordSince(t0)
    }
  }

  _sendBinary(data: Uint8Array, opts?: { ack?: boolean; lossy?: boolean }): void | Promise<unknown> | Promise<void> {
    if (this._phase !== 'open') throw new ChannelClosedError()
    if (opts?.lossy) assertUsage(opts.ack !== true, LOSSY_WITH_ACK_TEXT)
    const needsAck = opts?.ack === true
    if (opts?.lossy) return this.sendLossy(TAG.BINARY, data, data.byteLength)
    const refusal = this.refusal(data.byteLength)
    if (refusal) return Promise.reject<void>(refusal)
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
    this.lossySends?.flush()
    // Ack-bearing path bypasses credit; see `sendData` for rationale.
    if (needsAck) return this.sendAckRequest(encode.binaryAckReq, data)
    // Cooperative credit model; see `sendData`.
    return this.sendCredit.decrement(this.peer.sendSequenced(encode.binary, data))
  }

  /** A send of which only the newest matters, admitted for what it adds once it takes the place of the one waiting, so
   *  that one stays where this one is refused. With no page attached it waits as the offline buffer's last entry, which
   *  the next one replaces; attached, it goes, or waits as the newest while the page is behind or out of credit. */
  private sendLossy(tag: typeof TAG.TEXT | typeof TAG.BINARY, data: EntryData, bytes: number): void | Promise<void> {
    const held = bytes + HEADER + HELD_SEND_OVERHEAD_BYTES
    const added = held - this.replaceableHeld()
    if (added > 0 && !admit(this, added)) return Promise.reject<void>(this.refuseSend(added))
    if (!this.peer) return this.offlineBuffer.pushLossy(tag, data, bytes)
    this.lossySends ??= new LossyDelivery(this.lossyHost())
    this.lossySends.send({ data, held })
  }

  /** What its next lossy send takes the place of: the offline buffer's lossy last entry while no page is attached, the
   *  newest waiting for the page while one is. */
  private replaceableHeld(): number {
    if (!this.peer) return this.offlineBuffer.replaceableHeld
    const waiting = this.lossySends?.held
    return waiting ? waiting.held : 0
  }

  /** Its lossy sends wait only while its page is attached: the wire that goes hands the newest first. */
  private lossyHost(): LossyHost<LossySend> {
    return {
      peer: () => this.peer,
      replay: () => this._replayBuffer,
      hand: ({ data }) => this.handLossy(this.peer!, data),
      hold: (send) => {
        // The caller may write into its bytes once `sendBinary` returned.
        if (typeof send.data !== 'string') send.data = new Uint8Array(send.data)
        this._holdOffline(send.held)
      },
      letGo: ({ held }) => this._letGoOffline(held),
      // Those still in the wire's queue never left the server, so the credit they counted comes back.
      dropUnhanded: () => {
        const { index, sender } = this.peer!
        const { units, bytes } = this._replayBuffer!.dropNewestPublishes(sender.queue.waitingPublishes(index))
        if (units > 0) sender.queue.dropWaitingPublishes(index, units)
        this.sendCredit.refund(bytes, units)
      },
      outOfCredit: () => this.sendCredit.isOutOfCredit(),
    }
  }

  /** Sends a lossy send's `data` to `peer`, where a newer one may take its place while it waits on the wire. It counts
   *  against the page's credit, and nothing waits on that. */
  private handLossy(peer: IndexedPeer, data: EntryData): void {
    this.sendCredit.count(
      typeof data === 'string' ? peer.sendOwn(encode.text, data) : peer.sendOwn(encode.binary, data),
    )
  }

  /** Why a text send can't go, where it isn't admitted. A UTF-16 unit takes at most 3 UTF-8 bytes, so text that fits
   *  even at that is admitted unmeasured; other text is decided at its real size. */
  private textRefusal(text: string): ChannelOverflowError | null {
    if (fits(this, text.length * 3 + HEADER + HELD_SEND_OVERHEAD_BYTES)) return null
    return this.refusal(utf8ByteLength(text))
  }

  /** Why a send of `bytes` can't go, where it isn't admitted. It holds nothing and takes no seq, so the page sees no gap
   *  on the wire, and later sends that fit go through. */
  private refusal(bytes: number): ChannelOverflowError | null {
    const held = bytes + HEADER + HELD_SEND_OVERHEAD_BYTES
    return admit(this, held) ? null : this.refuseSend(held)
  }

  /** A send that would hold `bytes` wasn't admitted. */
  private refuseSend(bytes: number): ChannelOverflowError {
    if (!this.refusalWarned) {
      this.refusalWarned = true
      const limit =
        this.heldBytes + bytes > maxPendingBytesOf()
          ? `it would hold more than ${maxPendingBytesOf()} bytes, the most for one channel`
          : `this server's channels hold nearly \`config.channel.maxMemory\`, ${getChannelConfig().maxMemory} bytes`
      assertWarning(
        false,
        `A channel refused a \`send()\`: ${limit}, so that message isn't delivered and its promise rejects with \`ChannelOverflowError\`. Later sends that fit still go. Await each \`channel.send()\` or \`channel.sendBinary()\` before the next.`,
        { onlyOnce: false },
      )
    }
    return new ChannelOverflowError(
      'Send refused: the most for one channel, or config.channel.maxMemory, is held; await each send',
    )
  }

  /** Sends `data` as an ack request; it settles with the page's answer, and the close waits for it. */
  private sendAckRequest<D>(encodeFrame: SequencedEncoder<D>, data: D): Promise<ChannelAck<ServerToClient>> {
    return this.lifecycle.trackAck(
      new Promise<ChannelAck<ServerToClient>>((resolve, reject) => {
        this.peer!.sendSequenced(encodeFrame, data, (seq) => this.pendingAcks.set(seq, { resolve, reject }))
      }),
    )
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
    if (this.peer) this.peer.sendSequenced(encode.abort, serializedAbortValue)
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
        this.lossySends?.flush()
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
    // Off the ledger until now: a channel whose telefunction threw never gets here, and just goes.
    this.didRegister = true
    // Allocate the replay buffer up-front: registration is the moment the channel
    // becomes addressable on the wire, so a peer can attach immediately after this
    // returns.
    this._replayBuffer = new ServerReplay(this)
    this.heldBytes = 0
    // Each send was admitted by its channel alone; now they count with all this server's channels.
    this.offlineBuffer.admitAgain(
      (held) => admit(this, held),
      (held) => this.refuseSend(held),
    )
    if (this._phase === 'ended') return // the mux lets an ended channel go
    if (this.ttlTimer) clearTimeout(this.ttlTimer)
    this.ttlTimer = unrefTimer(
      setTimeout(() => {
        this.ttlTimer = null
        this.shutdown(
          new NetworkError('Channel timed out: no client connected within TTL after response was sent', true),
          { pageGone: true },
        )
      }, getChannelConfig().connectTtl),
    )
  }

  /** @internal Attaches the page's wire. An ended channel only sends what it produced while no page was attached;
   *  an open one applies the RECONCILE's broadcast subscriptions before `onOpen` fires. */
  _attachPeer(peer: IndexedPeer, state?: AttachState): void {
    if (this._phase === 'ended') {
      this.offlineBuffer.flush(peer)
      this.sendPendingAckRes(peer)
      this.sendPendingEnd(peer)
      return
    }
    if (this.ttlTimer) clearTimeout(this.ttlTimer)
    this.ttlTimer = null
    if (this.reconnectTimer) clearTimeout(this.reconnectTimer)
    this.reconnectTimer = null
    this.peer = peer
    this._replayBuffer?.moving()
    this.receiveCredit.attach(peer.sender.sendWait())
    this.offlineBuffer.flush(peer)
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
        void this.onPeerAckReqMessage(frame.text, frame.seq, frame.bytes)
        return
      case TAG.BINARY:
        this.onPeerBinaryMessage(frame.data, frame.bytes)
        return
      case TAG.BINARY_ACK_REQ:
        void this.onPeerBinaryAckReqMessage(frame.data, frame.seq, frame.bytes)
        return
      case TAG.ACK_RES:
        this.onPageHas(frame.ackedSeq)
        this.onPeerAckRes(frame.ackedSeq, frame.text, frame.status)
        return
      case TAG.PUBLISH:
      case TAG.PUBLISH_BINARY:
      case TAG.PUBLISH_TO:
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
        this.shutdown(replayLossError(), { pageGone: true })
        return
      case TAG.WINDOW:
        this.sendCredit.onPeerByteWindow(frame.bytes)
        this.onPageHas(seqThrough(frame.lastSeq, this._replayBuffer?.seq ?? 0))
        return
      case TAG.MSG_WINDOW:
        this.sendCredit.onPeerMessageWindow(frame.count)
        this.lossySends?.mayGo()
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
      this.holdWhile(bytes, () => {
        const data = parsePeerText(text) as ChannelData<ClientToServer>
        const validateData = this._validators.get('data')
        // Shield fail on a no-ack message: silent drop (validator auto-logs). The client doesn't
        // await a response, so there's no `ShieldValidationError` to surface — listeners simply
        // never see the bad value. Ack-bearing sends go through `answerAckReq` and *do*
        // reject the sender's promise via the `shield-error` wire status.
        if (validateData && validateData(data) !== true) return undefined
        return this.callListeners(this.listeners, data)
      })
    } finally {
      this.selfTime.recordSince(t0)
    }
  }

  protected onPeerAckReqMessage(text: string, seq: number, bytes: number): Promise<void> {
    // Parsed here rather than inside the async dispatch so a malformed payload throws in the recv
    // turn, where the violation still names the wire that sent it.
    const data = parsePeerText(text) as ChannelData<ClientToServer>
    const validate = this._validators.get('data')
    return this.answerHeld(seq, bytes, () => this.answerAckReq(this.listeners, data, seq, validate))
  }

  protected onPeerBinaryAckReqMessage(data: Uint8Array, seq: number, bytes: number): Promise<void> {
    return this.answerHeld(seq, bytes, () => this.answerAckReq(this.binaryListeners, data, seq))
  }

  protected onPeerBinaryMessage(data: Uint8Array, bytes: number): void {
    const t0 = performance.now()
    try {
      this.holdWhile(bytes, () => this.callListeners(this.binaryListeners, data))
    } finally {
      this.selfTime.recordSince(t0)
    }
  }

  /** Holds a frame of `bytes` for `deliver` and what it returns to wait for: consumed on every way out, as window
   *  refreshes are consumption-driven and a page's sends would stall. */
  private holdWhile(bytes: number, deliver: () => Promise<unknown> | undefined): void {
    this.received(bytes)
    let pending: Promise<unknown> | undefined
    try {
      pending = deliver()
    } finally {
      if (pending) void pending.finally(() => this.consumed(bytes))
      else this.consumed(bytes)
    }
  }

  /** Calls each of `listeners` with `value`, and returns what to wait for of those they return. A listener that throws
   *  `Abort` stops the rest. */
  private callListeners<T>(listeners: Listeners<(value: T) => unknown>, value: T): Promise<unknown> | undefined {
    const pending: Promise<unknown>[] = []
    const list = listeners.list()
    for (const [i, cb] of list.entries()) {
      try {
        const result = cb(forListener(value, list, i))
        if (isPromise(result)) {
          pending.push(result.catch((err: unknown) => this.lifecycle.handleCallbackError(err)))
        }
      } catch (err) {
        if (this.lifecycle.handleCallbackError(err)) break
      }
    }
    return pending.length > 0 ? Promise.all(pending) : undefined
  }

  /** Data within the credit granted arrived: nobody can refuse it, so it's charged, and it lowers what's granted next. */
  private received(bytes: number): void {
    hold(this, bytes)
    this.receiveCredit.onReceived(bytes)
  }

  private consumed(bytes: number): void {
    letGo(this, bytes)
    this.receiveCredit.onConsumed(bytes)
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
    // The page sends CLOSE_ACK when the close request arrives, so it has everything sent before it, or takes no more of it.
    this.onPageHas(this.closeRequestSeq)
    this.lifecycle.closeAckReceived()
  }

  /** @internal The page began moving to another wire: an upgrade's PREPARE. */
  _onPageMoving(): void {
    this._replayBuffer?.moving()
  }

  /** @internal The wire of `peer` went away. A channel a later RECONCILE already moved to another wire stays there. */
  _onPeerDisconnect(peer: IndexedPeer, reconnectTimeout: number): void {
    if (this._phase === 'ended' || this.peer?.sender !== peer.sender) return
    // Into the replay, which the reconnect sends: the wire that went drops it.
    this.lossySends?.flush()
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
  protected onPageHas(lastSeq: number): void {
    if (lastSeq > this.pageLastSeq) this.pageLastSeq = lastSeq
    this._replayBuffer?.acknowledge(lastSeq)
    this.lossySends?.mayGo()
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

  /** @internal `BufferedSender`. */
  _holdOffline(bytes: number): void {
    if (this.didRegister) hold(this, bytes)
    else this.heldBytes += bytes
  }

  /** @internal `BufferedSender`. */
  _letGoOffline(bytes: number): void {
    if (this.didRegister) letGo(this, bytes)
    else this.heldBytes -= bytes
  }

  /** @internal `BufferedSender`: sends `peer` a frame buffered while no page was attached, as it would have gone then.
   *  An ack request already rejected at shutdown has no callback left to answer. */
  _sendBuffered(peer: IndexedPeer, { tag, data, callback, lossy }: Entry): void {
    if (lossy) return this.handLossy(peer, data)
    switch (tag) {
      case TAG.TEXT:
        assert(typeof data === 'string' && callback)
        this.settleBuffered(peer.sendSequenced(encode.text, data), callback)
        break
      case TAG.TEXT_ACK_REQ:
        assert(typeof data === 'string')
        peer.sendSequenced(encode.textAckReq, data, (seq) => {
          if (callback) this.pendingAcks.set(seq, callback)
        })
        break
      case TAG.BINARY:
        assert(data instanceof Uint8Array && callback)
        this.settleBuffered(peer.sendSequenced(encode.binary, data), callback)
        break
      case TAG.BINARY_ACK_REQ:
        assert(data instanceof Uint8Array)
        peer.sendSequenced(encode.binaryAckReq, data, (seq) => {
          if (callback) this.pendingAcks.set(seq, callback)
        })
        break
    }
  }

  private sendPendingAckRes(peer: IndexedPeer): void {
    const answers = this.pendingAckRes
    this.pendingAckRes = []
    for (const ack of answers) {
      letGo(this, heldAnswerBytes(ack.result))
      peer.sendAckRes(ack.ackedSeq, ack.result, ack.status)
    }
  }

  private sendPendingEnd(peer: IndexedPeer): void {
    if (this.pendingEnd === null) return
    const { closeAck, closeRequest, abort, error } = this.pendingEnd
    if (closeAck) peer.sendCloseAck()
    if (closeRequest) this.closeRequestSeq = peer.sendCloseRequest(this.lifecycle.timeLeft())
    if (abort !== null) peer.sendSequenced(encode.abort, abort)
    if (error !== null) peer.sendSequenced(encode.error, error)
    this.pendingEnd = null
  }

  /** Ends the channel on both sides with an ERROR of `reason`; a page not attached receives it on its next attach. */
  protected endWithError(reason: ErrorReason, err: Error): void {
    if (this._phase === 'ended') return
    this.offlineBuffer.clear(err)
    // First, so the ERROR, held without asking, takes their room.
    this.letAnsweringGo()
    if (this.peer) this.peer.sendSequenced(encode.error, reason)
    else this.ensurePendingEnd().error = reason
    this.shutdown(err)
  }

  /** Send an ack response, buffering it if the peer is currently disconnected. It takes its request's place. One that
   *  isn't admitted becomes a refusal: the page's `send` rejects with `ChannelOverflowError` and the channel stays open.
   *  Its request held room for it, so it needs no more; only a request refused on arrival asks, and where even its
   *  refusal doesn't fit, the channel ends. Past the channel's end the page's send settles with that, so it isn't sent. */
  protected sendAckRes(ackedSeq: number, result: string, status: AckResultStatus = ACK_STATUS.OK): void {
    if (this._phase === 'ended') return
    const room = this.answering.get(ackedSeq)
    if (room !== undefined) {
      this.answering.delete(ackedSeq)
      letGo(this, room)
    }
    if (!admit(this, heldAnswerBytes(result))) {
      if (room === undefined && !admit(this, REFUSAL_BYTES)) {
        return this.endWithError(ERROR_REASON.OVERFLOW, new ChannelOverflowError(TOO_MANY_ANSWERS_TEXT))
      }
      result = ''
      status = ACK_STATUS.OVERFLOW
    }
    if (this.peer) {
      this.peer.sendAckRes(ackedSeq, result, status)
      if (ackedSeq === this._lastClientSeq) this.receiveCredit.answered()
    } else {
      hold(this, heldAnswerBytes(result))
      this.pendingAckRes.push({ ackedSeq, result, status })
    }
  }

  /** Answers the ack request `seq` of `bytes` with `answer`, which the channel's close waits for. The request is held
   *  until its answer takes its place, and at least as much as its refusal. One that isn't admitted is refused, and
   *  `answer` doesn't run. */
  protected async answerHeld(seq: number, bytes: number, answer: () => Promise<void>): Promise<void> {
    const held = Math.max(bytes, REFUSAL_BYTES)
    if (!admit(this, held)) return this.sendAckRes(seq, '', ACK_STATUS.OVERFLOW)
    hold(this, held)
    this.answering.set(seq, held)
    try {
      await this.lifecycle.trackAck(answer())
    } finally {
      // Where no answer took its place, nor the channel's end.
      if (this.answering.delete(seq)) letGo(this, held)
    }
  }

  /** Answers ack request `seq` with an error where its answer threw `err`; an `Abort` aborts the channel instead. */
  protected answerFailed(seq: number, err: unknown): void {
    if (this.lifecycle.handleCallbackError(err)) return
    this.sendAckRes(seq, `${STATUS_BODY_INTERNAL_SERVER_ERROR} — see server logs`, ACK_STATUS.ERROR)
  }

  private async answerAckReq<T>(
    listeners: Listeners<(data: T) => unknown>,
    data: T,
    seq: number,
    validate?: ShieldValidator,
  ): Promise<void> {
    if (listeners.size === 0) return this.sendAckRes(seq, 'No listener registered for ack request', ACK_STATUS.ERROR)
    const verdict = validate ? validate(data) : true
    // `shield-error` status lets the client reject its `send()` promise with a branded
    // ShieldValidationError — same identity every other shield-fail surface produces.
    if (verdict !== true) return this.sendAckRes(seq, verdict, ACK_STATUS.SHIELD_ERROR)
    let answer: string | null
    try {
      answer = await listenerAnswer(listeners.list(), data)
    } catch (err) {
      return this.answerFailed(seq, err)
    }
    if (answer === null) {
      return this.sendAckRes(seq, 'Only one channel listener may return an acknowledgement value', ACK_STATUS.ERROR)
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
    this.lossySends?.dispose()
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
      this.letAnsweringGo()
    })
  }

  /** Nothing answers its requests once it ends, so what they held goes. */
  private letAnsweringGo(): void {
    for (const held of this.answering.values()) letGo(this, held)
    this.answering.clear()
  }

  /** @internal Drops what the channel holds for its page: the replay buffer and all it buffered while detached. */
  _release(): void {
    this._replayBuffer?.dispose()
    this._replayBuffer = null
    this.dropPending(
      this.offlineBuffer.size === 0 ? undefined : (this.lifecycle.closeError ?? new ChannelClosedError()),
    )
    if (this.didRegister) releaseHolder(this)
  }

  /** Drops all buffered while no page was attached; buffered sends reject with `err` when it is given. */
  private dropPending(err?: Error): void {
    this.offlineBuffer.clear(err)
    this.dropPendingAnswers()
    this.pendingEnd = null
  }

  /** Drops the answers held for a page not attached, whose sends then settle as the channel ends. */
  protected dropPendingAnswers(): void {
    for (const { result } of this.pendingAckRes) letGo(this, heldAnswerBytes(result))
    this.pendingAckRes = []
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
