export { Channel, ServerChannel }

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
  Unsubscribe,
} from '../channel.js'
import {
  LOSSY_WITH_ACK_TEXT,
  neverOpened,
  serializeBoundedAbortReason,
  stringifyValue,
  unsubscribeOf,
  type AbortReason,
} from '../channel.js'
import type { TELEFUNC_SHIELDS } from '../../../node/shared/transformer/generateShield/shieldKey.js'

import type { IndexedPeer } from './IndexedPeer.js'
import type { ChannelMux } from './ChannelMux.js'
import { parse } from '@brillout/json-serializer/parse'
import { utf8ByteLength } from '../../../utils/utf8ByteLength.js'
import {
  admitSend,
  admit,
  fits,
  heldAnswerBytes,
  heldSendBytes,
  hold,
  holdsAtMost,
  holdsEnough,
  letGo,
  releaseHolder,
  RoomWait,
  tryHold,
  type MemoryHolder,
} from './channelMemory.js'
import { hasProp } from '../../../utils/hasProp.js'
import { unrefTimer } from '../../../utils/unrefTimer.js'
import type { ShieldValidator, ShieldValidators } from '../../../node/server/shield.js'
import { createAbortError, type AbortError } from '../../../shared/Abort.js'
import { ShieldValidationError } from '../../../shared/ShieldValidationError.js'
import { handleCallbackBug } from '../../../node/server/runTelefunc/validateTelefunctionError.js'
import { ChannelClosedError, ChannelOverflowError } from '../channelErrors.js'
import { END_CAUSE, type RefusalCause } from '../endCause.js'
import { NetworkError } from '../../../shared/NetworkError.js'
import { channelMessages } from '../channelMessages.js'
import { forListener, listenerAnswer } from '../listenerAnswer.js'
import { isPromise } from '../../../utils/isPromise.js'
import { TIMER_DELAY_MAX_MS, UPGRADE_MAX_ID_BYTES } from '../../constants.js'
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
import { getChannelConfig, reconnectWindow } from '../../../node/server/serverConfig.js'
import type { EndingFrame } from '../ChannelEnding.js'
import { assert, assertUsage, assertWarning } from '../../../utils/assert.js'
import { Listeners } from '../../../utils/Listeners.js'
import { ACK_STATUS, encode, isCloseOutcomeFrame, isSequencedFrame, tellsLastRead } from '../../wire/frame.js'
import { TAG, WIRE_ERROR_CODE, countsCredit, isChannelCtrlTag, type WireErrorCode } from '../../wire/tags.js'
import { isChannelId } from '../../wire/payloads.js'
import { seqNear, seqThrough } from '../../wire/seq.js'
import { ProtocolViolationError, assertProtocol } from '../../wire/protocolViolation.js'
import type {
  AckResultStatus,
  ChannelCtrlFrame,
  ChannelDataFrame,
  ChannelFrame,
  SequencedEncoder,
} from '../../wire/frame.js'
import type { AttachState, Want } from '../../wire/payloads.js'

/** Peer-authored JSON: a parse failure is the peer's, so it surfaces as a protocol violation. */
function parsePeerText(text: string): unknown {
  try {
    return parse(text)
  } catch {
    throw new ProtocolViolationError('peer payload is not parsable')
  }
}

/** What a refusal holds: every ack request being answered holds at least this, so its refusal can take its place. */
const REFUSAL_BYTES = heldAnswerBytes('')
const TOO_MANY_ANSWERS_TEXT =
  'Channel closed: more `{ ack: true }` sends and `publish()` calls wait for their answers than the server can hold even refusals for, see config.channel.maxMemory'
const UNHELD_MESSAGE_TEXT =
  "Channel closed: its client sent more than the most for one channel, or config.channel.maxMemory, while `listen()` callbacks or a stream's reader work on what it sent; on the client, await each send, or send smaller messages"

/** A `sendAck` awaiting the peer's ACK_RES. */
type PendingAck<T> = { resolve: (result: ChannelAck<T>) => void; reject: (err: Error) => void }
/** A `send(data, { lossy: true })` to an attached page, text or a copy of its bytes once held; `held` is what it holds
 *  while it waits. */
type LossySend = { data: EntryData; held: number }
/** A lossy send, and what it holds. */
type LossyBehind = { tag: typeof TAG.TEXT | typeof TAG.BINARY; data: EntryData; bytes: number; held: number }
/** A send that waits for room, with the lossy send behind it, and how its promise settles if the channel ends first. */
type WaitingSend = { wait: RoomWait<LossyBehind>; reject: (err: Error) => void }

type ChannelOptions = { ack?: boolean; id?: string }

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

  /** @internal Open; closing once it takes no more sends (`close`, `abort` or the page's CLOSE began it); ended.
   *  `ChannelEnding` owns it. */
  get _phase(): 'open' | 'closing' | 'ended' {
    if (this.lifecycle.ended) return 'ended'
    return this.lifecycle.stopped ? 'closing' : 'open'
  }
  private didRegister = false
  protected peer: IndexedPeer | null = null
  /** The request that opened the wire its page is attached on, while it is. */
  protected wireRequest: Request | undefined = undefined
  private readonly listeners = new Listeners<ChannelListener<ClientToServer>>()
  private readonly binaryListeners = new Listeners<ChannelBinaryListener>()
  protected offlineBuffer = new ServerChannelBuffer<ChannelAck<ServerToClient>>(this)
  /** What it holds for its page. */
  heldBytes = 0
  released = false
  protected pendingAcks = new Map<number, PendingAck<ServerToClient>>()
  protected readonly lifecycle = new ChannelLifecycle({
    owner: this,
    side: 'server',
    reconnectWindowMs: reconnectWindow,
    reportError: handleCallbackBug,
  })
  /** It ends because its page left or never attached: nothing it holds for the page can reach it. */
  protected endsPageGone = false
  /** Ends it where its page doesn't come: the connect window, from its registration to the first attach, then the grace
   *  period after each loss of its page's wire. */
  private pageWait: ReturnType<typeof setTimeout> | null = null
  protected selfTime = new SelfTime()
  /** Paces fire-and-forget TEXT and BINARY sends, lossy ones too, to what the page grants; see `constants.ts`. */
  protected sendCredit: SendCredit
  /** A send nobody awaits can't show its rejection, so the first refusal warns. */
  private refusalWarned = false
  protected receiveCredit: ReceiveCredit
  private responseAbort: ((abortValue?: unknown) => void) | null = null
  private pendingAckRes: Array<{ ackedSeq: number; result: string; status: AckResultStatus }> = []
  /** What each ack request being answered holds, by its seq: its answer takes that place. */
  private readonly answering = new Map<number, number>()
  /** The mux that registered this channel; when the channel ends, the mux keeps it or forgets it. */
  private mux: ChannelMux | null = null
  /** The frames that end it, made while no page was attached, in order: the next attach sends them. */
  private pendingEnd: Array<(peer: IndexedPeer) => void> = []
  /** The page closed its end, so it takes nothing more from this channel. */
  private pageClosed = false
  /** Its page attaches lacking an end its replay had no room to keep. */
  private endLost = false
  /** Its lossy sends while its page is attached, from the first one on. */
  private lossySends: LossyDelivery<LossySend> | null = null
  /** Its send that waits for room on the ledger, how its promise settles if the channel ends first, and the newest lossy
   *  send made since, which goes right after it. */
  private roomWait: WaitingSend | null = null

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

  constructor({ ack = false, id }: ChannelOptions = {}) {
    // The control-frame cap counts each channel's id at this length (`WIRE_MAX_CTRL_FRAME_BYTES`).
    assertUsage(
      id === undefined || isChannelId(id),
      `A channel's id is at most ${UPGRADE_MAX_ID_BYTES} bytes, as JSON writes it`,
    )
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

  get closed(): Promise<ChannelCloseResult> {
    return this.lifecycle.closed
  }

  messages(options?: { signal?: AbortSignal }) {
    return channelMessages<ChannelData<ClientToServer>>(this, options)
  }

  get isClosed(): boolean {
    return this._phase !== 'open'
  }

  /** @internal Whether a request registered it, now or earlier: its page may hold it. */
  get _wasRegistered(): boolean {
    return this.didRegister
  }

  send(data: ChannelData<ServerToClient>): Promise<void>
  send(data: ChannelData<ServerToClient>, opts: { ack: true }): Promise<ChannelAck<ServerToClient>>
  send(data: ChannelData<ServerToClient>, opts: { ack: false }): Promise<void>
  send(data: ChannelData<ServerToClient>, opts: { lossy: true; ack?: false }): Promise<void>
  send(data: ChannelData<ServerToClient>, opts?: { ack?: boolean; lossy?: boolean }): Promise<unknown> {
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
    opts?: { ack?: boolean; lossy?: boolean },
  ): void | Promise<unknown> {
    if (this._phase !== 'open') throw this.lifecycle.sendRefusal()
    if (opts?.lossy) assertUsage(opts.ack !== true, LOSSY_WITH_ACK_TEXT)
    const needsAck = opts?.ack !== false && (opts?.ack === true || this.ack === true)
    const serialized = stringifyValue(data, 'send')
    if (opts?.lossy) return this.sendLossy(TAG.TEXT, serialized, utf8ByteLength(serialized))
    // A UTF-16 unit takes at most 3 UTF-8 bytes, so text that fits even at that is admitted unmeasured.
    if (this.roomWait === null && fits(this, heldSendBytes(serialized.length * 3)))
      return this.sendText(serialized, needsAck)
    return this.admitted(heldSendBytes(utf8ByteLength(serialized)), () => this.sendText(serialized, needsAck))
  }

  private sendText(serialized: string, needsAck: boolean): void | Promise<unknown> {
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
    if (needsAck) return this.sendAckRequest(this.peer, encode.textAckReq, serialized)
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
    if (this._phase !== 'open') throw this.lifecycle.sendRefusal()
    if (opts?.lossy) assertUsage(opts.ack !== true, LOSSY_WITH_ACK_TEXT)
    const needsAck = opts?.ack === true
    if (opts?.lossy) return this.sendLossy(TAG.BINARY, data, data.byteLength)
    if (this.roomWait === null && fits(this, heldSendBytes(data.byteLength))) return this.sendBytes(data, needsAck)
    let bytes = data
    // The caller may write into its bytes once `sendBinary` returned, so a send that waits keeps a copy.
    const keep = () => void (bytes = new Uint8Array(data))
    return this.admitted(heldSendBytes(data.byteLength), () => this.sendBytes(bytes, needsAck), keep)
  }

  private sendBytes(data: Uint8Array, needsAck: boolean): void | Promise<unknown> | Promise<void> {
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
    if (needsAck) return this.sendAckRequest(this.peer, encode.binaryAckReq, data)
    // Cooperative credit model; see `sendData`.
    return this.sendCredit.decrement(this.peer.sendSequenced(encode.binary, data))
  }

  /** A send of which only the newest matters, admitted for what it adds once it takes the place of those waiting, so
   *  those stay where this one is refused. With no page attached it waits as the offline buffer's last entry, which
   *  the next one replaces; attached, it goes, or waits as the newest while the page is behind or out of credit. */
  private sendLossy(tag: typeof TAG.TEXT | typeof TAG.BINARY, data: EntryData, bytes: number): void | Promise<void> {
    const held = heldSendBytes(bytes)
    if (this.roomWait !== null) return this.sendLossyBehind(this.roomWait, { tag, data, bytes, held })
    const added = held - this.replaceableHeld()
    if (added > 0 && !admit(this, added)) return Promise.reject<void>(this.refuseSend(added))
    return this.makeLossy({ tag, data, bytes, held })
  }

  /** Makes a lossy send admitted, or held from what it reserved. */
  private makeLossy({ tag, data, bytes, held }: LossyBehind): void | Promise<void> {
    if (!this.peer) return this.offlineBuffer.pushLossy(tag, data, bytes)
    // Detached bytes read as empty: copying them throws before they take the place of the send that waits.
    if (typeof data !== 'string' && data.byteLength === 0) new Uint8Array(data)
    this.lossyDelivery().send({ data, held })
  }

  /** Behind a send of its channel that waits for room, a lossy one waits too, as the newest, reserved in that one's turn:
   *  it goes right after it, so nothing passes it. */
  private sendLossyBehind({ wait }: WaitingSend, lossy: LossyBehind): void | Promise<void> {
    // The two go together, so a lossy one they can't both fit in its channel is refused, leaving the other its turn.
    if (wait.bytes + lossy.held > holdsAtMost()) return Promise.reject<void>(this.refuseSend(lossy.held, false))
    // The caller may write into its bytes once `send` returned, as for a send that waits.
    const keep = () => ({ ...lossy, data: typeof lossy.data === 'string' ? lossy.data : new Uint8Array(lossy.data) })
    if (!wait.putBehind(lossy.held, keep))
      return Promise.reject<void>(this.refuseSend(lossy.held - wait.behindBytes, false))
  }

  /** What its next lossy send takes the place of, by what that holds: with no page attached, the offline buffer's lossy
   *  last entry; attached to a page that is behind or out of credit, those that wait and its sends still in the
   *  wire's queue since its last frame. Nothing for a page that keeps up. */
  private replaceableHeld(): number {
    if (!this.peer) return this.offlineBuffer.replaceableHeld
    // Made before the first attached lossy send is measured: the attach may have handed lossy sends that still wait.
    const sends = this.lossyDelivery()
    if (!sends.replaces()) return 0
    return sends.waitingBytes + this.peer.replaceableHeld()
  }

  private lossyDelivery(): LossyDelivery<LossySend> {
    return (this.lossySends ??= new LossyDelivery(this.lossyHost()))
  }

  /** Its lossy sends wait only while its page is attached: the wire that goes hands the newest first. */
  private lossyHost(): LossyHost<LossySend> {
    return {
      peer: () => this.peer,
      replay: () => this._replayBuffer,
      hand: ({ data }) => {
        assert(this.peer)
        this.handLossy(this.peer, data)
      },
      hold: (send) => {
        // The caller may write into its bytes once `sendBinary` returned.
        if (typeof send.data !== 'string') send.data = new Uint8Array(send.data)
        this._holdOffline(send.held)
      },
      letGo: ({ held }) => this._letGoOffline(held),
      dropReplaceable: () => this.dropReplaceable(),
      outOfCredit: () => this.sendCredit.isOutOfCredit(),
      bytes: ({ held }) => held,
    }
  }

  /** Drops its lossy sends that no wire has taken: they never left the server. While its page is attached, those are
   *  the newest that waits and the ones still in the wire's queue, whose credit comes back; while it is away, those the
   *  offline buffer holds. The page's own close and an end for lack of memory drop them, where a graceful `close()`
   *  hands the newest first; reliable sends stay, and reach the page ahead of its end. */
  protected dropUnhanded(): void {
    if (!this.peer) return this.offlineBuffer.dropLossy()
    this.lossySends?.dispose()
    const { units, bytes } = this.peer.dropUnhanded()
    this.sendCredit.refund(bytes, units)
  }

  /** Its abort is owed to its page, which gets it behind what was sent before it. What it drops of that: its lossy
   *  messages of which the socket has no byte (`WireQueue.dropUngiven`). While its page is away, every one it holds,
   *  also those its last wire had: a reconnect would deliver them anew. Reliable ones stay. */
  protected onAbortOwed(): void {
    this.lossySends?.dispose()
    if (this.peer) {
      this.peer.dropUngivenLossy()
      return
    }
    this.offlineBuffer.dropLossy()
    this._replayBuffer?.dropNewestPublishes(Number.POSITIVE_INFINITY)
  }

  /** A newer lossy send takes the place of its lossy sends still in the wire's queue, back to its last frame: a plain send
   *  keeps the lossy sends before it. The credit they counted comes back. */
  private dropReplaceable(): void {
    assert(this.peer)
    const { units, bytes } = this.peer.dropReplaceable()
    this.sendCredit.refund(bytes, units)
  }

  /** Sends a lossy send's `data` to `peer`, where a newer one may take its place while it waits on the wire. It counts
   *  against the page's credit, and nothing waits on that. */
  private handLossy(peer: IndexedPeer, data: EntryData): void {
    this.sendCredit.count(
      typeof data === 'string' ? peer.sendOwn(encode.text, data) : peer.sendOwn(encode.binary, data),
    )
  }

  /** Makes a send that holds `held` with `go` once the ledger admits it: at once, or when room frees, first in line
   *  first, `keep` running as it starts to wait. A send made while one of its channel's waits, and one that can never
   *  fit, is refused: it holds nothing and takes no seq, so the page sees no gap on the wire, and later sends that fit
   *  go through. Before its telefunction returns it, a send waits out of line, which it joins then. The returned promise
   *  settles once, with `go`'s outcome, or with the channel's end. */
  private admitted(held: number, go: () => void | Promise<unknown>, keep?: () => void): void | Promise<unknown> {
    if (this.roomWait !== null) return Promise.reject<void>(this.refuseSend(held))
    const verdict = admitSend(this, held)
    if (verdict === 'now') return go()
    if (verdict === 'refused') return Promise.reject<void>(this.refuseSend(held))
    keep?.()
    return new Promise<unknown>((resolve, reject) => {
      const made = () => {
        this.roomWait = null
        void settleMade(go(), resolve, reject)
        const lossy = wait.takeBehind()
        if (lossy !== null) void this.makeLossy(lossy)
      }
      const wait = new RoomWait<LossyBehind>({ holder: this, bytes: held, go: made, inLine: this.didRegister })
      this.roomWait = { wait, reject }
    })
  }

  /** Its send that waits for room goes now, ahead of its end, holding what it reserved, as the end holds what went
   *  before it; the lossy send behind it goes too, unless the end drops lossy sends. */
  private sendWaitingNow({ dropLossy }: { dropLossy: boolean }): void {
    const wait = this.roomWait?.wait
    if (wait === undefined) return
    if (dropLossy) wait.takeBehind()
    wait.goNow()
  }

  /** A send that would hold `bytes` wasn't admitted. */
  private refuseSend(bytes: number, othersWait = this.roomWait !== null): ChannelOverflowError {
    if (!this.refusalWarned) {
      this.refusalWarned = true
      const limit = othersWait
        ? 'another `send()` of this channel already waits for room'
        : bytes > holdsAtMost()
          ? `it would hold more than ${holdsAtMost()} bytes, the most for one channel`
          : `what this server's channels hold and the sends waiting for room fill \`config.channel.maxMemory\`, ${getChannelConfig().maxMemory} bytes`
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

  /** Sends `data` to `peer` as an ack request; it settles with the page's answer, and the close waits for it. */
  private sendAckRequest<D>(
    peer: IndexedPeer,
    encodeFrame: SequencedEncoder<D>,
    data: D,
  ): Promise<ChannelAck<ServerToClient>> {
    return this.lifecycle.trackAck(
      new Promise<ChannelAck<ServerToClient>>((resolve, reject) => {
        peer.sendSequenced(encodeFrame, data, (seq) => this.pendingAcks.set(seq, { resolve, reject }))
      }),
    )
  }

  /** In effect at once: the page sends whether the server listens or not. One added once the channel ended never is. */
  listen(callback: ChannelListener<ClientToServer>, options?: { signal?: AbortSignal }): Unsubscribe {
    if (this._phase === 'ended') return neverOpened(this.endError())
    return unsubscribeOf(this.listeners.add(callback, options))
  }

  listenBinary(callback: ChannelBinaryListener, options?: { signal?: AbortSignal }): Unsubscribe {
    if (this._phase === 'ended') return neverOpened(this.endError())
    return unsubscribeOf(this.binaryListeners.add(callback, options))
  }

  /** What it ended with, or `ChannelClosedError` for a graceful close (used to reject `opened` and waiting sends). */
  protected endError(): Error {
    return this.lifecycle.closeError ?? new ChannelClosedError()
  }

  onClose(callback: ChannelCloseCallback, options?: { signal?: AbortSignal }): () => void {
    return this.lifecycle.onClose(callback, options)
  }

  onOpen(callback: () => void, options?: { signal?: AbortSignal }): () => void {
    return this.lifecycle.onOpen(callback, options)
  }

  _setResponseAbort(abortResponse: (abortValue?: unknown) => void): void {
    this.responseAbort = abortResponse
  }

  abort(): void
  abort(abortValue: unknown, message?: string): void
  abort(abortValue?: unknown, message?: string): void {
    if (this._phase === 'ended') return
    const reason = serializeHeldAbortReason(abortValue)
    this.abortWith(reason, message)
    if (reason.refusal) throw reason.refusal.error
  }

  /** Ends the channel with `reason`, sent to its page, unless serializing it, which runs the app's getters and
   *  `toJSON`, ended it already. */
  private abortWith(reason: AbortReason, message?: string): void {
    this.lifecycle.endWith(createAbortError(reason.value, message), () => {
      // Owed before its close callbacks run: what it drops is gone by then, so they find its memory free.
      this.onAbortOwed()
      this.sendWaitingNow({ dropLossy: true })
      this.sendEnd((peer) => peer.sendSequenced(encode.abort, reason.serialized))
    })
  }

  close(opts?: ChannelCloseOptions): Promise<ChannelCloseResult> {
    const timeout = normalizeCloseTimeout(opts?.timeout)
    // A channel no telefunction returned has no page to wait for.
    return this.lifecycle.close(timeout, { peerAbsent: !this.didRegister })
  }

  /** @internal */
  _sendEnding(frame: EndingFrame): void {
    // The page reads what was sent, then the end.
    this.sendWaitingNow({ dropLossy: false })
    switch (frame.kind) {
      case 'close':
        return this.sendEnd((peer) => peer.sendCloseRequest(frame.timeoutMs))
      case 'closeAck':
        return this.sendEnd((peer) => peer.sendCloseAck(this._lastClientSeq))
      case 'closeDone':
        return this.sendEnd((peer) => peer.sendCloseDone(this._lastClientSeq))
      case 'timedOut':
        return this.sendEnd((peer) => peer.sendSequenced(encode.error, WIRE_ERROR_CODE.CLOSE_TIMED_OUT))
    }
  }

  /** @internal */
  _stopSending(): void {
    this.lossySends?.flush()
  }

  /** @internal Registers the channel with its mux and starts its connect timeout. */
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
    // Its sends counted on it alone until now; from here they count with all this server's channels.
    this.offlineBuffer.admitAgain(
      (held) => admit(this, held),
      (held) => this.refuseSend(held),
    )
    // Its send waiting for room joins the line behind them, or is refused where `maxMemory` has no room for it.
    const waiting = this.roomWait
    if (waiting !== null && !waiting.wait.joinLine()) {
      this.roomWait = null
      waiting.reject(this.refuseSend(waiting.wait.bytes))
    }
    if (this._phase === 'ended') return // the mux lets an ended channel go
    const { connectTtl } = getChannelConfig()
    this.pageWait = unrefTimer(
      setTimeout(() => {
        this.pageWait = null
        this.shutdown(
          new NetworkError('Channel timed out: no client connected within TTL after response was sent', true),
          { pageGone: true },
        )
      }, connectTtl),
    )
  }

  /** @internal Attaches the page's wire. An ended channel only sends what it produced while no page was attached;
   *  an open one applies the RECONCILE's broadcast want before `onOpen` fires. */
  _attachPeer(peer: IndexedPeer, state?: AttachState): void {
    const live = this._phase !== 'ended'
    if (live) {
      this.stopPageWait()
      this.peer = peer
      this._replayBuffer?.moving()
      this.receiveCredit.attach(peer.sender.sendWait())
      if (state?.want) this.onPeerWant(state.want)
    }
    // What the page gets on attach, in this order: the held sends, the offline buffer, the answers, the end.
    this.sendHeldForAttach(peer)
    this.offlineBuffer.flush(peer)
    this.sendPendingAckRes(peer)
    if (this.endLost) {
      // Lost to the page, which both ends say.
      this.endLost = false
      this.replaceEnd(WIRE_ERROR_CODE.END_NOT_KEPT, peer)
      this.shutdown(END_CAUSE.endNotKept.error())
    }
    this.sendPendingEnd(peer)
    if (live && this._phase !== 'closing') this.lifecycle.fireOpen()
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
    // An ended channel tracks the page's last seq and lets go of what its page confirms; it acts on nothing else.
    if (this._phase === 'ended') {
      if (tellsLastRead(frame)) this.onPageHasThrough(frame.lastSeq)
      return
    }
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
        assertProtocol(frame.timeoutMs <= TIMER_DELAY_MAX_MS, `CLOSE timeout ${frame.timeoutMs}`)
        this.lifecycle.peerClose(frame.timeoutMs)
        return
      case TAG.CLOSE_ACK:
        this.onPageHasThrough(frame.lastSeq)
        this.lifecycle.peerCloseAck()
        return
      case TAG.CLOSE_DONE:
        this.onPageHasThrough(frame.lastSeq)
        this.lifecycle.peerCloseDone()
        return
      case TAG.ABORT:
        this.shutdown(createAbortError(parsePeerText(frame.abortValue)), { pageGone: true })
        return
      case TAG.ERROR:
        assertProtocol(
          frame.reason === WIRE_ERROR_CODE.LOST || frame.reason === WIRE_ERROR_CODE.CLOSE_TIMED_OUT,
          `ERROR reason ${frame.reason} from a page`,
        )
        const cause = frame.reason === WIRE_ERROR_CODE.LOST ? END_CAUSE.lost : END_CAUSE.closeTimedOut
        this.shutdown(cause.error(), { pageGone: true })
        return
      case TAG.WINDOW:
        this.sendCredit.onPeerByteWindow(frame.bytes)
        this.onPageHasThrough(frame.lastSeq, frame.heldMs)
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
      case TAG.WANT:
        this.onPeerWant(frame.want)
        this._confirmWant()
    }
  }

  /** `ServerBroadcast` overrides it; a plain channel ignores wants. */
  protected onPeerWant(_want: Want): void {}

  /** What it held for its page's attach beyond its offline buffer goes ahead of that: `ServerBroadcast` overrides it. */
  protected sendHeldForAttach(_peer: IndexedPeer): void {}

  /** @internal Tells the page which of its wants is in effect, as the answer to its attach or to a WANT frame does:
   *  `ServerBroadcast` overrides it. */
  _confirmWant(): void {}

  /** @internal Applies a want a call of the page's carries, as one the wire brings, and returns what the call waits for
   *  before its telefunction runs: `ServerBroadcast` overrides it; a plain channel has no want. */
  _applyCallWant(_want: Want, _callAborted: AbortSignal, _waitAtMost: number): Promise<void> | undefined {
    return undefined
  }

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
    if (!this.received(bytes)) return
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
    for (const [i, { listener }] of list.entries()) {
      if (!listener) continue
      try {
        const result = listener(forListener(value, list, i))
        if (isPromise(result)) {
          pending.push(result.catch((err: unknown) => this.lifecycle.handleCallbackError(err)))
        }
      } catch (err) {
        if (this.lifecycle.handleCallbackError(err)) break
      }
    }
    return pending.length > 0 ? Promise.all(pending) : undefined
  }

  /** Charges a data frame that arrived, which lowers what's granted next; false where the channel ended instead. Credit
   *  only paces a page, and one that doesn't await its sends passes it: where the server can't hold what it sent, its
   *  channel ends. */
  private received(bytes: number): boolean {
    if (!tryHold(this, bytes)) {
      this.endForLackOfMemory(UNHELD_MESSAGE_TEXT)
      return false
    }
    this.receiveCredit.onReceived(bytes)
    return true
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
    this.wireRequest = undefined
    this.receiveCredit.detach()
    if (this.endUnconfirmed(reconnectTimeout)) return
    this._replayBuffer?.dateStall(performance.now())
    this.pageWait = unrefTimer(
      setTimeout(() => {
        this.pageWait = null
        this.loseConnection(new NetworkError('Channel timed out: client did not reconnect within grace period', true))
      }, reconnectTimeout),
    )
  }

  private stopPageWait(): void {
    if (this.pageWait) clearTimeout(this.pageWait)
    this.pageWait = null
  }

  /** Ends the channel, at a loss of its page's wire, if what it sent hasn't been confirmed for over `reconnectTimeout`
   *  of its page's reconnects: dated at a loss, and the page attached since, as a loss needs a page attached. What it
   *  held goes, and the page reads the end alone at its next attach. A page that stays away is the grace period's. */
  private endUnconfirmed(reconnectTimeout: number): boolean {
    const replay = this._replayBuffer
    const stall = replay?.stall
    if (!replay || !stall || !replay.unconfirmed || performance.now() - stall.since <= reconnectTimeout) return false
    replay.dispose()
    this.endWithError(WIRE_ERROR_CODE.UNCONFIRMED, END_CAUSE.unconfirmed.error(reconnectTimeout))
    return true
  }

  /** @internal The page reattached but never acknowledged this channel: its connection is lost for good. */
  _onPeerRecoveryFailure(): void {
    this.peer = null
    this.wireRequest = undefined
    this.loseConnection(new NetworkError('Channel not acknowledged by client after reconnect', true))
  }

  /** @internal The server ended the channel for `cause`: a frame of the channel's it can't take, or the end of its
   *  page's session. Told to its page now or at its next attach, as the page's refusal tells it. */
  _onServerEnd(cause: RefusalCause): void {
    const { code, error } = END_CAUSE[cause]
    this.endWithError(code, error())
  }

  /** @internal The page's wire ended for good, and the channel with it: with `ChannelPageLeftError` where the page left,
   *  a session cause's error where the server ended the page's session, which the wire's own end tells the page. */
  _onPeerGone(err: Error): void {
    if (this._phase === 'ended') return
    this.peer = null
    this.wireRequest = undefined
    this.shutdown(err, { pageGone: true })
  }

  /** Its page's connection is gone for good with `err`, which during a close leaves its outcome unknown. */
  private loseConnection(err: Error): void {
    if (this._phase === 'ended') return
    this.endsPageGone = true
    this.lifecycle.connectionLost(err)
  }

  /** @internal Called on each of the page's heartbeats; see `ReceiveCredit.acknowledge`. */
  _acknowledge(): void {
    this.receiveCredit.acknowledge()
  }

  /** @internal The page attaches on the wire `request` opened, having received this channel's frames through
   *  `lastSeq`. */
  _onPageAttach(lastSeq: number, request: Request | undefined): void {
    this.endLost = this._replayBuffer?.lacksEndAfter(lastSeq) ?? false
    this.onPageHas(lastSeq)
    this.wireRequest = request
  }

  /** @internal */
  _otherEnded(): void {
    this.pageClosed = true
  }

  /** @internal The page closed its end after receiving this channel's frames through `lastSeq`, the low 32 bits a PING
   *  carries. */
  _onPageClosed(lastSeq: number): void {
    this.pageClosed = true
    this.onPageHasThrough(lastSeq)
  }

  /** As `onPageHas`, from the low 32 bits of `lastSeq` the wire carries. */
  private onPageHasThrough(lastSeq: number, heldMs?: number): void {
    this.onPageHas(seqThrough(lastSeq, this._replayBuffer?.seq ?? 0), heldMs)
  }

  /** The page received this channel's frames through `lastSeq`, so the replay buffer lets them go. */
  protected onPageHas(lastSeq: number, heldMs?: number): void {
    this._replayBuffer?.acknowledge(lastSeq, heldMs)
    this.lossySends?.mayGo()
  }

  /** True once the page needs nothing more: it closed its end, or its replay holds nothing for it and nothing is
   *  pending. */
  private pageNeedsNothing(): boolean {
    if (this._replayBuffer === null) return false
    if (this.pageClosed) return true
    return this._replayBuffer.isEmpty && this.nothingPending()
  }

  /** Nothing waits for its page outside its replay: no send buffered while it was away, no answer, no end. */
  private nothingPending(): boolean {
    return this.offlineBuffer.size === 0 && this.pendingAckRes.length === 0 && this.pendingEnd.length === 0
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

  /** Sends the page a frame that ends the channel by `send`: now, or with no page attached, on its next attach. */
  private sendEnd(send: (peer: IndexedPeer) => void): void {
    if (this.peer) send(this.peer)
    else this.pendingEnd.push(send)
  }

  private sendPendingEnd(peer: IndexedPeer): void {
    for (const send of this.pendingEnd.splice(0)) send(peer)
  }

  /** Once it ended, an ERROR of `reason` takes the place of the end it held for its page: on `peer`, the wire its page
   *  reads, else on its next attach. */
  protected replaceEnd(reason: WireErrorCode, peer: IndexedPeer | null): void {
    this.pendingEnd = [(last) => last.sendSequenced(encode.error, reason)]
    if (peer) this.sendPendingEnd(peer)
  }

  /** Ends the channel on both sides with an ERROR of `reason`; a page not attached receives it on its next attach. */
  protected endWithError(reason: WireErrorCode, err: Error): void {
    if (this._phase === 'ended') return
    this.offlineBuffer.clear(err)
    this.sendEnd((peer) => peer.sendSequenced(encode.error, reason))
    this.shutdown(err)
  }

  /** Ends the channel for lack of memory, as a shed subscriber does. */
  private endForLackOfMemory(text: string): void {
    this.dropUnhanded()
    this.endWithError(WIRE_ERROR_CODE.OVERFLOW, new ChannelOverflowError(text))
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
      if (room === undefined && !admit(this, REFUSAL_BYTES)) return this.endForLackOfMemory(TOO_MANY_ANSWERS_TEXT)
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
    if (!tryHold(this, held)) return this.sendAckRes(seq, '', ACK_STATUS.OVERFLOW)
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

  /** @internal A callback threw `Abort`: aborts the response carrying the channel if there is one, else the channel.
   *  A reason JSON can't hold is reported, and the abort goes without it, to the response's other values too. */
  _abortFromCallback(err: AbortError): void {
    const reason = serializeHeldAbortReason(err.abortValue)
    if (this.responseAbort) this.responseAbort(reason.value)
    else this.abortWith(reason)
    if (reason.refusal) handleCallbackBug(reason.refusal.error)
  }

  /** `pageGone`: the page left or never attached, so nothing the channel holds for it can reach it. */
  protected shutdown(err?: Error, { pageGone = false } = {}): void {
    if (this._phase === 'ended') return
    this.endsPageGone = pageGone
    this.lifecycle.endWith(err)
  }

  /** @internal */
  _finalize(err: Error | undefined): void {
    const pageGone = this.endsPageGone
    // What of it still waits for the wire of a page that is gone, its sends too, would reach no one.
    if (pageGone && this.peer) this.peer.sender.dropWaitingFrom(this.peer.index, 'start')
    this.lossySends?.dispose()
    const pageAttached = this.peer !== null
    this.peer = null
    this.wireRequest = undefined
    this.receiveCredit.detach()
    this.stopPageWait()
    // Kept for a page that may still attach and lacks something, or for a response that has yet to carry it.
    const keep = !pageGone && !this.pageNeedsNothing()
    if (!keep) this._release()
    this.mux?.onChannelEnded(this, { keep, pageAttached })
    this.listeners.clear()
    this.binaryListeners.clear()
    this.sendCredit.shutdown(this.lifecycle.waitingSendOutcome)
    this.endRoomWait(err)
    // Building an error captures a stack trace, so one is built only while an ack is awaited or a frame is buffered.
    if (this.pendingAcks.size > 0 || this.offlineBuffer.size > 0) {
      const ackErr = err ?? new ChannelClosedError()
      for (const { reject } of this.pendingAcks.values()) reject(ackErr)
      // The buffered ack requests stay for the page, but their callbacks reject now.
      this.offlineBuffer.rejectAcks(ackErr)
    }
    this.pendingAcks.clear()
    // Nothing answers its requests once it ends, so what they held goes.
    for (const held of this.answering.values()) letGo(this, held)
    this.answering.clear()
  }

  /** Its send that waited for room is never made: an end that sends its own word made it first (`sendWaitingNow`), so
   *  this is an end it never reached the page ahead of, and it rejects with the channel's outcome. */
  private endRoomWait(err: Error | undefined): void {
    const waiting = this.roomWait
    if (waiting === null) return
    this.roomWait = null
    waiting.wait.cancel()
    waiting.reject(this.lifecycle.waitingSendOutcome ?? err ?? this.endError())
  }

  /** @internal `MemoryHolder`: ended with its page in the close, which confirmed reading what the server sent of it up to
   *  its CLOSE, it keeps only its close's own frames, no message, answer or abort, so letting go costs its page at most
   *  the close's outcome, where the network lost them. A page that never read the CLOSE learns from them alone that the
   *  channel ended. */
  letGoKept(): void {
    if (this._phase !== 'ended' || !this.keepsOnlyTheClose()) return
    assert(this.mux)
    this.mux.letGoEnded(this.id)
  }

  /** Its page lacks nothing of it but the outcome of a close it is in: a CLOSE still kept is one its page never
   *  confirmed. So a page that sent a CLOSE of its own, crossing the server's, keeps its channel kept until it confirms
   *  the server's too: stricter than its own CLOSE alone, and it loses nothing. */
  private keepsOnlyTheClose(): boolean {
    assert(this._replayBuffer)
    return this.nothingPending() && this._replayBuffer.holdsOnly(isCloseOutcomeFrame)
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
    this.dropPendingFrames()
  }

  /** Drops the answers and the frames that end it, held for a page not attached: the sends answered settle as the
   *  channel ends. */
  protected dropPendingFrames(): void {
    for (const { result } of this.pendingAckRes) letGo(this, heldAnswerBytes(result))
    this.pendingAckRes = []
    this.pendingEnd = []
  }
}

/** The abort is held until its page has it: a reason that alone is more than one channel may hold goes without it. */
function serializeHeldAbortReason(value: unknown): AbortReason {
  return serializeBoundedAbortReason(value, {
    max: holdsAtMost(),
    bytesOf: (serialized) => heldSendBytes(utf8ByteLength(serialized)),
    what: 'one channel may hold',
  })
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
  new <ClientToServer = unknown, ServerToClient = unknown>(
    opts?: ChannelOptions & {
      ack?: false
    },
  ): Channel<ClientToServer, ServerToClient, false>
  new <ClientToServer = unknown, ServerToClient = unknown>(
    opts: ChannelOptions & {
      ack: true
    },
  ): Channel<ClientToServer, ServerToClient, true>
  new <ClientToServer = unknown, ServerToClient = unknown>(
    opts?: ChannelOptions,
  ): Channel<ClientToServer, ServerToClient, false>
}

/** Settles a send that waited for room with the outcome of making it. */
async function settleMade(
  made: void | Promise<unknown>,
  resolve: (value: unknown) => void,
  reject: (err: unknown) => void,
): Promise<void> {
  try {
    resolve(await made)
  } catch (err) {
    reject(err)
  }
}
