export { ServerBroadcastEnd }
export type { BroadcastEndOwner }

import { ServerChannel } from './ServerChannel.js'
import { assert } from '../../../utils/assert.js'
import type { IndexedPeer } from './IndexedPeer.js'
import { fitsPublish, hold, letGo, maxPendingBytesOf, type MemoryHolder } from './channelMemory.js'
import type { PublishBody } from './PublishBody.js'
import { LossyDelivery } from './LossyDelivery.js'
import { EVERY_STREAM } from '../ownStream.js'
import { ChannelClosedError, ChannelOverflowError } from '../channelErrors.js'
import { isShieldValidationError } from '../../../shared/ShieldValidationError.js'
import { ACK_STATUS, ERROR_REASON, type AckResultStatus, type ErrorReason } from '../../wire/frame.js'
import { TAG } from '../../wire/tags.js'
import type { BroadcastKind, AttachState, Want } from '../../wire/payloads.js'
import type { ChannelCloseOptions, ChannelCloseResult } from '../channel.js'
import type { EndingFrame } from '../ChannelEnding.js'
import { createDeferred } from '../../../utils/createDeferred.js'
import { isPromise } from '../../../utils/isPromise.js'
import { unrefTimer } from '../../../utils/unrefTimer.js'
import { inRequestScope } from './requestScope.js'

const NOT_EVEN_NEWEST = 'the server could not hold even the newest message for its client'
const FELL_BEHIND =
  'its client fell behind what the server holds for it; a publish with `lossy: true` lets it skip to the newest'
const NOT_IN_EFFECT =
  'Broadcast closed: its client subscribed, then made a telefunction call that ended before the subscription took effect'
const NOT_IN_EFFECT_IN_TIME =
  'Broadcast closed: its subscription did not take effect within `connectTtl` of a telefunction call its client made after subscribing'

/** What a page's publishes, subscriptions and attaches go to. */
type BroadcastEndOwner = {
  /** What it returns answers the page's `publish()`. */
  _onPagePublish(serialized: string): unknown
  _onPagePublishBinary(data: Uint8Array): unknown
  /** The page subscribes or unsubscribes `kind`, and declares both at each attach and in each call it makes while the
   *  server hasn't confirmed them. The page learns it is in effect, and such a call goes on, once what it returns
   *  settles; where that rejects, the broadcast ends with the page told its subscription didn't take effect. */
  _onPageSubscription?(kind: BroadcastKind, on: boolean): Promise<void> | void
  /** What the page wants beyond the two kinds (`Want`'s `parts`), declared and confirmed as they are. */
  _onPageWantParts?(parts: unknown): Promise<void> | void
  /** Each attach of the page while the channel is open, after the page's subscriptions applied. */
  _onPageAttach?(): void
  /** Whether its page reads the publishes of `stream`; without one, whether it reads any. Left out, it reads them all. */
  _pageReads?(stream?: string): boolean
}

/** The server end of a page's `ClientBroadcast`: a channel to that page, which also takes the page's publishes and
 *  subscriptions, and delivers publishes to it. */
class ServerBroadcastEnd extends ServerChannel implements MemoryHolder {
  private owner: BroadcastEndOwner | null = null
  /** The page's channel on the wire it last attached on: a publish while the page is away joins the replay through it,
   *  as one a silently dead wire lost does. */
  private lastPeer: IndexedPeer | null = null
  /** Why `shed` ended it, which its page has yet to learn: see `shed`. */
  private overflowed: string | null = null
  /** Its abort, owed to its page: an overflow takes its place where its page would miss a message (`shed`). */
  private owesAbort = false
  /** While it owes its page that abort, the page's channel on the wire it reads, which the overflow then goes on. */
  private endedOn: IndexedPeer | null = null
  /** Its lossy publishes, from the first one on. */
  private lossyPublishes: LossyDelivery<PublishBody> | null = null
  /** The publishes its page wanted before its first attach (a call of the page's brought the want): that attach sends
   *  those its page still wants, ahead of the close where the broadcast closed meanwhile, as a plain channel does. */
  private beforeAttach: PublishBody[] = []
  /** The page's want it applied last: which of its publishes the page reads. */
  private pageWant: Readonly<Want> = { version: 0, text: false, binary: false }
  /** Whether `pageWant` is in effect: what its owner returned for it settled. */
  private pageWantInEffect = true
  /** The calls waiting for its page's want to take effect, which go on once it does or the broadcast ends. */
  private readonly callWaits = new Set<() => void>()

  setOwner(owner: BroadcastEndOwner): void {
    this.owner = owner
  }

  get isAttached(): boolean {
    return this.peer !== null
  }

  /** Ends its page's wire, as a frame that breaks the protocol does. */
  terminateWire(): void {
    this.peer?.sender.terminate()
  }

  /** @internal Its close waits for `promise`, as for an ack's answer. */
  trackAck<T>(promise: Promise<T>): Promise<T> {
    return this.lifecycle.trackAck(promise)
  }

  /** @internal A callback's `Abort` aborts the channel: true where `err` is one. */
  _handleCallbackError(err: unknown): boolean {
    return this.lifecycle.handleCallbackError(err)
  }

  /** A publish to its page, never refused, and held while the page is away. Where admitting this body or one before it
   *  cut the channel, it ends now and sends nothing, and the subscribers after it still get theirs. Where the server
   *  can't hold it for the page, the page ends as one that missed it. */
  deliver(body: PublishBody): void {
    if (this.overflowed !== null) return this.endOverflowed()
    if (!fitsPublish(body)) return this.overflow(body.lossy ? NOT_EVEN_NEWEST : FELL_BEHIND)
    const { stream } = body
    if (body.lossy && stream !== null) return this.lossyPublishDelivery().send(stream, body)
    // A plain publish goes after the lossy ones waiting before it in its stream, or in every stream, oldest first.
    if (stream === EVERY_STREAM) this.lossyPublishes?.flush()
    else if (stream !== null) this.lossyPublishes?.flush(stream)
    this.forward(body)
    const most = maxPendingBytesOf()
    if (this.heldBytes > most) this.shed(most)
  }

  /** @internal Where its owner refuses a publish for it, one that another object cut ends now. */
  _endOverflowed(): void {
    this.endOverflowed()
  }

  /** @internal What it ended with, as its `closed` rejects. */
  _endError(): Error {
    return this.endError()
  }

  /** Its subscription failed for good: it ends with `err` on the server, and on its page with a `ChannelClosedError`
   *  that says its subscription didn't take effect. */
  endNotInEffect(err: Error): void {
    this.endWithError(ERROR_REASON.NOT_IN_EFFECT, err)
  }

  /** Its backend ended its subscription for good: it ends with `err` on the server, and on its page with a
   *  `ChannelClosedError` that says whether the subscription its page wants now had taken effect. */
  endSubscriptionFailed(err: Error): void {
    const tookEffect = this.pageWant.version > 0 && this.pageWantInEffect
    this.endWithError(tookEffect ? ERROR_REASON.ENDED : ERROR_REASON.NOT_IN_EFFECT, err)
  }

  private forward(body: PublishBody): void {
    if (this.lastPeer) this.lastPeer.sendPublish(body)
    else this.holdBeforeAttach(body)
  }

  /** Holds a publish for the page's first attach, as its replay holds one for a page that is away: a lossy one takes the
   *  place of the newest held where that is a lossy one of its stream. */
  private holdBeforeAttach(body: PublishBody): void {
    const newest = this.beforeAttach.at(-1)
    if (body.lossy && newest?.lossy && newest.stream === body.stream) {
      this.beforeAttach.pop()
      letGo(this, newest)
    }
    this.beforeAttach.push(body)
    hold(this, body)
  }

  /** As its replay's `dropFromGap`, for what it holds for its first attach: all of it, where it holds more than `toBytes`
   *  and more than one waits (any at 0). True where any went. */
  private dropBeforeAttachFromGap(toBytes: number): boolean {
    const mayGo = toBytes === 0 ? this.beforeAttach.length : this.beforeAttach.length - 1
    if (mayGo <= 0 || this.heldBytes <= toBytes) return false
    this.dropBeforeAttach()
    return true
  }

  private dropBeforeAttach(): void {
    for (const body of this.beforeAttach) letGo(this, body)
    this.beforeAttach = []
  }

  /** @internal A broadcast whose page stopped wanting `kind` drops what it held of it for its first attach. Room's stub
   *  picks what it delivers itself, so its page gets all it held. */
  _dropHeldOf(kind: BroadcastKind): void {
    const kindTag = kind === 'text' ? TAG.PUBLISH : TAG.PUBLISH_BINARY
    for (const body of this.beforeAttach) if (body.frame[0] === kindTag) letGo(this, body)
    this.beforeAttach = this.beforeAttach.filter((body) => body.frame[0] !== kindTag)
  }

  /** What it held for its first attach goes first. */
  protected override sendHeldForAttach(peer: IndexedPeer): void {
    const held = this.beforeAttach
    this.dropBeforeAttach()
    for (const body of held) peer.sendPublish(body)
  }

  private lossyPublishDelivery(): LossyDelivery<PublishBody> {
    return (this.lossyPublishes ??= new LossyDelivery({
      peer: () => this.peer,
      replay: () => this._replayBuffer,
      hand: (body) => this.forward(body),
      hold: (body) => hold(this, body),
      letGo: (body) => letGo(this, body),
      // A plain publish of a stream ends the place of the stream's lossy ones before it: those stay.
      dropUnhanded: (stream) => this.dropLossyOf(stream),
      // A publish counts against no credit.
      outOfCredit: () => false,
      bytes: (body) => body.frame.byteLength,
    }))
  }

  /** A graceful close goes behind what waits for its page: all of it but the stale lossy publishes no wire has taken,
   *  of which each stream leaves only its newest and the ones a plain publish of it followed. An abort drops what
   *  waits. One that was cut ends with why instead. */
  override close(opts?: ChannelCloseOptions): Promise<ChannelCloseResult> {
    this.endOverflowed()
    if (this._phase === 'open' && this.lossyPublishes !== null) {
      this.peer?.dropStaleLossy()
      this.lossyPublishes.flush()
    }
    return super.close(opts)
  }

  /** One that was cut ends with why, which its page reads, as its `onClose` does. */
  override abort(abortValue?: unknown, message?: string): void {
    this.endOverflowed()
    super.abort(abortValue, message)
  }

  /** @internal What waits to replace a newer lossy publish goes to the wire: nothing newer comes. */
  override _stopSending(): void {
    super._stopSending()
    this.lossyPublishes?.flush()
  }

  protected override onPageHas(lastSeq: number, heldMs?: number): void {
    super.onPageHas(lastSeq, heldMs)
    this.lossyPublishes?.mayGo()
  }

  /** Its publishes go ahead of its ABORT, as a channel's reliable sends do, lossy ones too: a publish is lossy or not
   *  each on its own, so the wire's queue can't tell which of a channel's ungiven publishes may go. Only what waits for
   *  a newer lossy publish goes. */
  protected override onAbortOwed(): void {
    this.owesAbort = true
    this.endedOn = this.peer
    this.lossyPublishes?.dispose()
  }

  /** Its publishes that no wire has taken go, lossy or not, unless it ended: its replay is then what its page reads. */
  protected override dropUnhanded(): void {
    if (this._phase === 'ended') return
    this.dropBeforeAttach()
    if (this.peer) this.peer.dropUnhanded()
    else this._replayBuffer?.dropUnhanded(Number.POSITIVE_INFINITY)
  }

  /** The lossy publishes of `stream` that a newer one replaces go, unless it ended: its replay is then what its page
   *  reads. */
  private dropLossyOf(stream: string): void {
    if (this._phase === 'ended') return
    if (this.peer) this.peer.dropUnhanded(stream)
    else this._replayBuffer?.dropLossyOf(stream, Number.POSITIVE_INFINITY)
  }

  /** `MemoryHolder`: a newer lossy publish of `stream` is about to reach this object's pages. An open end on that object
   *  whose page reads the stream lets go of what the publish replaces. A closing one gets no newer publish, so it keeps
   *  what its page is owed. */
  replaceWaiting(stream: string): void {
    if (this._phase !== 'open' || !this.onOwnObject()) return
    if (this.owner?._pageReads?.(stream) === false) return
    this.lossyPublishes?.letWaitingGo(stream)
  }

  /** How many of its newest publishes no wire has: all of them while it has no wire. Ended, those of the wire its page
   *  reads its abort on. */
  private unhanded(): number {
    return (this.peer ?? this.endedOn)?.unhanded() ?? Number.POSITIVE_INFINITY
  }

  /** Its newest publish, where no wire has that yet. */
  get newestUnhanded(): PublishBody | undefined {
    if (this._phase === 'ended' || !this._replayBuffer) return undefined
    return (
      this.lossyPublishes?.newestHeld ??
      this.beforeAttach.at(-1) ??
      (this.unhanded() === 0 ? undefined : this._replayBuffer.newestPublish())
    )
  }

  /** `MemoryHolder`: its page gets every publish it is owed, so where it would miss one no wire has, the channel ends:
   *  from that gap on everything goes, receipts too, and its page reads on up to the gap and no further. A lossy stream
   *  holds no publish waiting but its newest and those a plain one of it followed, so a page only on lossy publishes
   *  ends at 0, where not even that fits. The bytes go at once and the channel ends after the call that cut it
   *  (`overflow`). Ended by its abort, it is cut the same way, and the overflow takes the abort's place; the abort alone
   *  needs no memory, so without a gap it ends as it was going to. */
  shed(toBytes: number): void {
    const replay = this._replayBuffer
    const ended = this._phase === 'ended'
    if (!replay || (ended && !this.owesAbort)) return
    if (toBytes === 0) this.lossyPublishes?.dispose()
    const gap = this.dropBeforeAttachFromGap(toBytes) || replay.dropFromGap(toBytes, this.unhanded()) > 0
    const peer = this.peer ?? this.endedOn
    if (gap) {
      peer?.sender.queue.dropWaitingFrom(peer.index, 'publish')
      this.dropPendingFrames()
    }
    if (toBytes === 0 && !ended) this.overflow(NOT_EVEN_NEWEST)
    else if (gap) this.overflow(FELL_BEHIND)
  }

  /** What it held for its first attach goes with the frames it held for that. */
  protected override dropPendingFrames(): void {
    this.dropBeforeAttach()
    super.dropPendingFrames()
  }

  /** An answer past the gap would cross it: the channel ends first, and the publish it answers settles with that. */
  protected override sendAckRes(ackedSeq: number, result: string, status?: AckResultStatus): void {
    if (this.overflowed !== null) return this.endOverflowed()
    // As a channel's reliable send, it goes behind its lossy publishes that wait.
    this.lossyPublishes?.flush()
    super.sendAckRes(ackedSeq, result, status)
  }

  /** It ends after the call that cut it, so no close callback runs inside that call. Only the object its page is on can
   *  write to its page, so cut from another it ends at the next delivery to it, its heartbeat, its attach, its answer
   *  or its close running out. */
  private overflow(why: string): void {
    this.overflowed ??= why
    if (this.onOwnObject()) queueMicrotask(() => this.endOverflowed())
  }

  private endOverflowed(): void {
    if (this.overflowed === null) return
    if (this._phase !== 'ended') {
      this.endWithError(ERROR_REASON.OVERFLOW, new ChannelOverflowError(`Broadcast closed: ${this.overflowed}`))
      return
    }
    if (!this.owesAbort) return
    this.owesAbort = false
    this.replaceEnd(ERROR_REASON.OVERFLOW, this.endedOn)
    this.endedOn = null
  }

  override _attachPeer(peer: IndexedPeer, state?: AttachState): void {
    if (this.owesAbort && this._phase === 'ended') this.endedOn = peer
    this.lastPeer = peer
    super._attachPeer(peer, state)
    this.endOverflowed()
    if (this.peer !== peer || this.isClosed) return
    assert(this.owner)
    this.owner._onPageAttach?.()
  }

  override _acknowledge(): void {
    super._acknowledge()
    this.endOverflowed()
  }

  /** Its close running out is an event that ends one that was cut with why, in the timeout's place. */
  override _sendEnding(frame: EndingFrame): void {
    if (frame.kind === 'timedOut' && this.overflowed !== null) return this.endOverflowed()
    super._sendEnding(frame)
  }

  /** What it held for its first attach goes with the rest it buffered: an error ends it without them. */
  protected override endWithError(reason: ErrorReason, err: Error): void {
    if (this._phase !== 'ended') this.dropBeforeAttach()
    super.endWithError(reason, err)
  }

  /** @internal Its page needs nothing more of it: no overflow can take the abort's place, and it lets go of the wire. */
  override _release(): void {
    this.owesAbort = false
    this.endedOn = null
    super._release()
  }

  /** A wire that closed has none of its publishes: once ended, its page away is cut as an open one's (`unhanded`). */
  override _onPeerDisconnect(peer: IndexedPeer, reconnectTimeout: number): void {
    if (this.endedOn?.sender === peer.sender) this.endedOn = null
    super._onPeerDisconnect(peer, reconnectTimeout)
  }

  override _finalize(err: Error | undefined): void {
    if (this.endsPageGone) this.dropBeforeAttach()
    this.lossyPublishes?.dispose()
    this.lastPeer = null
    super._finalize(err)
    this.releaseCallWaits()
  }

  /** @internal Its close callbacks have been called. */
  get _closeFired(): boolean {
    return this.lifecycle.closeFired
  }

  override _onPeerPublishAckReqMessage(text: string, seq: number): Promise<void> {
    const owner = this.owner
    assert(owner)
    // What the publish carries is its owner's to hold, so its request holds only the room for its answer.
    return this.answerHeld(seq, 0, () =>
      this.answerRequest(seq, [{ listener: (t: string) => owner._onPagePublish(t) }], text),
    )
  }

  override _onPeerPublishBinaryAckReqMessage(data: Uint8Array, seq: number): Promise<void> {
    const owner = this.owner
    assert(owner)
    return this.answerHeld(seq, 0, () =>
      this.answerRequest(seq, [{ listener: (d: Uint8Array) => owner._onPagePublishBinary(d) }], data),
    )
  }

  /** A full buffer or a shield failure answers with the status its page's `publish()` rejects with. */
  protected override answerFailed(seq: number, err: unknown): void {
    if (err instanceof ChannelOverflowError) return this.sendAckRes(seq, err.message, ACK_STATUS.OVERFLOW)
    if (isShieldValidationError(err)) return this.sendAckRes(seq, err.message, ACK_STATUS.SHIELD_ERROR)
    super.answerFailed(seq, err)
  }

  /** Applies a want newer than the one it has: the wire and a call can each bring the page's. A closed one takes none:
   *  it would subscribe with nothing left to stop it. Its owner applies it as code of the call that returned it, which
   *  ran on the object its page is on. */
  protected override onPeerWant(want: Want): void {
    if (this.isClosed || want.version <= this.pageWant.version) return
    const owner = this.owner
    assert(owner)
    this.pageWant = want
    const pending = inRequestScope(this._callRequest, () => [
      owner._onPageSubscription?.('text', want.text),
      owner._onPageSubscription?.('binary', want.binary),
      owner._onPageWantParts?.(want.parts),
    ])
    // A page that reads none of its publishes any more would only drop those still waiting for its wire.
    if (!want.text && !want.binary && owner._pageReads?.() === false) this.dropUnhanded()
    const waits = pending.filter((p) => isPromise(p))
    this.pageWantInEffect = waits.length === 0
    if (!this.pageWantInEffect) void this.whenInEffect(want.version, waits)
  }

  /** Once what its owner returned for want `version` settled, that want is in effect, where it is still the page's; where
   *  it rejected, the broadcast ends. */
  private async whenInEffect(version: number, pending: Promise<void>[]): Promise<void> {
    try {
      await Promise.all(pending)
    } catch (err) {
      if (version === this.pageWant.version) this.endNotInEffect(err instanceof Error ? err : new Error(String(err)))
      return
    }
    if (version !== this.pageWant.version) return
    this.pageWantInEffect = true
    this._confirmWant()
  }

  /** Tells the page its want is in effect, and the calls waiting for it, where it is; else that waits (`whenInEffect`).
   *  A page that never asked for one has nothing to confirm. */
  override _confirmWant(): void {
    if (this.pageWant.version === 0 || !this.pageWantInEffect) return
    this.peer?.sendWanted(this.pageWant.version)
    this.releaseCallWaits()
  }

  /** The call waits until the want it carries is in effect, or the broadcast ended. Where the call's request goes first
   *  (its page left, or aborted the call), or the call waited `waitAtMost` (`connectTtl`), the broadcast ends now with an
   *  error its page reads: its telefunction runs without the subscription. */
  override _applyCallWant(want: Want, callAborted: AbortSignal, waitAtMost: number): Promise<void> | undefined {
    this.onPeerWant(want)
    this._confirmWant()
    // A want a closing broadcast didn't take never takes effect: the call runs once the broadcast ended.
    if (want.version > this.pageWant.version) return this.closed.then(() => {})
    return this.pageWantInEffect ? undefined : this.inEffectOrEnded(callAborted, waitAtMost)
  }

  /** Every way the wait ends comes past its `await`, which drops its timer and abort listener. */
  private async inEffectOrEnded(callAborted: AbortSignal, waitAtMost: number): Promise<void> {
    const inEffect = createDeferred()
    this.callWaits.add(inEffect.resolve)
    const end = (message: string) => this.endWithError(ERROR_REASON.NOT_IN_EFFECT, new ChannelClosedError(message))
    const endAborted = () => end(NOT_IN_EFFECT)
    if (callAborted.aborted) endAborted()
    else callAborted.addEventListener('abort', endAborted, { once: true })
    const timer = unrefTimer(setTimeout(() => end(NOT_IN_EFFECT_IN_TIME), waitAtMost))
    await inEffect.promise
    clearTimeout(timer)
    callAborted.removeEventListener('abort', endAborted)
  }

  private releaseCallWaits(): void {
    for (const goOn of this.callWaits) goOn()
    this.callWaits.clear()
  }
}
