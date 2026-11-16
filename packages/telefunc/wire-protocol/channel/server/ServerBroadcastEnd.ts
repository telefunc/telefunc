export { ServerBroadcastEnd }
export type { BroadcastEndOwner }

import { ServerChannel } from './ServerChannel.js'
import { assert } from '../../../utils/assert.js'
import type { IndexedPeer } from './IndexedPeer.js'
import { fitsPublish, hold, letGo, maxPendingBytesOf, type MemoryHolder } from './channelMemory.js'
import type { PublishBody } from './PublishBody.js'
import { LossyDelivery } from './LossyDelivery.js'
import { EVERY_STREAM } from '../ownStream.js'
import { ChannelOverflowError, isChannelOverflowError } from '../channelErrors.js'
import { isShieldValidationError } from '../../../shared/ShieldValidationError.js'
import { ACK_STATUS, ERROR_REASON, type AckResultStatus } from '../../wire/frame.js'
import type { BroadcastKind, AttachState } from '../../wire/payloads.js'
import type { ChannelCloseOptions, ChannelCloseResult } from '../channel.js'

const NOT_EVEN_NEWEST = 'the server could not hold even the newest message for its client'
const FELL_BEHIND =
  'its client fell behind what the server holds for it; a publish with `lossy: true` lets it skip to the newest'

/** What a page's publishes, subscriptions and attaches go to. */
type BroadcastEndOwner = {
  /** What it returns answers the page's `publish()`. */
  _onPagePublish(serialized: string): unknown
  _onPagePublishBinary(data: Uint8Array): unknown
  /** The page subscribes or unsubscribes `kind`, and declares both at each attach. */
  _onPageSubscription?(kind: BroadcastKind, on: boolean): void
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
  /** Its lossy publishes, from the first one on. */
  private lossyPublishes: LossyDelivery<PublishBody> | null = null

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

  /** A publish to its page, never refused, and held while the page is away. Admitting its body may have ended the
   *  channel: this body or one before it may have cut it, and the close callbacks of the channels this one cuts may
   *  close or abort it. It then sends nothing, and the subscribers after it still get theirs. Where the server can't
   *  hold it for the page, the page ends as one that missed it. */
  deliver(body: PublishBody): void {
    if (this.overflowed !== null || this._phase !== 'open') return this.endOverflowed()
    if (!fitsPublish(body)) return this.overflow(body.lossy ? NOT_EVEN_NEWEST : FELL_BEHIND)
    const { stream } = body
    if (body.lossy && stream !== null) return this.lossyDelivery().send(stream, body)
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

  private forward(body: PublishBody): void {
    if (this.lastPeer) this.lastPeer.sendPublish(body)
    else this.offlineBuffer.pushPublish(body)
  }

  private lossyDelivery(): LossyDelivery<PublishBody> {
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
    }))
  }

  /** A graceful close goes behind what waits for its page: all of it but the stale lossy publishes no wire has taken,
   *  of which each stream leaves only its newest and the ones a plain publish of it followed. An abort drops what
   *  waits. */
  override close(opts?: ChannelCloseOptions): Promise<ChannelCloseResult> {
    if (this._phase === 'open' && this.lossyPublishes !== null) {
      this.peer?.dropStaleLossy()
      this.lossyPublishes.flush()
    }
    return super.close(opts)
  }

  protected override onPageHas(lastSeq: number): void {
    super.onPageHas(lastSeq)
    this.lossyPublishes?.mayGo()
  }

  /** Its publishes that no wire has taken go, lossy or not, unless it ended: its replay is then what its page reads. */
  protected override dropUnhanded(): void {
    if (this._phase === 'ended') return
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

  /** How many of its newest publishes no wire has: all of them while it has no wire. */
  private unhanded(): number {
    return this.peer?.unhanded() ?? Number.POSITIVE_INFINITY
  }

  /** Its newest publish, where no wire has that yet. */
  get newestUnhanded(): PublishBody | undefined {
    if (this._phase === 'ended' || !this._replayBuffer) return undefined
    return this.lossyPublishes?.newestHeld ?? (this.unhanded() === 0 ? undefined : this._replayBuffer.newestPublish())
  }

  /** `MemoryHolder`: its page gets every publish it is owed, so where it would miss one no wire has, the channel ends:
   *  from that gap on everything goes, receipts too, and its page reads on up to the gap and no further. A lossy stream
   *  holds no publish waiting but its newest and those a plain one of it followed, so a page only on lossy publishes
   *  ends at 0, where not even that fits. The bytes go at once, but only its own object can write to its page, so cut
   *  from another it ends at the next delivery to it, its heartbeat, its attach, its answer or its close running out. */
  shed(toBytes: number): void {
    const replay = this._replayBuffer
    if (this._phase === 'ended' || !replay) return
    if (toBytes === 0) this.lossyPublishes?.dispose()
    const gap = replay.dropFromGap(toBytes, this.unhanded()) > 0
    if (gap) {
      this.peer?.sender.queue.dropWaitingFrom(this.peer.index, 'publish')
      this.dropPendingFrames()
    }
    if (toBytes === 0) this.overflow(NOT_EVEN_NEWEST)
    else if (gap) this.overflow(FELL_BEHIND)
  }

  /** An answer past the gap would cross it: the channel ends first, and the publish it answers settles with that. */
  protected override sendAckRes(ackedSeq: number, result: string, status?: AckResultStatus): void {
    if (this.overflowed !== null) return this.endOverflowed()
    super.sendAckRes(ackedSeq, result, status)
  }

  /** Only the object its page is on can write to it: cut from another, it ends when its own next runs. */
  private overflow(why: string): void {
    this.overflowed ??= why
    if (this.onOwnObject()) this.endOverflowed()
  }

  private endOverflowed(): void {
    if (this.overflowed === null) return
    this.endWithError(ERROR_REASON.OVERFLOW, new ChannelOverflowError(`Broadcast closed: ${this.overflowed}`))
  }

  override _attachPeer(peer: IndexedPeer, state?: AttachState): void {
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

  /** Where its close ran out, one that was cut ends with why, which its page reads; a close its page asked for or
   *  answered ends gracefully. */
  override _end(err?: Error): void {
    if (err !== undefined && this.overflowed !== null) return this.endOverflowed()
    super._end(err)
  }

  protected override shutdown(err?: Error, options?: { pageGone?: boolean }): void {
    this.lossyPublishes?.dispose()
    this.lastPeer = null
    super.shutdown(err, options)
  }

  override _onPeerPublishAckReqMessage(text: string, seq: number): Promise<void> {
    const owner = this.owner
    assert(owner)
    // What the publish carries is its owner's to hold, so its request holds only the room for its answer.
    return this.answerHeld(seq, 0, () => this.answerRequest(seq, [(t: string) => owner._onPagePublish(t)], text))
  }

  override _onPeerPublishBinaryAckReqMessage(data: Uint8Array, seq: number): Promise<void> {
    const owner = this.owner
    assert(owner)
    return this.answerHeld(seq, 0, () =>
      this.answerRequest(seq, [(d: Uint8Array) => owner._onPagePublishBinary(d)], data),
    )
  }

  /** A full buffer or a shield failure answers with the status its page's `publish()` rejects with. */
  protected override answerFailed(seq: number, err: unknown): void {
    if (isChannelOverflowError(err)) return this.sendAckRes(seq, err.message, ACK_STATUS.OVERFLOW)
    if (isShieldValidationError(err)) return this.sendAckRes(seq, err.message, ACK_STATUS.SHIELD_ERROR)
    super.answerFailed(seq, err)
  }

  protected override onPeerSubscription(kind: BroadcastKind, on: boolean): void {
    assert(this.owner)
    this.owner._onPageSubscription?.(kind, on)
    // A page that reads none of its publishes any more would only drop those still waiting for its wire.
    if (!on && this.owner._pageReads?.() === false) this.dropUnhanded()
  }
}
