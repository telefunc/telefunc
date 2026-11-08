export { ServerBroadcastEnd }
export type { BroadcastEndOwner }

import { ServerChannel } from './ServerChannel.js'
import { getChannelMux } from './ChannelMux.js'
import { assert } from '../../../utils/assert.js'
import type { IndexedPeer } from './IndexedPeer.js'
import type { MemoryHolder } from './channelMemory.js'
import type { PublishBody } from './PublishBody.js'
import { CHANNEL_HOLD_MAX_BYTES, CHANNEL_RECONNECT_TIMEOUT_MS, CREDIT_WINDOW_MAX_BYTES } from '../../constants.js'
import { ChannelOverflowError, isChannelOverflowError } from '../channelErrors.js'
import { isShieldValidationError } from '../../../shared/ShieldValidationError.js'
import { ACK_STATUS, ERROR_REASON } from '../../wire/frame.js'
import type { BroadcastKind, AttachState } from '../../wire/payloads.js'

/** What a page's publishes, subscriptions and attaches go to. */
type BroadcastEndOwner = {
  /** What it returns answers the page's `publish()`. */
  _onPagePublish(serialized: string): unknown
  _onPagePublishBinary(data: Uint8Array): unknown
  /** The page subscribes or unsubscribes `kind`, and declares both at each attach. */
  _onPageSubscription?(kind: BroadcastKind, on: boolean): void
  /** Each attach of the page while the channel is open, after the page's subscriptions applied. */
  _onPageAttach?(): void
  /** What its page gets where it falls behind what the server holds for it: at `'missOldest'` it misses the oldest it
   *  may miss of each stream, at `'close'` none, and its channel closes there instead (see `shed`). */
  readonly _whenBehind: 'missOldest' | 'close'
  /** Its page fell behind and misses its oldest publishes, of those at `behind` (see `ShedLevel`). */
  _onPublishesMissed?(): void
}

/** The server end of a page's `ClientBroadcast`: a channel to that page, which also takes the page's publishes and
 *  subscriptions, and delivers publishes to it. */
class ServerBroadcastEnd extends ServerChannel implements MemoryHolder {
  private owner: BroadcastEndOwner | null = null
  /** The page's channel on the wire it last attached on: a publish while the page is away joins the replay through it,
   *  as one a silently dead wire lost does. */
  private lastPeer: IndexedPeer | null = null
  /** Why it ends, where `shed` couldn't tell its page yet: see `shed`. */
  private overflow: string | null = null

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

  /** A publish to its page, never refused, and held while the page is away. Its listeners or admitting its body can have
   *  ended the channel by now. */
  deliver(body: PublishBody): void {
    this.endOverflowed()
    if (this._phase === 'ended') return
    if (this.lastPeer) this.lastPeer.sendPublish(body)
    else this.offlineBuffer.pushPublish(body)
    const share = this.share()
    if (this.heldBytes > share) this.shed(share)
  }

  /** What it may hold: 60 s of what its page takes, within what any channel may. */
  private share(): number {
    const perMs = this.peer?.sender.queue.drainRate() ?? 0
    if (perMs === 0) return CHANNEL_HOLD_MAX_BYTES
    return Math.min(CHANNEL_HOLD_MAX_BYTES, Math.max(CREDIT_WINDOW_MAX_BYTES, perMs * CHANNEL_RECONNECT_TIMEOUT_MS))
  }

  /** How many of its newest publishes no wire has: all of them while it has no wire. */
  private unhanded(): number {
    const { peer } = this
    return peer ? peer.sender.queue.waitingPublishes(peer.index) : Number.POSITIVE_INFINITY
  }

  /** Its newest publish, where no wire has that yet. */
  get newestUnhanded(): PublishBody | undefined {
    if (this._phase === 'ended' || !this._replayBuffer || this.unhanded() === 0) return undefined
    return this._replayBuffer.newestPublish()
  }

  /** `MemoryHolder`: its oldest publishes no wire has that its page may miss go, down to `toBytes`, and its page sees the
   *  jump. Above 0 the newest of each stream stays, and at 0, where not even that fits, the channel ends: no later message
   *  would show the page. Where its owner's `_whenBehind` is `'close'` and one would go, or what its page can't miss alone
   *  is over `toBytes`, all of them go and the channel ends: what the page got has no hole. The bytes go at once, but only
   *  its own object can write to its page, so from another's it ends when its own next runs. */
  shed(toBytes: number): void {
    const replay = this._replayBuffer
    if (this._phase === 'ended' || !replay) return
    const owner = this.owner
    assert(owner)
    const missesOldest = owner._whenBehind === 'missOldest'
    if (toBytes > 0) {
      const { seqs, keptOver } = replay.dropOldestPublishes(toBytes, this.unhanded())
      this.dropWaiting(seqs)
      const missed = seqs.length > 0
      if (!keptOver && (missesOldest || !missed)) {
        if (missed) owner._onPublishesMissed?.()
        return
      }
    }
    const { seqs } = replay.dropOldestPublishes(0, this.unhanded())
    this.dropWaiting(seqs)
    if (toBytes === 0 && seqs.length > 0 && missesOldest) owner._onPublishesMissed?.()
    this.overflow =
      toBytes === 0
        ? 'Broadcast closed: the server could not hold even the newest message for its client'
        : 'Closed: its client fell behind what the server can hold for it'
    if (getChannelMux() === this.mux) this.endOverflowed()
  }

  private dropWaiting(seqs: number[]): void {
    if (this.peer && seqs.length > 0) this.peer.sender.queue.dropWaitingPublishes(this.peer.index, seqs)
  }

  private endOverflowed(): void {
    if (this.overflow === null) return
    this.endWithError(ERROR_REASON.OVERFLOW, new ChannelOverflowError(this.overflow))
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

  protected override shutdown(err?: Error, options?: { pageGone?: boolean }): void {
    this.lastPeer = null
    super.shutdown(err, options)
  }

  override _onPeerPublishAckReqMessage(text: string, seq: number): Promise<void> {
    const owner = this.owner
    assert(owner)
    return this.lifecycle.trackAck(this.answerRequest(seq, [(t: string) => owner._onPagePublish(t)], text))
  }

  override _onPeerPublishBinaryAckReqMessage(data: Uint8Array, seq: number): Promise<void> {
    const owner = this.owner
    assert(owner)
    return this.lifecycle.trackAck(this.answerRequest(seq, [(d: Uint8Array) => owner._onPagePublishBinary(d)], data))
  }

  /** A full buffer or a shield failure answers with the status its page's `publish()` rejects with. */
  protected override sendRequestFailure(seq: number, err: unknown): void {
    if (isChannelOverflowError(err)) return this.sendAckRes(seq, err.message, ACK_STATUS.OVERFLOW)
    if (isShieldValidationError(err)) return this.sendAckRes(seq, err.message, ACK_STATUS.SHIELD_ERROR)
    super.sendRequestFailure(seq, err)
  }

  protected override onPeerSubscription(kind: BroadcastKind, on: boolean): void {
    assert(this.owner)
    this.owner._onPageSubscription?.(kind, on)
  }
}
