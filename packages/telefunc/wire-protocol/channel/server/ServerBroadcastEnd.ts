export { ServerBroadcastEnd }
export type { BroadcastEndOwner }

import { ServerChannel } from './channel.js'
import { assert } from '../../../utils/assert.js'
import type { IndexedPeer } from './IndexedPeer.js'
import { ChannelOverflowError, isChannelOverflowError } from '../channelErrors.js'
import { isShieldValidationError } from '../../../shared/ShieldValidationError.js'
import { ACK_STATUS, ERROR_REASON } from '../../wire/connectionFrame.js'
import type { BroadcastKind, ReattachState } from '../../wire/connectionFrame.js'

/** What a page's publishes, subscriptions and attaches go to. */
type BroadcastEndOwner = {
  /** What it returns answers the page's `publish()`. */
  _onPagePublish(serialized: string): unknown
  _onPagePublishBinary(data: Uint8Array): unknown
  /** The page subscribes or unsubscribes `kind`, and declares both at each attach. */
  _onPageSubscription?(kind: BroadcastKind, on: boolean): void
  /** Each attach of the page while the channel is open, after the page's subscriptions applied. */
  _onPageAttach?(): void
}

/** The server end of a page's `ClientBroadcast`: a channel to that page, which also takes the page's publishes and
 *  subscriptions, and delivers publishes to it. */
class ServerBroadcastEnd extends ServerChannel {
  private _owner: BroadcastEndOwner | null = null

  constructor() {
    super({ publishes: true })
  }

  _setOwner(owner: BroadcastEndOwner): void {
    this._owner = owner
  }

  get isAttached(): boolean {
    return this._peer !== null
  }

  /** A PUBLISH frame to its page, never refused: a page too far behind is let go. One `resentOnAttach`, which the owner
   *  sends again at each attach, may be dropped while the page is away without leaving it a gap. */
  deliver(frame: string, opts?: { resentOnAttach?: boolean }): void {
    this._deliver(frame, opts?.resentOnAttach !== true)
  }

  deliverBinary(frame: Uint8Array): void {
    this._deliver(frame, true)
  }

  /** Buffered while its page is away, where dropping a `gap` frame, which its page doesn't get again as it attaches,
   *  lets the page go. */
  private _deliver(frame: string | Uint8Array, gap: boolean): void {
    const binary = typeof frame !== 'string'
    const peer = this._peer
    if (peer === null) {
      if (binary) this._prePeerBuffer.pushPublishBinary(frame, gap)
      else this._prePeerBuffer.pushPublish(frame, gap)
      if (this._prePeerBuffer.droppedGap) {
        const limit = binary ? 'bufferLimitBinary' : 'bufferLimit'
        this._endWithError(
          ERROR_REASON.OVERFLOW,
          new ChannelOverflowError(
            `Channel closed: more was sent to its client while it was offline than config.channel.${limit} lets the server hold`,
          ),
        )
      }
      return
    }
    if (this._flow.isPastByteCredit && this._isPeerBehind()) {
      this._endWithError(
        ERROR_REASON.OVERFLOW,
        new ChannelOverflowError('Channel closed: its client fell further behind than the server holds for a client'),
      )
      return
    }
    this._flow.countPublishSent(binary ? peer.sendPublishBinary(frame) : peer.sendPublish(frame))
  }

  override _attachPeer(peer: IndexedPeer, state?: ReattachState): void {
    super._attachPeer(peer, state)
    if (this._peer === peer && !this._isClosed) this._owner?._onPageAttach?.()
  }

  override _onPeerPublishAckReqMessage(text: string, seq: number): Promise<void> {
    const owner = this._owner
    assert(owner)
    return this._trackAck(this._answerRequest(seq, [(t: string) => owner._onPagePublish(t)], text))
  }

  override _onPeerPublishBinaryAckReqMessage(data: Uint8Array, seq: number): Promise<void> {
    const owner = this._owner
    assert(owner)
    return this._trackAck(this._answerRequest(seq, [(d: Uint8Array) => owner._onPagePublishBinary(d)], data))
  }

  /** A full buffer or a shield failure answers with the status its page's `publish()` rejects with. */
  protected override _sendRequestFailure(seq: number, err: unknown, violationPeer: IndexedPeer | null): void {
    if (isChannelOverflowError(err)) return this._sendAckRes(seq, err.message, ACK_STATUS.OVERFLOW)
    if (isShieldValidationError(err)) return this._sendAckRes(seq, err.message, ACK_STATUS.SHIELD_ERROR)
    super._sendRequestFailure(seq, err, violationPeer)
  }

  override _onPeerSubscription(kind: BroadcastKind, on: boolean): void {
    this._owner?._onPageSubscription?.(kind, on)
  }
}
