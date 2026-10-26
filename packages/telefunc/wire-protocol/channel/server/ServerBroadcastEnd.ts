export { ServerBroadcastEnd }
export type { BroadcastEndOptions }

import { stringify } from '@brillout/json-serializer/stringify'
import { ServerChannel } from './channel.js'
import type { IndexedPeer } from './IndexedPeer.js'
import { ChannelOverflowError } from '../channelErrors.js'
import { isShieldValidationError } from '../../../shared/ShieldValidationError.js'
import { ACK_STATUS, ERROR_REASON } from '../../wire/connectionFrame.js'
import type { BroadcastKind, ReattachState } from '../../wire/connectionFrame.js'

type BroadcastEndOptions = {
  /** A publish from its page: what it returns answers the page's `publish()`. */
  onPublish(serialized: string): unknown
  onPublishBinary(data: Uint8Array): unknown
  /** Its page subscribes or unsubscribes `kind`, and declares both at each attach. */
  onSubscription?(kind: BroadcastKind, on: boolean): void
  /** Each attach of its page while the channel is open, after the page's subscriptions applied. */
  onAttach?(): void
}

/** The server end of a page's `ClientBroadcast`: a channel to that page, which also takes the page's publishes and
 *  subscriptions, and delivers publishes to it. */
class ServerBroadcastEnd extends ServerChannel {
  private readonly _options: BroadcastEndOptions

  constructor(options: BroadcastEndOptions) {
    super({ publishes: true })
    this._options = options
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
    if (this._peer === peer && !this._isClosed) this._options.onAttach?.()
  }

  override _onPeerPublishAckReqMessage(text: string, seq: number): Promise<void> {
    return this._trackAck(this._answerPublish(seq, () => this._options.onPublish(text)))
  }

  override _onPeerPublishBinaryAckReqMessage(data: Uint8Array, seq: number): Promise<void> {
    return this._trackAck(this._answerPublish(seq, () => this._options.onPublishBinary(data)))
  }

  override _onPeerSubscription(kind: BroadcastKind, on: boolean): void {
    this._options.onSubscription?.(kind, on)
  }

  private async _answerPublish(seq: number, publish: () => unknown): Promise<void> {
    // Until `publish` returns, a ProtocolViolationError is this request's own and ends its wire.
    let violationPeer = this._peer
    let answer: string
    try {
      const result = publish()
      violationPeer = null
      answer = stringify(await result)
    } catch (err) {
      this._sendPublishFailure(seq, err, violationPeer)
      return
    }
    this._sendAckRes(seq, answer)
  }

  /** A full buffer refusing the publish is no bug: the page's publish() rejects with a ChannelOverflowError. */
  private _sendPublishFailure(seq: number, err: unknown, violationPeer: IndexedPeer | null): void {
    if (err instanceof ChannelOverflowError) return this._sendAckRes(seq, err.message, ACK_STATUS.OVERFLOW)
    if (isShieldValidationError(err)) return this._sendAckRes(seq, err.message, ACK_STATUS.SHIELD_ERROR)
    this._sendAckFailure(seq, err, violationPeer)
  }
}
