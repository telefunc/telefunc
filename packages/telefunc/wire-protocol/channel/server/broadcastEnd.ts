export { ServerBroadcastEnd }
export type { BroadcastEndOwner }

import { ServerChannel } from './channel.js'
import { assert } from '../../../utils/assert.js'
import type { IndexedPeer } from './IndexedPeer.js'
import { ChannelOverflowError, isChannelOverflowError } from '../channelErrors.js'
import { isShieldValidationError } from '../../../shared/ShieldValidationError.js'
import { ACK_STATUS, ERROR_REASON, encode } from '../../wire/frame.js'
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
}

/** The server end of a page's `ClientBroadcast`: a channel to that page, which also takes the page's publishes and
 *  subscriptions, and delivers publishes to it. */
class ServerBroadcastEnd extends ServerChannel {
  private owner: BroadcastEndOwner | null = null

  constructor() {
    super()
    // Its page grants it the largest window from the start (see `ClientBroadcast`).
    this.sendCredit.takePeerWindowMax()
  }

  _setOwner(owner: BroadcastEndOwner): void {
    this.owner = owner
  }

  get isAttached(): boolean {
    return this.peer !== null
  }

  /** Ends its page's wire, as a frame that breaks the protocol does. */
  _terminateWire(): void {
    this.peer?.sender.terminate()
  }

  /** @internal Its close waits for `promise`, as for an ack's answer. */
  _trackAck<T>(promise: Promise<T>): Promise<T> {
    return this.lifecycle.trackAck(promise)
  }

  /** @internal A callback's `Abort` aborts the channel: true where `err` is one. */
  _handleCallbackError(err: unknown): boolean {
    return this.lifecycle.handleCallbackError(err)
  }

  /** A PUBLISH frame to its page, never refused. One `resentOnAttach`, which the owner sends again at each attach, may
   *  be dropped while the page is away without leaving it a gap. */
  deliver(frame: string, opts?: { resentOnAttach?: boolean }): void {
    this.deliverOrHold(frame, opts?.resentOnAttach !== true)
  }

  deliverBinary(frame: Uint8Array): void {
    this.deliverOrHold(frame, true)
  }

  /** Buffered while its page is away, where dropping a `gap` frame, which its page doesn't get again as it attaches,
   *  lets the page go. */
  private deliverOrHold(frame: string | Uint8Array, gap: boolean): void {
    const binary = typeof frame !== 'string'
    const peer = this.peer
    if (peer === null) {
      if (binary) this.offlineBuffer.pushPublishBinary(frame, gap)
      else this.offlineBuffer.pushPublish(frame, gap)
      if (this.offlineBuffer.droppedGap) {
        const limit = binary ? 'bufferLimitBinary' : 'bufferLimit'
        this.endWithError(
          ERROR_REASON.OVERFLOW,
          new ChannelOverflowError(
            `Channel closed: more was sent to its client while it was offline than config.channel.${limit} lets the server hold`,
          ),
        )
      }
      return
    }
    const bytes = binary ? peer.sendSequenced(encode.publishBinary, frame) : peer.sendSequenced(encode.publish, frame)
    this.sendCredit.countPublishSent(bytes)
  }

  /** Its page grants the largest window at once: a quarter of it past credit is more than the page reads unreported. */
  protected override pastCreditAllowance(): number {
    return this.sendCredit.peerByteWindowMax / 4
  }

  override _attachPeer(peer: IndexedPeer, state?: AttachState): void {
    super._attachPeer(peer, state)
    if (this.peer !== peer || this.isClosed) return
    assert(this.owner)
    this.owner._onPageAttach?.()
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

  override _onPeerSubscription(kind: BroadcastKind, on: boolean): void {
    assert(this.owner)
    this.owner._onPageSubscription?.(kind, on)
  }
}
