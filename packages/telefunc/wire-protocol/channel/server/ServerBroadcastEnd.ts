export { ServerBroadcastEnd }
export type { BroadcastEndOptions }

import { stringify } from '@brillout/json-serializer/stringify'
import { ServerChannel } from './channel.js'
import type { IndexedPeer } from './IndexedPeer.js'
import { ChannelOverflowError } from '../channelErrors.js'
import { isShieldValidationError } from '../../../shared/ShieldValidationError.js'
import { STATUS_BODY_INTERNAL_SERVER_ERROR } from '../../../shared/constants.js'
import { ACK_STATUS, ProtocolViolationError } from '../../wire/connectionFrame.js'
import type { BroadcastKind, ReattachState } from '../../wire/connectionFrame.js'

type BroadcastEndOptions = {
  key: string
  /** A publish from its page: what it returns answers the page's `publish()`. */
  onPublish(serialized: string): unknown
  onPublishBinary(data: Uint8Array): unknown
  /** Its page subscribes or unsubscribes `kind`, and declares both at each attach. */
  onSubscription?(kind: BroadcastKind, on: boolean): void
  /** Each attach of its page while the channel is open, after the page's subscriptions applied. */
  onAttach?(): void
}

/** The server end of a page's `ClientBroadcast`: a channel to that page, which also takes the page's publishes and
 *  subscriptions on `key`, and delivers publishes to it. */
class ServerBroadcastEnd<ClientToServer = unknown, ServerToClient = unknown> extends ServerChannel<
  ClientToServer,
  ServerToClient
> {
  readonly key: string
  private readonly _options: BroadcastEndOptions

  constructor(options: BroadcastEndOptions) {
    super({ publishes: true })
    this.key = options.key
    this._options = options
  }

  get isAttached(): boolean {
    return this._peer !== null
  }

  /** A PUBLISH frame to its page, never refused: a page too far behind is let go. One `resentOnAttach`, which the owner
   *  sends again at each attach, may be dropped while the page is away without leaving it a gap. */
  deliver(frame: string, opts?: { resentOnAttach?: boolean }): void {
    this._sendPublish(frame, opts)
  }

  deliverBinary(frame: Uint8Array): void {
    this._sendPublishBinary(frame)
  }

  override _attachPeer(peer: IndexedPeer, state?: ReattachState): void {
    super._attachPeer(peer, state)
    if (this._peer === peer && !this._isClosed) this._options.onAttach?.()
  }

  override _onPeerPublishAckReqMessage(text: string, seq: number): Promise<void> {
    return this._answerPublish(seq, () => this._options.onPublish(text))
  }

  override _onPeerPublishBinaryAckReqMessage(data: Uint8Array, seq: number): Promise<void> {
    return this._answerPublish(seq, () => this._options.onPublishBinary(data))
  }

  override _onPeerSubscription(kind: BroadcastKind, on: boolean): void {
    this._options.onSubscription?.(kind, on)
  }

  /** Runs `publish` in the receive turn, where a ProtocolViolationError it throws ends the wire, as a malformed frame
   *  does. */
  private _answerPublish(seq: number, publish: () => unknown): Promise<void> {
    let result: unknown
    try {
      result = publish()
    } catch (err) {
      if (err instanceof ProtocolViolationError) throw err
      this._sendPublishFailure(seq, err)
      return Promise.resolve()
    }
    return this._trackAck(this._sendPublishAnswer(seq, result))
  }

  private async _sendPublishAnswer(seq: number, result: unknown): Promise<void> {
    try {
      this._sendAckRes(seq, stringify(await result))
    } catch (err) {
      this._sendPublishFailure(seq, err)
    }
  }

  /** A full buffer refusing the publish is no bug: the page's publish() rejects with a ChannelOverflowError. */
  private _sendPublishFailure(seq: number, err: unknown): void {
    if (err instanceof ChannelOverflowError) return this._sendAckRes(seq, err.message, ACK_STATUS.OVERFLOW)
    if (isShieldValidationError(err)) return this._sendAckRes(seq, err.message, ACK_STATUS.SHIELD_ERROR)
    if (this._handleCallbackError(err)) return
    this._sendAckRes(seq, `${STATUS_BODY_INTERNAL_SERVER_ERROR} — see server logs`, ACK_STATUS.ERROR)
  }
}
