export { ServerCreditOwner }

import type { CreditOwner, Urgency } from '../../flowControl/credit.js'
import type { IndexedPeer } from './IndexedPeer.js'

/** The channel whose credits these are: the page's end it is attached to, if any, read when a credit frame goes. */
type CreditChannel = { readonly _peer: IndexedPeer | null; readonly _lastClientSeq: number }

type ServerCreditOwnerOptions = {
  channel: CreditChannel
}

/** What a server channel's credits send to its page, and what they read of its wire; nothing while no page is
 *  attached. */
class ServerCreditOwner implements CreditOwner {
  private readonly channel: CreditChannel

  constructor({ channel }: ServerCreditOwnerOptions) {
    this.channel = channel
  }

  /** The limit carries the last seq the channel has, which acknowledges what arrived. */
  sendByteLimit(limit: number, urgency: Urgency): void {
    this.channel._peer?.sendByteLimit(limit, this.channel._lastClientSeq, urgency)
  }

  sendMessageLimit(limit: number, urgency: Urgency): void {
    this.channel._peer?.sendMessageLimit(limit, urgency)
  }

  sendBdpPing(probe: number): void {
    this.channel._peer?.sendBdpPing(probe)
  }

  backlogBytes(): number | undefined {
    return this.channel._peer?.sender.bufferedAmount()
  }

  deliveredBefore(ms: number): number {
    return this.channel._peer?.sender.deliveredBefore(ms) ?? 0
  }
}
