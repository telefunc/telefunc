export { ServerCreditSender }

import type { CreditOwner, Urgency } from '../../flowControl/credit.js'
import type { IndexedPeer } from './IndexedPeer.js'

type ServerCreditSenderOptions = {
  /** The page's end the channel is attached to, if any, read when a credit frame goes. */
  peer: () => IndexedPeer | null
  /** The last seq the channel has from its page. */
  lastClientSeq: () => number
}

/** The `CreditOwner` of a server channel's credits, kept off the channel's public surface: what they send to its page,
 *  and what they read of its wire; nothing while no page is attached. */
class ServerCreditSender implements CreditOwner {
  private readonly peer: () => IndexedPeer | null
  private readonly lastClientSeq: () => number

  constructor({ peer, lastClientSeq }: ServerCreditSenderOptions) {
    this.peer = peer
    this.lastClientSeq = lastClientSeq
  }

  /** The limit carries the last seq the channel has, which acknowledges what arrived. */
  sendByteLimit(limit: number, urgency: Urgency): void {
    this.peer()?.sendByteLimit(limit, this.lastClientSeq(), urgency)
  }

  sendMessageLimit(limit: number, urgency: Urgency): void {
    this.peer()?.sendMessageLimit(limit, urgency)
  }

  sendBdpPing(probe: number): void {
    this.peer()?.sendBdpPing(probe)
  }

  backlogBytes(): number | undefined {
    return this.peer()?.sender.bufferedAmount()
  }

  deliveredBefore(ms: number): number {
    return this.peer()?.sender.deliveredBefore(ms) ?? 0
  }
}
