export { ClientCreditOwner }

import type { CreditOwner, Urgency } from '../../flowControl/credit.js'
import type { MuxChannel, MuxConnection } from './ClientConnection.js'

/** The channel whose credits these are, and the connection it rides, read when a credit frame goes. */
type CreditChannel = MuxChannel & { readonly _connection: MuxConnection }

type ClientCreditOwnerOptions = {
  channel: CreditChannel
}

/** What a page channel's credits send to the server through its connection, and what they read of its wire. */
class ClientCreditOwner implements CreditOwner {
  private readonly channel: CreditChannel

  constructor({ channel }: ClientCreditOwnerOptions) {
    this.channel = channel
  }

  /** A closed channel's server ends its sends with the close, so the channel grants it no more credit. */
  sendByteLimit(limit: number, urgency: Urgency): void {
    if (!this.channel.isClosed) this.channel._connection.sendByteLimit(this.channel, limit, urgency)
  }

  sendMessageLimit(limit: number, urgency: Urgency): void {
    if (!this.channel.isClosed) this.channel._connection.sendMessageLimit(this.channel, limit, urgency)
  }

  sendBdpPing(probe: number): void {
    this.channel._connection.sendBdpPing(this.channel, probe)
  }

  backlogBytes(): number | undefined {
    return this.channel._connection.bufferedAmount()
  }

  deliveredBefore(ms: number): number {
    return this.channel._connection.deliveredBefore(ms)
  }
}
