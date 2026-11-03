export { ChannelAttach }

import type { AttachState } from '../../wire/payloads.js'
import type { ReceiveCredit } from '../../flowControl/ReceiveCredit.js'
import type { SendCredit } from '../../flowControl/SendCredit.js'
import type { Uplink } from '../../flowControl/client/Uplink.js'

type ChannelAttachOptions = {
  channel: { readonly isClosed: boolean }
  sendCredit: SendCredit
  receiveCredit: ReceiveCredit
}

/** A page channel's credits as its connection drives them across wires: what the channel's RECONCILE entry carries,
 *  the replay buffers' caps, and a wire turning to batch POSTs or joining an upgrade. */
class ChannelAttach {
  private readonly channel: { readonly isClosed: boolean }
  private readonly sendCredit: SendCredit
  readonly receiveCredit: ReceiveCredit
  /** A broadcast's subscriptions, which its entry declares in place of a probe. */
  private subscriptions: { readonly text: boolean; readonly binary: boolean } | null = null

  constructor({ channel, sendCredit, receiveCredit }: ChannelAttachOptions) {
    this.channel = channel
    this.sendCredit = sendCredit
    this.receiveCredit = receiveCredit
  }

  /** The channel's entry in the RECONCILE sent each time it attaches to `uplink`'s wire: a probe of a path it hasn't
   *  measured, or a broadcast's subscriptions. */
  state(uplink: Uplink): AttachState {
    if (this.subscriptions !== null) return { broadcast: this.subscriptions }
    const probe = this.receiveCredit.probeAttach(uplink.roundTrips)
    return probe === undefined ? {} : { probe }
  }

  /** A broadcast declares its subscriptions on every (re)attach: a SUB or UNSUB written to a dead wire isn't replayed.
   *  `subscriptions` is read as each RECONCILE is built. */
  declareSubscriptions(subscriptions: { readonly text: boolean; readonly binary: boolean }): void {
    this.subscriptions = subscriptions
  }

  /** Caps the credit windows at what the replay buffers allow: `window` for what the page grants, `peerWindow` for
   *  what the server grants it. */
  fitReplays(window: number, peerWindow: number): void {
    this.receiveCredit.fitReplay(window)
    this.sendCredit.fitReplay(peerWindow)
  }

  /** The wire's streaming upload failed after the channel opened, so its frames now go in batch POSTs. */
  onBatched(): void {
    if (!this.channel.isClosed) this.receiveCredit.useBatchTransportInitial()
  }

  /** The upgrade joined: the channel is on the WebSocket and has already received the frames the join held. */
  onUpgradeJoined(): void {
    if (this.channel.isClosed) return
    this.receiveCredit.undoBatchTransportInitial()
    this.receiveCredit.restartRound()
  }
}
