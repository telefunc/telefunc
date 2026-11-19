export { ChannelAttach }

import type { AttachState, Want } from '../../wire/payloads.js'
import type { Readiness } from '../channel.js'
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
  readonly sendCredit: SendCredit
  readonly receiveCredit: ReceiveCredit
  /** A broadcast's want, which its entry declares beside its probe; replaced, never changed, so an entry keeps the want
   *  it was built with. */
  private want: Readonly<Want> | null = null
  /** The newest version of the want the server confirmed is in effect. */
  private confirmed = 0
  /** The listeners waiting for a version the server hasn't confirmed yet, each with that version. */
  private readonly confirming = new Map<Readiness, number>()

  constructor({ channel, sendCredit, receiveCredit }: ChannelAttachOptions) {
    this.channel = channel
    this.sendCredit = sendCredit
    this.receiveCredit = receiveCredit
  }

  /** The channel's entry in the RECONCILE sent each time it attaches to `uplink`'s wire: a probe of a path it hasn't
   *  measured, and a broadcast's want. A page of broadcasts alone measures its round trip so: a lossy one's
   *  `behindAfter` needs it. */
  state(uplink: Uplink): AttachState {
    const probe = this.receiveCredit.probeAttach(uplink.roundTrips)
    return {
      ...(this.want !== null && { want: this.want }),
      ...(probe !== undefined && { probe }),
    }
  }

  /** A broadcast declares its want on every (re)attach: a WANT written to a dead wire isn't replayed. */
  get wants(): Readonly<Want> | null {
    return this.want
  }

  /** The want, where the server hasn't confirmed it yet: a telefunction call carries it. */
  get unconfirmedWant(): Readonly<Want> | null {
    return this.want !== null && this.want.version > this.confirmed ? this.want : null
  }

  /** Turns the want of `kind` on or off at the next version, and returns the new want. */
  changeWant(kind: 'text' | 'binary', on: boolean): Readonly<Want> {
    const { version, text, binary } = this.want ?? { version: 0, text: false, binary: false }
    this.want = { version: version + 1, text, binary, [kind]: on }
    return this.want
  }

  /** A listener is in effect once the server confirmed the want at `version`, or a newer one. */
  waitFor(readiness: Readiness, version: number): void {
    if (version <= this.confirmed) readiness.settle(true)
    else this.confirming.set(readiness, version)
  }

  /** The listener was removed: it waits no more, and settles with `outcome`. */
  stopWaiting(readiness: Readiness, outcome: true | Error): void {
    this.confirming.delete(readiness)
    readiness.settle(outcome)
  }

  confirmWant(version: number): void {
    if (version <= this.confirmed) return
    this.confirmed = version
    for (const [readiness, waitsFor] of this.confirming) {
      if (waitsFor > version) continue
      this.confirming.delete(readiness)
      readiness.settle(true)
    }
  }

  /** The channel ended: no want of it will take effect. */
  failWants(err: Error): void {
    for (const readiness of this.confirming.keys()) readiness.settle(err)
    this.confirming.clear()
  }

  /** Caps both credit windows at what the server's `maxMemory` allows. */
  capWindows(window: number): void {
    this.receiveCredit.capWindow(window)
    this.sendCredit.capWindow(window)
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
