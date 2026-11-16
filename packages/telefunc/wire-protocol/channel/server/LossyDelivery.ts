export { LossyDelivery }
export type { LossyHost }

import type { IndexedPeer } from './IndexedPeer.js'
import type { ServerReplay } from './ServerReplay.js'

/** What a channel gives the delivery of its lossy sends. `U` is one send: a broadcast's publish body, or a channel's
 *  own send. */
interface LossyHost<U> {
  /** The page's channel on the wire the page is attached on; null while it is away. */
  peer(): IndexedPeer | null
  replay(): ServerReplay | null
  /** Sends `unit` to its page. */
  hand(unit: U): void
  /** `unit` waits as the newest: what it holds for its page, until `letGo`. */
  hold(unit: U): void
  letGo(unit: U): void
  /** Its sends that a newer one takes the place of go. */
  dropReplaceable(): void
  /** Its page granted no more credit for what it sends. */
  outOfCredit(): boolean
}

/** A page that keeps up gets every send. One that is behind gets the newest once it catches up, so that older ones don't
 *  fill its link first. */
class LossyDelivery<U> {
  private readonly host: LossyHost<U>
  /** The newest send, waiting for its page to catch up: only a newer one replaces it. */
  private slot: U | null = null

  constructor(host: LossyHost<U>) {
    this.host = host
  }

  /** The newest send, while it waits for its page. */
  get held(): U | null {
    return this.slot
  }

  /** With no wire, which only a subscriber's publishes meet, `unit` goes as the replay's newest; behind, or out of
   *  credit, it waits as the newest. In both it takes the place of the sends its host lets it replace; otherwise it
   *  goes. */
  send(unit: U): void {
    if (!this.host.peer() || this.waits()) this.letWaitingGo()
    // Those that went gave their credit back, and may have been all its page left unconfirmed: this one may go at once.
    if (this.waits()) {
      this.slot = unit
      this.host.hold(unit)
      return
    }
    this.flush()
    this.host.hand(unit)
  }

  /** The newest goes now, where one waits. */
  flush(): void {
    const unit = this.slot
    if (unit === null) return
    this.slot = null
    this.host.hand(unit)
    this.host.letGo(unit)
  }

  /** Its page confirmed more, or granted more: the newest goes where it no longer has to wait. */
  mayGo(): void {
    if (this.slot !== null && !this.waits()) this.flush()
  }

  /** The sends a newer one takes the place of go, and the newest that waits. */
  letWaitingGo(): void {
    this.host.dropReplaceable()
    this.dispose()
  }

  /** The newest that waits goes, unsent. */
  dispose(): void {
    if (this.slot === null) return
    this.host.letGo(this.slot)
    this.slot = null
  }

  /** Whether a send made now waits, and takes the place of the sends its host lets it replace: its page is behind, or out
   *  of credit. */
  waits(): boolean {
    return this.isBehind() || this.host.outOfCredit()
  }

  /** Whether its page has an older send to confirm than a page that keeps up would. Before its first confirmation it
   *  isn't: that comes within a round trip of an idle link, and it measures the round trip. */
  private isBehind(): boolean {
    const peer = this.host.peer()
    const replay = this.host.replay()
    if (peer === null || !replay || replay.unconfirmedSince === undefined) return false
    return performance.now() - replay.unconfirmedSince > peer.sender.behindAfter(replay.leastConfirmMs)
  }
}
