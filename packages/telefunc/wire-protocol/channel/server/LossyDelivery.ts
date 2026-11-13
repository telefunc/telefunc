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
  /** Its sends that no wire has taken go. */
  dropUnhanded(): void
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

  /** With no wire, `unit` goes as the replay's newest; behind, or out of credit, it waits as the newest. */
  send(unit: U): void {
    if (!this.host.peer() || this.mustWait()) this.letWaitingGo()
    if (this.mustWait()) {
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
    if (this.slot !== null && !this.mustWait()) this.flush()
  }

  /** The sends that no wire has taken go, and the newest that waits, as a newer one takes their place. */
  letWaitingGo(): void {
    this.host.dropUnhanded()
    this.dispose()
  }

  /** The newest that waits goes, unsent. */
  dispose(): void {
    if (this.slot === null) return
    this.host.letGo(this.slot)
    this.slot = null
  }

  private mustWait(): boolean {
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
