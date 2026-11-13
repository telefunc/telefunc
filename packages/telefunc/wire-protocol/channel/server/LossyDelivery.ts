export { LossyDelivery }
export type { LossyHost }

import type { IndexedPeer } from './IndexedPeer.js'
import type { ServerReplay } from './ServerReplay.js'

/** What a channel gives the delivery of its lossy sends. `U` is one send: a broadcast's publish body. */
interface LossyHost<U> {
  /** The page's channel on the wire the page is attached on; null while it is away. */
  peer(): IndexedPeer | null
  replay(): ServerReplay | null
  /** Sends `unit` to its page. */
  hand(unit: U): void
  /** `unit` waits as the newest: what it holds for its page, until `letGo`. */
  hold(unit: U): void
  letGo(unit: U): void
  /** Its sends that no wire has taken go. Returns how many. */
  dropUnhanded(): number
  /** It let go of sends its page hadn't received. */
  skipped(): void
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

  /** With no wire, `unit` goes as the replay's newest; behind, it waits as the newest. */
  send(unit: U): void {
    if (!this.host.peer() || this.isBehind()) this.letWaitingGo()
    if (this.isBehind()) {
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

  /** Its page confirmed more: the newest goes where the page caught up. */
  mayGo(): void {
    if (this.slot !== null && !this.isBehind()) this.flush()
  }

  /** The sends that no wire has taken go, and the newest that waits, as a newer one takes their place. */
  letWaitingGo(): void {
    const dropped = this.host.dropUnhanded()
    const held = this.slot !== null
    this.dispose()
    if (held || dropped > 0) this.host.skipped()
  }

  /** The newest that waits goes, unsent. */
  dispose(): void {
    if (this.slot === null) return
    this.host.letGo(this.slot)
    this.slot = null
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
