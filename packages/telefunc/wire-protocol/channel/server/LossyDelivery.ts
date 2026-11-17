export { LossyDelivery }
export type { LossyHost }

import type { IndexedPeer } from './IndexedPeer.js'
import type { ServerReplay } from './ServerReplay.js'

/** What a channel gives the delivery of its lossy sends. `U` is one send: a broadcast's publish body, or a channel's
 *  own send, each of a stream. */
interface LossyHost<U> {
  /** The page's channel on the wire the page is attached on; null while it is away. */
  peer(): IndexedPeer | null
  replay(): ServerReplay | null
  /** Sends `unit` to its page. */
  hand(unit: U): void
  /** `unit` waits as its stream's newest: what it holds for its page, until `letGo`. */
  hold(unit: U): void
  letGo(unit: U): void
  /** Its sends of `stream` that a newer one replaces go, of those no wire has taken: where the hosts' rules differ. */
  dropUnhanded(stream: string): void
  /** Its page granted no more credit for what it sends. */
  outOfCredit(): boolean
}

/** A page that keeps up gets every send. One that is behind gets each stream's newest once it catches up, so that older
 *  ones don't fill its link first. */
class LossyDelivery<U> {
  private readonly host: LossyHost<U>
  /** Each stream's newest send, waiting for its page to catch up, in the order they began to wait: only a newer one of
   *  its stream replaces it. */
  private readonly waiting = new Map<string, U>()

  constructor(host: LossyHost<U>) {
    this.host = host
  }

  /** The one that began to wait last, of whichever stream. */
  get newestHeld(): U | undefined {
    let newest: U | undefined
    for (const unit of this.waiting.values()) newest = unit
    return newest
  }

  /** With no wire, which only a subscriber's publishes meet, `unit` goes as the replay's newest; behind, or out of
   *  credit, it waits as its stream's newest. In both it takes the place of those of its stream that no wire has taken;
   *  otherwise it goes. */
  send(stream: string, unit: U): void {
    if (!this.host.peer() || this.waits()) this.letWaitingGo(stream)
    // Those that went gave their credit back, and may have been all its page left unconfirmed: this one may go at once.
    if (this.waits()) {
      this.waiting.set(stream, unit)
      this.host.hold(unit)
      return
    }
    this.flush()
    this.host.hand(unit)
  }

  /** The waiting one of `stream` goes now; all of them without `stream`, in the order they began to wait. */
  flush(stream?: string): void {
    if (stream !== undefined) return this.handWaiting(stream)
    for (const waiting of this.waiting.keys()) this.handWaiting(waiting)
  }

  /** Its page confirmed more, or granted more: those waiting go where they no longer have to wait. */
  mayGo(): void {
    if (this.waiting.size > 0 && !this.waits()) this.flush()
  }

  /** The sends of `stream` that a newer one replaces go, and the one that waits, as the newer one takes their place. */
  letWaitingGo(stream: string): void {
    this.host.dropUnhanded(stream)
    this.dispose(stream)
  }

  /** The one of `stream` that waits goes, unsent; all of them without `stream`. */
  dispose(stream?: string): void {
    for (const [waiting, unit] of this.waiting) {
      if (stream !== undefined && waiting !== stream) continue
      this.waiting.delete(waiting)
      this.host.letGo(unit)
    }
  }

  /** Whether a send made now waits, and takes the place of those of its stream that no wire has taken: its page is
   *  behind, or out of credit. */
  waits(): boolean {
    return this.isBehind() || this.host.outOfCredit()
  }

  private handWaiting(stream: string): void {
    const unit = this.waiting.get(stream)
    if (unit === undefined) return
    this.waiting.delete(stream)
    this.host.hand(unit)
    this.host.letGo(unit)
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
