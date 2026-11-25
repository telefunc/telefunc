export { LossyDelivery }
export type { LossyHost }

import { WIRE_SEND_AHEAD_MIN_BYTES } from '../../constants.js'
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
  /** `unit` waits: what it holds for its page, until `letGo`. */
  hold(unit: U): void
  letGo(unit: U): void
  /** Its sends that a newer one takes the place of go. */
  dropReplaceable(): void
  /** Its page granted no more credit for what it sends. */
  outOfCredit(): boolean
  /** The bytes `unit` holds. */
  bytes(unit: U): number
}

/** A page that keeps up gets every send. One that is behind gets the newest once it catches up, so that older ones don't
 *  fill its link first. Before its first rate, what waits waits in order. */
class LossyDelivery<U> {
  private readonly host: LossyHost<U>
  /** The sends waiting for its page, oldest first: past one only before its page confirmed a rate. */
  private waiting: U[] = []

  constructor(host: LossyHost<U>) {
    this.host = host
  }

  /** The newest send, while it waits for its page. */
  get held(): U | null {
    return this.waiting.at(-1) ?? null
  }

  /** The bytes the sends waiting for its page hold. */
  get waitingBytes(): number {
    let bytes = 0
    for (const unit of this.waiting) bytes += this.host.bytes(unit)
    return bytes
  }

  /** With no wire, which only a subscriber's publishes meet, `unit` goes as the replay's newest; behind, or out of
   *  credit, it waits as the newest, and takes the place of the sends its host lets it replace. Before its page
   *  confirmed a rate, past what any sender sends ahead, it waits behind those waiting. Otherwise it goes. */
  send(unit: U): void {
    if (!this.host.peer() || this.replaces()) this.letWaitingGo()
    // Those that went gave their credit back, and may have been all its page left unconfirmed: this one may go at once.
    if (this.waits()) {
      this.waiting.push(unit)
      this.host.hold(unit)
      return
    }
    this.flush()
    this.host.hand(unit)
  }

  /** Those waiting go now, oldest first. */
  flush(): void {
    const waiting = this.waiting
    this.waiting = []
    for (const unit of waiting) {
      this.host.hand(unit)
      this.host.letGo(unit)
    }
  }

  /** Its page confirmed more, or granted more: those waiting go, or, behind with them, only the newest. */
  mayGo(): void {
    if (this.waiting.length > 1 && this.isBehind(this.waitingBytes)) this.keepNewest()
    if (this.waiting.length > 0 && !this.holdsBack()) this.flush()
  }

  /** The sends a newer one takes the place of go, and those that wait. */
  letWaitingGo(): void {
    this.host.dropReplaceable()
    this.dispose()
  }

  /** Those that wait go, unsent. */
  dispose(): void {
    const waiting = this.waiting
    this.waiting = []
    for (const unit of waiting) this.host.letGo(unit)
  }

  private keepNewest(): void {
    const newest = this.waiting.pop()
    this.dispose()
    if (newest !== undefined) this.waiting.push(newest)
  }

  /** Whether a send made now waits: others wait, or its page can't take it now. */
  waits(): boolean {
    return this.waiting.length > 0 || this.holdsBack()
  }

  /** Whether its page can't take a send now: one replaces, or it holds all it may before its first rate. */
  private holdsBack(): boolean {
    return this.replaces() || this.isUnmeasuredFull()
  }

  /** Whether a send made now takes the place of those its host lets it replace: behind, or out of credit. */
  replaces(): boolean {
    return this.isBehind() || this.host.outOfCredit()
  }

  /** Whether its page, sent `waiting` bytes more, confirms later or holds more than one that keeps up. */
  private isBehind(waiting = 0): boolean {
    const peer = this.host.peer()
    const replay = this.host.replay()
    if (peer === null || !replay) return false
    const behindAfter = peer.sender.behindAfter(replay.leastConfirmMs)
    const since = replay.unconfirmedSince
    if (since !== undefined && performance.now() - since > behindAfter) return true
    // A newer send would wait behind all the wire took past the publish its page takes now.
    const rate = replay.confirmRate
    return rate > 0 && (replay.publishBytesBehindOldest + waiting) / rate > behindAfter
  }

  /** Before its first rate, whether it holds more than any sender sends ahead of the publish it takes now. */
  private isUnmeasuredFull(): boolean {
    const replay = this.host.replay()
    if (this.host.peer() === null || !replay) return false
    return replay.confirmRate === 0 && replay.publishBytesBehindOldest > WIRE_SEND_AHEAD_MIN_BYTES
  }
}
