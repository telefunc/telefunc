export { LossyDelivery }
export type { LossyHost }

import { WIRE_SEND_AHEAD_MIN_BYTES } from '../../constants.js'
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
  /** `unit` waits: what it holds for its page, until `letGo`. */
  hold(unit: U): void
  letGo(unit: U): void
  /** Its sends of `stream` that a newer one replaces go, of those no wire has taken: where the hosts' rules differ. */
  dropUnhanded(stream: string): void
  /** Its page granted no more credit for what it sends. */
  outOfCredit(): boolean
  /** The bytes `unit` holds. */
  bytes(unit: U): number
}

/** A send waiting for its page, and the stream it is of. */
interface Waiting<U> {
  stream: string
  unit: U
}

/** A page that keeps up gets every send. One that is behind gets each stream's newest once it catches up, so that older
 *  ones don't fill its link first. Before its first rate, what waits waits in order. */
class LossyDelivery<U> {
  private readonly host: LossyHost<U>
  /** The sends waiting for its page, oldest first: past one of a stream only before its page confirmed a rate. */
  private waiting: Waiting<U>[] = []

  constructor(host: LossyHost<U>) {
    this.host = host
  }

  /** The one that began to wait last, of whichever stream. */
  get newestHeld(): U | undefined {
    return this.waiting.at(-1)?.unit
  }

  /** The bytes the sends waiting for its page hold. */
  get waitingBytes(): number {
    let bytes = 0
    for (const { unit } of this.waiting) bytes += this.host.bytes(unit)
    return bytes
  }

  /** With no wire, which only a subscriber's publishes meet, `unit` goes as the replay's newest; behind, or out of
   *  credit, it waits as its stream's newest, and takes the place of those of its stream that no wire has taken. Before
   *  its page confirmed a rate, past what any sender sends ahead, it waits behind those waiting. Otherwise it goes. */
  send(stream: string, unit: U): void {
    if (!this.host.peer() || this.replaces()) this.letWaitingGo(stream)
    // Those that went gave their credit back, and may have been all its page left unconfirmed: this one may go at once.
    if (this.waits()) {
      this.host.hold(unit)
      this.waiting.push({ stream, unit })
      return
    }
    this.flush()
    this.host.hand(unit)
  }

  /** Those waiting of `stream` go now; all of them without `stream`; oldest first. */
  flush(stream?: string): void {
    for (const { unit } of this.takeWaiting(stream)) {
      this.host.hand(unit)
      this.host.letGo(unit)
    }
  }

  /** Its page confirmed more, or granted more: those waiting go, or, behind with them, only each stream's newest. */
  mayGo(): void {
    if (this.waiting.length > 1 && this.isBehind(this.waitingBytes)) this.keepNewest()
    if (this.waiting.length > 0 && !this.holdsBack()) this.flush()
  }

  /** The sends of `stream` that a newer one replaces go, and those of it that wait, as the newer one takes their place. */
  letWaitingGo(stream: string): void {
    this.host.dropUnhanded(stream)
    this.dispose(stream)
  }

  /** Those of `stream` that wait go, unsent; all of them without `stream`. */
  dispose(stream?: string): void {
    for (const { unit } of this.takeWaiting(stream)) this.host.letGo(unit)
  }

  /** Whether a send made now waits: others wait, or its page can't take it now. */
  waits(): boolean {
    return this.waiting.length > 0 || this.holdsBack()
  }

  /** Whether a send made now takes the place of those of its stream that no wire has taken: behind, or out of credit. */
  replaces(): boolean {
    return this.isBehind() || this.host.outOfCredit()
  }

  /** Takes out those waiting of `stream`, all of them without `stream`, oldest first. */
  private takeWaiting(stream: string | undefined): readonly Waiting<U>[] {
    const waiting = this.waiting
    if (waiting.length === 0) return waiting
    if (stream === undefined) {
      this.waiting = []
      return waiting
    }
    this.waiting = waiting.filter((each) => each.stream !== stream)
    return waiting.filter((each) => each.stream === stream)
  }

  /** Of each stream, only its newest keeps waiting. */
  private keepNewest(): void {
    const newest = new Map<string, Waiting<U>>()
    for (const each of this.waiting) newest.set(each.stream, each)
    const waiting = this.waiting
    this.waiting = []
    for (const each of waiting) {
      if (newest.get(each.stream) === each) this.waiting.push(each)
      else this.host.letGo(each.unit)
    }
  }

  /** Whether its page can't take a send now: one replaces, or it holds all it may before its first rate. */
  private holdsBack(): boolean {
    return this.replaces() || this.isUnmeasuredFull()
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
