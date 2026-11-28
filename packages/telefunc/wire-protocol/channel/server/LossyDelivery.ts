export { LossyDelivery }
export type { LossyHost }

import { WIRE_SEND_AHEAD_MIN_BYTES } from '../../constants.js'
import { nearBudget } from './channelMemory.js'
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
  /** Drops the replay entries a newer send of `stream` replaces and cancels those still waiting on the wire. */
  dropSuperseded(stream: string): void
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
 *  ones don't fill its link first. Before its first rate, what waits waits in order while memory has room. */
class LossyDelivery<U> {
  private readonly host: LossyHost<U>
  /** The sends waiting for its page, oldest first. */
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

  /** A page that is behind, out of credit, or short of memory keeps only the newest send of this stream waiting. */
  send(stream: string, unit: U, held = this.host.bytes(unit)): void {
    if (!this.host.peer() || this.replaces(held)) this.replace(stream)
    if (this.waiting.length > 0 || this.holdsBack() || this.memoryFull(held)) {
      this.host.hold(unit)
      this.waiting.push({ stream, unit })
      return
    }
    this.host.hand(unit)
  }

  /** Those waiting of `stream` go now; all of them without `stream`; oldest first. Behind, as a send then does, only
   *  each stream's newest goes, replacing superseded replay entries. */
  flush(stream?: string): void {
    let waiting = this.takeWaiting(stream)
    if (waiting.length > 0 && this.replaces()) {
      waiting = this.newestOfEach(waiting)
      for (const each of waiting) this.host.dropSuperseded(each.stream)
    }
    for (const { unit } of waiting) {
      this.host.letGo(unit)
      this.host.hand(unit)
    }
  }

  /** Its page confirmed more, or granted more: those waiting go, or, behind with them, only each stream's newest. */
  mayGo(): void {
    if (this.waiting.length > 1 && this.isBehind(this.waitingBytes)) this.waiting = this.newestOfEach(this.waiting)
    if (this.waiting.length === 0 || this.holdsBack() || this.memoryFull(0)) return
    for (const { unit } of this.takeWaiting(undefined)) {
      this.host.letGo(unit)
      this.host.hand(unit)
    }
  }

  /** The sends of `stream` that a newer one replaces go, and those of it that wait, as the newer one takes their place. */
  replace(stream: string): void {
    this.host.dropSuperseded(stream)
    this.dispose(stream)
  }

  /** Those of `stream` that wait go, unsent; all of them without `stream`. */
  dispose(stream?: string): void {
    for (const { unit } of this.takeWaiting(stream)) this.host.letGo(unit)
  }

  /** Whether a newer send takes the place of superseded sends. */
  replaces(held?: number): boolean {
    return this.isBehind() || this.host.outOfCredit() || (held !== undefined && this.memoryFull(held))
  }

  /** Includes superseded bytes the wire may still be sending, even after replay freed their payloads. */
  private memoryFull(held: number): boolean {
    if (this.host.peer() === null) return false
    const replay = this.host.replay()
    return replay?.unconfirmedSince !== undefined && nearBudget(held + replay.supersededPublishBytes)
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

  /** Of `waiting`, each stream's newest, in order: the others go, unsent. */
  private newestOfEach(waiting: readonly Waiting<U>[]): Waiting<U>[] {
    const newest = new Map<string, Waiting<U>>()
    for (const each of waiting) newest.set(each.stream, each)
    const kept: Waiting<U>[] = []
    for (const each of waiting) {
      if (newest.get(each.stream) === each) kept.push(each)
      else this.host.letGo(each.unit)
    }
    return kept
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
