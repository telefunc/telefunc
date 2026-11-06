export { ServerReplay }

import { assert } from '../../../utils/assert.js'
import { hold, letGo, type MemoryHolder } from './channelMemory.js'
import { HELD_SEND_OVERHEAD_BYTES } from './ServerChannelBuffer.js'
import type { PublishBody } from './PublishBody.js'

/** What a channel sent and its page hasn't confirmed, oldest first, for a reconnect to send again. A publish holds its
 *  head and a reference to the body its subscribers share. */
class ServerReplay {
  // In parallel arrays, so a scan by seq reads only numbers.
  private seqs: number[] = []
  /** Each entry's frame, or a publish's head; null once dropped, so that nothing keeps its bytes. */
  private frames: (Uint8Array<ArrayBuffer> | null)[] = []
  private bodies: (PublishBody | null)[] = []
  /** Where the live entries start, and how many are dropped past that. */
  private first = 0
  private dropped = 0
  /** The live publishes. */
  private publishes = 0
  private readonly holder: MemoryHolder
  private issuedSeq = 0
  private pushedSeq = 0

  constructor(holder: MemoryHolder) {
    this.holder = holder
  }

  /** The last seq handed out (see `nextSeq`). */
  get seq(): number {
    return this.issuedSeq
  }

  nextSeq(): number {
    return ++this.issuedSeq
  }

  push(seq: number, frame: Uint8Array<ArrayBuffer>): void {
    this.add(seq, frame, null)
    hold(this.holder, frame.byteLength + HELD_SEND_OVERHEAD_BYTES)
  }

  pushPublish(seq: number, head: Uint8Array<ArrayBuffer>, body: PublishBody): void {
    this.add(seq, head, body)
    hold(this.holder, body)
    this.publishes++
  }

  /** The page has every frame through `lastSeq`, which a reconnect never asks for again: they go. */
  acknowledge(lastSeq: number): void {
    for (let i = this.first; i < this.seqs.length && this.seqs[i]! <= lastSeq; i++) this.drop(i)
    this.compact()
  }

  /** The frames after `afterSeq`, each publish's head and then its body. A channel's are all there, as nothing enters
   *  unadmitted and nothing leaves unacknowledged; what a public subscriber sheds its page sees in `info.seq`. */
  getAfter(afterSeq: number): Uint8Array<ArrayBuffer>[] {
    const frames: Uint8Array<ArrayBuffer>[] = []
    for (let i = this.first; i < this.seqs.length; i++) {
      const frame = this.frames[i]
      if (this.seqs[i]! <= afterSeq || !frame) continue
      frames.push(frame)
      const body = this.bodies[i]
      if (body) frames.push(body.frame)
    }
    return frames
  }

  /** Of its newest `droppable` publishes, the oldest go until its holder holds `toBytes`, but for the newest of all above 0:
   *  a later message then shows the page what it missed. Returns how many went. The older ones are on the wire, which a
   *  reconnect may have to send again. */
  dropOldestPublishes(toBytes: number, droppable: number): number {
    const waiting = Math.min(this.publishes, droppable)
    let pinned = this.publishes - waiting
    const mayGo = toBytes === 0 ? waiting : waiting - 1
    let dropped = 0
    for (let i = this.first; i < this.seqs.length && dropped < mayGo && this.holder.heldBytes > toBytes; i++) {
      if (!this.bodies[i]) continue
      if (pinned > 0) {
        pinned--
        continue
      }
      this.drop(i)
      dropped++
    }
    this.compact()
    return dropped
  }

  /** The body of the newest publish it holds. */
  newestPublish(): PublishBody | undefined {
    for (let i = this.seqs.length - 1; i >= this.first; i--) {
      const body = this.bodies[i]
      if (body) return body
    }
    return undefined
  }

  dispose(): void {
    for (let i = this.first; i < this.seqs.length; i++) this.drop(i)
  }

  private add(seq: number, frame: Uint8Array<ArrayBuffer>, body: PublishBody | null): void {
    assert(seq <= this.issuedSeq && seq >= this.pushedSeq)
    this.pushedSeq = seq
    this.seqs.push(seq)
    this.frames.push(frame)
    this.bodies.push(body)
  }

  private drop(i: number): void {
    const frame = this.frames[i]
    if (!frame) return
    const body = this.bodies[i]
    if (body) {
      letGo(this.holder, body)
      this.publishes--
    } else letGo(this.holder, frame.byteLength + HELD_SEND_OVERHEAD_BYTES)
    this.frames[i] = null
    this.bodies[i] = null
    this.dropped++
  }

  /** Forgets the dropped entries once they are as many as the live ones, which keeps a drop amortised O(1). */
  private compact(): void {
    while (this.first < this.seqs.length && this.frames[this.first] === null) {
      this.first++
      this.dropped--
    }
    const live = this.seqs.length - this.first - this.dropped
    if (this.first + this.dropped < live + 16) return
    const keep = (_: unknown, i: number) => i >= this.first && this.frames[i] !== null
    this.seqs = this.seqs.filter(keep)
    this.bodies = this.bodies.filter(keep)
    this.frames = this.frames.filter(keep)
    this.first = 0
    this.dropped = 0
  }
}
