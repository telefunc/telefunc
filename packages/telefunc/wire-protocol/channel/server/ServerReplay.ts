export { ServerReplay }

import { replayKindOf } from '../../wire/tags.js'
import { assert } from '../../../utils/assert.js'
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
  private heldBytes = 0
  private readonly maxBytes: number
  private issuedSeq = 0
  private pushedSeq = 0

  constructor(maxBytes: number) {
    this.maxBytes = maxBytes
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
    this.heldBytes += frame.byteLength
    this.trim()
  }

  pushPublish(seq: number, head: Uint8Array<ArrayBuffer>, body: PublishBody): void {
    this.add(seq, head, body)
    this.heldBytes += head.byteLength + body.frame.byteLength
    this.trim()
  }

  /** The page has every frame through `lastSeq`, which a reconnect never asks for again: they go. */
  acknowledge(lastSeq: number): void {
    for (let i = this.first; i < this.seqs.length && this.seqs[i]! <= lastSeq; i++) this.drop(i)
    this.compact()
  }

  /** The frames after `afterSeq`, each publish's head and then its body, or null if it dropped one of them. */
  getAfter(afterSeq: number): Uint8Array<ArrayBuffer>[] | null {
    const frames: Uint8Array<ArrayBuffer>[] = []
    let entries = 0
    for (let i = this.first; i < this.seqs.length; i++) {
      const frame = this.frames[i]
      if (this.seqs[i]! <= afterSeq || !frame) continue
      entries++
      frames.push(frame)
      const body = this.bodies[i]
      if (body) frames.push(body.frame)
    }
    return entries >= this.pushedSeq - afterSeq ? frames : null
  }

  dispose(): void {
    this.seqs = []
    this.frames = []
    this.bodies = []
    this.first = 0
    this.dropped = 0
    this.heldBytes = 0
    this.issuedSeq = 0
    this.pushedSeq = 0
  }

  private add(seq: number, frame: Uint8Array<ArrayBuffer>, body: PublishBody | null): void {
    assert(seq <= this.issuedSeq && seq >= this.pushedSeq)
    this.pushedSeq = seq
    this.seqs.push(seq)
    this.frames.push(frame)
    this.bodies.push(body)
  }

  /** The oldest go past the budget, but for the frame that ends a channel. */
  private trim(): void {
    if (this.heldBytes <= this.maxBytes) return
    for (let i = this.first; i < this.seqs.length && this.heldBytes > this.maxBytes; i++) {
      const frame = this.frames[i]
      if (frame && replayKindOf(frame[0]!) !== 'closing') this.drop(i)
    }
    this.compact()
  }

  private drop(i: number): void {
    const frame = this.frames[i]
    if (!frame) return
    this.heldBytes -= frame.byteLength + (this.bodies[i]?.frame.byteLength ?? 0)
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
