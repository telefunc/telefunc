export { ServerReplay }

import { assert } from '../../../utils/assert.js'
import { HELD_SEND_OVERHEAD_BYTES, hold, letGo, type MemoryHolder } from './channelMemory.js'
import type { PublishBody } from './PublishBody.js'

type Publish = { head: Uint8Array<ArrayBuffer>; body: PublishBody; at: number }

/** What a channel sent and its page hasn't confirmed, for a reconnect to send again: its own frames, and its publishes,
 *  each a head and a reference to the body its subscribers share. Each queue is in seq order and loses entries only at
 *  its ends: acknowledged ones from the oldest, publishes skipped or past a gap from the newest. */
class ServerReplay {
  private readonly frames = new SeqQueue<Uint8Array<ArrayBuffer>>()
  private readonly publishes = new SeqQueue<Publish>()
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
    this.pushed(seq)
    this.frames.push(seq, frame)
    hold(this.holder, frame.byteLength + HELD_SEND_OVERHEAD_BYTES)
  }

  pushPublish(seq: number, head: Uint8Array<ArrayBuffer>, body: PublishBody): void {
    this.pushed(seq)
    this.publishes.push(seq, { head, body, at: performance.now() })
    hold(this.holder, body)
  }

  /** The page has every frame through `lastSeq`, which a reconnect never asks for again: they go. */
  acknowledge(lastSeq: number): void {
    while (this.frames.length > 0 && this.frames.seqAt(0) <= lastSeq) this.letFrameGo(this.frames.shift())
    while (this.publishes.length > 0 && this.publishes.seqAt(0) <= lastSeq) this.letPublishGo(this.publishes.shift())
  }

  /** The frames after `afterSeq` in seq order, each publish's head and then its body. A channel's are all there, as
   *  nothing enters unadmitted and nothing leaves unacknowledged; what a public subscriber sheds its page sees in
   *  `info.seq`. */
  getAfter(afterSeq: number): Uint8Array<ArrayBuffer>[] {
    const out: Uint8Array<ArrayBuffer>[] = []
    let f = this.frames.indexAfter(afterSeq)
    let p = this.publishes.indexAfter(afterSeq)
    while (f < this.frames.length || p < this.publishes.length) {
      if (p >= this.publishes.length || (f < this.frames.length && this.frames.seqAt(f) < this.publishes.seqAt(p))) {
        out.push(this.frames.at(f++))
        continue
      }
      const { head, body } = this.publishes.at(p++)
      out.push(head, body.frame)
    }
    return out
  }

  /** Its newest `droppable` publishes go, for a newer one to take their place: the page's `info.seq` shows what it
   *  skipped. Receipts stay. Returns how many went. The older ones are on the wire, which a reconnect may have to send
   *  again. */
  dropNewestPublishes(droppable: number): number {
    const dropping = Math.min(this.publishes.length, droppable)
    for (let i = 0; i < dropping; i++) this.letPublishGo(this.publishes.pop())
    return dropping
  }

  /** For a page that may miss none: where its holder holds more than `toBytes`, and one of its newest `droppable`
   *  publishes waits (any at 0; above 0, where more than one waits), the oldest of them goes with every frame after it,
   *  receipts too, so nothing reaches the page past the gap. Returns how many publishes went. */
  dropFromGap(toBytes: number, droppable: number): number {
    const waiting = Math.min(this.publishes.length, droppable)
    const mayGo = toBytes === 0 ? waiting : waiting - 1
    if (mayGo <= 0 || this.holder.heldBytes <= toBytes) return 0
    const gap = this.publishes.seqAt(this.publishes.length - waiting)
    while (this.frames.length > 0 && this.frames.seqAt(this.frames.length - 1) >= gap)
      this.letFrameGo(this.frames.pop())
    for (let i = 0; i < waiting; i++) this.letPublishGo(this.publishes.pop())
    return waiting
  }

  /** When the oldest publish its page hasn't confirmed was sent, `undefined` with none. */
  get oldestPublishAt(): number | undefined {
    return this.publishes.length > 0 ? this.publishes.at(0).at : undefined
  }

  /** The body of the newest publish it holds. */
  newestPublish(): PublishBody | undefined {
    return this.publishes.length > 0 ? this.publishes.at(this.publishes.length - 1).body : undefined
  }

  dispose(): void {
    while (this.frames.length > 0) this.letFrameGo(this.frames.pop())
    while (this.publishes.length > 0) this.letPublishGo(this.publishes.pop())
  }

  private pushed(seq: number): void {
    assert(seq <= this.issuedSeq && seq >= this.pushedSeq)
    this.pushedSeq = seq
  }

  private letFrameGo(frame: Uint8Array<ArrayBuffer>): void {
    letGo(this.holder, frame.byteLength + HELD_SEND_OVERHEAD_BYTES)
  }

  private letPublishGo({ body }: Publish): void {
    letGo(this.holder, body)
  }
}

/** Entries in seq order that leave only at the ends, as `channel/client/ReplayBuffer.ts`'s queues: a slot left at the
 *  oldest end holds nothing, and the slots go once they are as many as the live ones, which keeps a shift amortised
 *  O(1). Indices count from the oldest live entry. */
class SeqQueue<T> {
  // Seqs in an array of their own, so a scan by seq reads only numbers.
  private seqs: number[] = []
  private items: (T | undefined)[] = []
  private head = 0

  get length(): number {
    return this.items.length - this.head
  }

  seqAt(i: number): number {
    return this.seqs[this.head + i]!
  }

  at(i: number): T {
    return this.items[this.head + i]!
  }

  /** The index of the oldest entry after `seq`, `length` where none is. */
  indexAfter(seq: number): number {
    let i = 0
    while (i < this.length && this.seqAt(i) <= seq) i++
    return i
  }

  push(seq: number, item: T): void {
    this.seqs.push(seq)
    this.items.push(item)
  }

  shift(): T {
    const item = this.items[this.head]!
    this.items[this.head++] = undefined
    if (this.head >= this.items.length) this.clear()
    else if (this.head >= this.length) {
      this.seqs = this.seqs.slice(this.head)
      this.items = this.items.slice(this.head)
      this.head = 0
    }
    return item
  }

  pop(): T {
    this.seqs.pop()
    const item = this.items.pop()!
    if (this.head >= this.items.length) this.clear()
    return item
  }

  private clear(): void {
    this.seqs.length = 0
    this.items.length = 0
    this.head = 0
  }
}
