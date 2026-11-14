export { ServerReplay }

import { assert } from '../../../utils/assert.js'
import { HELD_SEND_OVERHEAD_BYTES, hold, letGo, type MemoryHolder } from './channelMemory.js'
import type { PublishBody } from './PublishBody.js'
import { SeqQueue } from '../SeqQueue.js'

/** `alone`: sent with nothing of its channel's unconfirmed, publish or frame, so nothing waits ahead of its confirmation. */
type Publish = { head: Uint8Array<ArrayBuffer>; body: PublishBody; at: number; alone: boolean }

/** What a channel sent and its page hasn't confirmed, for a reconnect to send again: its own frames, and its publishes,
 *  each a head and a reference to the body its subscribers share. Each queue is in seq order and loses entries only at
 *  its ends: acknowledged ones from the oldest, publishes skipped or past a gap from the newest. */
class ServerReplay {
  private readonly frames = new SeqQueue<Uint8Array<ArrayBuffer>>()
  private readonly publishes = new SeqQueue<Publish>()
  private readonly holder: MemoryHolder
  private issuedSeq = 0
  private pushedSeq = 0
  /** When its page last began moving to a wire: what it hadn't confirmed then is confirmed over that wire. */
  private movedAt = 0

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
    const alone = this.publishes.length === 0 && this.frames.length === 0
    this.publishes.push(seq, { head, body, at: performance.now(), alone })
    hold(this.holder, body)
  }

  /** The least time a publish waited for its page's confirmation, `Infinity` before the first. One sent with nothing
   *  unconfirmed ahead of it waited for the path alone, so its confirmation sets it, also where the path got slower; one
   *  sent behind others, publishes or frames, only lowers it. */
  leastConfirmMs = Number.POSITIVE_INFINITY

  /** The page has every frame through `lastSeq`, which a reconnect never asks for again: they go. */
  acknowledge(lastSeq: number): void {
    const frames = this.frames.indexAfter(lastSeq)
    for (let i = 0; i < frames; i++) this.letFrameGo(this.frames.at(i))
    this.frames.dropOldest(frames)
    const publishes = this.publishes.indexAfter(lastSeq)
    if (publishes === 0) return
    // Only the oldest can be alone: the others were sent while it was unconfirmed.
    const alone = this.publishes.at(0).alone
    const newest = this.publishes.at(publishes - 1)
    for (let i = 0; i < publishes; i++) this.letPublishGo(this.publishes.at(i))
    this.publishes.dropOldest(publishes)
    // Sent before the wire its confirmation came on: its page may have had it long before.
    if (newest.at < this.movedAt) return
    const confirmMs = performance.now() - newest.at
    this.leastConfirmMs = alone ? confirmMs : Math.min(this.leastConfirmMs, confirmMs)
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
   *  skipped. Receipts stay. Returns how many went, and their bodies' bytes. The older ones are on the wire, which a
   *  reconnect may have to send again. */
  dropNewestPublishes(droppable: number): { units: number; bytes: number } {
    const units = Math.min(this.publishes.length, droppable)
    let bytes = 0
    for (let i = 0; i < units; i++) {
      const publish = this.publishes.pop()
      bytes += publish.body.frame.byteLength
      this.letPublishGo(publish)
    }
    return { units, bytes }
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

  /** Its page began moving to a wire, or attached one: what it hasn't confirmed is confirmed over that wire. */
  moving(): void {
    this.movedAt = performance.now()
  }

  /** Since when the oldest publish its page hasn't confirmed waits for that: since it was sent, or since its page began
   *  moving to the wire that confirms it. `undefined` with none. */
  get unconfirmedSince(): number | undefined {
    return this.publishes.length > 0 ? Math.max(this.publishes.at(0).at, this.movedAt) : undefined
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
