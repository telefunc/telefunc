export { ServerReplay }

import { assert } from '../../../utils/assert.js'
import {
  HELD_SEND_OVERHEAD_BYTES,
  hasRoomFor,
  heldPublishBytes,
  hold,
  letGo,
  type MemoryHolder,
} from './channelMemory.js'
import { encode } from '../../wire/frame.js'
import { isClosingTag } from '../../wire/tags.js'
import type { PublishBody } from './PublishBody.js'
import { SeqQueue } from '../SeqQueue.js'
import { DrainRate } from '../../flowControl/DrainRate.js'

/** When a channel's unconfirmed data was dated, at a loss of its page's wire. */
type Stall = { since: number }

/** `alone`: sent with nothing of its channel's unconfirmed, publish or frame, so nothing waits ahead of its confirmation. */
type Publish = { index: number; body: PublishBody; at: number; alone: boolean; confirmedBefore: number }

/** What a channel sent and its page hasn't confirmed, for a reconnect to send again: its own frames, and its publishes,
 *  each a channel index and a reference to the body its subscribers share. Each queue is in seq order and loses entries only at
 *  its ends: acknowledged ones from the oldest, publishes skipped or past a gap from the newest. */
class ServerReplay {
  private readonly frames = new SeqQueue<Uint8Array<ArrayBuffer>>()
  private readonly publishes = new SeqQueue<Publish>()
  private readonly holder: MemoryHolder
  private issuedSeq = 0
  private pushedSeq = 0
  /** The seq of an end it sent without keeping it, `maxMemory` having no room for it; 0 once its page has it. */
  private unkeptEnd = 0
  /** When its page last began moving to a wire: what it hadn't confirmed then is confirmed over that wire. */
  private movedAt = 0
  private stallDate: Stall | null = null
  /** The bytes its page confirmed, frames and publishes, and the publish bytes it holds unconfirmed. */
  private confirmedBytes = 0
  private heldPublishBytes = 0
  /** The rate its page confirms at: a publish that went behind others measures what crossed ahead of it, over what its
   *  confirmation took past the least. */
  private readonly confirms = new DrainRate()

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
    const bytes = frame.byteLength + HELD_SEND_OVERHEAD_BYTES
    // An end that doesn't fit goes unkept: a page that misses it gets a replay loss in its place (`lacksEndAfter`).
    const tag = frame[0]
    if (tag !== undefined && isClosingTag(tag) && !hasRoomFor(bytes)) {
      this.unkeptEnd = seq
      return
    }
    this.frames.push(seq, frame)
    hold(this.holder, bytes)
  }

  /** Whether a page that has every frame through `lastSeq` lacks an end it didn't keep. */
  lacksEndAfter(lastSeq: number): boolean {
    return this.unkeptEnd > lastSeq
  }

  pushPublish(seq: number, index: number, body: PublishBody): void {
    this.pushed(seq)
    const alone = this.publishes.length === 0 && this.frames.length === 0
    this.publishes.push(seq, { index, body, at: performance.now(), alone, confirmedBefore: this.confirmedBytes })
    this.heldPublishBytes += body.frame.byteLength
    hold(this.holder, body)
  }

  /** The least time a publish waited for its page's confirmation, `Infinity` before the first. One sent with nothing
   *  unconfirmed ahead of it waited for the path alone, so its confirmation sets it, also where the path got slower; one
   *  sent behind others, publishes or frames, only lowers it. */
  leastConfirmMs = Number.POSITIVE_INFINITY

  /** Since when its page hasn't confirmed what it holds, across the page's losses of its wire; null while the page
   *  confirms something of it after each. */
  get stall(): Stall | null {
    return this.stallDate
  }

  /** Whether it holds something its page hasn't confirmed. */
  get unconfirmed(): boolean {
    return this.frames.length > 0 || this.publishes.length > 0
  }

  /** Its page's wire went at `at`: what the page hasn't confirmed is dated from then, unless it is already. */
  dateStall(at: number): void {
    if (this.stallDate === null && this.unconfirmed) this.stallDate = { since: at }
  }

  /** The page has every frame through `lastSeq`, which a reconnect never asks for again: they go. */
  acknowledge(lastSeq: number, heldMs = 0): void {
    if (lastSeq >= this.unkeptEnd) this.unkeptEnd = 0
    const frames = this.frames.indexAfter(lastSeq)
    for (let i = 0; i < frames; i++) {
      this.confirmedBytes += this.frames.at(i).byteLength
      this.letFrameGo(this.frames.at(i))
    }
    this.frames.dropOldest(frames)
    const publishes = this.publishes.indexAfter(lastSeq)
    if (frames > 0 || publishes > 0) this.stallDate = null
    if (publishes === 0) return
    // Only the oldest can be alone: the others were sent while it was unconfirmed.
    const alone = this.publishes.at(0).alone
    const newest = this.publishes.at(publishes - 1)
    const hold = heldMs > 0 && this.publishes.seqAt(publishes - 1) === lastSeq ? heldMs : 0
    for (let i = 0; i < publishes; i++) {
      this.confirmedBytes += this.publishes.at(i).body.frame.byteLength
      this.letPublishGo(this.publishes.at(i))
    }
    this.publishes.dropOldest(publishes)
    // Sent before the wire its confirmation came on: its page may have had it long before.
    if (newest.at < this.movedAt) return
    const confirmMs = Math.max(1, performance.now() - newest.at - hold)
    // One that went alone, or before any least, took a round trip that no rate is in.
    if (!(publishes === 1 && alone) && this.leastConfirmMs !== Number.POSITIVE_INFINITY)
      this.confirms.measured(
        (this.confirmedBytes - newest.confirmedBefore) / Math.max(1, confirmMs - this.leastConfirmMs),
      )
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
      const { index, body } = this.publishes.at(p)
      out.push(encode.publishTo(index, this.publishes.seqAt(p++)), body.frame)
    }
    return out
  }

  /** It holds nothing: its page confirmed all it sent, or what it didn't went unsent. */
  get isEmpty(): boolean {
    return this.frames.length === 0 && this.publishes.length === 0 && this.unkeptEnd === 0
  }

  /** Whether all it holds is frames `test` passes, no publish among them. */
  holdsOnly(test: (frame: Uint8Array<ArrayBuffer>) => boolean): boolean {
    if (this.publishes.length > 0) return false
    for (let i = 0; i < this.frames.length; i++) if (!test(this.frames.at(i))) return false
    return true
  }

  /** What its newest `count` publishes hold. */
  heldByNewestPublishes(count: number): number {
    let held = 0
    for (let i = Math.max(0, this.publishes.length - count); i < this.publishes.length; i++)
      held += heldPublishBytes(this.publishes.at(i).body)
    return held
  }

  /** How many of its newest `count` publishes came after its newest frame. A frame its page confirmed is gone, and so is
   *  every publish before it. */
  sinceFrame(count: number): number {
    const frame = this.frames.length > 0 ? this.frames.seqAt(this.frames.length - 1) : 0
    const publishes = this.publishes
    let since = 0
    while (since < count && since < publishes.length && publishes.seqAt(publishes.length - 1 - since) > frame) since++
    return since
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

  /** Of its newest `count` publishes, all but the newest go: the page's `info.seq` shows what it skipped. */
  dropBeforeNewestPublish(count: number): void {
    const waiting = Math.min(this.publishes.length, count)
    if (waiting < 2) return
    const seq = this.publishes.seqAt(this.publishes.length - 1)
    const newest = this.publishes.pop()
    for (let i = 1; i < waiting; i++) this.letPublishGo(this.publishes.pop())
    this.publishes.push(seq, newest)
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

  /** The publish bytes its page hasn't confirmed behind the oldest, which it may be taking now. */
  get publishBytesBehindOldest(): number {
    return this.publishes.length > 0 ? this.heldPublishBytes - this.publishes.at(0).body.frame.byteLength : 0
  }

  /** The rate its page confirms publishes at, in bytes per ms: 0 before a confirmation measured it. */
  get confirmRate(): number {
    return this.confirms.rate
  }

  /** The body of the newest publish it holds. */
  newestPublish(): PublishBody | undefined {
    return this.publishes.length > 0 ? this.publishes.at(this.publishes.length - 1).body : undefined
  }

  dispose(): void {
    this.stallDate = null
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
    this.heldPublishBytes -= body.frame.byteLength
    letGo(this.holder, body)
  }
}
