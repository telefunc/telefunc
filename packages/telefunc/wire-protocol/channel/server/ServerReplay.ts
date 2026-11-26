export { ServerReplay }
export type { Dropped }

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
import { PublishQueue } from './PublishQueue.js'
import { SeqQueue } from '../SeqQueue.js'
import { DrainRate } from '../../flowControl/DrainRate.js'
import { EVERY_STREAM } from '../ownStream.js'

/** When a channel's unconfirmed data was dated, at a loss of its page's wire. */
type Stall = { since: number }
/** The publishes that went, by seq, oldest first, and their bodies' bytes. */
type Dropped = { seqs: number[]; bytes: number }

/** What a channel sent and its page hasn't confirmed, for a reconnect to send again: its own frames, and its publishes,
 *  each a channel index and a reference to the body its subscribers share. Each queue is in seq order: acknowledged
 *  entries leave from its oldest end, publishes past a gap from its newest, and skipped ones from among the newest. */
class ServerReplay {
  private readonly frames = new SeqQueue<Uint8Array<ArrayBuffer>>()
  private readonly publishes = new PublishQueue()
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
  /** How many lossy publishes of each stream it holds, from its first lossy publish on: until then every publish is
   *  owed, and nothing below walks. */
  private lossyHeld: Map<string, number> | null = null

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
    this.publishes.push(seq, index, body, performance.now(), alone, this.confirmedBytes)
    this.heldPublishBytes += body.frame.byteLength
    hold(this.holder, body)
    const { stream } = body
    if (body.lossy && stream !== null) {
      this.lossyHeld ??= new Map()
      this.lossyHeld.set(stream, (this.lossyHeld.get(stream) ?? 0) + 1)
    }
  }

  /** The quickest confirmation and the bytes it covered: a lone publish's sets them, any other's only lowers them. */
  private leastConfirmAtMs = Number.POSITIVE_INFINITY
  private leastConfirmBytes = 0

  /** The least a publish waits for its page's confirmation, its own transfer aside: `Infinity` before the first. */
  get leastConfirmMs(): number {
    if (!Number.isFinite(this.leastConfirmAtMs)) return Number.POSITIVE_INFINITY
    const rate = this.confirms.rate
    return rate > 0 ? Math.max(0, this.leastConfirmAtMs - this.leastConfirmBytes / rate) : this.leastConfirmAtMs
  }

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
    const alone = this.publishes.wasAlone(0)
    const newest = publishes - 1
    const sentAt = this.publishes.timeAt(newest)
    const confirmedBefore = this.publishes.confirmedBefore(newest)
    const hold = heldMs > 0 && this.publishes.seqAt(newest) === lastSeq ? heldMs : 0
    for (let i = 0; i < publishes; i++) {
      const body = this.publishes.bodyAt(i)
      this.confirmedBytes += body.frame.byteLength
      this.letPublishGo(body)
    }
    this.publishes.dropOldest(publishes)
    // Sent before the wire its confirmation came on: its page may have had it long before.
    if (sentAt < this.movedAt) return
    const confirmMs = Math.max(1, performance.now() - sentAt - hold)
    const bytes = this.confirmedBytes - confirmedBefore
    const lone = publishes === 1 && alone
    // Both hold the round trip: past the quickest by half its bytes or more, what lies between is the rate.
    if (
      !lone &&
      Number.isFinite(this.leastConfirmAtMs) &&
      bytes - this.leastConfirmBytes >= this.leastConfirmBytes / 2 &&
      confirmMs > this.leastConfirmAtMs
    )
      this.confirms.measured((bytes - this.leastConfirmBytes) / (confirmMs - this.leastConfirmAtMs))
    if (lone || confirmMs < this.leastConfirmAtMs) {
      this.leastConfirmAtMs = confirmMs
      this.leastConfirmBytes = bytes
    }
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
      out.push(encode.publishTo(this.publishes.indexAt(p), this.publishes.seqAt(p)), this.publishes.bodyAt(p).frame)
      p++
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
      held += heldPublishBytes(this.publishes.bodyAt(i))
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
   *  skipped. Receipts stay. The older ones are on the wire, which a reconnect may have to send again. */
  dropUnhanded(droppable: number): Dropped {
    const dropped: Dropped = { seqs: [], bytes: 0 }
    for (let i = Math.min(this.publishes.length, droppable); i > 0; i--) {
      dropped.seqs.push(this.publishes.seqAt(this.publishes.length - 1))
      const body = this.publishes.pop()
      dropped.bytes += body.frame.byteLength
      this.letPublishGo(body)
    }
    dropped.seqs.reverse()
    return dropped
  }

  /** Replacement. Of its newest `droppable`, the lossy publishes of `stream` that no plain one of the stream followed go:
   *  its page's `info.seq` shows what it skipped. */
  dropLossyOf(stream: string, droppable: number): Dropped {
    // Nothing of the stream older than its oldest lossy publish can go, so the walk ends there.
    let lossyLeft = this.lossyHeld?.get(stream) ?? 0
    if (lossyLeft === 0) return { seqs: [], bytes: 0 }
    const goes: number[] = []
    this.walkNewest(droppable, (index, body, owed) => {
      if (body.stream !== stream) return
      // A plain publish of the stream, or a lossy one before it: every older one of the stream is owed too.
      if (owed) return false
      goes.push(index)
      return --lossyLeft > 0
    })
    return this.take(goes)
  }

  /** A close. Of its newest `droppable`, each stream keeps its newest lossy publish and those its page is owed; the
   *  lossy ones a newer one of their stream made stale go. */
  dropStaleLossy(droppable: number): Dropped {
    const newestKept = new Set<string>()
    const goes: number[] = []
    this.walkNewest(droppable, (index, { stream }, owed) => {
      if (owed || stream === null) return
      if (newestKept.has(stream)) goes.push(index)
      else newestKept.add(stream)
    })
    return this.take(goes)
  }

  /** Shedding. Where its holder holds more than `toBytes` and one of its newest `droppable` publishes would have to go
   *  (any at 0; above 0, one its page is owed), the oldest of them goes with every frame after it, receipts too, so
   *  nothing reaches the page past the gap. Returns how many publishes went. */
  dropFromGap(toBytes: number, droppable: number): number {
    const waiting = Math.min(this.publishes.length, droppable)
    if (waiting === 0 || this.holder.heldBytes <= toBytes) return 0
    if (toBytes > 0 && !this.owedOver(toBytes, waiting)) return 0
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
    return this.publishes.length > 0 ? Math.max(this.publishes.timeAt(0), this.movedAt) : undefined
  }

  /** The publish bytes its page hasn't confirmed behind the oldest, which it may be taking now. */
  get publishBytesBehindOldest(): number {
    return this.publishes.length > 0 ? this.heldPublishBytes - this.publishes.bodyAt(0).frame.byteLength : 0
  }

  /** The rate its page confirms publishes at, in bytes per ms: 0 before a confirmation measured it. */
  get confirmRate(): number {
    return this.confirms.rate
  }

  /** The body of the newest publish it holds. */
  newestPublish(): PublishBody | undefined {
    return this.publishes.length > 0 ? this.publishes.bodyAt(this.publishes.length - 1) : undefined
  }

  dispose(): void {
    this.stallDate = null
    while (this.frames.length > 0) this.letFrameGo(this.frames.pop())
    while (this.publishes.length > 0) this.letPublishGo(this.publishes.pop())
  }

  /** Walks its newest `count` publishes from the newest. `owed`: its page must get it, as a plain one, or as a lossy one
   *  that a plain one of its stream, or of every stream, followed. `visit` returns false to stop. */
  private walkNewest(count: number, visit: (index: number, body: PublishBody, owed: boolean) => boolean | void): void {
    let followed: Set<string> | null = null
    const oldest = Math.max(0, this.publishes.length - count)
    for (let i = this.publishes.length - 1; i >= oldest; i--) {
      const body = this.publishes.bodyAt(i)
      const { stream, lossy } = body
      const owed =
        !lossy || stream === null || (followed !== null && (followed.has(stream) || followed.has(EVERY_STREAM)))
      if (!lossy && stream !== null) (followed ??= new Set()).add(stream)
      if (visit(i, body, owed) === false) return
    }
  }

  /** Whether more than one publish its page is owed is among its newest `waiting`, and what it holds without the
   *  replaceable ones among them is over `toBytes`. */
  private owedOver(toBytes: number, waiting: number): boolean {
    if (this.lossyHeld === null) return waiting > 1
    let owedCount = 0
    let replaceableBytes = 0
    this.walkNewest(waiting, (_index, body, owed) => {
      if (owed) owedCount++
      else replaceableBytes += body.frame.byteLength
    })
    return owedCount > 1 && this.holder.heldBytes - replaceableBytes > toBytes
  }

  /** Takes out the publishes at `indices`, newest first. */
  private take(indices: readonly number[]): Dropped {
    const dropped: Dropped = { seqs: [], bytes: 0 }
    for (const index of indices) {
      dropped.seqs.push(this.publishes.seqAt(index))
      const body = this.publishes.removeAt(index)
      dropped.bytes += body.frame.byteLength
      this.letPublishGo(body)
    }
    dropped.seqs.reverse()
    return dropped
  }

  private pushed(seq: number): void {
    assert(seq <= this.issuedSeq && seq >= this.pushedSeq)
    this.pushedSeq = seq
  }

  private letFrameGo(frame: Uint8Array<ArrayBuffer>): void {
    letGo(this.holder, frame.byteLength + HELD_SEND_OVERHEAD_BYTES)
  }

  private letPublishGo(body: PublishBody): void {
    this.heldPublishBytes -= body.frame.byteLength
    letGo(this.holder, body)
    const { stream } = body
    if (!body.lossy || stream === null || this.lossyHeld === null) return
    const held = this.lossyHeld.get(stream) ?? 0
    if (held > 1) this.lossyHeld.set(stream, held - 1)
    else this.lossyHeld.delete(stream)
  }
}
