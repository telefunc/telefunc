export { ServerReplay }
export type { Dropped }

import { assert } from '../../../utils/assert.js'
import { HELD_SEND_OVERHEAD_BYTES, heldPublishBytes, hold, letGo, type MemoryHolder } from './channelMemory.js'
import type { PublishBody } from './PublishBody.js'
import { SeqQueue } from '../SeqQueue.js'

/** `alone`: sent with nothing of its channel's unconfirmed, publish or frame, so nothing waits ahead of its confirmation. */
type Publish = { head: Uint8Array<ArrayBuffer>; body: PublishBody; at: number; alone: boolean }
/** The publishes that went, by seq, oldest first, and their bodies' bytes. */
type Dropped = { seqs: number[]; bytes: number }

/** What a channel sent and its page hasn't confirmed, for a reconnect to send again: its own frames, and its publishes,
 *  each a head and a reference to the body its subscribers share. Each queue is in seq order: acknowledged entries leave
 *  from its oldest end, publishes past a gap from its newest, and skipped ones from among the newest. */
class ServerReplay {
  private readonly frames = new SeqQueue<Uint8Array<ArrayBuffer>>()
  private readonly publishes = new SeqQueue<Publish>()
  private readonly holder: MemoryHolder
  private issuedSeq = 0
  private pushedSeq = 0
  /** When its page last began moving to a wire: what it hadn't confirmed then is confirmed over that wire. */
  private movedAt = 0
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
    this.frames.push(seq, frame)
    hold(this.holder, frame.byteLength + HELD_SEND_OVERHEAD_BYTES)
  }

  pushPublish(seq: number, head: Uint8Array<ArrayBuffer>, body: PublishBody): void {
    this.pushed(seq)
    const alone = this.publishes.length === 0 && this.frames.length === 0
    this.publishes.push(seq, { head, body, at: performance.now(), alone })
    hold(this.holder, body)
    const { stream } = body
    if (body.lossy && stream !== null) {
      this.lossyHeld ??= new Map()
      this.lossyHeld.set(stream, (this.lossyHeld.get(stream) ?? 0) + 1)
    }
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

  /** What its newest `count` publishes hold. */
  heldByNewestPublishes(count: number): number {
    let held = 0
    for (let i = Math.max(0, this.publishes.length - count); i < this.publishes.length; i++)
      held += heldPublishBytes(this.publishes.at(i).body)
    return held
  }

  /** Its newest `droppable` publishes go, for a newer one to take their place: the page's `info.seq` shows what it
   *  skipped. Receipts stay. The older ones are on the wire, which a reconnect may have to send again. */
  dropUnhanded(droppable: number): Dropped {
    const dropped: Dropped = { seqs: [], bytes: 0 }
    for (let i = Math.min(this.publishes.length, droppable); i > 0; i--) {
      dropped.seqs.push(this.publishes.seqAt(this.publishes.length - 1))
      const publish = this.publishes.pop()
      dropped.bytes += publish.body.frame.byteLength
      this.letPublishGo(publish)
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
    this.walkNewest(droppable, (index, { body }, owed) => {
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
    this.walkNewest(droppable, (index, { body: { stream } }, owed) => {
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

  /** Walks its newest `count` publishes from the newest. `owed`: its page must get it, as a plain one, or as a lossy one
   *  that a plain one of its stream followed. `visit` returns false to stop. */
  private walkNewest(count: number, visit: (index: number, publish: Publish, owed: boolean) => boolean | void): void {
    let followed: Set<string> | null = null
    const oldest = Math.max(0, this.publishes.length - count)
    for (let i = this.publishes.length - 1; i >= oldest; i--) {
      const publish = this.publishes.at(i)
      const { stream, lossy } = publish.body
      const owed = !lossy || stream === null || followed?.has(stream) === true
      if (!lossy && stream !== null) (followed ??= new Set()).add(stream)
      if (visit(i, publish, owed) === false) return
    }
  }

  /** Whether more than one publish its page is owed is among its newest `waiting`, and what it holds without the
   *  replaceable ones among them is over `toBytes`. */
  private owedOver(toBytes: number, waiting: number): boolean {
    if (this.lossyHeld === null) return waiting > 1
    let owedCount = 0
    let replaceableBytes = 0
    this.walkNewest(waiting, (_index, { body }, owed) => {
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
      const publish = this.publishes.removeAt(index)
      dropped.bytes += publish.body.frame.byteLength
      this.letPublishGo(publish)
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

  private letPublishGo({ body }: Publish): void {
    letGo(this.holder, body)
    const { stream } = body
    if (!body.lossy || stream === null || this.lossyHeld === null) return
    const held = this.lossyHeld.get(stream) ?? 0
    if (held > 1) this.lossyHeld.set(stream, held - 1)
    else this.lossyHeld.delete(stream)
  }
}
