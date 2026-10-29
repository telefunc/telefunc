import { replayLaneOf, type ReplayLaneKind } from '../wire/connectionFrame.js'
import { assert } from '../../utils/assert.js'

type ReplayBufferOptions = {
  maxBytes: number
  binaryMaxBytes: number
}

/** Sent sequenced frames till their peer acks them, in lanes by tag; a data lane evicts its oldest past its budget. */
export class ReplayBuffer {
  /** Each lane, from its first frame on. */
  private readonly lanes: Partial<Record<ReplayLaneKind, ReplayLane>> = {}
  private readonly budgets: Record<ReplayLaneKind, number>
  private _seq = 0
  /** The highest seq pushed. */
  private pushedSeq = 0
  /** The highest seq the peer acknowledged. */
  private acknowledgedSeq = 0
  /** Lower budgets than the lanes', which apply once the peer acknowledged every seq through `throughSeq`. */
  private lowered: { budgets: Record<ReplayLaneKind, number>; throughSeq: number } | null = null

  /** Current sequence number. */
  get seq(): number {
    return this._seq
  }

  constructor({ maxBytes, binaryMaxBytes }: ReplayBufferOptions) {
    this.budgets = laneBudgets(maxBytes, binaryMaxBytes)
  }

  /** A budget lower than a lane's applies once the peer has every seq issued before: they flew under the higher one. */
  setLimits(maxBytes: number, binaryMaxBytes: number): void {
    const budgets = laneBudgets(maxBytes, binaryMaxBytes)
    const unacknowledged = this.acknowledgedSeq < this._seq
    let lowered = false
    for (const kind of LANE_KINDS) {
      if (unacknowledged && budgets[kind] < this.budgets[kind]) lowered = true
      this.setBudget(kind, unacknowledged ? Math.max(budgets[kind], this.budgets[kind]) : budgets[kind])
    }
    this.lowered = lowered ? { budgets, throughSeq: this.lowered?.throughSeq ?? this._seq } : null
  }

  /** Increment and return the next sequence number. */
  nextSeq(): number {
    return ++this._seq
  }

  /** Stores a frame, its seq no less than the last pushed, in its lane; none larger than the lane's budget. */
  push(seq: number, frame: Uint8Array<ArrayBuffer>): void {
    assert(seq <= this._seq)
    assert(seq >= this.pushedSeq)
    this.pushedSeq = seq
    const kind = replayLaneOf(frame[0]!)
    const lane = (this.lanes[kind] ??= new ReplayLane(this.budgets[kind]))
    lane.push(seq, frame)
  }

  /** The peer has every frame through `lastSeq`, which a reconnect never asks for again: they go. */
  acknowledge(lastSeq: number): void {
    // Nothing new: its lanes dropped through it, and a lowered budget waits for a later seq.
    if (lastSeq <= this.acknowledgedSeq) return
    this.acknowledgedSeq = lastSeq
    for (const kind of LANE_KINDS) this.lanes[kind]?.acknowledge(lastSeq)
    if (this.lowered === null || lastSeq < this.lowered.throughSeq) return
    const { budgets } = this.lowered
    this.lowered = null
    for (const kind of LANE_KINDS) this.setBudget(kind, budgets[kind])
  }

  /** The frames with afterSeq < seq <= throughSeq, by seq, or null if it dropped one of them. */
  getAfter(afterSeq: number, throughSeq = Infinity): Uint8Array<ArrayBuffer>[] | null {
    let run: Run = { seqs: [], frames: [] }
    for (const kind of LANE_KINDS) {
      const lane = this.lanes[kind]
      if (lane) run = mergeBySeq(run, lane.getAfter(afterSeq, throughSeq))
    }
    // Every seq through the highest pushed was pushed, each to one lane, so one missing was dropped.
    return run.frames.length >= Math.min(throughSeq, this.pushedSeq) - afterSeq ? run.frames : null
  }

  get length(): number {
    let length = 0
    for (const kind of LANE_KINDS) length += this.lanes[kind]?.length ?? 0
    return length
  }

  get byteLength(): number {
    let byteLength = 0
    for (const kind of LANE_KINDS) byteLength += this.lanes[kind]?.byteLength ?? 0
    return byteLength
  }

  dispose(): void {
    for (const kind of LANE_KINDS) this.lanes[kind]?.dispose()
    this._seq = 0
    this.pushedSeq = 0
    this.acknowledgedSeq = 0
    this.lowered = null
  }

  private setBudget(kind: ReplayLaneKind, budget: number): void {
    this.budgets[kind] = budget
    this.lanes[kind]?.setBudget(budget)
  }
}

const LANE_KINDS: readonly ReplayLaneKind[] = ['text', 'binary', 'closing']

/** A lane's byte budget by kind: the frame that ends a channel is small, and none is dropped for size. */
function laneBudgets(maxBytes: number, binaryMaxBytes: number): Record<ReplayLaneKind, number> {
  return { text: maxBytes, binary: binaryMaxBytes, closing: Infinity }
}

type Run = { seqs: number[]; frames: Uint8Array<ArrayBuffer>[] }

/** Merges two sorted runs by their stored seq values. */
function mergeBySeq(a: Run, b: Run): Run {
  if (a.frames.length === 0) return b
  if (b.frames.length === 0) return a
  const length = a.frames.length + b.frames.length
  const seqs: number[] = new Array(length)
  const frames: Uint8Array<ArrayBuffer>[] = new Array(length)
  let ai = 0
  let bi = 0
  for (let ri = 0; ri < length; ri++) {
    if (bi >= b.frames.length || (ai < a.frames.length && a.seqs[ai]! <= b.seqs[bi]!)) {
      seqs[ri] = a.seqs[ai]!
      frames[ri] = a.frames[ai++]!
    } else {
      seqs[ri] = b.seqs[bi]!
      frames[ri] = b.frames[bi++]!
    }
  }
  return { seqs, frames }
}

/** Bounded FIFO lane with parallel arrays and amortised O(1) compaction. */
class ReplayLane {
  // Parallel arrays — seqs separate for cache-friendly access
  private seqs: number[] = []
  private frames: Uint8Array<ArrayBuffer>[] = []
  private head = 0
  private totalBytes = 0
  private maxBytes: number

  constructor(maxBytes: number) {
    this.maxBytes = maxBytes
  }

  get length(): number {
    return this.frames.length - this.head
  }

  get byteLength(): number {
    return this.totalBytes
  }

  /** Stores an encoded frame; none larger than the budget. */
  push(seq: number, frame: Uint8Array<ArrayBuffer>): void {
    const bytes = frame.byteLength
    if (bytes > this.maxBytes) return
    this.seqs.push(seq)
    this.frames.push(frame)
    this.totalBytes += bytes
    this.trim()
  }

  acknowledge(lastSeq: number): void {
    while (this.head < this.frames.length && this.seqs[this.head]! <= lastSeq) this.dropHead()
    this.compact()
  }

  setBudget(maxBytes: number): void {
    this.maxBytes = maxBytes
    this.trim()
  }

  /** The stored frames with afterSeq < seq <= throughSeq. */
  getAfter(afterSeq: number, throughSeq: number): Run {
    const len = this.frames.length
    let lo = this.head
    while (lo < len && this.seqs[lo]! <= afterSeq) lo++
    let hi = lo
    while (hi < len && this.seqs[hi]! <= throughSeq) hi++
    return { seqs: this.seqs.slice(lo, hi), frames: this.frames.slice(lo, hi) }
  }

  dispose(): void {
    this.seqs.length = 0
    this.frames.length = 0
    this.head = 0
    this.totalBytes = 0
  }

  // ── Private ──

  /** Evicts the oldest entries while over the byte budget. */
  private trim(): void {
    while (this.head < this.frames.length && this.totalBytes > this.maxBytes) this.dropHead()
    this.compact()
  }

  /** Drops the oldest entry, leaving its slot for `compact`. */
  private dropHead(): void {
    this.totalBytes -= this.frames[this.head]!.byteLength
    this.head++
  }

  /** Compact when dead zone ≥ live zone (amortised O(1)). */
  private compact(): void {
    if (this.head >= this.frames.length) {
      this.seqs.length = 0
      this.frames.length = 0
      this.head = 0
      this.totalBytes = 0
      return
    }
    if (this.head > 0 && this.head >= this.frames.length - this.head) {
      this.seqs = this.seqs.slice(this.head)
      this.frames = this.frames.slice(this.head)
      this.head = 0
    }
  }
}
