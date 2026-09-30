import { payloadBytes, replayLaneOf, type ReplayLaneKind } from './shared-ws.js'

/**
 * High-performance replay buffer for outgoing WebSocket data frames.
 *
 * Stores encoded frames keyed by monotonic sequence number for replay on reconnect, until the peer acknowledges them,
 * however long that takes: its channel lets it go when it gives up on a peer away for `reconnectTimeout`. Bounded by
 * the bytes of their payloads, what flow control counts: oldest entries are evicted when full. A budget lowered while
 * the peer lacks frames sent under the higher one applies once the peer has them.
 *
 * A frame goes in the lane its tag names (`replayLaneOf`), each with its own byte budget, none for the frame that ends
 * a channel.
 *
 * A frame larger than its lane's budget isn't stored. `getAfter` gives a peer
 * all it lacks, or null if a dropped frame is among it.
 *
 * Requires non-decreasing seq values.
 */
export class ReplayBuffer {
  private readonly lanes: Record<ReplayLaneKind, ReplayLane>
  private readonly allLanes: readonly ReplayLane[]
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

  constructor(maxBytes: number, binaryMaxBytes: number) {
    const budgets = laneBudgets(maxBytes, binaryMaxBytes)
    this.lanes = {
      text: new ReplayLane(budgets.text),
      binary: new ReplayLane(budgets.binary),
      closing: new ReplayLane(budgets.closing),
    }
    this.allLanes = Object.values(this.lanes)
  }

  /** Applies new budgets to what is stored and to what comes next. One lower than a lane's applies once the peer has
   *  every seq issued before it was first set: flow control let them be in flight under the higher one. */
  setLimits(maxBytes: number, binaryMaxBytes: number): void {
    const budgets = laneBudgets(maxBytes, binaryMaxBytes)
    const unacknowledged = this.acknowledgedSeq < this._seq
    let lowered = false
    for (const kind of LANE_KINDS) {
      const lane = this.lanes[kind]
      if (unacknowledged && budgets[kind] < lane.budget) lowered = true
      lane.setBudget(unacknowledged ? Math.max(budgets[kind], lane.budget) : budgets[kind])
    }
    this.lowered = lowered ? { budgets, throughSeq: this.lowered?.throughSeq ?? this._seq } : null
  }

  /** Increment and return the next sequence number. */
  nextSeq(): number {
    return ++this._seq
  }

  /**
   * Store an already-encoded sequenced frame in its lane.
   * @returns `true` if the frame was stored, `false` if it was larger than its lane's budget.
   */
  push(seq: number, frame: Uint8Array<ArrayBuffer>): boolean {
    this.pushed(seq)
    return this.lanes[replayLaneOf(frame[0]!)].push(seq, frame)
  }

  /** The peer has every frame through `lastSeq`, which a reconnect never asks for again: they go. */
  acknowledge(lastSeq: number): void {
    if (lastSeq > this.acknowledgedSeq) this.acknowledgedSeq = lastSeq
    for (const lane of this.allLanes) lane.acknowledge(lastSeq)
    if (this.lowered === null || lastSeq < this.lowered.throughSeq) return
    const { budgets } = this.lowered
    this.lowered = null
    for (const kind of LANE_KINDS) this.lanes[kind].setBudget(budgets[kind])
  }

  /** The frames with afterSeq < seq <= throughSeq, merged by seq, or null if it dropped one of them to stay within its
   *  budget. */
  getAfter(afterSeq: number, throughSeq = Infinity): Uint8Array<ArrayBuffer>[] | null {
    let run: Run = { seqs: [], frames: [] }
    for (const lane of this.allLanes) run = mergeBySeq(run, lane.getAfter(afterSeq, throughSeq))
    // Every seq through the highest pushed was pushed, each to one lane, so one missing was dropped.
    return run.frames.length >= Math.min(throughSeq, this.pushedSeq) - afterSeq ? run.frames : null
  }

  get length(): number {
    let length = 0
    for (const lane of this.allLanes) length += lane.length
    return length
  }

  get byteLength(): number {
    let byteLength = 0
    for (const lane of this.allLanes) byteLength += lane.byteLength
    return byteLength
  }

  dispose(): void {
    for (const lane of this.allLanes) lane.dispose()
    this._seq = 0
    this.pushedSeq = 0
    this.acknowledgedSeq = 0
    this.lowered = null
  }

  // ── Private ──

  private pushed(seq: number): void {
    if (seq > this._seq) this._seq = seq
    if (seq > this.pushedSeq) this.pushedSeq = seq
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

  get budget(): number {
    return this.maxBytes
  }

  /**
   * Store an already-encoded frame.
   * @returns `true` if the frame was stored, `false` if it was larger than the budget.
   */
  push(seq: number, frame: Uint8Array<ArrayBuffer>): boolean {
    if (payloadBytes(frame) > this.maxBytes) return false
    this.seqs.push(seq)
    this.frames.push(frame)
    this.totalBytes += payloadBytes(frame)
    this.trim()
    return true
  }

  acknowledge(lastSeq: number): void {
    const head = this.head
    while (this.head < this.frames.length && this.seqs[this.head]! <= lastSeq) {
      this.totalBytes -= payloadBytes(this.frames[this.head]!)
      this.head++
    }
    if (this.head > head) this.compact()
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
    const head = this.head
    while (this.head < this.frames.length && this.totalBytes > this.maxBytes) {
      this.totalBytes -= payloadBytes(this.frames[this.head]!)
      this.head++
    }
    if (this.head > head) this.compact()
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
