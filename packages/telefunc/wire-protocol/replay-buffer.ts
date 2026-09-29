import { unrefTimer } from '../utils/unrefTimer.js'
import { ERROR_REASON, payloadBytes, replayLaneOf, type ReplayLaneKind, type ReplayLoss } from './shared-ws.js'

/**
 * High-performance replay buffer for outgoing WebSocket data frames.
 *
 * Stores encoded frames keyed by monotonic sequence number for replay on reconnect, until the peer acknowledges them.
 * Bounded by the bytes of their payloads, what flow control counts: oldest entries are evicted when full. Also bounded
 * by age: entries older than `maxAgeMs` are evicted.
 *
 * A frame goes in the lane its tag names (`replayLaneOf`), each with its own byte budget, and the frame that ends a
 * channel is dropped for its age alone.
 *
 * A frame larger than its lane's budget isn't stored. `getAfter` gives a peer
 * all it lacks, or, if a dropped frame is among it, why it can't.
 *
 * Requires non-decreasing seq values.
 */
export class ReplayBuffer {
  private readonly lanes: Record<ReplayLaneKind, ReplayLane>
  private readonly allLanes: readonly ReplayLane[]
  private maxAgeMs: number
  private _seq = 0
  /** The highest seq pushed. */
  private pushedSeq = 0
  private cleanupTimer: ReturnType<typeof setTimeout> | null = null
  private cleanupScheduledAt = Infinity

  /** Current sequence number. */
  get seq(): number {
    return this._seq
  }

  constructor(maxBytes: number, maxAgeMs: number, binaryMaxBytes: number) {
    const budgets = laneBudgets(maxBytes, binaryMaxBytes)
    this.lanes = {
      text: new ReplayLane(budgets.text, maxAgeMs),
      binary: new ReplayLane(budgets.binary, maxAgeMs),
      closing: new ReplayLane(budgets.closing, maxAgeMs),
    }
    this.allLanes = Object.values(this.lanes)
    this.maxAgeMs = maxAgeMs
  }

  /** Applies new budgets to what is stored and to what comes next. */
  setLimits(maxBytes: number, maxAgeMs: number, binaryMaxBytes: number): void {
    this.maxAgeMs = maxAgeMs
    const budgets = laneBudgets(maxBytes, binaryMaxBytes)
    for (const kind of Object.keys(this.lanes) as ReplayLaneKind[]) this.lanes[kind].setLimits(budgets[kind], maxAgeMs)
    this.scheduleCleanup()
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
    const stored = this.lanes[replayLaneOf(frame[0]!)].push(seq, frame)
    this.scheduleCleanup()
    return stored
  }

  /** The peer has every frame through `lastSeq`, which a reconnect never asks for again: they go. */
  acknowledge(lastSeq: number): void {
    for (const lane of this.allLanes) lane.acknowledge(lastSeq)
  }

  /** The frames with afterSeq < seq <= throughSeq, merged by seq, or why they can't all be given. */
  getAfter(afterSeq: number, throughSeq = Infinity): Uint8Array<ArrayBuffer>[] | ReplayLoss {
    let run: Run = { seqs: [], frames: [] }
    let overBudgetThrough = 0
    for (const lane of this.allLanes) {
      run = mergeBySeq(run, lane.getAfter(afterSeq, throughSeq))
      overBudgetThrough = Math.max(overBudgetThrough, lane.overBudgetThrough)
    }
    // Every seq through the highest pushed was pushed, each to one lane, so one missing was dropped.
    if (run.frames.length >= Math.min(throughSeq, this.pushedSeq) - afterSeq) return run.frames
    return overBudgetThrough > afterSeq ? ERROR_REASON.LOST : ERROR_REASON.EXPIRED
  }

  /**
   * Eagerly evict expired entries without pushing a new frame.
   * Call this periodically on idle channels to return memory promptly.
   */
  evict(now = Date.now()): void {
    for (const lane of this.allLanes) lane.evict(now)
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
    if (this.cleanupTimer !== null) {
      clearTimeout(this.cleanupTimer)
      this.cleanupTimer = null
      this.cleanupScheduledAt = Infinity
    }
  }

  // ── Private ──

  private pushed(seq: number): void {
    if (seq > this._seq) this._seq = seq
    if (seq > this.pushedSeq) this.pushedSeq = seq
  }

  /** Schedule cleanup for the oldest entry's expiry. Skips if a timer already
   *  fires at or before that deadline — new entries are always newer than existing
   *  ones (FIFO), so the oldest entry never moves earlier on push. */
  private scheduleCleanup(): void {
    let oldest = Infinity
    for (const lane of this.allLanes) oldest = Math.min(oldest, lane.oldestTime)
    if (oldest === Infinity) return

    const deadlineAt = oldest + this.maxAgeMs
    if (this.cleanupTimer !== null && this.cleanupScheduledAt <= deadlineAt) return

    if (this.cleanupTimer !== null) {
      clearTimeout(this.cleanupTimer)
      this.cleanupTimer = null
    }
    this.cleanupScheduledAt = deadlineAt
    const timer = setTimeout(
      () => {
        this.cleanupTimer = null
        this.cleanupScheduledAt = Infinity
        this.evict()
        this.scheduleCleanup()
      },
      Math.max(0, deadlineAt - Date.now()),
    )
    unrefTimer(timer)
    this.cleanupTimer = timer
  }
}

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
  private times: number[] = []
  /** The highest seq dropped to stay within the byte budget. */
  overBudgetThrough = 0
  private head = 0
  private totalBytes = 0
  private maxBytes: number
  private maxAgeMs: number

  constructor(maxBytes: number, maxAgeMs: number) {
    this.maxBytes = maxBytes
    this.maxAgeMs = maxAgeMs
  }

  /** Time of the oldest buffered entry, or Infinity if empty. */
  get oldestTime(): number {
    return this.head < this.times.length ? this.times[this.head]! : Infinity
  }

  get length(): number {
    return this.frames.length - this.head
  }

  get byteLength(): number {
    return this.totalBytes
  }

  /**
   * Store an already-encoded frame.
   * @returns `true` if the frame was stored, `false` if it was larger than the budget.
   */
  push(seq: number, frame: Uint8Array<ArrayBuffer>): boolean {
    const now = Date.now()

    if (payloadBytes(frame) > this.maxBytes) {
      this.overBudgetThrough = seq
      // Still evict by age, as the normal push path does.
      this._evict(now)
      return false
    }

    this.seqs.push(seq)
    this.frames.push(frame)
    this.times.push(now)
    this.totalBytes += payloadBytes(frame)
    this._evict(now)
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

  setLimits(maxBytes: number, maxAgeMs: number): void {
    this.maxBytes = maxBytes
    this.maxAgeMs = maxAgeMs
    this._evict(Date.now())
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

  /**
   * Eagerly evict expired entries without pushing a new frame.
   */
  evict(now: number): void {
    if (this.head >= this.frames.length) return
    const cutoff = now - this.maxAgeMs
    while (this.head < this.frames.length && this.times[this.head]! < cutoff) {
      this.totalBytes -= payloadBytes(this.frames[this.head]!)
      this.head++
    }
    this.compact()
  }

  dispose(): void {
    this.seqs.length = 0
    this.frames.length = 0
    this.times.length = 0
    this.head = 0
    this.totalBytes = 0
    this.overBudgetThrough = 0
  }

  // ── Private ──

  private _evict(now: number): void {
    // Single pass: evict entries that are too old OR push us over the byte budget.
    const cutoff = now - this.maxAgeMs
    while (this.head < this.frames.length) {
      const expired = this.times[this.head]! < cutoff
      if (!expired && this.totalBytes <= this.maxBytes) break
      if (!expired) this.overBudgetThrough = Math.max(this.overBudgetThrough, this.seqs[this.head]!)
      this.totalBytes -= payloadBytes(this.frames[this.head]!)
      this.head++
    }
    this.compact()
  }

  /** Compact when dead zone ≥ live zone (amortised O(1)). */
  private compact(): void {
    if (this.head >= this.frames.length) {
      this.seqs.length = 0
      this.frames.length = 0
      this.times.length = 0
      this.head = 0
      this.totalBytes = 0
      return
    }
    if (this.head > 0 && this.head >= this.frames.length - this.head) {
      this.seqs = this.seqs.slice(this.head)
      this.frames = this.frames.slice(this.head)
      this.times = this.times.slice(this.head)
      this.head = 0
    }
  }
}
