import { unrefTimer } from '../utils/unrefTimer.js'

/**
 * High-performance replay buffer for outgoing WebSocket data frames.
 *
 * Stores encoded frames keyed by monotonic sequence number for replay
 * on reconnect. Bounded by byte size — oldest entries are evicted when full.
 * Optionally also bounded by age — entries older than `maxAgeMs` are evicted
 * at push time, which matches the reconnect window: frames that arrived before
 * the reconnect deadline can never be replayed anyway.
 *
 * When `binaryMaxBytes` is provided, binary frames are stored in a separate
 * lane with its own byte budget. A large binary cannot evict text frames,
 * and a binary gap does not block text replay.
 *
 * Oversized frames are stored as `null` gap markers. `getAfter` stops at the
 * first gap per lane so the peer only receives a continuous run.
 *
 * Requires non-decreasing seq values (use `nextSeq()` / `pushFrame()` for normal operation).
 */
export class ReplayBuffer {
  readonly #text: ReplayLane
  readonly #binary: ReplayLane
  readonly #maxAgeMs: number
  #seq = 0
  /** Debounced cleanup timer — at most one pending per maxAgeMs window. */
  #cleanupTimer: ReturnType<typeof setTimeout> | null = null

  /** Current sequence number. */
  get seq(): number {
    return this.#seq
  }

  constructor(maxBytes: number, maxAgeMs: number, binaryMaxBytes: number) {
    if (maxBytes <= 0) throw new Error('maxBytes must be > 0')
    this.#text = new ReplayLane(maxBytes, maxAgeMs)
    this.#binary = new ReplayLane(binaryMaxBytes, maxAgeMs)
    this.#maxAgeMs = maxAgeMs
  }

  /** Increment and return the next sequence number. */
  nextSeq(): number {
    return ++this.#seq
  }

  /**
   * Convenience wrapper: advance the internal seq and push the frame.
   * Equivalent to `push(buf.nextSeq(), frame)`.
   */
  pushFrame(frame: Uint8Array<ArrayBuffer>, isBinary?: boolean): boolean {
    return this.push(this.nextSeq(), frame, isBinary)
  }

  /**
   * Store an already-encoded frame.
   * Oversized frames are stored as a `null` gap marker so `getAfter` stops there.
   * @returns `true` if the frame was stored, `false` if it was too large.
   */
  push(seq: number, frame: Uint8Array<ArrayBuffer>, isBinary?: boolean): boolean {
    if (seq > this.#seq) this.#seq = seq
    const lane = isBinary ? this.#binary : this.#text
    const stored = lane.push(seq, frame)
    this.#scheduleCleanup()
    return stored
  }

  /** Get all frames with seq > afterSeq, stopping at the first gap per lane.
   *  When binary lane is active, merge-iterates both lanes by seq order. */
  getAfter(afterSeq: number): Uint8Array<ArrayBuffer>[] {
    const t = this.#text.getAfter(afterSeq)
    const b = this.#binary.getAfter(afterSeq)
    if (t.frames.length === 0) return b.frames
    if (b.frames.length === 0) return t.frames

    // Merge two sorted runs by their stored seq values.
    const result: Uint8Array<ArrayBuffer>[] = new Array(t.frames.length + b.frames.length)
    let ti = 0
    let bi = 0
    let ri = 0
    while (ti < t.frames.length && bi < b.frames.length) {
      if (t.seqs[ti]! <= b.seqs[bi]!) {
        result[ri++] = t.frames[ti++]!
      } else {
        result[ri++] = b.frames[bi++]!
      }
    }
    while (ti < t.frames.length) result[ri++] = t.frames[ti++]!
    while (bi < b.frames.length) result[ri++] = b.frames[bi++]!
    return result
  }

  /**
   * Eagerly evict expired entries without pushing a new frame.
   * Call this periodically on idle channels to return memory promptly.
   */
  evict(now = Date.now()): void {
    this.#text.evict(now)
    this.#binary.evict(now)
  }

  get length(): number {
    return this.#text.length + this.#binary.length
  }

  get byteLength(): number {
    return this.#text.byteLength + this.#binary.byteLength
  }

  dispose(): void {
    this.#text.dispose()
    this.#binary.dispose()
    this.#seq = 0
    if (this.#cleanupTimer !== null) {
      clearTimeout(this.#cleanupTimer)
      this.#cleanupTimer = null
    }
  }

  // ── Private ──

  /**
   * Schedule (or reschedule) a cleanup timer to fire precisely when the oldest
   * entry expires. Cancels any existing timer first so there is always at most
   * one pending. After eviction, re-schedules for the new oldest entry if any
   * remain.
   */
  #scheduleCleanup(): void {
    const oldest = Math.min(this.#text.oldestTime, this.#binary.oldestTime)
    if (oldest === Infinity) return

    if (this.#cleanupTimer !== null) {
      clearTimeout(this.#cleanupTimer)
      this.#cleanupTimer = null
    }
    const delay = Math.max(0, oldest + this.#maxAgeMs - Date.now())
    const timer = setTimeout(() => {
      this.#cleanupTimer = null
      this.evict()
      this.#scheduleCleanup()
    }, delay)
    unrefTimer(timer)
    this.#cleanupTimer = timer
  }
}

/**
 * Bounded FIFO lane with parallel arrays and amortised O(1) compaction.
 * ReplayBuffer uses two instances — one for text, one for binary —
 * each with its own byte budget so binary can never evict text.
 */

class ReplayLane {
  // Parallel arrays — seqs separate for cache-friendly access
  #seqs: number[] = []
  #frames: (Uint8Array<ArrayBuffer> | null)[] = []
  #times: number[] = []
  #head = 0
  #totalBytes = 0
  readonly #maxBytes: number
  readonly #maxAgeMs: number

  constructor(maxBytes: number, maxAgeMs: number) {
    this.#maxBytes = maxBytes
    this.#maxAgeMs = maxAgeMs
  }

  /** Time of the oldest buffered entry, or Infinity if empty. */
  get oldestTime(): number {
    return this.#head < this.#times.length ? this.#times[this.#head]! : Infinity
  }

  get length(): number {
    return this.#frames.length - this.#head
  }

  get byteLength(): number {
    return this.#totalBytes
  }

  /**
   * Store an already-encoded frame.
   * Oversized frames are stored as a `null` gap marker so `getAfter` stops there.
   * @returns `true` if the frame was stored, `false` if it was too large.
   */
  push(seq: number, frame: Uint8Array<ArrayBuffer>): boolean {
    const now = Date.now()

    if (frame.byteLength > this.#maxBytes) {
      // Mark gap — replay must stop here. Still evict age/size so the buffer
      // stays as fresh as possible (consistent with the normal push path).
      this.#seqs.push(seq)
      this.#frames.push(null)
      this.#times.push(now)
      this.#evict(now)
      return false
    }

    this.#seqs.push(seq)
    this.#frames.push(frame)
    this.#times.push(now)
    this.#totalBytes += frame.byteLength
    this.#evict(now)
    return true
  }

  /** Get all frames with seq > afterSeq, stopping at the first gap. */
  getAfter(afterSeq: number): { seqs: number[]; frames: Uint8Array<ArrayBuffer>[] } {
    const len = this.#frames.length
    let lo = this.#head

    // 1. Skip everything the peer already received (ignore past gaps)
    while (lo < len && this.#seqs[lo]! <= afterSeq) lo++

    // 2. Nothing left or the very next frame is unrecoverable → abort
    if (lo >= len || this.#frames[lo] === null) return { seqs: [], frames: [] }

    // 3. Collect continuous real frames (stop at next gap)
    let hi = lo + 1
    while (hi < len && this.#frames[hi] !== null) hi++

    return {
      seqs: this.#seqs.slice(lo, hi),
      frames: this.#frames.slice(lo, hi) as Uint8Array<ArrayBuffer>[],
    }
  }

  /**
   * Eagerly evict expired entries without pushing a new frame.
   */
  evict(now: number): void {
    if (this.#head >= this.#frames.length) return
    const cutoff = now - this.#maxAgeMs
    while (this.#head < this.#frames.length && this.#times[this.#head]! < cutoff) {
      const f = this.#frames[this.#head]
      if (f) this.#totalBytes -= f.byteLength
      this.#head++
    }
    this.#compact()
  }

  dispose(): void {
    this.#seqs.length = 0
    this.#frames.length = 0
    this.#times.length = 0
    this.#head = 0
    this.#totalBytes = 0
  }

  // ── Private ──

  #evict(now: number): void {
    // Single pass: evict entries that are too old OR push us over the byte budget.
    const cutoff = now - this.#maxAgeMs
    while (this.#head < this.#frames.length) {
      if (this.#times[this.#head]! >= cutoff && this.#totalBytes <= this.#maxBytes) break
      const f = this.#frames[this.#head]
      if (f) this.#totalBytes -= f.byteLength
      this.#head++
    }
    this.#compact()
  }

  /** Compact when dead zone ≥ live zone (amortised O(1)). */
  #compact(): void {
    if (this.#head >= this.#frames.length) {
      this.#seqs.length = 0
      this.#frames.length = 0
      this.#times.length = 0
      this.#head = 0
      this.#totalBytes = 0
      return
    }
    if (this.#head > 0 && this.#head >= this.#frames.length - this.#head) {
      this.#seqs = this.#seqs.slice(this.#head)
      this.#frames = this.#frames.slice(this.#head)
      this.#times = this.#times.slice(this.#head)
      this.#head = 0
    }
  }
}
