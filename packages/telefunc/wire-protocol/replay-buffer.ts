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
 * Oversized frames are stored as `null` gap markers. `getAfter` stops at the
 * first gap so the peer only receives a continuous run.
 *
 * Requires non-decreasing seq values (use `nextSeq()` / `pushFrame()` for normal operation).
 */
export class ReplayBuffer {
  // Parallel arrays — seqs separate for cache-friendly access
  #seqs: number[] = []
  #frames: (Uint8Array | null)[] = []
  #times: number[] = []
  #head = 0
  #totalBytes = 0
  readonly #maxBytes: number
  readonly #maxAgeMs: number | undefined
  #seq = 0
  /** Debounced cleanup timer — at most one pending per maxAgeMs window. */
  #cleanupTimer: ReturnType<typeof setTimeout> | null = null

  /** Current sequence number. */
  get seq(): number {
    return this.#seq
  }

  constructor(maxBytes: number, maxAgeMs?: number) {
    if (maxBytes <= 0) throw new Error('maxBytes must be > 0')
    this.#maxBytes = maxBytes
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
  pushFrame(frame: Uint8Array): boolean {
    return this.push(this.nextSeq(), frame)
  }

  /**
   * Store an already-encoded frame.
   * Oversized frames are stored as a `null` gap marker so `getAfter` stops there.
   * @returns `true` if the frame was stored, `false` if it was too large.
   */
  push(seq: number, frame: Uint8Array): boolean {
    const now = Date.now()
    if (seq > this.#seq) this.#seq = seq

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
    this.#scheduleCleanup()
    return true
  }

  /** Get all frames with seq > afterSeq, stopping at the first gap. */
  getAfter(afterSeq: number): Uint8Array[] {
    const len = this.#frames.length
    let lo = this.#head

    // 1. Skip everything the client already received (ignore past gaps)
    while (lo < len && this.#seqs[lo]! <= afterSeq) lo++

    // 2. Nothing left or the very next frame is unrecoverable → abort
    if (lo >= len || this.#frames[lo] === null) return []

    // 3. Collect continuous real frames (stop at next gap)
    let hi = lo + 1
    while (hi < len && this.#frames[hi] !== null) hi++

    return this.#frames.slice(lo, hi) as Uint8Array[]
  }

  /**
   * Eagerly evict expired entries without pushing a new frame.
   * Call this periodically on idle channels to return memory promptly.
   * No-op when `maxAgeMs` was not set.
   */
  evict(now = Date.now()): void {
    if (this.#maxAgeMs === undefined || this.#head >= this.#frames.length) return
    const cutoff = now - this.#maxAgeMs
    while (this.#head < this.#frames.length && this.#times[this.#head]! < cutoff) {
      const f = this.#frames[this.#head]
      if (f) this.#totalBytes -= f.byteLength
      this.#head++
    }
    this.#compact()
  }

  get length(): number {
    return this.#frames.length - this.#head
  }

  get byteLength(): number {
    return this.#totalBytes
  }

  dispose(): void {
    this.#seqs.length = 0
    this.#frames.length = 0
    this.#times.length = 0
    this.#head = 0
    this.#totalBytes = 0
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
    if (this.#maxAgeMs === undefined) return
    if (this.#head >= this.#frames.length) return
    if (this.#cleanupTimer !== null) {
      clearTimeout(this.#cleanupTimer)
      this.#cleanupTimer = null
    }
    const oldestExpiry = this.#times[this.#head]! + this.#maxAgeMs
    const delay = Math.max(0, oldestExpiry - Date.now())
    const timer = setTimeout(() => {
      this.#cleanupTimer = null
      this.evict()
      this.#scheduleCleanup()
    }, delay)
    unrefTimer(timer)
    this.#cleanupTimer = timer
  }

  #evict(now: number): void {
    // Single pass: evict entries that are too old OR push us over the byte budget.
    // cutoff = -Infinity when maxAgeMs is unset → age test is always false (zero cost).
    const cutoff = this.#maxAgeMs !== undefined ? now - this.#maxAgeMs : Number.NEGATIVE_INFINITY
    while (this.#head < this.#frames.length) {
      if (this.#times[this.#head]! >= cutoff && this.#totalBytes <= this.#maxBytes) break
      const f = this.#frames[this.#head]
      if (f) this.#totalBytes -= f.byteLength
      this.#head++
    }
    this.#compact()
  }

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
