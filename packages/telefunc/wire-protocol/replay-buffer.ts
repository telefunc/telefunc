export { ReplayBuffer }

/**
 * High-performance replay buffer for outgoing WebSocket data frames.
 *
 * Stores encoded frames keyed by monotonic sequence number for replay
 * on reconnect. Bounded by byte size — oldest entries are evicted when full.
 *
 */
class ReplayBuffer {
  // Parallel arrays — seqs separate for cache-friendly binary search
  #seqs: number[] = []
  #frames: Uint8Array[] = []
  #head = 0
  #totalBytes = 0
  readonly #maxBytes: number

  /** Current sequence number (updated automatically by nextSeq + push) */
  seq = 0

  constructor(maxBytes: number) {
    if (maxBytes <= 0) throw new Error('maxBytes must be > 0')
    this.#maxBytes = maxBytes
  }

  /** Get the next sequence number. */
  nextSeq(): number {
    return ++this.seq
  }

  /**
   * Store an already-encoded frame.
   * @returns `true` if the frame was stored, `false` if it was dropped (too large).
   */
  push(seq: number, frame: Uint8Array): boolean {
    if (frame.byteLength > this.#maxBytes) return false // never store unstorable frames

    this.#seqs.push(seq)
    this.#frames.push(frame)
    this.#totalBytes += frame.byteLength

    // Keep internal seq up-to-date (supports mixed usage)
    if (seq > this.seq) this.seq = seq

    // Evict oldest entries until within budget
    while (this.#totalBytes > this.#maxBytes && this.#head < this.#frames.length) {
      this.#totalBytes -= this.#frames[this.#head]!.byteLength
      this.#head++
    }

    // Full eviction → reset (cheaper than slice when everything is gone)
    if (this.#head >= this.#frames.length) {
      this.#seqs.length = 0
      this.#frames.length = 0
      this.#head = 0
      this.#totalBytes = 0
      return false
    }

    // Compact when dead zone ≥ live zone (amortized O(1))
    if (this.#head > 0 && this.#head >= this.#frames.length - this.#head) {
      this.#seqs = this.#seqs.slice(this.#head)
      this.#frames = this.#frames.slice(this.#head)
      this.#head = 0
    }

    return true
  }

  /** Get all frames with seq > afterSeq. O(log n) + O(1) slice. */
  getAfter(afterSeq: number): Uint8Array[] {
    const len = this.#frames.length
    if (this.#head >= len) return []

    // Binary search: first index where seq > afterSeq
    let lo = this.#head
    let hi = len
    while (lo < hi) {
      const mid = (lo + hi) >>> 1
      if (this.#seqs[mid]! <= afterSeq) lo = mid + 1
      else hi = mid
    }

    return lo >= len ? [] : this.#frames.slice(lo)
  }

  get length(): number {
    return this.#frames.length - this.#head
  }

  get byteLength(): number {
    return this.#totalBytes
  }

  clear(): void {
    this.#seqs.length = 0
    this.#frames.length = 0
    this.#head = 0
    this.#totalBytes = 0
    this.seq = 0
  }
}
