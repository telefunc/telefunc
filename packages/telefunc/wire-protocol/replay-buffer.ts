/**
 * High-performance replay buffer for outgoing WebSocket data frames.
 *
 * Stores encoded frames keyed by monotonic sequence number for replay
 * on reconnect. Bounded by byte size — oldest entries are evicted when full.
 *
 * Oversized frames are stored as `null` gap markers. `getAfter` stops at the
 * first gap so the peer only receives a continuous run.
 */
export class ReplayBuffer {
  // Parallel arrays — seqs separate for cache-friendly access
  #seqs: number[] = []
  #frames: (Uint8Array | null)[] = []
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
   * Oversized frames are stored as a `null` gap marker so `getAfter` stops there.
   * @returns `true` if the frame was stored, `false` if it was too large.
   */
  push(seq: number, frame: Uint8Array): boolean {
    if (frame.byteLength > this.#maxBytes) {
      // Mark gap — replay must stop here
      this.#seqs.push(seq)
      this.#frames.push(null)
      if (seq > this.seq) this.seq = seq
      return false
    }

    this.#seqs.push(seq)
    this.#frames.push(frame)
    this.#totalBytes += frame.byteLength

    // Keep internal seq up-to-date (supports mixed usage)
    if (seq > this.seq) this.seq = seq

    // Evict oldest entries until within budget
    while (this.#totalBytes > this.#maxBytes && this.#head < this.#frames.length) {
      const f = this.#frames[this.#head]
      if (f) this.#totalBytes -= f.byteLength
      this.#head++
    }

    // Full eviction → reset (cheaper than slice when everything is gone)
    if (this.#head >= this.#frames.length) {
      this.#seqs.length = 0
      this.#frames.length = 0
      this.#head = 0
      this.#totalBytes = 0
      return true
    }

    // Compact when dead zone ≥ live zone (amortized O(1))
    if (this.#head > 0 && this.#head >= this.#frames.length - this.#head) {
      this.#seqs = this.#seqs.slice(this.#head)
      this.#frames = this.#frames.slice(this.#head)
      this.#head = 0
    }

    return true
  }

  /** Get all frames with seq > afterSeq, stopping at the first gap. */
  getAfter(afterSeq: number): Uint8Array[] {
    const len = this.#frames.length
    let lo = this.#head

    // 1. Skip everything the client already received (ignore past gaps)
    while (lo < len && this.#seqs[lo]! <= afterSeq) {
      lo++
    }

    // 2. Nothing left or the very next frame is unrecoverable → abort
    if (lo >= len || this.#frames[lo] === null) {
      return []
    }

    // 3. Collect continuous real frames (stop at next gap)
    let hi = lo + 1
    while (hi < len && this.#frames[hi] !== null) hi++

    return this.#frames.slice(lo, hi) as Uint8Array[]
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
