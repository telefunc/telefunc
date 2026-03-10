export { ServerChannelBuffer }

/**
 * Hard-capped ring buffer for pre-peer channel messages.
 *
 * Buffers text and binary messages sent before a peer connects.
 * After every push, oldest entries are evicted (FIFO) until the byte
 * total is within the cap. The cap is never exceeded.
 *
 * A single message larger than maxBytes causes the entire buffer to be
 * cleared and the message dropped — sequential delivery is paramount;
 * a gap in the middle is never acceptable, so the buffer resets to a
 * clean slate instead.
 *
 * Sequential delivery is guaranteed: flush() sends all buffered messages
 * in insertion order. There are no gaps or null markers.
 */
class ServerChannelBuffer {
  // Parallel arrays for cache-friendly access: 0 = text, 1 = binary
  #tags: number[] = []
  #data: (string | Uint8Array)[] = []
  #sizes: number[] = []
  #head = 0
  #totalBytes = 0
  readonly #maxBytes: number

  constructor(maxBytes: number) {
    if (maxBytes <= 0) throw new Error('maxBytes must be > 0')
    this.#maxBytes = maxBytes
  }

  get byteLength(): number {
    return this.#totalBytes
  }

  get size(): number {
    return this.#tags.length - this.#head
  }

  pushText(data: string): void {
    const bytes = utf8ByteLength(data)
    if (bytes > this.#maxBytes) {
      this.clear()
      return
    } // oversized — drain to avoid a gap, then drop
    this.#tags.push(0)
    this.#data.push(data)
    this.#sizes.push(bytes)
    this.#totalBytes += bytes
    this.#evict()
  }

  pushBinary(data: Uint8Array): void {
    const bytes = data.byteLength
    if (bytes > this.#maxBytes) {
      this.clear()
      return
    } // oversized — drain to avoid a gap, then drop
    this.#tags.push(1)
    this.#data.push(data)
    this.#sizes.push(bytes)
    this.#totalBytes += bytes
    this.#evict()
  }

  #evict(): void {
    // Evict oldest entries until within the cap.
    // Safe to run after push: the oversized guard ensures the new entry has
    // bytes ≤ maxBytes, so eviction drains old entries and always leaves it.
    while (this.#totalBytes > this.#maxBytes && this.#head < this.#data.length) {
      this.#totalBytes -= this.#sizes[this.#head]!
      this.#head++
    }
    // Compact when dead zone ≥ live zone (amortised O(1)).
    if (this.#head > 0 && this.#head >= this.#data.length - this.#head) {
      this.#tags = this.#tags.slice(this.#head)
      this.#data = this.#data.slice(this.#head)
      this.#sizes = this.#sizes.slice(this.#head)
      this.#head = 0
    }
  }

  /**
   * Flush all buffered messages to the peer in insertion order, then clear.
   * @param sendText - callback to send a text message
   * @param sendBinary - callback to send a binary message
   */
  flush(sendText: (data: string) => void, sendBinary: (data: Uint8Array) => void): void {
    for (let i = this.#head; i < this.#data.length; i++) {
      if (this.#tags[i] === 0) sendText(this.#data[i] as string)
      else sendBinary(this.#data[i] as Uint8Array)
    }
    this.#tags.length = 0
    this.#data.length = 0
    this.#sizes.length = 0
    this.#head = 0
    this.#totalBytes = 0
  }

  clear(): void {
    this.#tags.length = 0
    this.#data.length = 0
    this.#sizes.length = 0
    this.#head = 0
    this.#totalBytes = 0
  }
}

/**
 * Exact UTF-8 byte length of a string — allocation-free.
 * Matches what WebSocket actually transmits on the wire.
 *
 * Surrogate pairs are detected via bitmask: (c & 0xfc00) === 0xd800 catches
 * high surrogates (0xD800–0xDBFF); the same mask with 0xdc00 catches low
 * surrogates (0xDC00–0xDFFF).  No bounds check needed: charCodeAt past end
 * returns NaN, NaN & 0xfc00 === 0, which never matches 0xdc00.
 */
function utf8ByteLength(s: string): number {
  let n = 0
  for (let i = 0; i < s.length; i++) {
    const c = s.charCodeAt(i)
    if (c < 0x80) n += 1
    else if (c < 0x800) n += 2
    else if ((c & 0xfc00) === 0xd800 && (s.charCodeAt(i + 1) & 0xfc00) === 0xdc00) {
      n += 4
      i++
    } else n += 3 // BMP char or lone surrogate (CESU-8, matching Node.js Buffer)
  }
  return n
}
