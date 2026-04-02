export { ServerChannelBuffer }

import { ChannelOverflowError } from '../channel-errors.js'
import { utf8ByteLength } from '../../utils/utf8ByteLength.js'
import { TAG } from '../shared-ws.js'
import { assert } from '../../utils/assert.js'

/** Callback pair for buffered entries — resolve on flush, reject on eviction. */
type EntryCallback = {
  resolve: (...args: any[]) => void
  reject: (err: Error) => void
}

/**
 * Hard-capped ring buffer for channel messages sent before the client connects.
 *
 * Buffers text, publish, binary, and ack-request messages sent before the client connects.
 * After every push, oldest entries are evicted (FIFO) until the byte
 * total is within the cap. Delivery callbacks on evicted entries are rejected
 * immediately so channel memory stays hard-capped.
 *
 * Binary messages (tags 1 and 4) are stored in a separate lane with its own
 * byte budget. An oversized binary only clears the binary lane — text messages
 * are never affected.
 *
 * A single message larger than its lane's maxBytes causes that lane to be
 * cleared and the message dropped — sequential delivery is paramount;
 * a gap in the middle is never acceptable, so the lane resets to a
 * clean slate instead.
 *
 * Sequential delivery is guaranteed: flush() sends all buffered messages
 * in insertion order by merge-iterating both lanes. There are no gaps or null markers.
 */
class ServerChannelBuffer<TAck = never> {
  readonly #text: BufferLane
  readonly #binary: BufferLane
  #insertionSeq = 0

  constructor(maxBytes: number, binaryMaxBytes: number) {
    if (maxBytes <= 0) throw new Error('maxBytes must be > 0')
    this.#text = new BufferLane(maxBytes)
    this.#binary = new BufferLane(binaryMaxBytes)
  }

  get byteLength(): number {
    return this.#text.byteLength + this.#binary.byteLength
  }

  get size(): number {
    return this.#text.size + this.#binary.size
  }

  pushText(data: string, resolve: () => void, reject: (err: Error) => void): void {
    this.#text.push(TAG.TEXT, data, utf8ByteLength(data), { resolve, reject }, this.#insertionSeq++)
  }

  pushTextAck(data: string, resolve: (value: TAck) => void, reject: (err: Error) => void): void {
    this.#text.push(TAG.TEXT_ACK_REQ, data, utf8ByteLength(data), { resolve, reject }, this.#insertionSeq++)
  }

  pushPublish(data: string): void {
    this.#text.push(TAG.PUBLISH, data, utf8ByteLength(data), null, this.#insertionSeq++)
  }

  pushBinary(data: Uint8Array, resolve: () => void, reject: (err: Error) => void): void {
    this.#binary.push(TAG.BINARY, data, data.byteLength, { resolve, reject }, this.#insertionSeq++)
  }

  pushBinaryAck(data: Uint8Array, resolve: (value: unknown) => void, reject: (err: Error) => void): void {
    this.#binary.push(TAG.BINARY_ACK_REQ, data, data.byteLength, { resolve, reject }, this.#insertionSeq++)
  }

  pushPublishBinary(data: Uint8Array): void {
    this.#binary.push(TAG.PUBLISH_BINARY, data, data.byteLength, null, this.#insertionSeq++)
  }

  /**
   * Flush all buffered messages to the client connection in insertion order, then clear.
   * Merge-iterates text and binary lanes by insertion order so the caller
   * sees the same sequence as if both lanes were a single interleaved buffer.
   */
  flush(handlers: {
    sendText: (data: string) => void
    sendPublish: (data: string) => void
    sendBinary: (data: Uint8Array) => void
    sendTextAck: (data: string, cb: EntryCallback) => void
    sendBinaryAck: (data: Uint8Array, cb: EntryCallback) => void
    sendPublishBinary: (data: Uint8Array) => void
  }): void {
    let ti = this.#text.head
    let bi = this.#binary.head
    const textLen = this.#text.dataLength
    const binaryLen = this.#binary.dataLength

    while (ti < textLen && bi < binaryLen) {
      if (this.#text.orderAt(ti) < this.#binary.orderAt(bi)) {
        this.#flushEntry(this.#text, ti++, handlers)
      } else {
        this.#flushEntry(this.#binary, bi++, handlers)
      }
    }
    while (ti < textLen) this.#flushEntry(this.#text, ti++, handlers)
    while (bi < binaryLen) this.#flushEntry(this.#binary, bi++, handlers)

    this.#text.clear()
    this.#binary.clear()
    this.#insertionSeq = 0
  }

  #flushEntry(
    lane: BufferLane,
    i: number,
    h: {
      sendText: (data: string) => void
      sendPublish: (data: string) => void
      sendBinary: (data: Uint8Array) => void
      sendTextAck: (data: string, cb: EntryCallback) => void
      sendBinaryAck: (data: Uint8Array, cb: EntryCallback) => void
      sendPublishBinary: (data: Uint8Array) => void
    },
  ): void {
    const tag = lane.tagAt(i)
    const data = lane.dataAt(i)
    const cb = lane.callbackAt(i)
    switch (tag) {
      case TAG.TEXT:
        assert(typeof data === 'string' && cb)
        h.sendText(data)
        cb.resolve()
        break
      case TAG.TEXT_ACK_REQ:
        assert(typeof data === 'string' && cb)
        h.sendTextAck(data, cb)
        break
      case TAG.PUBLISH:
        assert(typeof data === 'string')
        h.sendPublish(data)
        break
      case TAG.BINARY:
        assert(data instanceof Uint8Array && cb)
        h.sendBinary(data)
        cb.resolve()
        break
      case TAG.BINARY_ACK_REQ:
        assert(data instanceof Uint8Array && cb)
        h.sendBinaryAck(data, cb)
        break
      case TAG.PUBLISH_BINARY:
        assert(data instanceof Uint8Array)
        h.sendPublishBinary(data)
        break
    }
  }

  clear(err?: Error): void {
    this.#text.clear(err)
    this.#binary.clear(err)
    this.#insertionSeq = 0
  }
}

/**
 * Bounded FIFO lane with parallel arrays and amortised O(1) compaction.
 * ServerChannelBuffer uses two instances — one for text, one for binary —
 * each with its own byte budget so binary can never evict text.
 */

class BufferLane {
  // Parallel arrays for cache-friendly access
  #tags: number[] = []
  #data: (string | Uint8Array)[] = []
  #sizes: number[] = []
  #callbacks: (EntryCallback | EntryCallback | null)[] = []
  #order: number[] = []
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
    return this.#data.length - this.#head
  }

  get head(): number {
    return this.#head
  }

  get dataLength(): number {
    return this.#data.length
  }

  tagAt(i: number): number {
    return this.#tags[i]!
  }

  dataAt(i: number): string | Uint8Array {
    return this.#data[i]!
  }

  callbackAt(i: number): EntryCallback | EntryCallback | null {
    return this.#callbacks[i]!
  }

  orderAt(i: number): number {
    return this.#order[i]!
  }

  push(
    tag: number,
    data: string | Uint8Array,
    bytes: number,
    callback: EntryCallback | EntryCallback | null,
    order: number,
  ): void {
    const overflowErr = new ChannelOverflowError()
    if (bytes > this.#maxBytes) {
      this.clear(overflowErr)
      callback?.reject(overflowErr)
      return
    }
    this.#tags.push(tag)
    this.#data.push(data)
    this.#sizes.push(bytes)
    this.#callbacks.push(callback)
    this.#order.push(order)
    this.#totalBytes += bytes
    this.#evict(overflowErr)
  }

  clear(err?: Error): void {
    if (err) {
      for (let i = this.#head; i < this.#callbacks.length; i++) {
        this.#callbacks[i]?.reject(err)
      }
    }
    this.#tags.length = 0
    this.#data.length = 0
    this.#sizes.length = 0
    this.#callbacks.length = 0
    this.#order.length = 0
    this.#head = 0
    this.#totalBytes = 0
  }

  // ── Private ──

  #evict(evictionErr: Error): void {
    // Evict oldest entries until within the cap.
    // Safe to run after push: the oversized guard ensures the new entry has
    // bytes ≤ maxBytes, so eviction drains old entries and always leaves it.
    while (this.#totalBytes > this.#maxBytes && this.#head < this.#data.length) {
      this.#callbacks[this.#head]?.reject(evictionErr)
      this.#totalBytes -= this.#sizes[this.#head]!
      this.#head++
    }
    // Compact when dead zone ≥ live zone (amortised O(1)).
    if (this.#head > 0 && this.#head >= this.#data.length - this.#head) {
      this.#tags = this.#tags.slice(this.#head)
      this.#data = this.#data.slice(this.#head)
      this.#sizes = this.#sizes.slice(this.#head)
      this.#callbacks = this.#callbacks.slice(this.#head)
      this.#order = this.#order.slice(this.#head)
      this.#head = 0
    }
  }
}
