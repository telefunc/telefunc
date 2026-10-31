export { ServerChannelBuffer }
export type { BufferedSender, EntryCallback }

import { ChannelOverflowError } from '../channelErrors.js'
import { utf8ByteLength } from '../../../utils/utf8ByteLength.js'
import { TAG } from '../../wire/tags.js'
import type { IndexedPeer } from './IndexedPeer.js'

/** What holding an entry retains beyond its payload, so many small messages are bounded by memory too: a send's
 *  callbacks and the promise its caller got (about 480 bytes measured), and a publish's slots (about 52). */
const HELD_SEND_OVERHEAD_BYTES = 512
const HELD_PUBLISH_OVERHEAD_BYTES = 64

/** Callback pair for buffered entries — resolve on flush, reject on eviction. */
type EntryCallback = {
  resolve: (...args: any[]) => void
  reject: (err: Error) => void
}

/** The channel `flush` hands each held entry to, with the entry's tag, data and callbacks (null once rejected). */
interface BufferedSender {
  _sendBuffered(peer: IndexedPeer, tag: number, data: string | Uint8Array, callback: EntryCallback | null): void
}

/**
 * Hard-capped FIFO buffer for channel messages sent before the client connects.
 *
 * Buffers text, publish, binary, and ack-request messages sent before the client connects.
 * After every push, oldest entries are evicted (FIFO) until the byte
 * total, each entry counted with what holding it retains, is within the cap.
 * Delivery callbacks on evicted entries are rejected immediately so channel
 * memory stays hard-capped.
 *
 * Binary messages (binary sends, binary ack requests, binary publishes) are stored in a separate queue with its own
 * byte budget. An oversized binary only clears the binary queue — text messages
 * are never affected.
 *
 * A single message larger than its queue's maxBytes causes that queue to be
 * cleared and the message dropped — sequential delivery is paramount;
 * a gap in the middle is never acceptable, so the queue resets to a
 * clean slate instead.
 *
 * Sequential delivery is guaranteed: flush() sends all buffered messages
 * in insertion order by merge-iterating both queues. There are no gaps or null markers.
 *
 * An entry pushed as a `gap` is one its page would lack, if dropped, with nothing to tell it so: `droppedGap` says one
 * was.
 */
class ServerChannelBuffer<TAck = never> {
  private readonly text: BufferQueue
  private readonly binary: BufferQueue
  private insertionSeq = 0

  constructor(maxBytes: number, binaryMaxBytes: number) {
    this.text = new BufferQueue(maxBytes)
    this.binary = new BufferQueue(binaryMaxBytes)
  }

  get byteLength(): number {
    return this.text.byteLength + this.binary.byteLength
  }

  get size(): number {
    return this.text.size + this.binary.size
  }

  /** An entry pushed as a `gap` was dropped to stay within the budget since the last flush. */
  get droppedGap(): boolean {
    return this.text.droppedGap || this.binary.droppedGap
  }

  pushText(data: string, resolve: () => void, reject: (err: Error) => void): void {
    this.text.push(TAG.TEXT, data, utf8ByteLength(data), { resolve, reject }, this.insertionSeq++, false)
  }

  pushTextAck(data: string, resolve: (value: TAck) => void, reject: (err: Error) => void): void {
    this.text.push(TAG.TEXT_ACK_REQ, data, utf8ByteLength(data), { resolve, reject }, this.insertionSeq++, false)
  }

  pushPublish(data: string, gap: boolean): void {
    this.text.push(TAG.PUBLISH, data, utf8ByteLength(data), null, this.insertionSeq++, gap)
  }

  /** Holds a copy, as an attached send encodes the caller's bytes at once and the caller may then reuse them. */
  pushBinary(data: Uint8Array, resolve: () => void, reject: (err: Error) => void): void {
    this.binary.pushCopy(TAG.BINARY, data, { resolve, reject }, this.insertionSeq++, false)
  }

  pushBinaryAck(data: Uint8Array, resolve: (value: unknown) => void, reject: (err: Error) => void): void {
    this.binary.pushCopy(TAG.BINARY_ACK_REQ, data, { resolve, reject }, this.insertionSeq++, false)
  }

  pushPublishBinary(data: Uint8Array, gap: boolean): void {
    this.binary.push(TAG.PUBLISH_BINARY, data, data.byteLength, null, this.insertionSeq++, gap)
  }

  /**
   * Flush all buffered messages to the client connection in insertion order, then clear.
   * Merge-iterates text and binary queues by insertion order so the caller
   * sees the same sequence as if both queues were a single interleaved buffer.
   */
  flush(sender: BufferedSender, peer: IndexedPeer): void {
    let ti = this.text.head
    let bi = this.binary.head
    const textLen = this.text.dataLength
    const binaryLen = this.binary.dataLength

    while (ti < textLen && bi < binaryLen) {
      if (this.text.orderAt(ti) < this.binary.orderAt(bi)) {
        this.flushEntry(this.text, ti++, sender, peer)
      } else {
        this.flushEntry(this.binary, bi++, sender, peer)
      }
    }
    while (ti < textLen) this.flushEntry(this.text, ti++, sender, peer)
    while (bi < binaryLen) this.flushEntry(this.binary, bi++, sender, peer)

    this.text.clear()
    this.binary.clear()
    this.insertionSeq = 0
  }

  private flushEntry(queue: BufferQueue, i: number, sender: BufferedSender, peer: IndexedPeer): void {
    sender._sendBuffered(peer, queue.tagAt(i), queue.dataAt(i), queue.callbackAt(i))
  }

  clear(err?: Error): void {
    this.text.clear(err)
    this.binary.clear(err)
    this.insertionSeq = 0
  }

  /** Rejects the ack requests, whose answers can't be taken, and keeps them to flush with the rest. */
  rejectAcks(err: Error): void {
    this.text.rejectAcks(err)
    this.binary.rejectAcks(err)
  }
}

/**
 * Bounded FIFO queue with parallel arrays and amortised O(1) compaction.
 * ServerChannelBuffer uses two instances — one for text, one for binary —
 * each with its own byte budget so binary can never evict text.
 */

class BufferQueue {
  // Parallel arrays for cache-friendly access
  private tags: number[] = []
  private data: (string | Uint8Array)[] = []
  private sizes: number[] = []
  private callbacks: (EntryCallback | null)[] = []
  private order: number[] = []
  private gaps: boolean[] = []
  head = 0
  private totalBytes = 0
  private readonly maxBytes: number
  droppedGap = false

  constructor(maxBytes: number) {
    this.maxBytes = maxBytes
  }

  get byteLength(): number {
    return this.totalBytes
  }

  get size(): number {
    return this.data.length - this.head
  }

  get dataLength(): number {
    return this.data.length
  }

  tagAt(i: number): number {
    return this.tags[i]!
  }

  dataAt(i: number): string | Uint8Array {
    return this.data[i]!
  }

  callbackAt(i: number): EntryCallback | null {
    return this.callbacks[i]!
  }

  orderAt(i: number): number {
    return this.order[i]!
  }

  push(
    tag: number,
    data: string | Uint8Array,
    bytes: number,
    callback: EntryCallback | null,
    order: number,
    gap: boolean,
  ): void {
    const held = this.admit(bytes, callback, gap)
    if (held !== null) this.append(tag, data, held, callback, order, gap)
  }

  /** Holds a copy of `data`, made only once the queue admits it. */
  pushCopy(tag: number, data: Uint8Array, callback: EntryCallback, order: number, gap: boolean): void {
    const held = this.admit(data.byteLength, callback, gap)
    if (held !== null) this.append(tag, new Uint8Array(data), held, callback, order, gap)
  }

  clear(err?: Error): void {
    if (err) {
      for (let i = this.head; i < this.callbacks.length; i++) {
        this.callbacks[i]?.reject(err)
      }
    }
    this.tags.length = 0
    this.data.length = 0
    this.sizes.length = 0
    this.callbacks.length = 0
    this.order.length = 0
    this.gaps.length = 0
    this.head = 0
    this.totalBytes = 0
    this.droppedGap = false
  }

  rejectAcks(err: Error): void {
    for (let i = this.head; i < this.callbacks.length; i++) {
      if (!isAckTag(this.tags[i]!)) continue
      this.callbacks[i]?.reject(err)
      this.callbacks[i] = null
    }
  }

  // ── Private ──

  /** The bytes holding the entry takes, or `null` once the queue is cleared for an entry larger than it. */
  private admit(bytes: number, callback: EntryCallback | null, gap: boolean): number | null {
    const held = bytes + (callback === null ? HELD_PUBLISH_OVERHEAD_BYTES : HELD_SEND_OVERHEAD_BYTES)
    if (held <= this.maxBytes) return held
    const overflowErr = new ChannelOverflowError()
    const dropsGap = gap || this.gaps.includes(true, this.head)
    this.clear(overflowErr)
    if (dropsGap) this.droppedGap = true
    callback?.reject(overflowErr)
    return null
  }

  private append(
    tag: number,
    data: string | Uint8Array,
    bytes: number,
    callback: EntryCallback | null,
    order: number,
    gap: boolean,
  ): void {
    this.tags.push(tag)
    this.data.push(data)
    this.sizes.push(bytes)
    this.callbacks.push(callback)
    this.order.push(order)
    this.gaps.push(gap)
    this.totalBytes += bytes
    this.evict()
  }

  private evict(): void {
    let evictionErr: ChannelOverflowError | undefined
    // Evict oldest entries until within the cap.
    // Safe to run after push: the oversized guard ensures the new entry has
    // bytes ≤ maxBytes, so eviction drains old entries and always leaves it.
    while (this.totalBytes > this.maxBytes && this.head < this.data.length) {
      if (this.gaps[this.head]) this.droppedGap = true
      this.callbacks[this.head]?.reject((evictionErr ??= new ChannelOverflowError()))
      this.totalBytes -= this.sizes[this.head]!
      this.head++
    }
    // Compact when dead zone ≥ live zone (amortised O(1)).
    if (this.head > 0 && this.head >= this.data.length - this.head) {
      this.tags = this.tags.slice(this.head)
      this.data = this.data.slice(this.head)
      this.sizes = this.sizes.slice(this.head)
      this.callbacks = this.callbacks.slice(this.head)
      this.order = this.order.slice(this.head)
      this.gaps = this.gaps.slice(this.head)
      this.head = 0
    }
  }
}

function isAckTag(tag: number): boolean {
  return tag === TAG.TEXT_ACK_REQ || tag === TAG.BINARY_ACK_REQ
}
