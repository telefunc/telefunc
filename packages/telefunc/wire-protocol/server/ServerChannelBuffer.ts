export { ServerChannelBuffer }

import { ChannelOverflowError } from '../channel-errors.js'
import { utf8ByteLength } from '../../utils/utf8ByteLength.js'

type BufferedAckEntry<TAck> = {
  resolve: (value: TAck) => void
  reject: (err: Error) => void
}

/**
 * Hard-capped ring buffer for channel messages sent before the client connects.
 *
 * Buffers text, publish, binary, and ack-request messages sent before the client connects.
 * After every push, oldest entries are evicted (FIFO) until the byte
 * total is within the cap. Ack requests rejected by eviction are failed
 * immediately so channel memory stays hard-capped.
 *
 * A single message larger than maxBytes causes the entire buffer to be
 * cleared and the message dropped — sequential delivery is paramount;
 * a gap in the middle is never acceptable, so the buffer resets to a
 * clean slate instead.
 *
 * Sequential delivery is guaranteed: flush() sends all buffered messages
 * in insertion order. There are no gaps or null markers.
 */
class ServerChannelBuffer<TAck = never> {
  // Parallel arrays for cache-friendly access: 0 = text, 1 = binary, 2 = text+ack, 3 = publish
  #tags: number[] = []
  #data: (string | Uint8Array)[] = []
  #sizes: number[] = []
  #ackEntries: (BufferedAckEntry<TAck> | null)[] = []
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
    this.#push(0, data, bytes, null)
  }

  pushBinary(data: Uint8Array): void {
    const bytes = data.byteLength
    this.#push(1, data, bytes, null)
  }

  pushPublish(data: string): void {
    const bytes = utf8ByteLength(data)
    this.#push(3, data, bytes, null)
  }

  pushPublishBinary(data: Uint8Array): void {
    const bytes = data.byteLength
    this.#push(4, data, bytes, null)
  }

  pushTextAck(data: string, resolve: (value: TAck) => void, reject: (err: Error) => void): void {
    const bytes = utf8ByteLength(data)
    this.#push(2, data, bytes, { resolve, reject })
  }

  #push(
    tag: 0 | 1 | 2 | 3 | 4,
    data: string | Uint8Array,
    bytes: number,
    ackEntry: BufferedAckEntry<TAck> | null,
  ): void {
    const overflowErr = new ChannelOverflowError()
    if (bytes > this.#maxBytes) {
      this.clear(overflowErr)
      ackEntry?.reject(overflowErr)
      return
    }
    this.#tags.push(tag)
    this.#data.push(data)
    this.#sizes.push(bytes)
    this.#ackEntries.push(ackEntry)
    this.#totalBytes += bytes
    this.#evict(overflowErr)
  }

  #evict(evictionErr: Error): void {
    // Evict oldest entries until within the cap.
    // Safe to run after push: the oversized guard ensures the new entry has
    // bytes ≤ maxBytes, so eviction drains old entries and always leaves it.
    while (this.#totalBytes > this.#maxBytes && this.#head < this.#data.length) {
      this.#ackEntries[this.#head]?.reject(evictionErr)
      this.#totalBytes -= this.#sizes[this.#head]!
      this.#head++
    }
    // Compact when dead zone ≥ live zone (amortised O(1)).
    if (this.#head > 0 && this.#head >= this.#data.length - this.#head) {
      this.#tags = this.#tags.slice(this.#head)
      this.#data = this.#data.slice(this.#head)
      this.#sizes = this.#sizes.slice(this.#head)
      this.#ackEntries = this.#ackEntries.slice(this.#head)
      this.#head = 0
    }
  }

  /**
   * Flush all buffered messages to the client connection in insertion order, then clear.
   * @param sendText - callback to send a text message
   * @param sendBinary - callback to send a binary message
   * @param sendTextAck - callback to send a text+ack message
   */
  flush(
    sendText: (data: string) => void,
    sendPublish: (data: string) => void,
    sendBinary: (data: Uint8Array) => void,
    sendTextAck: (data: string, ackEntry: BufferedAckEntry<TAck>) => void,
    sendPublishBinary?: (data: Uint8Array) => void,
  ): void {
    for (let i = this.#head; i < this.#data.length; i++) {
      if (this.#tags[i] === 0) {
        sendText(this.#data[i] as string)
      } else if (this.#tags[i] === 3) {
        sendPublish(this.#data[i] as string)
      } else if (this.#tags[i] === 4) {
        sendPublishBinary?.(this.#data[i] as Uint8Array)
      } else if (this.#tags[i] === 1) {
        sendBinary(this.#data[i] as Uint8Array)
      } else {
        sendTextAck(this.#data[i] as string, this.#ackEntries[i]!)
      }
    }
    this.#tags.length = 0
    this.#data.length = 0
    this.#sizes.length = 0
    this.#ackEntries.length = 0
    this.#head = 0
    this.#totalBytes = 0
  }

  clear(err?: Error): void {
    if (err) {
      for (let i = this.#head; i < this.#ackEntries.length; i++) {
        this.#ackEntries[i]?.reject(err)
      }
    }
    this.#tags.length = 0
    this.#data.length = 0
    this.#sizes.length = 0
    this.#ackEntries.length = 0
    this.#head = 0
    this.#totalBytes = 0
  }
}
