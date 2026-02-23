export { StreamReader }

import { assert, assertUsage, assertWarning } from '../../../utils/assert.js'

/** Shared sentinel — avoids zero-length subarray views that pin large ArrayBuffers. */
const EMPTY = new Uint8Array(0)

/**
 * Pull-based byte-counting stream reader for the binary frame protocol.
 *
 * Wire format: [u32 metadata length][metadata bytes][file0 bytes][file1 bytes]...
 * File sizes are known from the metadata — no boundary scanning needed,
 * just read exact byte counts sequentially.
 */
class StreamReader {
  #reader: ReadableStreamDefaultReader<Uint8Array>
  #buffer: Uint8Array = EMPTY
  #fileSizes: Map<number, number> = new Map()
  #nextFileIndex = 0
  #queue: Promise<void> = Promise.resolve()

  constructor(bodyStream: ReadableStream<Uint8Array>) {
    this.#reader = bodyStream.getReader()
  }

  /** Read the metadata: [u32 big-endian length][UTF-8 bytes]. */
  async readMetadata(): Promise<string> {
    const lengthBytes = await this.#readExact(4)
    const length = new DataView(lengthBytes.buffer, lengthBytes.byteOffset, 4).getUint32(0, false)
    const metadataBytes = await this.#readExact(length)
    return new TextDecoder().decode(metadataBytes)
  }

  /** Register a file's size (called during deserialization). */
  registerFile(index: number, size: number): void {
    this.#fileSizes.set(index, size)
  }

  /**
   * Consume file at given index. Returns a ReadableStream of exactly `size` bytes.
   *
   * Queued — concurrent calls are serialized. Out-of-order access skips earlier files.
   */
  consumeFile(index: number, size: number): Promise<ReadableStream<Uint8Array>> {
    const queuePrevious = this.#queue

    let resolveStream!: (s: ReadableStream<Uint8Array>) => void
    let rejectStream!: (e: unknown) => void
    const streamReady = new Promise<ReadableStream<Uint8Array>>((res, rej) => {
      resolveStream = res
      rejectStream = rej
    })

    this.#queue = (async () => {
      await queuePrevious
      try {
        assertUsage(
          index >= this.#nextFileIndex,
          `File argument ${index} has already been consumed (currently at ${this.#nextFileIndex}). File arguments must be read in order.`,
        )

        assertWarning(
          index === this.#nextFileIndex,
          `File arguments are being consumed out of order (reading ${index}, expected ${this.#nextFileIndex}). Skipped files will be unreadable. For correct behavior, consume file arguments in the order they appear.`,
          { onlyOnce: true },
        )

        // Skip earlier files by reading and discarding their bytes
        while (this.#nextFileIndex < index) {
          const skipSize = this.#fileSizes.get(this.#nextFileIndex)
          assert(skipSize !== undefined)
          await this.#skipBytes(skipSize)
          this.#nextFileIndex++
        }

        this.#nextFileIndex++
        const { stream, done } = this.#createFileStream(size)
        resolveStream(stream)
        await done
      } catch (err) {
        rejectStream(err)
      }
    })()

    return streamReady
  }

  // --- Internal ---

  async #readExact(n: number): Promise<Uint8Array> {
    while (this.#buffer.length < n) {
      const { done, value } = await this.#reader.read()
      assert(!done, 'Unexpected end of stream')
      this.#buffer = this.#buffer.length === 0 ? value : concat(this.#buffer, value)
    }
    const result = this.#buffer.subarray(0, n)
    this.#buffer = n < this.#buffer.length ? this.#buffer.subarray(n) : EMPTY
    return result
  }

  async #skipBytes(n: number): Promise<void> {
    let remaining = n
    // Use buffered bytes first
    if (this.#buffer.length > 0) {
      const take = Math.min(this.#buffer.length, remaining)
      this.#buffer = take < this.#buffer.length ? this.#buffer.subarray(take) : EMPTY
      remaining -= take
    }
    while (remaining > 0) {
      const { done, value } = await this.#reader.read()
      assert(!done, 'Unexpected end of stream')
      remaining -= value.length
      if (remaining < 0) {
        // Over-read: keep the excess in the buffer
        this.#buffer = value.subarray(value.length + remaining)
      }
    }
  }

  #createFileStream(size: number): { stream: ReadableStream<Uint8Array>; done: Promise<void> } {
    let remaining = size
    let resolveDone!: () => void
    const done = new Promise<void>((r) => {
      resolveDone = r
    })
    const stream = new ReadableStream<Uint8Array>({
      pull: async (controller) => {
        if (remaining <= 0) {
          controller.close()
          resolveDone()
          return
        }
        // Use buffered bytes first
        if (this.#buffer.length > 0) {
          const take = Math.min(this.#buffer.length, remaining)
          controller.enqueue(this.#buffer.subarray(0, take))
          this.#buffer = take < this.#buffer.length ? this.#buffer.subarray(take) : EMPTY
          remaining -= take
          if (remaining <= 0) {
            controller.close()
            resolveDone()
          }
          return
        }
        const { done: streamDone, value } = await this.#reader.read()
        if (streamDone) {
          controller.close()
          resolveDone()
          return
        }
        const take = Math.min(value.length, remaining)
        controller.enqueue(value.subarray(0, take))
        if (take < value.length) {
          this.#buffer = value.subarray(take)
        }
        remaining -= take
        if (remaining <= 0) {
          controller.close()
          resolveDone()
        }
      },
      cancel: async () => {
        // Drain remaining bytes so the next file can be read
        if (remaining > 0) await this.#skipBytes(remaining)
        resolveDone()
      },
    })
    return { stream, done }
  }
}

function concat(a: Uint8Array, b: Uint8Array): Uint8Array {
  const result = new Uint8Array(a.length + b.length)
  result.set(a, 0)
  result.set(b, a.length)
  return result
}
