export { StreamReader }

import { assert, assertUsage, assertWarning } from '../../../utils/assert.js'

/** Shared sentinel — avoids zero-length subarray views that pin large ArrayBuffers. */
const EMPTY = new Uint8Array(0)
const DISCONNECT_MSG = 'Client disconnected during file upload'

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
  #disconnected = false

  constructor(bodyStream: ReadableStream<Uint8Array>) {
    this.#reader = bodyStream.getReader()
  }

  /** Read the metadata: [u32 big-endian length][UTF-8 bytes]. */
  async readMetadata(): Promise<string> {
    const lengthBytes = await this.#readExact(4)
    const length = new DataView(lengthBytes.buffer, lengthBytes.byteOffset, 4).getUint32(0, false)
    return new TextDecoder().decode(await this.#readExact(length))
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

  // ── Primitives ──

  /** Pull one chunk from the underlying reader, or null if disconnected. */
  async #pullChunk(): Promise<Uint8Array | null> {
    if (this.#disconnected) return null
    try {
      const { done, value } = await this.#reader.read()
      if (done) {
        this.#disconnected = true
        return null
      }
      return value
    } catch {
      this.#disconnected = true
      return null
    }
  }

  /** Take up to `max` bytes from the internal buffer, or null if empty. */
  #takeBuffered(max: number): Uint8Array | null {
    if (this.#buffer.length === 0) return null
    const take = Math.min(this.#buffer.length, max)
    const result = this.#buffer.subarray(0, take)
    this.#buffer = take < this.#buffer.length ? this.#buffer.subarray(take) : EMPTY
    return result
  }

  /** Read exactly `n` bytes. Throws on disconnect. */
  async #readExact(n: number): Promise<Uint8Array> {
    while (this.#buffer.length < n) {
      const chunk = await this.#pullChunk()
      if (!chunk) throw new Error(DISCONNECT_MSG)
      this.#buffer = this.#buffer.length === 0 ? chunk : concat(this.#buffer, chunk)
    }
    const result = this.#buffer.subarray(0, n)
    this.#buffer = n < this.#buffer.length ? this.#buffer.subarray(n) : EMPTY
    return result
  }

  /** Skip exactly `n` bytes. Throws on disconnect. */
  async #skipBytes(n: number): Promise<void> {
    let remaining = n
    const buffered = this.#takeBuffered(remaining)
    if (buffered) remaining -= buffered.length
    while (remaining > 0) {
      const chunk = await this.#pullChunk()
      if (!chunk) throw new Error(DISCONNECT_MSG)
      remaining -= chunk.length
      if (remaining < 0) this.#buffer = chunk.subarray(chunk.length + remaining)
    }
  }

  // ── File stream factory ──

  #createFileStream(size: number): { stream: ReadableStream<Uint8Array>; done: Promise<void> } {
    let remaining = size
    let resolveDone!: () => void
    const done = new Promise<void>((r) => {
      resolveDone = r
    })

    const stream = new ReadableStream<Uint8Array>({
      pull: async (controller) => {
        try {
          if (remaining <= 0) {
            controller.close()
            return
          }

          const buffered = this.#takeBuffered(remaining)
          if (buffered) {
            controller.enqueue(buffered)
            remaining -= buffered.length
            if (remaining <= 0) controller.close()
            return
          }

          const chunk = await this.#pullChunk()
          if (!chunk) {
            remaining = 0
            controller.error(new Error(DISCONNECT_MSG))
            return
          }

          const take = Math.min(chunk.length, remaining)
          controller.enqueue(chunk.subarray(0, take))
          if (take < chunk.length) this.#buffer = chunk.subarray(take)
          remaining -= take
          if (remaining <= 0) controller.close()
        } catch (err) {
          remaining = 0
          try {
            controller.error(err)
          } catch {}
        } finally {
          // Single resolveDone call site for pull — unblocks the queue
          // when the stream is fully consumed, errored, or disconnected.
          if (remaining <= 0) resolveDone()
        }
      },
      cancel: async () => {
        try {
          if (remaining > 0) await this.#skipBytes(remaining)
        } catch {}
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
