export { StreamReader }

import type { Readable } from 'node:stream'
import { assert, assertUsage, assertWarning } from '../../../utils/assert.js'
import { decodeU32 } from '../../frame.js'

/** Shared sentinel — avoids zero-length subarray views that pin large ArrayBuffers. */
const EMPTY = new Uint8Array(0)
const DISCONNECT_MSG = 'Client disconnected during file upload'

/**
 * Pull-based byte-counting stream reader for the binary frame protocol.
 *
 * Wire format: [u32 metadata length][metadata bytes][file0 bytes][file1 bytes]...
 * File sizes are known from the metadata — no boundary scanning needed,
 * just read exact byte counts sequentially.
 *
 * Accepts either a Web `ReadableStream` or a Node `Readable`. The caller picks —
 * the Node `serve()` adapter passes `IncomingMessage` directly to skip the
 * `Readable.toWeb` round-trip (whose pure-JS `dequeueValue` dominates CPU on hot
 * SSE / binary-RPC paths); other adapters pass `request.body`. Both shapes
 * collapse to one `AsyncIterator<Uint8Array>` so the rest of the class is
 * source-agnostic.
 *
 * Fields use TS `private` rather than JS `#private`: the inner read loop touches
 * `buffer`/`source` per chunk, and `#fields` compile to per-access `WeakMap.get/set`
 * which dominates CPU on hot streaming paths. Plain property access is cheap.
 */
class StreamReader {
  private source: AsyncIterator<Uint8Array<ArrayBuffer>>
  private buffer: Uint8Array<ArrayBuffer> = EMPTY
  private fileSizes: Map<number, number> = new Map()
  private nextFileIndex = 0
  private queue: Promise<void> = Promise.resolve()
  private disconnected = false

  constructor(source: ReadableStream<Uint8Array> | Readable) {
    // Both shapes expose `Symbol.asyncIterator` directly: Web `ReadableStream` does
    // since Node 18 / Bun / Deno, and Node `Readable` does natively. The latter
    // bypasses Node's webstreams `dequeueValue` overhead, which is why we accept
    // `Readable` at all — `serve/node.ts` passes its `IncomingMessage` through
    // to skip the `Readable.toWeb` round-trip.
    this.source = (source as AsyncIterable<Uint8Array<ArrayBuffer>>)[Symbol.asyncIterator]()
  }

  /** Read the metadata: [u32 big-endian length][UTF-8 bytes]. */
  async readMetadata() {
    const length = await this.readU32()
    return new TextDecoder().decode(await this.readExact(length))
  }

  /** Read one length-prefixed chunk, or null if the stream is cleanly exhausted. */
  async readLengthPrefixedBytesOrNull() {
    const lengthBytes = await this.readExactOrNull(4)
    if (!lengthBytes) return null
    return this.readExact(decodeU32(lengthBytes))
  }

  /** Ensure no trailing bytes remain. */
  async assertDone() {
    const chunk = await this.pullChunk()
    assert(chunk === null && this.buffer.length === 0, 'Malformed request body')
  }

  /** Register a file's size (called during deserialization). */
  registerFile(index: number, size: number) {
    this.fileSizes.set(index, size)
  }

  /**
   * Consume file at given index — returns a `ReadableStream` of exactly `size` bytes.
   *
   * Queued: concurrent calls are serialized via `start`/`pull` waiting on `queue`.
   * Out-of-order access skips earlier files (their bytes are read and discarded).
   */
  consumeFile(index: number, size: number): ReadableStream<Uint8Array<ArrayBuffer>> {
    const queuePrevious = this.queue
    let remaining = size
    let resolveDone!: () => void
    const done = new Promise<void>((r) => {
      resolveDone = r
    })
    this.queue = done

    return new ReadableStream<Uint8Array<ArrayBuffer>>({
      start: async () => {
        await queuePrevious
        assertUsage(
          index >= this.nextFileIndex,
          `File argument ${index} has already been consumed (currently at ${this.nextFileIndex}). File arguments must be read in order.`,
        )
        assertWarning(
          index === this.nextFileIndex,
          `File arguments are being consumed out of order (reading ${index}, expected ${this.nextFileIndex}). Skipped files will be unreadable. For correct behavior, consume file arguments in the order they appear.`,
          { onlyOnce: true },
        )
        // Skip earlier files by reading and discarding their bytes
        while (this.nextFileIndex < index) {
          const skipSize = this.fileSizes.get(this.nextFileIndex)
          assert(skipSize !== undefined)
          await this.skipBytes(skipSize)
          this.nextFileIndex++
        }
        this.nextFileIndex++
      },
      pull: async (controller) => {
        try {
          if (remaining <= 0) {
            controller.close()
            return
          }
          const buffered = this.takeBuffered(remaining)
          if (buffered) {
            controller.enqueue(buffered)
            remaining -= buffered.length
            if (remaining <= 0) controller.close()
            return
          }
          const chunk = await this.pullChunk()
          if (!chunk) {
            remaining = 0
            controller.error(new Error(DISCONNECT_MSG))
            return
          }
          const take = Math.min(chunk.length, remaining)
          controller.enqueue(chunk.subarray(0, take))
          if (take < chunk.length) this.buffer = chunk.subarray(take)
          remaining -= take
          if (remaining <= 0) controller.close()
        } catch (err) {
          remaining = 0
          try {
            controller.error(err)
          } catch {}
        } finally {
          // Single resolveDone call site — unblocks the queue when this file is fully
          // consumed, errored, or disconnected.
          if (remaining <= 0) resolveDone()
        }
      },
      cancel: async () => {
        try {
          if (remaining > 0) await this.skipBytes(remaining)
        } catch {}
        resolveDone()
      },
    })
  }

  // ── Primitives ──

  /** Pull one chunk from the underlying source, or null if disconnected. */
  private async pullChunk() {
    if (this.disconnected) return null
    try {
      const { value, done } = await this.source.next()
      if (done) {
        this.disconnected = true
        return null
      }
      return value
    } catch {
      this.disconnected = true
      return null
    }
  }

  /** Take up to `max` bytes from the internal buffer, or null if empty. */
  private takeBuffered(max: number) {
    if (this.buffer.length === 0) return null
    const take = Math.min(this.buffer.length, max)
    const result = this.buffer.subarray(0, take)
    this.buffer = take < this.buffer.length ? this.buffer.subarray(take) : EMPTY
    return result
  }

  /** Read a big-endian u32. Throws on disconnect. */
  private async readU32() {
    return decodeU32(await this.readExact(4)) // sizeof uint32
  }

  /** Read exactly `n` bytes. Throws on disconnect. */
  private async readExact(n: number) {
    while (this.buffer.length < n) {
      const chunk = await this.pullChunk()
      if (!chunk) throw new Error(DISCONNECT_MSG)
      this.buffer = this.buffer.length === 0 ? chunk : concat(this.buffer, chunk)
    }
    const result = this.buffer.subarray(0, n)
    this.buffer = n < this.buffer.length ? this.buffer.subarray(n) : EMPTY
    return result
  }

  /** Read exactly `n` bytes, or null on clean EOF before any bytes are read. */
  private async readExactOrNull(n: number) {
    if (this.buffer.length === 0) {
      const chunk = await this.pullChunk()
      if (!chunk) return null
      this.buffer = chunk
    }
    return this.readExact(n)
  }

  /** Skip exactly `n` bytes. Throws on disconnect. */
  private async skipBytes(n: number) {
    let remaining = n
    const buffered = this.takeBuffered(remaining)
    if (buffered) remaining -= buffered.length
    while (remaining > 0) {
      const chunk = await this.pullChunk()
      if (!chunk) throw new Error(DISCONNECT_MSG)
      remaining -= chunk.length
      if (remaining < 0) this.buffer = chunk.subarray(chunk.length + remaining)
    }
  }
}

function concat(a: Uint8Array<ArrayBuffer>, b: Uint8Array<ArrayBuffer>) {
  const result = new Uint8Array(a.length + b.length)
  result.set(a, 0)
  result.set(b, a.length)
  return result
}
