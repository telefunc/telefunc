export { BodyReader, OversizeFrameError, StreamTruncatedError }

import type { Readable } from 'node:stream'
import { assert, assertUsage, assertWarning } from '../../utils/assert.js'
import { createDeferred } from '../../utils/createDeferred.js'
import { concatAll, decodeU32 } from './envelope.js'

/** Shared sentinel — avoids zero-length subarray views that pin large ArrayBuffers. */
const EMPTY = new Uint8Array(0)

/** A length-prefixed chunk says it is over the reader's limit. `head`: as much of the chunk's start as the reader was
 *  asked to read of it, so that the caller can say whose it was. */
class OversizeFrameError extends Error {
  readonly head: Uint8Array<ArrayBuffer>
  constructor(head: Uint8Array<ArrayBuffer> = EMPTY) {
    super()
    this.head = head
  }
}

/** The request body stopped short: mid-frame, or its client left mid-read. Typed so error sinks
 *  discriminate an expected client truncation from a real server bug by class, not message text. */
class StreamTruncatedError extends Error {
  constructor(options?: ErrorOptions) {
    super('The client disconnected before the end of the request body', options)
  }
}

/**
 * Pull-based byte-counting reader of a request body in the binary frame protocol.
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
 * source-agnostic, and both can be let go of with a read under way (`letGo`).
 *
 * Fields use TS `private` rather than JS `#private`: the inner read loop touches
 * `buffer`/`source` per chunk, and `#fields` compile to per-access `WeakMap.get/set`
 * which dominates CPU on hot streaming paths. Plain property access is cheap.
 */
class BodyReader {
  private source: AsyncIterator<Uint8Array<ArrayBuffer>>
  /** Hands the source back to its owner unread (`letGo`): a Web body's lock released, a Node body's read under way
   *  ended and its reading listeners off. */
  private readonly releaseSource: () => void
  private released = false
  private buffer: Uint8Array<ArrayBuffer> = EMPTY
  private fileSizes: Map<number, number> = new Map()
  private nextFileIndex = 0
  private queue: Promise<void> = Promise.resolve()
  private disconnected = false
  /** Why a read of the source failed: the app's own error, or not (its client left, or nothing says: the body is
   *  truncated). */
  private failure: { error: unknown; appsError: boolean } | undefined
  /** Whether a read's failure is the client leaving, or null where nothing says, on the evidence of the server that
   *  received the request, passed by the caller. */
  private clientLeft: (err: unknown) => boolean | null
  /** Called for each chunk the source yields. */
  onChunk?: () => void

  constructor(
    source: ReadableStream<Uint8Array<ArrayBuffer>> | Readable,
    clientLeft: (err: unknown) => boolean | null,
  ) {
    this.clientLeft = clientLeft
    if (source instanceof ReadableStream) {
      const reader = source.getReader()
      this.source = { next: () => reader.read() }
      // Node, Deno and Bun fail the read under way. workerd throws instead and keeps the lock, as the streams standard
      // did before 2022: that read ends only at the body's next chunk, which it still gives, and nothing is read after.
      this.releaseSource = () => {
        try {
          reader.releaseLock()
        } catch {}
      }
    } else {
      // Read as a Node `Readable` is, which bypasses Node's webstreams `dequeueValue` overhead: that is why we accept
      // `Readable` at all — `serve/node.ts` passes its `IncomingMessage` through to skip the `Readable.toWeb` round-trip.
      const chunks = new ReadableChunks(source)
      this.source = chunks
      this.releaseSource = () => chunks.letGo()
    }
  }

  /** Reads no more of the body and hands it back to its owner, neither cancelled nor read: the read under way and every
   *  one after it end as at the body's end. A Web body's lock is released; a Node body keeps only this reader's
   *  error listener, which reads nothing and takes an error it emits later. */
  letGo(): void {
    if (this.released) return
    this.released = true
    this.disconnected = true
    this.onChunk = undefined
    this.releaseSource()
  }

  /** Read one frame: [u32 big-endian length][bytes], or null where the body ends before its first byte. A frame over
   *  `maxBytes` throws once its length is read, with its first `headBytes` bytes read. */
  async readFrame(maxBytes: number, headBytes = 0) {
    const lengthBytes = await this.readExactOrNull(4)
    if (!lengthBytes) return null
    const length = decodeU32(lengthBytes)
    if (length > maxBytes) throw new OversizeFrameError(await this.readExact(Math.min(headBytes, length)))
    return this.readExact(length)
  }

  /** Reads the rest of the body and drops it, a chunk at a time, to its end or its client's leaving. */
  async discardRest(): Promise<void> {
    this.buffer = EMPTY
    let chunk = await this.pullChunk()
    while (chunk !== null) chunk = await this.pullChunk()
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
    const { promise: done, resolve: resolveDone } = createDeferred()
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
            controller.error(this.truncation())
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

  /** Pull one chunk from the underlying source, or null once it ended or its client left, or failed where nothing says
   *  why. A read that fails as the app's own error throws it, and so does every pull after it. */
  private async pullChunk() {
    if (this.disconnected) {
      if (this.failure?.appsError) throw this.failure.error
      return null
    }
    try {
      const { value, done } = await this.source.next()
      if (done) {
        this.disconnected = true
        return null
      }
      this.onChunk?.()
      return value
    } catch (error) {
      this.disconnected = true
      // The read under way of a body let go of (a Web reader's released lock fails it) ends as at the body's end.
      if (this.released) return null
      this.failure = { error, appsError: this.clientLeft(error) === false }
      if (this.failure.appsError) throw error
      return null
    }
  }

  /** The body stopped short, with the runtime's error as the cause where a read failed. */
  truncation() {
    return new StreamTruncatedError(this.failure && { cause: this.failure.error })
  }

  /** Take up to `max` bytes from the internal buffer, or null if empty. */
  private takeBuffered(max: number) {
    if (this.buffer.length === 0) return null
    const take = Math.min(this.buffer.length, max)
    const result = this.buffer.subarray(0, take)
    this.buffer = take < this.buffer.length ? this.buffer.subarray(take) : EMPTY
    return result
  }

  /** Read exactly `n` bytes. Throws on disconnect. */
  private async readExact(n: number) {
    if (this.buffer.length < n) {
      const parts = this.buffer.length > 0 ? [this.buffer] : []
      let length = this.buffer.length
      let last = EMPTY
      while (length < n) {
        const chunk = await this.pullChunk()
        if (!chunk) throw this.truncation()
        parts.push(chunk)
        last = chunk
        length += chunk.length
      }
      // One part is the one chunk read, with nothing buffered before it.
      this.buffer = parts.length === 1 ? last : concatAll(parts)
    }
    const result = this.buffer.subarray(0, n)
    this.buffer = n < this.buffer.length ? this.buffer.subarray(n) : EMPTY
    return result
  }

  /** Read exactly `n` bytes, or null on clean EOF before any bytes are read. */
  private async readExactOrNull(n: number) {
    while (this.buffer.length === 0) {
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
      if (!chunk) throw this.truncation()
      remaining -= chunk.length
      if (remaining < 0) this.buffer = chunk.subarray(chunk.length + remaining)
    }
  }
}

const DONE: IteratorReturnResult<undefined> = { done: true, value: undefined }

/** A Node `Readable`'s chunks: what `read()` gives at each `'readable'`, to `'end'`, or failing at `'error'` or at a
 *  close before the end. It listens from its construction on, so the stream asks its source for data before the first
 *  read; and one read waits at a time: a read asked while one waits takes its place, and that one never settles. Unlike
 *  Node's own iterator, it can be let go of with a read under way (`letGo`): Node's ends a read under way only by
 *  destroying the stream. */
class ReadableChunks implements AsyncIterator<Uint8Array<ArrayBuffer>> {
  private readonly stream: Readable
  private waiting: {
    resolve: (result: IteratorResult<Uint8Array<ArrayBuffer>>) => void
    reject: (error: unknown) => void
  } | null = null
  private ended: boolean
  private failed: { error: unknown } | null = null
  /** Whether the stream's owner had paused it, read before this reader's `'readable'` listener holds it out of flowing
   *  mode, which `isPaused()` reports from then on too. */
  private readonly pausedByOwner: boolean

  constructor(stream: Readable) {
    this.stream = stream
    this.pausedByOwner = stream.isPaused()
    this.ended = stream.readableEnded
    if (stream.errored) this.failed = { error: stream.errored }
    else if (stream.destroyed && !this.ended) this.failed = { error: prematureClose() }
    stream.on('readable', this.wake)
    stream.on('end', this.end)
    stream.on('error', this.fail)
    stream.on('close', this.close)
  }

  next(): Promise<IteratorResult<Uint8Array<ArrayBuffer>>> {
    const value: Uint8Array<ArrayBuffer> | null = this.stream.destroyed ? null : this.stream.read()
    if (value !== null) return Promise.resolve({ value, done: false })
    if (this.failed) return Promise.reject(this.failed.error)
    if (this.ended) return Promise.resolve(DONE)
    return new Promise((resolve, reject) => (this.waiting = { resolve, reject }))
  }

  /** Ends the read under way as at the stream's end, and takes off the listeners that read the stream, so that its
   *  owner reads it as before (`'readable'` holds it out of flowing mode): flowing, or paused where its owner paused it,
   *  until the owner resumes it. Its error listener stays, reading nothing: an error the stream emits later reaches it,
   *  not the process. */
  letGo(): void {
    this.stream.off('end', this.end)
    this.stream.off('close', this.close)
    // Taking off the last `'readable'` listener resumes a stream that has a `'data'` listener, also one its owner paused:
    // that one keeps the listener, which reads nothing any more, until the owner resumes it.
    if (this.pausedByOwner) this.stream.once('resume', () => this.stream.off('readable', this.wake))
    else this.stream.off('readable', this.wake)
    this.settle(DONE)
  }

  private readonly wake = (): void => {
    if (this.waiting === null) return
    const value: Uint8Array<ArrayBuffer> | null = this.stream.read()
    if (value !== null) this.settle({ value, done: false })
  }

  private readonly end = (): void => {
    this.ended = true
    this.settle(DONE)
  }

  private readonly fail = (error: unknown): void => {
    this.failed ??= { error }
    const waiting = this.waiting
    this.waiting = null
    waiting?.reject(this.failed.error)
  }

  private readonly close = (): void => {
    if (!this.ended && this.failed === null) this.fail(prematureClose())
  }

  private settle(result: IteratorResult<Uint8Array<ArrayBuffer>>): void {
    const waiting = this.waiting
    this.waiting = null
    waiting?.resolve(result)
  }
}

/** A stream closed before its end, as Node's own iterator reports it (`ERR_STREAM_PREMATURE_CLOSE`). */
function prematureClose(): Error {
  return Object.assign(new Error('Premature close'), { code: 'ERR_STREAM_PREMATURE_CLOSE' })
}
