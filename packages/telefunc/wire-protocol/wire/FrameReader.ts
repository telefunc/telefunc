export { FrameReader, OversizeFrameError, ReadableChunks, StreamTruncatedError }
export type { ChunkSource }

import type { Readable } from 'node:stream'
import { assert } from '../../utils/assert.js'
import { concatAll, decodeU32 } from './envelope.js'
import { unwatchEarlyReadableError } from '../../utils/nodeReadableToWebRequest.js'
import { releaseOrCancel } from '../../workarounds/workerdReaderReleaseLock.js'

/** Shared sentinel — avoids zero-length subarray views that pin large ArrayBuffers. */
const EMPTY = new Uint8Array(0)

/** What the reader reads the body through: a Web body's reader, or a Node body's chunks (`ReadableChunks`). `letGo`
 *  reads no more of the body, and does nothing more where called again: it hands the body back to its owner, or cancels
 *  it where the runtime keeps the reader's lock (workerd). The reader lets go of it at its end or its failed read too. */
type ChunkSource = {
  next(): Promise<IteratorResult<Uint8Array<ArrayBuffer>>>
  letGo(): void
}

type Stop = { why: 'ended' | 'letGo' } | { why: 'client'; error: unknown }

/** A length-prefixed chunk says it is over the reader's limit. `head`: as much of the chunk's start as the reader was
 *  asked to read of it, so that the caller can say whose it was. */
class OversizeFrameError extends Error {
  readonly head: Uint8Array<ArrayBuffer>
  constructor(head: Uint8Array<ArrayBuffer> = EMPTY) {
    super()
    this.head = head
  }
}

/** Who stopped the request body short: its client, which left (also where a read failed and nothing says why, which a
 *  Web runtime's body does as its client leaves); the body itself, which ended mid-frame; or the server, which answered
 *  or let it go before reading it to its end. */
type StoppedBy = 'client' | 'body' | 'server'

const STOPPED_BY: Record<StoppedBy, string> = {
  client: 'The client disconnected before the end of the request body',
  body: 'The request body ended before its end',
  server: 'The server answered before reading the request body to its end',
}

/** The request body stopped short. Typed so error sinks discriminate an expected truncation from a real server bug by
 *  class, not message text; the message says who stopped it. */
class StreamTruncatedError extends Error {
  readonly by: StoppedBy
  constructor(by: StoppedBy, options?: ErrorOptions) {
    super(STOPPED_BY[by], options)
    this.by = by
  }
}

/** Reads length-prefixed frames from a Web or Node request body. */
class FrameReader {
  protected static readonly EMPTY = EMPTY
  private readonly source: ChunkSource
  protected buffer: Uint8Array<ArrayBuffer> = EMPTY
  /** The parts a read has collected, which can be released while the source is still reading. */
  private collected: Uint8Array<ArrayBuffer>[] = []
  protected stop: Stop | null = null
  /** The source's read under way. */
  private reading: Promise<IteratorResult<Uint8Array<ArrayBuffer>>> | null = null
  /** Who discarded the body for its consumer, independently of how the source stopped. */
  protected discardedBy: 'client' | 'server' | null = null
  private discarding: Promise<void> | null = null
  /** Called for each chunk the source yields. */
  onBytes?: () => void
  /** Called once the source ends, read to its end. */
  onEnd?: () => void

  constructor(source: ReadableStream<Uint8Array<ArrayBuffer>> | Readable) {
    if (source instanceof ReadableStream) {
      const reader = source.getReader()
      this.source = {
        next: () => reader.read(),
        letGo: () => releaseOrCancel(reader),
      }
    } else {
      // Read as a Node `Readable` is, which bypasses Node's webstreams `dequeueValue` overhead: that is why we accept
      // `Readable` at all — `serve/node.ts` passes its `IncomingMessage` through to skip the `Readable.toWeb` round-trip.
      this.source = new ReadableChunks(source)
    }
  }

  /** Ends pending and future reads, releasing the Web reader or handing the Node body back without reading further. */
  letGo(): void {
    if (this.stop?.why === 'letGo') return
    // A stop already there stays: a read after it ends as it did.
    this.stop ??= { why: 'letGo' }
    this.clearBuffer()
    this.onBytes = undefined
    this.onEnd = undefined
    this.source.letGo()
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

  /** Stop an outstanding read when the adapter knows that the client left. */
  leftByClient(reason: unknown): void {
    if (this.stop === null) {
      this.stop = { why: 'client', error: reason }
      this.clearBuffer()
      this.onEnd = undefined
      this.source.letGo()
    }
  }

  /** Discards the remainder without retaining it, so the host can reuse the connection. */
  async discardRest(by: 'client' | 'server', onDrained?: () => void): Promise<void> {
    this.discardedBy ??= by
    this.clearBuffer()
    await (this.discarding ??= this.drain(onDrained))
  }

  private async drain(onDrained: (() => void) | undefined): Promise<void> {
    try {
      // A pending frame or file read drops its chunk when the drain starts.
      if (this.reading !== null) await this.reading.catch(() => {})
      if (this.stop) return
      while ((await this.readChunk()) !== null) {
        // Discard each chunk as it arrives.
      }
    } finally {
      onDrained?.()
    }
  }

  private clearBuffer(): void {
    this.buffer = EMPTY
    this.collected.length = 0
  }

  protected async pullChunk(): Promise<Uint8Array<ArrayBuffer> | null> {
    if (this.discarding !== null) return null
    const chunk = await this.readChunk()
    return this.discarding === null ? chunk : null
  }

  /** Pulls a source chunk, or returns null when the body stops. */
  private async readChunk(): Promise<Uint8Array<ArrayBuffer> | null> {
    if (this.stop) return null
    try {
      this.reading = this.source.next()
      const { value, done } = await this.reading
      if (done) {
        // A Node body let go of with a read under way ends that read as at its end.
        if (this.stop === null) {
          this.stop = { why: 'ended' }
          this.onEnd?.()
          this.onEnd = undefined
        }
        this.source.letGo()
        return null
      }
      if (this.stop !== null) return null
      this.onBytes?.()
      return value
    } catch (error) {
      // A stop that came while the read was under way ends it as that stop: the body let go of (a Web reader's released
      // lock fails the read), or its client's leaving as the request's signal says it (`leftByClient`).
      if (this.stop !== null) return null
      this.source.letGo()
      this.stop = { why: 'client', error }
      return null
    } finally {
      this.reading = null
    }
  }

  /** The cause of a short read, with the runtime's error where a read failed. */
  truncation(): StreamTruncatedError {
    const stop = this.stop
    if (stop?.why === 'client') return new StreamTruncatedError('client', { cause: stop.error })
    if (stop?.why === 'letGo') return new StreamTruncatedError('server')
    return new StreamTruncatedError(this.discardedBy ?? (stop?.why === 'ended' ? 'body' : 'server'))
  }

  /** Take up to `max` bytes from the internal buffer, or null if empty. */
  protected takeBuffered(max: number) {
    if (this.buffer.length === 0) return null
    const take = Math.min(this.buffer.length, max)
    const result = this.buffer.subarray(0, take)
    this.buffer = take < this.buffer.length ? this.buffer.subarray(take) : EMPTY
    return result
  }

  /** Read exactly `n` bytes. Throws on disconnect. */
  private async readExact(n: number) {
    if (this.buffer.length < n) {
      this.collected = this.buffer.length > 0 ? [this.buffer] : []
      let length = this.buffer.length
      try {
        while (length < n) {
          const chunk = await this.pullChunk()
          if (!chunk) throw this.truncation()
          this.collected.push(chunk)
          length += chunk.length
        }
        // One part is the one chunk read, with nothing buffered before it.
        const only = this.collected[0]
        this.buffer = this.collected.length === 1 && only !== undefined ? only : concatAll(this.collected)
      } finally {
        this.collected = []
      }
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
}

const DONE: IteratorReturnResult<undefined> = { done: true, value: undefined }
const ENDED = { failed: false } as const

/** A Node `Readable`'s chunks: what `read()` gives at each `'readable'`, to `'end'`, or failing at `'error'` or at a
 *  close before the end. It listens from construction, so the source can supply data before the first read. Only one
 *  read waits at a time. `letGo` ends a pending read without destroying the stream. */
class ReadableChunks implements ChunkSource {
  private readonly stream: Readable
  private waiting: {
    resolve: (result: IteratorResult<Uint8Array<ArrayBuffer>>) => void
    reject: (error: unknown) => void
  } | null = null
  /** How the stream ended: at its end, or failing (a failure after its end wins); null while it goes on. */
  private over: { failed: false } | { failed: true; error: unknown } | null = null

  constructor(stream: Readable) {
    this.stream = stream
    if (stream.errored) this.over = { failed: true, error: stream.errored }
    else if (stream.readableEnded) this.over = ENDED
    else if (stream.destroyed) this.over = { failed: true, error: prematureClose() }
    stream.on('readable', this.wake)
    stream.on('end', this.end)
    stream.on('error', this.fail)
    if (!stream.closed) stream.once('close', this.close)
    unwatchEarlyReadableError.call(stream)
    if (stream.closed) stream.off('error', this.fail)
  }

  next(): Promise<IteratorResult<Uint8Array<ArrayBuffer>>> {
    assert(this.waiting === null)
    const value: Uint8Array<ArrayBuffer> | null = this.stream.destroyed ? null : this.stream.read()
    if (value !== null) return Promise.resolve({ value, done: false })
    if (this.over?.failed) return Promise.reject(this.over.error)
    if (this.over) return Promise.resolve(DONE)
    return new Promise((resolve, reject) => (this.waiting = { resolve, reject }))
  }

  /** Ends the read under way as at the stream's end, and takes off the listeners that read the stream, so that its
   *  owner reads it as before (`'readable'` holds it out of flowing mode). Its error listener stays, reading nothing: an
   *  error the stream emits later reaches it, not the process. */
  letGo(): void {
    this.stream.off('end', this.end)
    this.stream.off('readable', this.wake)
    this.settle(DONE)
  }

  private readonly wake = (): void => {
    if (this.waiting === null) return
    const value: Uint8Array<ArrayBuffer> | null = this.stream.read()
    if (value !== null) this.settle({ value, done: false })
  }

  private readonly end = (): void => {
    this.over ??= ENDED
    this.settle(DONE)
  }

  private readonly fail = (error: unknown): void => {
    if (!this.over?.failed) this.over = { failed: true, error }
    const waiting = this.waiting
    this.waiting = null
    waiting?.reject(this.over.error)
  }

  private readonly close = (): void => {
    this.stream.off('error', this.fail)
    if (this.over === null) this.fail(prematureClose())
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
