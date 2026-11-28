export { BodyReader, OversizeFrameError, StreamTruncatedError }

import type { Readable } from 'node:stream'
import { assert, assertUsage, assertWarning } from '../../utils/assert.js'
import { createDeferred, type Deferred } from '../../utils/createDeferred.js'
import { concatAll, decodeU32 } from './envelope.js'
import { ProtocolViolationError } from './protocolViolation.js'
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

/** Reads [u32 metadata length][metadata bytes][file0 bytes][file1 bytes]... from a Web or Node stream, using the file
 *  sizes declared in the metadata. */
class BodyReader {
  private readonly source: ChunkSource
  private buffer: Uint8Array<ArrayBuffer> = EMPTY
  /** The parts of the frame `readExact` collects, on the reader so that `discardRest` lets go of them while it waits. */
  private collected: Uint8Array<ArrayBuffer>[] = []
  private readonly fileSizes = new Map<number, number>()
  /** Files with a stream, including cancelled files whose bytes a later pull still has to skip. */
  private readonly files = new Map<number, FileRead>()
  /** The position: the file the next byte of the body belongs to, and its bytes not yet read (unset until it is first
   *  read or passed). */
  private next = 0
  private left: number | undefined
  /** The file whose pull owns the body position, including while it skips earlier files. */
  private holder: FileRead | null = null
  /** Wakes waiting pulls when the position is released or an earlier file closes, cancels, or unlocks. */
  private positionChanged: Deferred<void> | null = null
  private stop: Stop | null = null
  /** Who ended the file reads: the client, whose request ended through its abort, or the server, which answered or let
   *  the body go. */
  private filesEndedBy: 'client' | 'server' | null = null
  /** The source's read under way. */
  private reading: Promise<IteratorResult<Uint8Array<ArrayBuffer>>> | null = null
  /** Once set, the drain alone may read the source. */
  private discarding: Promise<void> | null = null
  private readonly drainEnd = createDeferred<void>()
  /** Called for each chunk the source yields. */
  onBytes?: () => void
  /** Called once the source ends, read to its end. */
  onEnd?: () => void

  /** Settles once the drain stops, however it does: at the body's end, at its client's leaving, or at a failed read. */
  get restRead(): Promise<void> {
    return this.drainEnd.promise
  }

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

  /** Reads no more of the body: the read under way and every one after it end as at the body's end, and every open file
   *  stream errors at once, with a truncation. A Web body's lock is released, handing it back to its owner unread, or,
   *  where the runtime keeps the lock (workerd), the body is cancelled; a Node body is handed back with only this
   *  reader's error listener, which reads nothing and takes an error it emits later. */
  letGo(): void {
    if (this.stop?.why === 'letGo') return
    // A stop already there stays: a read after it ends as it did.
    this.stop ??= { why: 'letGo' }
    this.buffer = EMPTY
    this.collected.length = 0
    this.onBytes = undefined
    this.onEnd = undefined
    this.source.letGo()
    this.endFiles('server')
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

  /** Ends file reads and drains the remaining body without retaining it, so the connection can be reused. */
  async discardRest(endedBy: 'client' | 'server'): Promise<void> {
    this.endFiles(endedBy)
    this.buffer = EMPTY
    this.collected.length = 0
    await (this.discarding ??= this.drain())
  }

  private async drain(): Promise<void> {
    try {
      // An outstanding file or frame read observes that the drain has started before using its chunk.
      if (this.reading !== null) await this.reading.catch(() => {})
      if (this.stop) return
      while ((await this.readChunk()) !== null) {
        // Discard each chunk as it arrives.
      }
    } finally {
      this.drainEnd.resolve()
    }
  }

  /** Stop an outstanding read when the adapter knows that the client left. */
  leftByClient(reason: unknown): void {
    if (this.stop === null) {
      this.stop = { why: 'client', error: reason }
      this.buffer = EMPTY
      this.collected.length = 0
      this.onEnd = undefined
      this.source.letGo()
    }
    if (this.stop.why !== 'ended') this.endFiles('client')
  }

  /** Registers the size declared in the metadata. The file pulls bytes when it is read. */
  registerFile(index: number, size: number): () => ReadableStream<Uint8Array<ArrayBuffer>> {
    this.fileSizes.set(index, size)
    return () => this.fileStream(index)
  }

  /** A later pull waits for a locked earlier file. It discards earlier files without a reader. */
  private fileStream(index: number): ReadableStream<Uint8Array<ArrayBuffer>> {
    const file = new FileRead(index, this.pullFile, this.cancelFile, () => this.wake())
    if (this.filesEndedBy === null) this.files.set(index, file)
    else file.fail(this.fileError())
    return file.stream
  }

  private readonly pullFile = async (file: FileRead): Promise<void> => {
    if (file.over) return
    if (this.holder === null && this.next === file.index) this.holder = file
    else {
      try {
        if (!(await this.takePosition(file))) return
      } catch (error) {
        file.fail(error)
        return
      }
    }
    try {
      const left = this.leftOfNext()
      if (left === 0) return this.fileDone(file)
      let bytes = this.buffer
      if (bytes.length === 0) {
        const pulled = await this.pullChunk()
        if (pulled === null) throw this.truncation()
        bytes = pulled
      }
      const chunk = this.handOut(bytes, left)
      this.left = left - chunk.length
      // Cancelled while the read was under way: the bytes it took are read, and dropped.
      if (!file.over) file.enqueue(chunk)
      if (this.left === 0) this.fileDone(file)
    } catch (error) {
      file.fail(error)
    } finally {
      this.release()
    }
  }

  /** The first `left` bytes of `bytes`, for the file, and the rest kept to be read as a view. What the file gets owns its
   *  whole buffer: it may be handed on with its buffer transferred (`postMessage(chunk, [chunk.buffer])`), which detaches
   *  every other view of that buffer, and it may be kept, which keeps its whole buffer. So a source chunk read whole is
   *  handed on as it is, and any other part of one is copied. */
  private handOut(bytes: Uint8Array<ArrayBuffer>, left: number): Uint8Array<ArrayBuffer> {
    if (bytes.length > left) {
      this.buffer = bytes.subarray(left)
      return new Uint8Array(bytes.subarray(0, left))
    }
    this.buffer = EMPTY
    return bytes.byteLength === bytes.buffer.byteLength ? bytes : new Uint8Array(bytes)
  }

  private readonly cancelFile = (file: FileRead): void => {
    file.cancel()
    // An in-flight pull advances the position before waking later files.
    if (this.holder !== file) this.wake()
  }

  private async takePosition(file: FileRead): Promise<boolean> {
    while (!file.over) {
      assertUsage(
        file.index >= this.next,
        `File argument ${file.index} has already been consumed (currently at ${this.next}). File arguments must be read in order.`,
      )
      if (this.holder === null) {
        if (this.next === file.index) {
          this.holder = file
          return true
        }
        const earlier = this.files.get(this.next)
        if (earlier?.over || !earlier?.stream.locked) {
          this.holder = file
          try {
            assertWarning(
              earlier?.over === true,
              `File arguments are being consumed out of order (reading ${file.index}, expected ${this.next}). Skipped files will be unreadable. For correct behavior, consume file arguments in the order they appear.`,
              { onlyOnce: true },
            )
            await this.skipBytes(this.leftOfNext())
            this.files.delete(this.next)
            this.next++
            this.left = undefined
          } finally {
            this.release()
          }
          continue
        }
      }
      this.positionChanged ??= createDeferred()
      await this.positionChanged.promise
    }
    return false
  }

  /** The bytes not yet read of the file at the position. A file the call's metadata doesn't declare is malformed. */
  private leftOfNext(): number {
    this.left ??= this.fileSizes.get(this.next)
    if (this.left === undefined) throw new ProtocolViolationError(`the call's metadata declares no file ${this.next}`)
    return this.left
  }

  private fileDone(file: FileRead): void {
    file.close()
    this.files.delete(file.index)
    this.next++
    this.left = undefined
  }

  /** Errors every open file stream, and every one asked for later, with the stop's own error; the reads that wait wake and
   *  throw it. A file's pull under way finishes its read of the body, and drops it. No file is left to read. */
  private endFiles(by: 'client' | 'server'): void {
    if (this.filesEndedBy !== null) return
    this.filesEndedBy = by
    if (this.files.size > 0) {
      const error = this.fileError()
      for (const file of this.files.values()) file.fail(error)
      this.files.clear()
    }
    this.wake()
  }

  /** Lets go of the position, and wakes the reads that wait for it. */
  private release(): void {
    this.holder = null
    this.wake()
  }

  /** Wakes the reads that wait for the position. */
  private wake(): void {
    const change = this.positionChanged
    this.positionChanged = null
    change?.resolve()
  }

  // ── Primitives ──

  private async pullChunk() {
    if (this.discarding !== null) return null
    const chunk = await this.readChunk()
    return this.discarding === null ? chunk : null
  }

  /** Pulls a source chunk, or returns null when the body stops. */
  private async readChunk() {
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

  /** A server-ended file stays truncated even if the background drain later fails. */
  private fileError(): StreamTruncatedError {
    if (this.filesEndedBy === 'server') return new StreamTruncatedError('server')
    return this.truncation()
  }

  /** The body stopped short, said by who stopped it, with the runtime's error as the cause where a read failed. With no
   *  stop, or at the end the drain reached once the file reads ended, who ended them stopped it. */
  truncation() {
    const stop = this.stop
    if (stop === null || (stop.why === 'ended' && this.filesEndedBy !== null))
      return new StreamTruncatedError(this.filesEndedBy ?? 'server')
    if (!('error' in stop)) return new StreamTruncatedError(stop.why === 'letGo' ? 'server' : 'body')
    return new StreamTruncatedError('client', { cause: stop.error })
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
      this.collected = this.buffer.length > 0 ? [this.buffer] : []
      let length = this.buffer.length
      try {
        while (length < n) {
          const chunk = await this.pullChunk()
          // Past the drain's start, `discardRest` let go of the parts.
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

class FileRead {
  readonly index: number
  readonly stream: ReadableStream<Uint8Array<ArrayBuffer>>
  private controller: ReadableStreamDefaultController<Uint8Array<ArrayBuffer>> | null = null

  constructor(
    index: number,
    pull: (file: FileRead) => Promise<void>,
    cancel: (file: FileRead) => void,
    onReaderReleased: () => void,
  ) {
    this.index = index
    this.stream = new ReadableStream<Uint8Array<ArrayBuffer>>(
      {
        start: (controller) => void (this.controller = controller),
        pull: () => pull(this),
        cancel: () => cancel(this),
      },
      { highWaterMark: 0 },
    )
    const stream = this.stream
    Object.defineProperty(stream, 'getReader', {
      configurable: true,
      writable: true,
      value(this: typeof stream, ...args: Parameters<typeof stream.getReader>) {
        const reader = ReadableStream.prototype.getReader.apply(this, args)
        if (this === stream) {
          void reader.closed.catch(() => {
            if (!stream.locked) onReaderReleased()
          })
        }
        return reader
      },
    })
  }

  get over(): boolean {
    return this.controller === null
  }

  enqueue(chunk: Uint8Array<ArrayBuffer>): void {
    this.controller?.enqueue(chunk)
  }

  cancel(): void {
    this.controller = null
  }

  close(): void {
    this.controller?.close()
    this.controller = null
  }

  fail(error: unknown): void {
    this.controller?.error(error)
    this.controller = null
  }
}

const DONE: IteratorReturnResult<undefined> = { done: true, value: undefined }
const ENDED = { failed: false } as const

/** A Node `Readable`'s chunks: what `read()` gives at each `'readable'`, to `'end'`, or failing at `'error'` or at a
 *  close before the end. It listens from its construction on, so the stream asks its source for data before the first
 *  read; and one read waits at a time, as its one caller, the reader's `readChunk`, awaits each before the next, and the
 *  drain and a file's pull reach it in turn (`pullChunk` reads nothing while the drain runs). Unlike
 *  Node's own iterator, it can be let go of with a read under way (`letGo`): Node's ends a read under way only by
 *  destroying the stream. */
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
