export { BodyReader, OversizeFrameError, StreamTruncatedError }

import type { Readable } from 'node:stream'
import { assert, assertUsage, assertWarning } from '../../utils/assert.js'
import { createDeferred, type Deferred } from '../../utils/createDeferred.js'
import { concatAll, decodeU32 } from './envelope.js'
import { ProtocolViolationError } from './protocolViolation.js'
import type { QueueMemory } from '../channel/channelMessages.js'

/** Shared sentinel — avoids zero-length subarray views that pin large ArrayBuffers. */
const EMPTY = new Uint8Array(0)

/** What the reader reads the body through: a Web body's reader, or a Node body's chunks (`ReadableChunks`). `letGo`
 *  reads no more of the body, and does nothing more where called again: it hands the body back to its owner unread, or
 *  cancels it where the runtime keeps the reader's lock (workerd). */
type ChunkSource = {
  next(): Promise<IteratorResult<Uint8Array<ArrayBuffer>>>
  letGo(): void
}

/** Why the reader reads no more: the body ended, it was let go of, or a read failed as the client leaving, as nothing
 *  says (`unknown`, a truncation either way), or as the app's own error. */
type Stop = { why: 'ended' | 'letGo' } | { why: 'clientLeft' | 'unknown' | 'app'; error: unknown }

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
 * collapse to one `ChunkSource` so the rest of the class is
 * source-agnostic, and both can be let go of with a read under way (`letGo`).
 *
 * Fields use TS `private` rather than JS `#private`: the inner read loop touches
 * `buffer`/`source` per chunk, and `#fields` compile to per-access `WeakMap.get/set`
 * which dominates CPU on hot streaming paths. Plain property access is cheap.
 */
class BodyReader {
  private readonly source: ChunkSource
  private buffer: Uint8Array<ArrayBuffer> = EMPTY
  private readonly fileSizes = new Map<number, number>()
  /** Each file's stream, the latest one where a file's stream was asked for more than once. */
  private readonly files = new Map<number, FileRead>()
  /** The position: the file the next byte of the body belongs to, and its bytes not yet read (unset until it is first
   *  read or passed). */
  private next = 0
  private left: number | undefined
  /** The file read whose pull is in flight: it alone reads the source, also while it passes earlier files. */
  private holder: FileRead | null = null
  /** What the reads that wait for the position await: a file's end, a pull letting go of the position, a cancel. */
  private fileEnd: Deferred<void> | null = null
  private stop: Stop | null = null
  /** Who ended the file reads: the client, whose request ended through its abort, or the server, which answered or let
   *  the body go. */
  private filesEndedBy: 'client' | 'server' | null = null
  /** The source's read under way. */
  private reading: Promise<IteratorResult<Uint8Array<ArrayBuffer>>> | null = null
  /** The read-off of the rest of the body, once it began: from then on it alone reads the source. */
  private readOff: Promise<void> | null = null
  private readOffDone = false
  /** What the read-off keeps for the file reads, where it began at the call's answer (`keepRest`): the chunks it read,
   *  in order, each held on `keeping` until a file read takes it. Null where it drops what it reads. */
  private keeping: QueueMemory | null = null
  private kept: Uint8Array<ArrayBuffer>[] = []
  /** Whether `keeping` refused a chunk: that chunk and every one after it were dropped. */
  private dropped = false
  /** What a file read that waits for the read-off's next chunk awaits. */
  private keptMore: Deferred<void> | null = null
  private readonly readOffEnd = createDeferred<void>()
  /** Settles once the read-off has read the body to its end, or its client left: the client's upload is over. */
  readonly restRead: Promise<void> = this.readOffEnd.promise
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
      this.source = {
        next: () => reader.read(),
        // Node, Deno and Bun fail the read under way. workerd throws instead and keeps the lock, as the streams standard
        // did before 2022, so that read would end only at the body's next chunk: the cancel ends it now.
        letGo: () => {
          try {
            reader.releaseLock()
          } catch {
            reader.cancel().catch(() => {})
          }
        },
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
    this.onChunk = undefined
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

  /** Ends the file reads, lets go of what the read-off kept for them, then reads the rest of the body and drops it, a
   *  chunk at a time, to its end or its client's leaving. A read of the body under way finishes first, and every read but
   *  this one ends as at the body's end. A body whose reads stopped already has no rest: an app's error that stopped them
   *  went to the read it failed. `endedBy`: who ended the request, as the file reads say it where nothing stopped the
   *  body. */
  async discardRest(endedBy: 'client' | 'server'): Promise<void> {
    this.endFiles(endedBy)
    this.keeping?.release()
    this.keeping = null
    this.kept = []
    this.buffer = EMPTY
    this.wakeKept()
    await (this.readOff ??= this.readRest())
  }

  /** Reads the rest of the body from now on, at the answer of a call that a value it returned still holds, keeping each
   *  chunk on `memory` for the file reads, which take them in order as they read: the client's upload ends, so that a
   *  browser, which delivers an answer only once it has sent the whole body, gets it. A chunk `memory` refuses is
   *  dropped, and so is every one after it: a file read that reaches them fails, saying why. `discardRest`, at the end
   *  of the call's holds, lets go of what is kept and drops the rest. */
  keepRest(memory: QueueMemory): void {
    if (this.readOff !== null || this.filesEndedBy !== null) return
    this.keeping = memory
    this.readOff = this.readRest()
    // A failure, the app's own error, goes to `discardRest`, which awaits the same read-off.
    this.readOff.catch(() => {})
  }

  /** The read-off: the source's chunks to its end or its client's leaving, each kept or dropped. A read of the body
   *  under way finishes first: its own pull, awaiting it since it began, takes its outcome. */
  private async readRest(): Promise<void> {
    try {
      if (this.reading !== null) await this.reading.catch(() => {})
      if (this.keeping === null) this.buffer = EMPTY
      if (this.stop) return
      for (let chunk = await this.readChunk(); chunk !== null; chunk = await this.readChunk()) this.keep(chunk)
    } finally {
      this.readOffDone = true
      this.wakeKept()
      this.readOffEnd.resolve()
    }
  }

  private keep(chunk: Uint8Array<ArrayBuffer>): void {
    if (this.keeping === null || this.dropped) return
    if (this.keeping.tryHold(chunk.length)) this.kept.push(chunk)
    else this.dropped = true
    this.wakeKept()
  }

  /** The read-off's next chunk, for `by`'s read: null where it drops what it reads, as at the body's end; at the end of
   *  what it read, the source's, as `readChunk` says it. */
  private async takeKept(by: FileRead | null): Promise<Uint8Array<ArrayBuffer> | null> {
    for (;;) {
      if (this.keeping === null) return null
      const chunk = this.kept.shift()
      if (chunk !== undefined) {
        this.keeping.letGo(chunk.length)
        return chunk
      }
      if (this.dropped) throw this.droppedError(by)
      if (this.readOffDone) return this.readChunk()
      this.keptMore ??= createDeferred()
      await this.keptMore.promise
    }
  }

  private wakeKept(): void {
    const keptMore = this.keptMore
    if (keptMore === null) return
    this.keptMore = null
    keptMore.resolve()
  }

  /** A file read reached what the read-off dropped. Only a file read takes kept chunks: the call's metadata is read
   *  before its answer. */
  private droppedError(by: FileRead | null): Error {
    assert(by !== null)
    return new Error(
      `The telefunction returned before it read this File (${this.fileSizes.get(by.index)} bytes), and the server keeps no more of a File left unread than config.channel.maxMemory and config.channel.maxPendingBytes allow: the rest was dropped. Read a File before the telefunction returns, or while what it returns streams, to keep all of it.`,
    )
  }

  /** The client left before the body's end, where the body's own read may not end (Node's, once the call answered):
   *  the read under way and every one after it end as at its leaving, `reason` their cause. */
  leftByClient(reason: unknown): void {
    if (this.stop) return
    this.stop = { why: 'clientLeft', error: reason }
    this.source.letGo()
  }

  /** Settles once the source's read under way, if any, has settled, and its reader has taken its outcome. */
  async readSettled(): Promise<void> {
    if (this.reading !== null) await this.reading.catch(() => {})
  }

  /** Registers a file's size, as the call's metadata declares it, while the call is revived, and returns the getter of
   *  the file's stream. */
  registerFile(index: number, size: number): () => ReadableStream<Uint8Array<ArrayBuffer>> {
    this.fileSizes.set(index, size)
    return () => this.fileStream(index)
  }

  /**
   * The file at `index`: a stream of its bytes, read off the body only as the app reads it (`highWaterMark: 0`).
   *
   * The body is one forward-only sequence, so the reader owns its position and lends it to one pull at a time. A read of
   * a later file passes every earlier file nobody holds (no pull in flight, its stream not locked), dropping its bytes,
   * and waits only behind one that somebody holds. A cancel reads nothing: the file's bytes are dropped once a later read
   * passes them.
   */
  private fileStream(index: number): ReadableStream<Uint8Array<ArrayBuffer>> {
    const file = new FileRead(index, this.pullFile, this.cancelFile)
    this.files.set(index, file)
    if (this.filesEndedBy !== null) file.fail(this.stopped())
    return file.stream
  }

  /** One chunk of `file`, once it holds the position: the buffer's, or the source's. */
  private readonly pullFile = async (file: FileRead): Promise<void> => {
    if (file.over) return
    if (this.holder === null && this.next === file.index) this.holder = file
    else await this.takePosition(file)
    try {
      const left = this.leftOfNext()
      if (left === 0) return this.fileDone(file)
      let bytes = this.buffer
      if (bytes.length === 0) {
        const pulled = await this.pullChunk(file)
        // Let go of while the read was under way: `letGo` errored the stream, and what came after is dropped.
        if (this.stop?.why === 'letGo') return
        if (pulled === null) throw this.truncation()
        bytes = pulled
      }
      const chunk = this.handOut(bytes, left)
      this.left = left - chunk.length
      // Cancelled while the read was under way: the bytes it took are read, and dropped.
      if (!file.over) file.enqueue(chunk)
      if (this.left === 0) this.fileDone(file)
    } catch (error) {
      file.over = true
      throw error
    } finally {
      this.release()
    }
  }

  /** The first `left` bytes of `bytes`, for the file, and the rest kept to be read. The rest is the reader's own copy:
   *  a file's bytes may be handed on with their buffer transferred (`postMessage(chunk, [chunk.buffer])`), which would
   *  detach every other view of that buffer. Only a chunk that spans a file's end is copied, its part after the end. */
  private handOut(bytes: Uint8Array<ArrayBuffer>, left: number): Uint8Array<ArrayBuffer> {
    if (bytes.length <= left) {
      this.buffer = EMPTY
      return bytes
    }
    // A copy: on a Node `Buffer`, `slice` is a view of the same buffer.
    this.buffer = new Uint8Array(bytes.subarray(left))
    return bytes.subarray(0, left)
  }

  private readonly cancelFile = (file: FileRead): void => {
    file.over = true
    // A pull of it under way sees the cancel once its read is done, and wakes the waiting reads then.
    if (this.holder !== file) this.wake()
  }

  /** Waits until `file`'s read holds the position at its own index, passing every earlier file that nobody holds. */
  private async takePosition(file: FileRead): Promise<void> {
    for (;;) {
      if (this.filesEndedBy !== null) throw this.stopped()
      assertUsage(
        file.index >= this.next,
        `File argument ${file.index} has already been consumed (currently at ${this.next}). File arguments must be read in order.`,
      )
      if (this.holder === null && !this.isHeld(this.next, file)) {
        if (this.next === file.index) {
          this.holder = file
          return
        }
        await this.pass(file)
        continue
      }
      this.fileEnd ??= createDeferred()
      await this.fileEnd.promise
    }
  }

  /** Whether a read other than `by` holds the file at `index`: its stream is locked, and it reads on. */
  private isHeld(index: number, by: FileRead): boolean {
    const file = this.files.get(index)
    return file !== undefined && file !== by && !file.over && file.stream.locked
  }

  /** Drops the bytes of the file at the position, for `by`, which holds the position meanwhile. A failure is the stop's
   *  own: the truncation, or the app's error. */
  private async pass(by: FileRead): Promise<void> {
    this.holder = by
    try {
      assertWarning(
        this.files.get(this.next)?.over === true,
        `File arguments are being consumed out of order (reading ${by.index}, expected ${this.next}). Skipped files will be unreadable. For correct behavior, consume file arguments in the order they appear.`,
        { onlyOnce: true },
      )
      await this.skipBytes(this.leftOfNext(), by)
      this.next++
      this.left = undefined
    } finally {
      this.release()
    }
  }

  /** The bytes not yet read of the file at the position. A file the call's metadata doesn't declare is malformed. */
  private leftOfNext(): number {
    this.left ??= this.fileSizes.get(this.next)
    if (this.left === undefined) throw new ProtocolViolationError(`the call's metadata declares no file ${this.next}`)
    return this.left
  }

  private fileDone(file: FileRead): void {
    file.close()
    this.next++
    this.left = undefined
  }

  /** Errors every open file stream, and every one asked for later, with the stop's own error; the reads that wait wake and
   *  throw it. A file's pull under way finishes its read of the body, and drops it. */
  private endFiles(by: 'client' | 'server'): void {
    this.filesEndedBy ??= by
    for (const file of this.files.values()) file.fail(this.stopped())
    this.wake()
  }

  /** Lets go of the position, and wakes the reads that wait for it. */
  private release(): void {
    this.holder = null
    this.wake()
  }

  /** Wakes the reads that wait for the position. */
  private wake(): void {
    const fileEnd = this.fileEnd
    if (fileEnd === null) return
    this.fileEnd = null
    fileEnd.resolve()
  }

  // ── Primitives ──

  /** Pull one chunk, for `by`'s read (null: the metadata's), from the underlying source, or null once it ended or its
   *  client left, or failed where nothing says why; once the rest of the body is being read off, from what the read-off
   *  keeps (`takeKept`). A read that fails as the app's own error throws it, and so does every pull after it. */
  private async pullChunk(by: FileRead | null) {
    // The read-off reads the source alone, as one read of it at a time holds (`ReadableChunks.next()` asserts it). A pass
    // under way when the read-off starts reaches this line, but no row sees what it prevents: without it, that pass reads
    // on beside the read-off, and on a Node body meets the assert (probed: it fires, and its error goes into a file that
    // has already failed), on a Web body takes a chunk the read-off would drop.
    if (this.readOff !== null) return this.takeKept(by)
    return this.readChunk()
  }

  /** One chunk of the source, as `pullChunk` says. */
  private async readChunk() {
    if (this.stop) {
      if (this.stop.why === 'app') throw this.stop.error
      return null
    }
    this.reading = this.source.next()
    try {
      const { value, done } = await this.reading
      if (done) {
        this.stop ??= { why: 'ended' }
        return null
      }
      this.onChunk?.()
      return value
    } catch (error) {
      // A stop that came while the read was under way ends it as that stop: the body let go of (a Web reader's released
      // lock fails the read), or its client's leaving as the request's signal says it (`leftByClient`).
      if (this.stop !== null) return null
      const clientLeft = this.clientLeft(error)
      this.stop = { why: clientLeft === null ? 'unknown' : clientLeft ? 'clientLeft' : 'app', error }
      if (this.stop.why === 'app') throw error
      return null
    } finally {
      this.reading = null
    }
  }

  /** What a read after the reader's stop throws: the app's own error, as the read it failed threw, or a truncation. */
  private stopped(): unknown {
    return this.stop?.why === 'app' ? this.stop.error : this.truncation()
  }

  /** The body stopped short, said by who stopped it, with the runtime's error as the cause where a read failed. With no
   *  stop, or at the end the read-off reached once the file reads ended, who ended them stopped it. */
  truncation() {
    const stop = this.stop
    if (stop === null || (stop.why === 'ended' && this.filesEndedBy !== null))
      return new StreamTruncatedError(this.filesEndedBy ?? 'server')
    if (!('error' in stop)) return new StreamTruncatedError(stop.why === 'letGo' ? 'server' : 'body')
    return new StreamTruncatedError(stop.why === 'app' ? 'body' : 'client', { cause: stop.error })
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
        const chunk = await this.pullChunk(null)
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
      const chunk = await this.pullChunk(null)
      if (!chunk) return null
      this.buffer = chunk
    }
    return this.readExact(n)
  }

  /** Skip exactly `n` bytes, for `by`'s read. Throws on disconnect. */
  private async skipBytes(n: number, by: FileRead) {
    let remaining = n
    const buffered = this.takeBuffered(remaining)
    if (buffered) remaining -= buffered.length
    while (remaining > 0) {
      const chunk = await this.pullChunk(by)
      if (!chunk) throw this.truncation()
      remaining -= chunk.length
      if (remaining < 0) this.buffer = chunk.subarray(chunk.length + remaining)
    }
  }
}

/** A file's stream, as the reader serves it. `over`: it reads no more, ended, cancelled or failed. */
class FileRead {
  readonly index: number
  readonly stream: ReadableStream<Uint8Array<ArrayBuffer>>
  over = false
  private controller: ReadableStreamDefaultController<Uint8Array<ArrayBuffer>> | null = null

  constructor(index: number, pull: (file: FileRead) => Promise<void>, cancel: (file: FileRead) => void) {
    this.index = index
    this.stream = new ReadableStream<Uint8Array<ArrayBuffer>>(
      {
        start: (controller) => void (this.controller = controller),
        pull: () => pull(this),
        cancel: () => cancel(this),
      },
      { highWaterMark: 0 },
    )
  }

  enqueue(chunk: Uint8Array<ArrayBuffer>): void {
    this.controller?.enqueue(chunk)
  }

  close(): void {
    if (this.over) return
    this.over = true
    this.controller?.close()
  }

  fail(error: unknown): void {
    if (this.over) return
    this.over = true
    this.controller?.error(error)
  }
}

const DONE: IteratorReturnResult<undefined> = { done: true, value: undefined }

/** A Node `Readable`'s chunks: what `read()` gives at each `'readable'`, to `'end'`, or failing at `'error'` or at a
 *  close before the end. It listens from its construction on, so the stream asks its source for data before the first
 *  read; and one read waits at a time, as its one caller, the reader's `readChunk`, awaits each before the next, and the
 *  read-off and a file's pull reach it in turn (`pullChunk` reads nothing while the read-off runs). Unlike
 *  Node's own iterator, it can be let go of with a read under way (`letGo`): Node's ends a read under way only by
 *  destroying the stream. */
class ReadableChunks implements ChunkSource {
  private readonly stream: Readable
  private waiting: {
    resolve: (result: IteratorResult<Uint8Array<ArrayBuffer>>) => void
    reject: (error: unknown) => void
  } | null = null
  private ended: boolean
  private failed: { error: unknown } | null = null

  constructor(stream: Readable) {
    this.stream = stream
    this.ended = stream.readableEnded
    if (stream.errored) this.failed = { error: stream.errored }
    else if (stream.destroyed && !this.ended) this.failed = { error: prematureClose() }
    stream.on('readable', this.wake)
    stream.on('end', this.end)
    stream.on('error', this.fail)
    stream.on('close', this.close)
  }

  next(): Promise<IteratorResult<Uint8Array<ArrayBuffer>>> {
    assert(this.waiting === null)
    const value: Uint8Array<ArrayBuffer> | null = this.stream.destroyed ? null : this.stream.read()
    if (value !== null) return Promise.resolve({ value, done: false })
    if (this.failed) return Promise.reject(this.failed.error)
    if (this.ended) return Promise.resolve(DONE)
    return new Promise((resolve, reject) => (this.waiting = { resolve, reject }))
  }

  /** Ends the read under way as at the stream's end, and takes off the listeners that read the stream, so that its
   *  owner reads it as before (`'readable'` holds it out of flowing mode). Its error listener stays, reading nothing: an
   *  error the stream emits later reaches it, not the process. */
  letGo(): void {
    this.stream.off('end', this.end)
    this.stream.off('close', this.close)
    this.stream.off('readable', this.wake)
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
