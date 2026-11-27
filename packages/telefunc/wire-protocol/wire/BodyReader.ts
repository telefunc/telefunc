export { BodyReader, OversizeFrameError, StreamTruncatedError }

import type { Readable } from 'node:stream'
import { assert, assertUsage, assertWarning } from '../../utils/assert.js'
import { createDeferred, type Deferred } from '../../utils/createDeferred.js'
import { concatAll, decodeU32 } from './envelope.js'
import { ProtocolViolationError } from './protocolViolation.js'
import { Fifo } from '../../utils/Fifo.js'
import type { QueueMemory } from '../channel/channelMessages.js'
import type { RequestFile } from '../values/types.js'
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
  /** The parts of the frame `readExact` collects, on the reader so that `discardRest` lets go of them while it waits. */
  private collected: Uint8Array<ArrayBuffer>[] = []
  private readonly fileSizes = new Map<number, number>()
  /** The files whose bytes a File can still read: neither read to their end, cancelled or failed, nor let go of. Each
   *  one's callbacks run once it leaves. */
  private readonly unread = new Map<number, Array<() => void>>()
  /** Each file's stream, the latest one where a file's stream was asked for more than once. */
  private readonly files = new Map<number, FileRead>()
  /** The position: the file the next byte of the body belongs to, and its bytes not yet read (unset until it is first
   *  read or passed). */
  private next = 0
  private left: number | undefined
  /** The file read whose pull is in flight: it alone reads the source, also while it passes earlier files or reads one
   *  ahead for them. */
  private holder: FileRead | null = null
  /** What the reads that wait for the position await: a file's end, a pull letting go of the position, a cancel, a file
   *  taking the piece read ahead for it, or a reader releasing its lock. */
  private fileEnd: Deferred<void> | null = null
  private stop: Stop | null = null
  /** Who ended the file reads: the client, whose request ended through its abort, or the server, which answered or let
   *  the body go. */
  private filesEndedBy: 'client' | 'server' | null = null
  /** The source's read under way. */
  private reading: Promise<IteratorResult<Uint8Array<ArrayBuffer>>> | null = null
  /** The read-off of the rest of the body, once it began: from then on it alone reads the source. */
  private readOff: Promise<void> | null = null
  /** Where the reader counts what it keeps of the body for the file reads: what the read-off keeps (`kept`), and what a
   *  file's reads read ahead for an earlier file the app began and left without a reader. */
  private readonly memory: () => QueueMemory
  /** What the read-off keeps for the file reads, from the call's answer or end on (`keepRest`), while a file is left to
   *  read. Null where it drops what it reads. */
  private kept: Kept | null = null
  private readonly readOffEnd = createDeferred<void>()
  /** Whether a read's failure is the client leaving, or null where nothing says, on the evidence of the server that
   *  received the request, passed by the caller. */
  private clientLeft: (err: unknown) => boolean | null
  /** Called for each chunk the source yields. */
  onChunk?: () => void
  /** Called once the source ends, read to its end. */
  onEnd?: () => void

  /** Settles once the read-off has read the body to its end, or its client left: the client's upload is over. */
  get restRead(): Promise<void> {
    return this.readOffEnd.promise
  }

  constructor(
    source: ReadableStream<Uint8Array<ArrayBuffer>> | Readable,
    clientLeft: (err: unknown) => boolean | null,
    memory: () => QueueMemory,
  ) {
    this.clientLeft = clientLeft
    this.memory = memory
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

  /** Ends the file reads, which lets go of what the read-off kept for them, then reads the rest of the body and drops it,
   *  a chunk at a time, to its end or its client's leaving. A read of the body under way finishes first, and every read
   *  but this one ends as at the body's end; a frame under read is let go of at once, and its read ends with a
   *  truncation. A body whose reads stopped already has no rest: an app's error that stopped them went to the read it
   *  failed. `endedBy`: who ended the request, as the file reads say it where nothing stopped the body. */
  async discardRest(endedBy: 'client' | 'server'): Promise<void> {
    this.endFiles(endedBy)
    this.buffer = EMPTY
    // Emptied in place: the read that collects into it may hold the array while it waits.
    this.collected.length = 0
    await (this.readOff ??= this.readRest())
  }

  /** At the call's end: each File the app can still read keeps its bytes, kept from the answer on (`keepRest`) or from
   *  now, until it reads them or lets go of the File, and the rest of the body is read off and dropped, to its end or its
   *  client's leaving. Where no File is left to read, as `discardRest`. */
  async endCall(endedBy: 'client' | 'server'): Promise<void> {
    if (this.unread.size === 0) return this.discardRest(endedBy)
    this.keepRest()
    await this.readOff
  }

  /** Reads the rest of the body from now on, at the answer of a call that a value it returned still holds, or at the
   *  call's end (`endCall`): the client's upload ends, so that a browser, which delivers an answer only once it has sent
   *  the whole body, gets it. While a file is left to read, each chunk is kept on the reader's memory for the file reads,
   *  which take them in order as they read; a chunk it refuses is dropped, and so is every one after it: a file read that
   *  reaches them fails, saying why. What is kept goes once no file is left to read. */
  keepRest(): void {
    if (this.readOff !== null || this.filesEndedBy !== null) return
    if (this.unread.size > 0)
      this.kept = { memory: this.memory(), chunks: [], refused: false, readAll: false, more: null }
    this.readOff = this.readRest()
    // A failure, the app's own error, goes to `endCall` or `discardRest`, which await the same read-off.
    this.readOff.catch(() => {})
  }

  /** The client that sends the body isn't coming for the call's answer (it left while the call ran, or the connect window
   *  of a channel the call returned ran out, with no byte of the body for a whole window): where the body stopped short
   *  of its end, the file reads end as at its client's leaving, and what is kept for them goes. A body read to its end
   *  keeps its files' bytes. */
  clientGaveUp(): void {
    if (this.stop?.why !== 'ended') this.endFiles('client')
  }

  /** The read-off: the source's chunks to its end or its client's leaving, each kept or dropped. A read of the body
   *  under way finishes first: its own pull, awaiting it since it began, takes its outcome. */
  private async readRest(): Promise<void> {
    try {
      if (this.reading !== null) await this.reading.catch(() => {})
      if (this.kept === null) this.buffer = EMPTY
      if (this.stop) return
      for (let chunk = await this.readChunk(); chunk !== null; chunk = await this.readChunk()) this.keep(chunk)
    } finally {
      if (this.kept !== null) {
        this.kept.readAll = true
        wakeKept(this.kept)
      }
      this.readOffEnd.resolve()
    }
  }

  private keep(chunk: Uint8Array<ArrayBuffer>): void {
    const kept = this.kept
    if (kept === null || kept.refused) return
    if (kept.memory.tryHold(chunk.length)) kept.chunks.push(chunk)
    else kept.refused = true
    wakeKept(kept)
  }

  /** The read-off's next chunk, for `by`'s read: null where it drops what it reads, as at the body's end; at the end of
   *  what it read, the source's, as `readChunk` says it. */
  private async takeKept(by: FileRead | null): Promise<Uint8Array<ArrayBuffer> | null> {
    for (;;) {
      const kept = this.kept
      if (kept === null) return null
      const chunk = kept.chunks.shift()
      if (chunk !== undefined) {
        kept.memory.letGo(chunk.length)
        return chunk
      }
      if (kept.refused) throw this.droppedError(by)
      if (kept.readAll) return this.readChunk()
      kept.more ??= createDeferred()
      await kept.more.promise
    }
  }

  /** Lets go of what the read-off keeps: no file is left to read it. From then on it drops what it reads. */
  private dropKept(): void {
    const kept = this.kept
    if (kept === null) return
    kept.memory.release()
    this.kept = null
    wakeKept(kept)
  }

  /** A file read reached what the read-off dropped. Only a file read takes kept chunks: the call's metadata is read
   *  before its answer. */
  private droppedError(by: FileRead | null): Error {
    assert(by !== null)
    return new Error(
      `The telefunction returned before it read this File (${this.fileSizes.get(by.index)} bytes), and the server keeps no more of a File left unread than config.channel.maxMemory and config.channel.maxPendingBytes allow: the rest was dropped. Read a File before the telefunction returns to keep all of it: a read while what it returns streams fails too once the upload gets further ahead of it than those limits allow.`,
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

  /** Registers a file's size, as the call's metadata declares it, while the call is revived: the file, as its File reads
   *  it. */
  registerFile(index: number, size: number): RequestFile {
    this.fileSizes.set(index, size)
    this.unread.set(index, [])
    return {
      stream: () => this.fileStream(index),
      letGo: () => this.fileGone(index),
      onGone: (callback) => {
        const onGone = this.unread.get(index)
        if (onGone) onGone.push(callback)
        else callback()
      },
    }
  }

  /** No File reads the file at `index` anymore: what was read ahead for it goes, its end's callbacks run, and once no
   *  file is left to read, what is kept goes. */
  private fileGone(index: number): void {
    const onGone = this.unread.get(index)
    if (onGone === undefined) return
    this.unread.delete(index)
    this.files.get(index)?.dropAhead()
    for (const callback of onGone) callback()
    if (this.unread.size === 0) this.dropKept()
  }

  /**
   * The file at `index`: a stream of its bytes, read off the body only as the app reads it (`highWaterMark: 0`).
   *
   * The body is one forward-only sequence, so the reader owns its position and lends it to one pull at a time. A read of
   * a later file reaches each earlier file in turn. One the app never read whose stream isn't locked, or one that was
   * cancelled, is passed: its bytes are dropped. One whose stream is locked gets its next piece read for it, kept until
   * its own read takes it, and the later read waits until the piece is read, the reader releases its lock, the file is
   * cancelled, or the files end. One the app began and left without a reader may take a reader again: its rest is read
   * for it, kept within the reader's memory, and the later read goes on; where its memory refuses a piece, the file
   * fails, saying why, and the rest of it is dropped. A cancelled file's bytes are dropped once a later read passes it.
   * The files end at the body's let-go, at a read that fails, and where the client leaves.
   */
  private fileStream(index: number): ReadableStream<Uint8Array<ArrayBuffer>> {
    const file = new FileRead(index, this.pullFile, this.cancelFile, this.fileOver, () => this.wake(), this.memory)
    this.files.set(index, file)
    if (this.filesEndedBy !== null) file.fail(this.stopped())
    return file.stream
  }

  /** One chunk of `file`: a piece read ahead for it, or once it holds the position, the buffer's or the source's. */
  private readonly pullFile = async (file: FileRead): Promise<void> => {
    if (file.over) return
    file.start()
    const turn =
      !file.hasAhead && this.holder === null && this.next === file.index
        ? this.hold(file)
        : await this.takePosition(file)
    if (turn === 'ahead') return this.takeAhead(file)
    if (turn === 'none') return
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
      file.markOver()
      throw error
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

  /** The oldest piece read ahead for `file`, handed to its stream, which closes at its last piece; the reads waiting
   *  behind it look again. */
  private takeAhead(file: FileRead): void {
    const piece = file.takeAhead()
    if (piece !== undefined) file.enqueue(piece)
    if (file.aheadEnded) file.close()
    this.wake()
  }

  private hold(file: FileRead): 'position' {
    this.holder = file
    return 'position'
  }

  private readonly cancelFile = (file: FileRead): void => {
    file.markOver()
    // A pull of it under way sees the cancel once its read is done, and wakes the waiting reads then.
    if (this.holder !== file) this.wake()
  }

  /** Waits until `file`'s read holds the position at its own index (`'position'`), a piece was read ahead for it
   *  (`'ahead'`), or it was cancelled meanwhile (`'none'`), passing or reading ahead for each earlier file on its way. */
  private async takePosition(file: FileRead): Promise<'position' | 'ahead' | 'none'> {
    for (;;) {
      if (this.filesEndedBy !== null) throw this.stopped()
      if (file.over) return 'none'
      if (file.hasAhead) return 'ahead'
      assertUsage(
        file.index >= this.next,
        `File argument ${file.index} has already been consumed (currently at ${this.next}). File arguments must be read in order.`,
      )
      if (this.holder === null) {
        if (this.next === file.index) return this.hold(file)
        const earlier = this.files.get(this.next)
        if (earlier === undefined || earlier.over || !(earlier.stream.locked || earlier.started)) {
          await this.pass(file)
          continue
        }
        // A locked file's own reads take its pieces one at a time; one begun and left without a reader has its rest read.
        if (!earlier.stream.locked || !earlier.hasAhead) {
          await this.readAheadFor(earlier, file)
          continue
        }
      }
      this.fileEnd ??= createDeferred()
      await this.fileEnd.promise
    }
  }

  /** Reads the next piece of `earlier`, the file at the position whose stream is locked or that the app began, for it, as
   *  its own pull would, for `by`, which holds the position meanwhile. A file cancelled meanwhile drops it; one whose
   *  memory refuses it fails. */
  private async readAheadFor(earlier: FileRead, by: FileRead): Promise<void> {
    this.holder = by
    try {
      const left = this.leftOfNext()
      let piece = EMPTY
      if (left > 0) {
        let bytes = this.buffer
        if (bytes.length === 0) {
          const pulled = await this.pullChunk(earlier)
          if (this.stop?.why === 'letGo') return
          if (pulled === null) throw this.truncation()
          bytes = pulled
        }
        piece = this.handOut(bytes, left)
        this.left = left - piece.length
      }
      const last = this.left === 0
      if (last) {
        this.next++
        this.left = undefined
      }
      if (!earlier.over && !earlier.keepAhead(piece, last)) earlier.fail(leftBehindError(earlier.index))
    } finally {
      this.release()
    }
  }

  /** Drops the bytes of the file at the position, and what was read ahead for it, for `by`, which holds the position
   *  meanwhile. A failure is the stop's own: the truncation, or the app's error. */
  private async pass(by: FileRead): Promise<void> {
    this.holder = by
    try {
      const passed = this.files.get(this.next)
      assertWarning(
        passed?.over === true,
        `File arguments are being consumed out of order (reading ${by.index}, expected ${this.next}). Skipped files will be unreadable. For correct behavior, consume file arguments in the order they appear.`,
        { onlyOnce: true },
      )
      passed?.dropAhead()
      await this.skipBytes(this.leftOfNext(), by)
      this.fileGone(this.next)
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

  private readonly fileOver = (file: FileRead): void => this.fileGone(file.index)

  /** Errors every open file stream, and every one asked for later, with the stop's own error; the reads that wait wake and
   *  throw it. A file's pull under way finishes its read of the body, and drops it. No file is left to read. */
  private endFiles(by: 'client' | 'server'): void {
    this.filesEndedBy ??= by
    for (const file of this.files.values()) file.fail(this.stopped())
    for (const index of this.unread.keys()) this.fileGone(index)
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
        // A Node body let go of with a read under way ends that read as at its end.
        if (this.stop === null) {
          this.stop = { why: 'ended' }
          this.onEnd?.()
        }
        this.source.letGo()
        return null
      }
      this.onChunk?.()
      return value
    } catch (error) {
      // A stop that came while the read was under way ends it as that stop: the body let go of (a Web reader's released
      // lock fails the read), or its client's leaving as the request's signal says it (`leftByClient`).
      if (this.stop !== null) return null
      this.source.letGo()
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
      this.collected = this.buffer.length > 0 ? [this.buffer] : []
      let length = this.buffer.length
      try {
        while (length < n) {
          const chunk = await this.pullChunk(null)
          // Past the read-off's start, `discardRest` let go of the parts.
          if (!chunk || this.readOff !== null) throw this.truncation()
          this.collected.push(chunk)
          length += chunk.length
        }
        // One part is the one chunk read, with nothing buffered before it.
        const [only, ...more] = this.collected
        this.buffer = only !== undefined && more.length === 0 ? only : concatAll(this.collected)
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

/** A file's stream, as the reader serves it: unread, read (its stream was pulled), or over, as it reads no more, ended,
 *  cancelled or failed (`markOver`), which `onOver` is told. */
class FileRead {
  readonly index: number
  readonly stream: ReadableStream<Uint8Array<ArrayBuffer>>
  private phase: 'unread' | 'started' | 'over' = 'unread'
  /** The pieces of it later files' reads read ahead for it (`readAheadFor`), oldest first, and whether the newest is its
   *  last. The oldest is the one piece the reader keeps for any file; each behind it is held on `aheadMemory`. */
  private ahead = new Fifo<Uint8Array<ArrayBuffer>>()
  private aheadLast = false
  private aheadMemory: QueueMemory | null = null
  private readonly memory: () => QueueMemory
  private controller: ReadableStreamDefaultController<Uint8Array<ArrayBuffer>> | null = null
  private readonly onOver: (file: FileRead) => void

  constructor(
    index: number,
    pull: (file: FileRead) => Promise<void>,
    cancel: (file: FileRead) => void,
    onOver: (file: FileRead) => void,
    onReaderReleased: () => void,
    memory: () => QueueMemory,
  ) {
    this.index = index
    this.onOver = onOver
    this.memory = memory
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
    return this.phase === 'over'
  }

  /** Whether the app read it: its stream was pulled. */
  get started(): boolean {
    return this.phase === 'started'
  }

  /** Its stream was pulled. */
  start(): void {
    if (this.phase === 'unread') this.phase = 'started'
  }

  enqueue(chunk: Uint8Array<ArrayBuffer>): void {
    this.controller?.enqueue(chunk)
  }

  /** Whether a piece read ahead for it, or its end, waits for its read. */
  get hasAhead(): boolean {
    return this.ahead.length > 0 || this.aheadLast
  }

  /** Whether its read took every piece read ahead for it, the last of it among them. */
  get aheadEnded(): boolean {
    return this.aheadLast && this.ahead.length === 0
  }

  /** Keeps `piece`, read ahead for it, and whether it is its last; false where its memory refuses the piece. */
  keepAhead(piece: Uint8Array<ArrayBuffer>, last: boolean): boolean {
    if (piece.length > 0) {
      if (this.ahead.length > 0 && !(this.aheadMemory ??= this.memory()).tryHold(piece.length)) return false
      this.ahead.push(piece)
    }
    this.aheadLast = last
    return true
  }

  /** The oldest piece read ahead for it; the one behind it becomes the one the reader keeps for it. */
  takeAhead(): Uint8Array<ArrayBuffer> | undefined {
    const piece = this.ahead.shift()
    const next = this.ahead.peek()
    if (next !== undefined) this.aheadMemory?.letGo(next.length)
    return piece
  }

  /** Lets go of what was read ahead for it. */
  dropAhead(): void {
    if (this.ahead.length > 0) this.ahead = new Fifo()
    this.aheadLast = false
    this.aheadMemory?.release()
    this.aheadMemory = null
  }

  markOver(): void {
    if (this.over) return
    this.phase = 'over'
    this.dropAhead()
    this.onOver(this)
  }

  close(): void {
    if (this.over) return
    this.markOver()
    this.controller?.close()
  }

  fail(error: unknown): void {
    if (this.over) return
    this.markOver()
    this.controller?.error(error)
  }
}

/** What the read-off keeps for the file reads: the chunks it read, in order, each held on `memory` until a file read
 *  takes it; whether `memory` refused one (`refused`: that chunk and every one after it were dropped); whether the
 *  read-off read the rest of the body (`readAll`); and what a file read that waits for its next chunk awaits (`more`). */
type Kept = {
  readonly memory: QueueMemory
  readonly chunks: Uint8Array<ArrayBuffer>[]
  refused: boolean
  readAll: boolean
  more: Deferred<void> | null
}

/** Wakes the file read that waits for the read-off's next chunk. */
function wakeKept(kept: Kept): void {
  const more = kept.more
  if (more === null) return
  kept.more = null
  more.resolve()
}

/** A file the app began and left without a reader, whose rest a later file's read read for it past what its memory
 *  allows. */
function leftBehindError(index: number): Error {
  return new Error(
    `File argument ${index} was read in part and left without a reader while a later File argument was read, and the server keeps no more of a File read ahead than config.channel.maxMemory and config.channel.maxPendingBytes allow: the rest was dropped. Read a File to its end, or keep its reader, before reading the next one.`,
  )
}

const DONE: IteratorReturnResult<undefined> = { done: true, value: undefined }
const ENDED = { failed: false } as const

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
    stream.on('close', this.close)
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
