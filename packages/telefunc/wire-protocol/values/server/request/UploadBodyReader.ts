export { UploadBodyReader }

import { assert, assertUsage, assertWarning } from '../../../../utils/assert.js'
import { createDeferred, type Deferred } from '../../../../utils/createDeferred.js'
import { FrameReader, StreamTruncatedError } from '../../../wire/FrameReader.js'
import { ProtocolViolationError } from '../../../wire/protocolViolation.js'

/** Reads a framed metadata envelope followed by files in their declared order. */
class UploadBodyReader extends FrameReader {
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
  private readonly drainEnd = createDeferred<void>()

  /** Settles once the drain stops, however it does: at the body's end, at its client's leaving, or at a failed read. */
  get restRead(): Promise<void> {
    return this.drainEnd.promise
  }

  override letGo(): void {
    super.letGo()
    this.endFiles('server')
  }

  override leftByClient(reason: unknown): void {
    super.leftByClient(reason)
    if (this.stop?.why !== 'ended') this.endFiles('client')
  }

  override discardRest(by: 'client' | 'server'): Promise<void> {
    this.endFiles(by)
    return super.discardRest(by, this.drainEnd.resolve)
  }

  /** Registers the size declared in the metadata. The file pulls bytes when it is read. */
  registerFile(index: number, size: number): () => ReadableStream<Uint8Array<ArrayBuffer>> {
    this.fileSizes.set(index, size)
    return () => this.fileStream(index)
  }

  /** A later pull waits for a locked earlier file. It discards earlier files without a reader. */
  private fileStream(index: number): ReadableStream<Uint8Array<ArrayBuffer>> {
    const file = new FileRead(index, this.pullFile, this.cancelFile, () => this.wake())
    if (this.discardedBy === null) this.files.set(index, file)
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
    this.buffer = UploadBodyReader.EMPTY
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
    if (this.discardedBy !== null) return
    this.discardedBy = by
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

  /** A server-ended file stays truncated even if the background drain later fails. */
  private fileError(): StreamTruncatedError {
    if (this.discardedBy === 'server') return new StreamTruncatedError('server')
    return this.truncation()
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
