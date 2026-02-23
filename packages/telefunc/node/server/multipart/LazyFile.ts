export { LazyBlob }
export { LazyFile }
export { isLazyBlob }
export { isLazyFile }

import type { StreamReader } from './StreamReader.js'
import type { FileMetadata, BlobMetadata } from '../../../shared/multipart/constants.js'
import { assertUsage } from '../../../utils/assert.js'

const LAZY_BLOB_BRAND = Symbol.for('telefunc.LazyBlob')
const LAZY_FILE_BRAND = Symbol.for('telefunc.LazyFile')

function isLazyBlob(value: unknown): value is LazyBlob {
  return typeof value === 'object' && value !== null && (value as any)[LAZY_BLOB_BRAND] === true
}

function isLazyFile(value: unknown): value is LazyFile {
  return typeof value === 'object' && value !== null && (value as any)[LAZY_FILE_BRAND] === true
}

/** Shared Blob implementation — subclasses only provide `stream()`. */
abstract class BaseStreamBlob implements Blob {
  abstract readonly size: number
  abstract readonly type: string
  abstract stream(): ReadableStream<Uint8Array<ArrayBuffer>>

  async arrayBuffer(): Promise<ArrayBuffer> {
    return (await this.bytes()).buffer
  }

  async bytes(): Promise<Uint8Array<ArrayBuffer>> {
    const stream = this.stream()
    const reader = stream.getReader()
    let total = 0
    const chunks: Uint8Array[] = []
    while (true) {
      const { done, value } = await reader.read()
      if (done) break
      chunks.push(value)
      total += value.byteLength
    }
    const result = new Uint8Array(total)
    let offset = 0
    for (const c of chunks) {
      result.set(c, offset)
      offset += c.byteLength
    }
    return result
  }

  async text(): Promise<string> {
    return new TextDecoder().decode(await this.bytes())
  }

  slice(start?: number, end?: number, contentType?: string): Blob {
    const size = this.size
    const relStart = start === undefined ? 0 : start < 0 ? Math.max(size + start, 0) : Math.min(start, size)
    const relEnd = end === undefined ? size : end < 0 ? Math.max(size + end, 0) : Math.min(end, size)
    const sliceSize = Math.max(relEnd - relStart, 0)
    return new SlicedBlob(this, relStart, sliceSize, contentType ?? '')
  }

  get [Symbol.toStringTag](): string {
    return 'Blob'
  }
}

/**
 * A Blob backed by a pull-based StreamReader.
 *
 * `size` and `type` are available immediately from metadata.
 * Body data is pulled from the stream on demand — no background pump.
 * Each instance is **one-shot**: once consumed, further reads throw.
 */
class LazyBlob extends BaseStreamBlob {
  readonly size: number
  readonly type: string
  readonly [LAZY_BLOB_BRAND] = true

  #reader: StreamReader
  #index: number
  #consumed = false

  constructor(reader: StreamReader, metadata: BlobMetadata) {
    super()
    this.#reader = reader
    this.#index = metadata.index
    this.size = metadata.size
    this.type = metadata.type
    reader.registerFile(metadata.index, metadata.size)
  }

  stream(): ReadableStream<Uint8Array<ArrayBuffer>> {
    assertUsage(!this.#consumed, 'Stream has already been consumed. Each streaming Blob/File can only be read once.')
    this.#consumed = true
    let inner: ReadableStreamDefaultReader<Uint8Array>
    return new ReadableStream<Uint8Array<ArrayBuffer>>({
      start: async () => {
        inner = (await this.#reader.consumeFile(this.#index, this.size)).getReader()
      },
      pull: async (controller) => {
        const { done, value } = await inner.read()
        if (done) controller.close()
        else controller.enqueue(value as Uint8Array<ArrayBuffer>)
      },
      cancel: async () => {
        await inner?.cancel()
      },
    })
  }
}

/** A byte-range view over a parent stream blob. Consuming the slice consumes the parent. */
class SlicedBlob extends BaseStreamBlob {
  readonly size: number
  readonly type: string

  #parent: BaseStreamBlob
  #start: number

  constructor(parent: BaseStreamBlob, start: number, size: number, type: string) {
    super()
    this.#parent = parent
    this.#start = start
    this.size = size
    this.type = type
  }

  stream(): ReadableStream<Uint8Array<ArrayBuffer>> {
    let parentReader: ReadableStreamDefaultReader<Uint8Array>
    let pos = 0
    const start = this.#start
    const end = start + this.size
    return new ReadableStream<Uint8Array<ArrayBuffer>>({
      start: () => {
        parentReader = this.#parent.stream().getReader()
      },
      pull: async (controller) => {
        const { done, value } = await parentReader.read()
        if (done) {
          controller.close()
          return
        }
        const from = Math.max(start - pos, 0)
        const to = Math.min(end - pos, value.byteLength)
        pos += value.byteLength
        if (to > from) {
          controller.enqueue(value.subarray(from, to) as Uint8Array<ArrayBuffer>)
        }
        if (pos >= end) {
          controller.close()
          await parentReader.cancel()
        }
      },
      cancel: async () => {
        await parentReader?.cancel()
      },
    })
  }
}

/**
 * A File implementation backed by a pull-based StreamReader.
 * Extends LazyBlob with `name`, `lastModified`, and `webkitRelativePath`.
 */
class LazyFile extends LazyBlob implements File {
  readonly name: string
  readonly lastModified: number
  readonly webkitRelativePath: string = ''
  readonly [LAZY_FILE_BRAND] = true

  constructor(reader: StreamReader, metadata: FileMetadata) {
    super(reader, metadata)
    this.name = metadata.name
    this.lastModified = metadata.lastModified
  }

  get [Symbol.toStringTag](): string {
    return 'File'
  }
}
