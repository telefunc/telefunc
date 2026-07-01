export { download }
export { isFileDownload, isBlobDownload }
export { getFileDownloadInternal, getBlobDownloadInternal }
export type { FileDownloadInternal, BlobDownloadInternal }

import type { FileDownload, BlobDownload } from './types.js'

const FILE_DOWNLOAD_BRAND = Symbol.for('telefunc.FileDownload')
const BLOB_DOWNLOAD_BRAND = Symbol.for('telefunc.BlobDownload')

type FileDownloadInternal = {
  stream: ReadableStream<Uint8Array<ArrayBuffer>>
  name: string
  type: string
  lastModified: number
  size?: number
}

type BlobDownloadInternal = {
  stream: ReadableStream<Uint8Array<ArrayBuffer>>
  type: string
  size?: number
}

function isFileDownload(value: unknown): value is FileDownload {
  return (
    typeof value === 'object' &&
    value !== null &&
    (value as { [FILE_DOWNLOAD_BRAND]?: FileDownloadInternal })[FILE_DOWNLOAD_BRAND] !== undefined
  )
}

function isBlobDownload(value: unknown): value is BlobDownload {
  return (
    typeof value === 'object' &&
    value !== null &&
    (value as { [BLOB_DOWNLOAD_BRAND]?: BlobDownloadInternal })[BLOB_DOWNLOAD_BRAND] !== undefined
  )
}

function getFileDownloadInternal(value: FileDownload): FileDownloadInternal {
  return (value as unknown as { [FILE_DOWNLOAD_BRAND]: FileDownloadInternal })[FILE_DOWNLOAD_BRAND]
}

function getBlobDownloadInternal(value: BlobDownload): BlobDownloadInternal {
  return (value as unknown as { [BLOB_DOWNLOAD_BRAND]: BlobDownloadInternal })[BLOB_DOWNLOAD_BRAND]
}

const placeholderError = (): never => {
  throw new Error('`download()` value methods are only valid on the client side.')
}

/** Return a `File`, `Blob`, or `ReadableStream` from a telefunction. The client receives a
 *  `FileDownload`/`BlobDownload` — Blob/File-shaped with `onProgress`, `cancel`,
 *  `saveToMemory()`, `saveToDisk(path?)`. https://telefunc.com/file-download */
function download(file: File): FileDownload
function download(blob: Blob): BlobDownload
function download(
  stream: ReadableStream<Uint8Array<ArrayBuffer>>,
  options: { name: string; size?: number; type?: string; lastModified?: number },
): FileDownload
function download(
  stream: ReadableStream<Uint8Array<ArrayBuffer>>,
  options?: { type?: string; size?: number },
): BlobDownload
function download(
  value: File | Blob | ReadableStream<Uint8Array<ArrayBuffer>>,
  options: { name?: string; size?: number; type?: string; lastModified?: number } = {},
): FileDownload | BlobDownload {
  if (value instanceof ReadableStream) {
    if (options.name !== undefined) {
      return makeFileDownload({
        stream: value,
        name: options.name,
        type: options.type ?? '',
        lastModified: options.lastModified ?? Date.now(),
        size: options.size,
      })
    }
    return makeBlobDownload({
      stream: value,
      type: options.type ?? '',
      size: options.size,
    })
  }
  // File extends Blob, so check File first.
  if (value instanceof File) {
    return makeFileDownload({
      stream: value.stream() as ReadableStream<Uint8Array<ArrayBuffer>>,
      name: value.name,
      type: value.type,
      lastModified: value.lastModified,
      size: value.size,
    })
  }
  return makeBlobDownload({
    stream: value.stream() as ReadableStream<Uint8Array<ArrayBuffer>>,
    type: value.type,
    size: value.size,
  })
}

function makeFileDownload(internal: FileDownloadInternal): FileDownload {
  // Server-side placeholder. The class instance is only constructed on the client by the
  // reviver — server-side accessors are decorative; the brand is what the replacer reads.
  return {
    name: internal.name,
    type: internal.type,
    size: internal.size ?? 0,
    lastModified: internal.lastModified,
    loaded: 0,
    onProgress: placeholderError,
    cancel: placeholderError,
    saveToMemory: placeholderError,
    saveToDisk: placeholderError,
    [FILE_DOWNLOAD_BRAND]: internal,
  } as unknown as FileDownload
}

function makeBlobDownload(internal: BlobDownloadInternal): BlobDownload {
  return {
    type: internal.type,
    size: internal.size ?? 0,
    loaded: 0,
    onProgress: placeholderError,
    cancel: placeholderError,
    saveToMemory: placeholderError,
    saveToDisk: placeholderError,
    [BLOB_DOWNLOAD_BRAND]: internal,
  } as unknown as BlobDownload
}
