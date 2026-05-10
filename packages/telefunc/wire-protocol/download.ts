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

/** Return a `File`, `Blob`, or `ReadableStream` from a telefunction with download progress
 *  and cancellation on the client. https://telefunc.com/file-download */
function download(file: File): FileDownload<number>
function download(blob: Blob): BlobDownload<number>
function download<TSize extends number | undefined = undefined>(
  stream: ReadableStream<Uint8Array<ArrayBuffer>>,
  options: { name: string; size?: TSize; type?: string; lastModified?: number },
): FileDownload<TSize>
function download<TSize extends number | undefined = undefined>(
  stream: ReadableStream<Uint8Array<ArrayBuffer>>,
  options?: { type?: string; size?: TSize },
): BlobDownload<TSize>
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
  return {
    name: internal.name,
    type: internal.type,
    size: internal.size,
    lastModified: internal.lastModified,
    loaded: 0,
    file: new Promise<File>(() => {}),
    onProgress: placeholderError,
    cancel: placeholderError,
    [FILE_DOWNLOAD_BRAND]: internal,
  } as FileDownload
}

function makeBlobDownload(internal: BlobDownloadInternal): BlobDownload {
  return {
    type: internal.type,
    size: internal.size,
    loaded: 0,
    blob: new Promise<Blob>(() => {}),
    onProgress: placeholderError,
    cancel: placeholderError,
    [BLOB_DOWNLOAD_BRAND]: internal,
  } as BlobDownload
}
