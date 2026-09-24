export {
  // Eager native — goes through `new File`/`new Blob`, auto-materializes on client.
  onDownloadFileEager,
  onDownloadBlobEager,
  // Streaming via `download()` — returns a FileDownload/BlobDownload tuple on client.
  onDownloadFileStream,
  onDownloadFileStreamNoSize,
  onDownloadBlobStream,
  onDownloadBlobStreamNoSize,
  // `download()` wrapping native File/Blob — opt-in to progress on already-buffered bytes.
  onDownloadWrapFile,
  onDownloadWrapBlob,
  // Progress / cancel exercise.
  onDownloadProgressKnownSize,
  onDownloadProgressUnknownSize,
  // Edge cases.
  onDownloadFileBinary,
  onDownloadFileLarge,
  onDownloadFileEmpty,
  onDownloadFileNested,
}

import { download } from 'telefunc'

const TEXT = 'Hello, file download!'
const TEXT_BYTES = new TextEncoder().encode(TEXT)
const FIXED_LAST_MODIFIED = 1700000000000

function streamOf(bytes: Uint8Array): ReadableStream<Uint8Array<ArrayBuffer>> {
  return new ReadableStream<Uint8Array<ArrayBuffer>>({
    start(controller) {
      controller.enqueue(bytes as Uint8Array<ArrayBuffer>)
      controller.close()
    },
  })
}

// ----- eager native File/Blob (auto-materialize) -----

const onDownloadFileEager = async () => {
  return new File([TEXT], 'eager.txt', { type: 'text/plain', lastModified: FIXED_LAST_MODIFIED })
}

const onDownloadBlobEager = async () => {
  return new Blob([TEXT], { type: 'text/plain' })
}

// ----- streaming via download() (returns tuple) -----

const onDownloadFileStream = async () => {
  return download(streamOf(TEXT_BYTES), {
    name: 'streamed.txt',
    size: TEXT_BYTES.byteLength,
    type: 'text/plain',
    lastModified: FIXED_LAST_MODIFIED,
  })
}

const onDownloadFileStreamNoSize = async () => {
  return download(streamOf(TEXT_BYTES), {
    name: 'streamed-nosize.txt',
    type: 'text/plain',
    lastModified: FIXED_LAST_MODIFIED,
  })
}

const onDownloadBlobStream = async () => {
  return download(streamOf(TEXT_BYTES), {
    size: TEXT_BYTES.byteLength,
    type: 'text/plain',
  })
}

const onDownloadBlobStreamNoSize = async () => {
  return download(streamOf(TEXT_BYTES), { type: 'text/plain' })
}

// ----- download() wrapping native File/Blob -----

const onDownloadWrapFile = async () => {
  const native = new File([TEXT], 'wrapped.txt', { type: 'text/plain', lastModified: FIXED_LAST_MODIFIED })
  return download(native)
}

const onDownloadWrapBlob = async () => {
  return download(new Blob([TEXT], { type: 'text/plain' }))
}

// ----- progress / cancel exercise -----

// 2 MB across 32 KB chunks with a small per-chunk delay — gives the client enough wall-time
// to observe multiple `onProgress` ticks instead of a single fire.
const onDownloadProgressKnownSize = async () => {
  const TOTAL = 2 * 1024 * 1024
  const CHUNK = 32 * 1024
  let sent = 0
  const stream = new ReadableStream<Uint8Array<ArrayBuffer>>({
    async pull(controller) {
      if (sent >= TOTAL) {
        controller.close()
        return
      }
      const size = Math.min(CHUNK, TOTAL - sent)
      controller.enqueue(new Uint8Array(size).fill((sent / CHUNK) % 256))
      sent += size
      await new Promise((resolve) => setTimeout(resolve, 5))
    },
  })
  return download(stream, {
    name: 'progress-known.bin',
    size: TOTAL,
    type: 'application/octet-stream',
  })
}

const onDownloadProgressUnknownSize = async () => {
  const TOTAL = 1 * 1024 * 1024
  const CHUNK = 32 * 1024
  let sent = 0
  const stream = new ReadableStream<Uint8Array<ArrayBuffer>>({
    async pull(controller) {
      if (sent >= TOTAL) {
        controller.close()
        return
      }
      const size = Math.min(CHUNK, TOTAL - sent)
      controller.enqueue(new Uint8Array(size).fill((sent / CHUNK) % 256))
      sent += size
      await new Promise((resolve) => setTimeout(resolve, 5))
    },
  })
  return download(stream, { name: 'progress-unknown.bin', type: 'application/octet-stream' })
}

// ----- edge cases -----

// Binary content with non-ASCII bytes, multi-chunk source.
const onDownloadFileBinary = async () => {
  const bytes = new Uint8Array(1024)
  for (let i = 0; i < bytes.length; i++) bytes[i] = i % 256
  const stream = new ReadableStream<Uint8Array<ArrayBuffer>>({
    start(controller) {
      controller.enqueue(bytes.subarray(0, 512))
      controller.enqueue(bytes.subarray(512))
      controller.close()
    },
  })
  return download(stream, {
    name: 'binary.bin',
    size: bytes.byteLength,
    type: 'application/octet-stream',
  })
}

// 5 MB file streamed across many chunks — verifies large-payload buffering on the client.
const onDownloadFileLarge = async () => {
  const TOTAL = 5 * 1024 * 1024
  const CHUNK = 64 * 1024
  let sent = 0
  const stream = new ReadableStream<Uint8Array<ArrayBuffer>>({
    pull(controller) {
      if (sent >= TOTAL) {
        controller.close()
        return
      }
      const size = Math.min(CHUNK, TOTAL - sent)
      controller.enqueue(new Uint8Array(size).fill((sent / CHUNK) % 256))
      sent += size
    },
  })
  return download(stream, {
    name: 'large.bin',
    size: TOTAL,
    type: 'application/octet-stream',
  })
}

// Zero-byte stream — terminator immediately, no chunks.
const onDownloadFileEmpty = async () => {
  const stream = new ReadableStream<Uint8Array<ArrayBuffer>>({
    start(controller) {
      controller.close()
    },
  })
  return download(stream, { name: 'empty.txt', size: 0, type: 'text/plain' })
}

const onDownloadFileNested = async () => {
  return {
    label: 'wrapped',
    count: 7,
    nested: {
      payload: download(streamOf(TEXT_BYTES), {
        name: 'nested.txt',
        size: TEXT_BYTES.byteLength,
        type: 'text/plain',
        lastModified: FIXED_LAST_MODIFIED,
      }),
    },
  }
}
