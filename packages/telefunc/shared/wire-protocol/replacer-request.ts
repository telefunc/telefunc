export { createFileReplacer }

import { SERIALIZER_PREFIX_FILE, SERIALIZER_PREFIX_BLOB, type FileMetadata, type BlobMetadata } from './constants.js'

/**
 * Serialize File/Blob → metadata strings with prefix.
 * `onFile`/`onBlob` collect the file parts for the binary frame.
 */
function createFileReplacer(callbacks: {
  onFile: (index: number, file: File) => void
  onBlob: (index: number, blob: Blob) => void
}) {
  let nextIndex = 0
  return (_key: string, value: unknown, serializer: (v: unknown) => string) => {
    if (value instanceof File) {
      const index = nextIndex++
      callbacks.onFile(index, value)
      const fileMetadata: FileMetadata = {
        index,
        name: value.name,
        size: value.size,
        type: value.type,
        lastModified: value.lastModified,
      }
      return {
        replacement: SERIALIZER_PREFIX_FILE + serializer(fileMetadata),
        resolved: true,
      }
    }
    if (value instanceof Blob) {
      const index = nextIndex++
      callbacks.onBlob(index, value)
      const blobMetadata: BlobMetadata = { index, size: value.size, type: value.type }
      return {
        replacement: SERIALIZER_PREFIX_BLOB + serializer(blobMetadata),
        resolved: true,
      }
    }
    return undefined
  }
}
