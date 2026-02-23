export { createFileReviver }

import type { Reviver } from '@brillout/json-serializer/parse'
import type { LazyBlob, LazyFile } from '../../node/server/multipart/LazyFile.js'
import { SERIALIZER_PREFIX_FILE, SERIALIZER_PREFIX_BLOB, type FileMetadata, type BlobMetadata } from './constants.js'
import { assertIsNotBrowser } from '../../utils/assertIsNotBrowser.js'
assertIsNotBrowser()

/**
 * Deserialize:
 * - FileMetadata => LazyFile
 * - BlobMetadata => LazyBlob
 */
function createFileReviver(callbacks: {
  createFile: (fileMetadata: FileMetadata) => LazyFile
  createBlob: (blobMetadata: BlobMetadata) => LazyBlob
}): Reviver {
  return (_key: undefined | string, value: string, parser: (str: string) => unknown) => {
    if (value.startsWith(SERIALIZER_PREFIX_FILE)) {
      const fileMetadata = parser(value.slice(SERIALIZER_PREFIX_FILE.length)) as FileMetadata
      return { replacement: callbacks.createFile(fileMetadata) }
    }
    if (value.startsWith(SERIALIZER_PREFIX_BLOB)) {
      const blobMetadata = parser(value.slice(SERIALIZER_PREFIX_BLOB.length)) as BlobMetadata
      return { replacement: callbacks.createBlob(blobMetadata) }
    }
    return undefined
  }
}
