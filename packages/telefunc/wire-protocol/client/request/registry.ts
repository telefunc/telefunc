export { createRequestReplacer }
export type { RequestFileEntry }

import { fileClientType } from './file.js'
import { blobClientType } from './blob.js'
import type { ClientRequestType } from '../../request-types.js'

/** File before Blob — File extends Blob, so must be checked first. */
const clientRequestTypes: ClientRequestType[] = [fileClientType, blobClientType]

type RequestFileEntry = {
  index: number
  body: Blob
}

/**
 * Creates a JSON-serializer replacer that detects File/Blob values and replaces
 * them with prefixed metadata placeholders. Returns the replacer function and
 * the collected file bodies.
 */
function createRequestReplacer() {
  const files: RequestFileEntry[] = []
  let nextIndex = 0
  const replacer = (_key: string, value: unknown, serializer: (v: unknown) => string) => {
    for (const type of clientRequestTypes) {
      if (type.detect(value)) {
        const index = nextIndex++
        const meta = type.getMetadata(value, index)
        files.push({ index, body: type.getBody(value) })
        return {
          replacement: type.prefix + serializer(meta),
          resolved: true,
        }
      }
    }
    return undefined
  }
  return { replacer, files }
}
