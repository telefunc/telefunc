export { createRequestReplacer }
export type { RequestFileEntry }

import { fileClientType } from './file.js'
import { blobClientType } from './blob.js'
import { functionClientRequestType } from './function.js'
import type { ClientRequestType } from '../../request-types.js'
import type { PlaceholderReplacerType } from '../../placeholder-types.js'

/** File before Blob — File extends Blob, so must be checked first. */
const clientRequestTypes: ClientRequestType[] = [fileClientType, blobClientType]
const clientRequestPlaceholderTypes: PlaceholderReplacerType[] = [functionClientRequestType]

type RequestFileEntry = {
  index: number
  body: Blob
}

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
    for (const type of clientRequestPlaceholderTypes) {
      if (type.detect(value)) {
        return {
          replacement: type.prefix + serializer(type.getMetadata(value)),
          resolved: true,
        }
      }
    }
    return undefined
  }
  return { replacer, files }
}
