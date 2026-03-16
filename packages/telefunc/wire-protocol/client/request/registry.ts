export { createRequestReplacer }
export type { RequestFileEntry }

import { fileClientType } from './file.js'
import { blobClientType } from './blob.js'
import { functionClientRequestType } from './function.js'
import type { ClientRequestContext, ClientRequestType } from '../../request-types.js'
import type { ChannelTransport } from '../../constants.js'

/** File before Blob — File extends Blob, so must be checked first. */
const clientRequestTypes: ClientRequestType[] = [fileClientType, blobClientType, functionClientRequestType]

type RequestFileEntry = {
  index: number
  body: Blob
}

function createRequestReplacer(channelTransport: ChannelTransport) {
  const files: RequestFileEntry[] = []
  let nextIndex = 0
  const context: ClientRequestContext = {
    channelTransport,
    registerFile(body) {
      const index = nextIndex++
      files.push({ index, body })
      return index
    },
  }
  const replacer = (_key: string, value: unknown, serializer: (v: unknown) => string) => {
    for (const type of clientRequestTypes) {
      if (type.detect(value)) {
        return {
          replacement: type.prefix + serializer(type.getMetadata(value, context)),
          resolved: true,
        }
      }
    }
    return undefined
  }
  return { replacer, files }
}
