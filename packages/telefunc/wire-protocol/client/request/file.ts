export { fileClientType }

import { SERIALIZER_PREFIX_FILE } from '../../constants.js'
import type { ClientRequestType, FileRequestContract } from '../../request-types.js'

const fileClientType: ClientRequestType<FileRequestContract> = {
  prefix: SERIALIZER_PREFIX_FILE,
  detect: (value: unknown): value is File => value instanceof File,
  getMetadata: (value, context) => {
    const index = context.registerFile(value)
    return {
      index,
      name: value.name,
      size: value.size,
      type: value.type,
      lastModified: value.lastModified,
    }
  },
}
