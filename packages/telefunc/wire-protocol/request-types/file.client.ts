export { fileClientType }

import { SERIALIZER_PREFIX_FILE } from '../constants.js'
import type { ClientRequestType, FileRequestContract } from './interface.js'

const fileClientType: ClientRequestType<FileRequestContract> = {
  prefix: SERIALIZER_PREFIX_FILE,
  detect: (value: unknown): value is File => value instanceof File,
  getMetadata: (value, index) => ({
    index,
    name: value.name,
    size: value.size,
    type: value.type,
    lastModified: value.lastModified,
  }),
  getBody: (value) => value,
}
