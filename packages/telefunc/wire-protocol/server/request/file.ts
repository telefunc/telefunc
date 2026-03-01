export { fileServerType }

import { SERIALIZER_PREFIX_FILE } from '../../constants.js'
import { LazyFile } from './LazyFile.js'
import type { ServerRequestType, FileRequestContract } from '../../request-types.js'

const fileServerType: ServerRequestType<FileRequestContract> = {
  prefix: SERIALIZER_PREFIX_FILE,
  createValue: (metadata, reader) => new LazyFile(metadata, reader),
}
