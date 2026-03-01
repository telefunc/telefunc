export { fileServerType }

import { SERIALIZER_PREFIX_FILE } from '../constants.js'
import { LazyFile } from './server/LazyFile.js'
import type { ServerRequestType, FileRequestContract } from './interface.js'

const fileServerType: ServerRequestType<FileRequestContract> = {
  prefix: SERIALIZER_PREFIX_FILE,
  createValue: (metadata, reader) => new LazyFile(metadata, reader),
}
