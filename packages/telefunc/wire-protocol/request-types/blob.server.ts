export { blobServerType }

import { SERIALIZER_PREFIX_BLOB } from '../constants.js'
import { LazyBlob } from './server/LazyFile.js'
import type { ServerRequestType, BlobRequestContract } from './interface.js'

const blobServerType: ServerRequestType<BlobRequestContract> = {
  prefix: SERIALIZER_PREFIX_BLOB,
  createValue: (metadata, reader) => new LazyBlob(metadata, reader),
}
