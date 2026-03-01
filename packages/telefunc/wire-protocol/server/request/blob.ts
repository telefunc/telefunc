export { blobServerType }

import { SERIALIZER_PREFIX_BLOB } from '../../constants.js'
import { LazyBlob } from './LazyFile.js'
import type { ServerRequestType, BlobRequestContract } from '../../request-types.js'

const blobServerType: ServerRequestType<BlobRequestContract> = {
  prefix: SERIALIZER_PREFIX_BLOB,
  createValue: (metadata, reader) => new LazyBlob(metadata, reader),
}
