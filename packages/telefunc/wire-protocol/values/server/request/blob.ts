export { blobReviver }

import type { BlobRequestContract, ServerReviverContext, ReviverType } from '../../types.js'
import { SERIALIZER_PREFIX_BLOB } from '../../../constants.js'
import { LazyBlob } from '../../LazyFile.js'

const blobReviver: ReviverType<BlobRequestContract, ServerReviverContext> = {
  prefix: SERIALIZER_PREFIX_BLOB,
  revive: (metadata, context) => {
    const stream = context.registerFile(metadata.index, metadata.size)
    const lazyBlob = new LazyBlob(metadata.size, metadata.type, stream)
    return { value: lazyBlob, close() {}, abort() {} }
  },
}
