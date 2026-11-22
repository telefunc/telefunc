export { blobReviver }

import type { BlobRequestContract, ServerReviverContext, ReviverType } from '../../types.js'
import { SERIALIZER_PREFIX_BLOB } from '../../../constants.js'
import { LazyBlob } from '../../LazyFile.js'

const blobReviver: ReviverType<BlobRequestContract, ServerReviverContext> = {
  prefix: SERIALIZER_PREFIX_BLOB,
  // The blob's bytes are the request body's, read as its stream is read: nothing of the blob's own to close.
  revive: (metadata, context) => {
    const getStream = context.registerFile(metadata.index, metadata.size)
    const lazyBlob = new LazyBlob(metadata.size, metadata.type, getStream)
    return { value: lazyBlob, close() {}, abort() {} }
  },
}
