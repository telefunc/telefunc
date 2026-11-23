export { blobReviver }

import type { BlobRequestContract, ServerReviverContext, ReviverType } from '../../types.js'
import { SERIALIZER_PREFIX_BLOB } from '../../../constants.js'
import { LazyBlob } from '../../LazyFile.js'

const blobReviver: ReviverType<BlobRequestContract, ServerReviverContext> = {
  prefix: SERIALIZER_PREFIX_BLOB,
  // The blob's bytes are the request body's, kept for it until it reads them: closing it lets go of them.
  revive: (metadata, context) => {
    const file = context.registerFile(metadata.index, metadata.size)
    const lazyBlob = new LazyBlob(metadata.size, metadata.type, file.stream)
    return { value: lazyBlob, close: file.letGo, abort() {}, onEnd: file.onGone }
  },
}
