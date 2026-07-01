export { blobReviver }

import type { BlobRequestContract, ServerReviverContext, ReviverType } from '../../types.js'
import { SERIALIZER_PREFIX_BLOB } from '../../constants.js'
import { LazyBlob } from '../../LazyFile.js'

const blobReviver: ReviverType<BlobRequestContract, ServerReviverContext> = {
  prefix: SERIALIZER_PREFIX_BLOB,
  // Blobs are fully consumed during request parsing — no ongoing resource to clean up.
  revive: (metadata, context) => {
    context.registerFile(metadata.index, metadata.size)
    const lazyBlob = new LazyBlob(metadata.size, metadata.type, () =>
      context.consumeFile(metadata.index, metadata.size),
    )
    return { value: lazyBlob, close() {}, abort() {} }
  },
}
