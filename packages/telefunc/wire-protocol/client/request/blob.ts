export { blobReplacer }

import type { BlobRequestContract, ClientReplacerContext, ReplacerType } from '../../types.js'
import { SERIALIZER_PREFIX_BLOB } from '../../constants.js'

const blobReplacer: ReplacerType<BlobRequestContract, ClientReplacerContext> = {
  prefix: SERIALIZER_PREFIX_BLOB,
  detect: (value: unknown): value is Blob => value instanceof Blob,
  getMetadata: (value, context) => {
    const index = context.registerFile(value)
    return {
      metadata: { index, size: value.size, type: value.type },
      // Blobs are fully consumed during request parsing — no ongoing resource to clean up.
      close() {},
      abort() {},
    }
  },
}
