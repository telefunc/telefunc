export { blobReplacer }

import type { BlobRequestContract, ClientReplacerContext, ReplacerType } from '../../types.js'
import { SERIALIZER_PREFIX_BLOB } from '../../constants.js'

const blobReplacer: ReplacerType<BlobRequestContract, ClientReplacerContext> = {
  prefix: SERIALIZER_PREFIX_BLOB,
  detect: (value: unknown): value is Blob => value instanceof Blob,
  getMetadata: (value, context) => {
    const index = context.registerFile(value)
    return {
      index,
      size: value.size,
      type: value.type,
    }
  },
}
