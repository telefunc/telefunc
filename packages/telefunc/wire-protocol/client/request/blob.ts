export { blobClientType }

import { SERIALIZER_PREFIX_BLOB } from '../../constants.js'
import type { ClientRequestType, BlobRequestContract } from '../../request-types.js'

const blobClientType: ClientRequestType<BlobRequestContract> = {
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
