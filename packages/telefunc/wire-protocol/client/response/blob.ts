export { blobReviver }

import type { ReviverType, BlobResponseContract, ClientReviverContext } from '../../types.js'
import { SERIALIZER_PREFIX_BLOB } from '../../constants.js'
import { PendingValue } from './PendingValue.js'

const blobReviver: ReviverType<BlobResponseContract, ClientReviverContext> = {
  prefix: SERIALIZER_PREFIX_BLOB,
  revive: (metadata, context) => {
    const { bytes, cancel, abort } = context.receiveStream(metadata)
    const blobPromise = bytes({ expectedSize: metadata.size }).then((buf) => new Blob([buf], { type: metadata.type }))
    return { value: new PendingValue(blobPromise), close: cancel, abort }
  },
}
