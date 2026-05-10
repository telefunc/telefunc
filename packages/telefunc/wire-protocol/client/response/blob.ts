export { blobReviver }

import type { ReviverType, BlobResponseContract, ClientReviverContext } from '../../types.js'
import { SERIALIZER_PREFIX_BLOB } from '../../constants.js'

const blobReviver: ReviverType<BlobResponseContract, ClientReviverContext> = {
  prefix: SERIALIZER_PREFIX_BLOB,
  revive: (metadata, context) => {
    const { arrayBuffer, cancel, abort } = context.receiveStreamReader(metadata)
    const blobPromise = arrayBuffer(metadata.size).then((buf) => new Blob([buf], { type: metadata.type }))
    context.waitFor(blobPromise)
    return { value: blobPromise, close: cancel, abort }
  },
}
