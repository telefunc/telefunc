export { blobDownloadReviver }

import type { ReviverType, BlobDownloadResponseContract, ClientReviverContext } from '../../types.js'
import { SERIALIZER_PREFIX_BLOB_DOWNLOAD } from '../../constants.js'
import { BlobDownload } from './DownloadClasses.js'

const blobDownloadReviver: ReviverType<BlobDownloadResponseContract, ClientReviverContext> = {
  prefix: SERIALIZER_PREFIX_BLOB_DOWNLOAD,
  revive: (metadata, context) => {
    const source = context.receiveStream(metadata)
    const value = new BlobDownload(metadata, source)
    return { value, close: source.cancel, abort: source.abort }
  },
}
