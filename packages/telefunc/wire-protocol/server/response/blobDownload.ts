export { blobDownloadReplacer }

import type { StreamingReplacerType, BlobDownloadResponseContract, ServerReplacerContext } from '../../types.js'
import { SERIALIZER_PREFIX_BLOB_DOWNLOAD } from '../../constants.js'
import { createStreamProducer } from './createStreamProducer.js'
import { isBlobDownload, getBlobDownloadInternal } from '../../download.js'

const blobDownloadReplacer: StreamingReplacerType<BlobDownloadResponseContract, ServerReplacerContext> = {
  prefix: SERIALIZER_PREFIX_BLOB_DOWNLOAD,
  detect: isBlobDownload,
  replace: (value, context) => {
    const {
      metadata: streamMetadata,
      close,
      abort,
    } = context.sendStream(() => blobDownloadReplacer.createProducer(value))
    const internal = getBlobDownloadInternal(value)
    const metadata = {
      ...streamMetadata,
      type: internal.type,
      ...(internal.size !== undefined && internal.size >= 0 ? { size: internal.size } : {}),
    }
    return { metadata, close, abort }
  },
  createProducer: (value) => createStreamProducer(getBlobDownloadInternal(value).stream),
}
