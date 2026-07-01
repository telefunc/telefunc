export { fileDownloadReplacer }

import type { StreamingReplacerType, FileDownloadResponseContract, ServerReplacerContext } from '../../types.js'
import { SERIALIZER_PREFIX_FILE_DOWNLOAD } from '../../constants.js'
import { createStreamProducer } from './createStreamProducer.js'
import { isFileDownload, getFileDownloadInternal } from '../../download.js'

const fileDownloadReplacer: StreamingReplacerType<FileDownloadResponseContract, ServerReplacerContext> = {
  prefix: SERIALIZER_PREFIX_FILE_DOWNLOAD,
  detect: isFileDownload,
  replace: (value, context) => {
    const {
      metadata: streamMetadata,
      close,
      abort,
    } = context.sendStream(() => fileDownloadReplacer.createProducer(value))
    const internal = getFileDownloadInternal(value)
    const metadata = {
      ...streamMetadata,
      name: internal.name,
      type: internal.type,
      lastModified: internal.lastModified,
      ...(internal.size !== undefined && internal.size >= 0 ? { size: internal.size } : {}),
    }
    return { metadata, close, abort }
  },
  createProducer: (value) => createStreamProducer(getFileDownloadInternal(value).stream),
}
