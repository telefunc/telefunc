export { fileDownloadReviver }

import type { ReviverType, FileDownloadResponseContract, ClientReviverContext } from '../../types.js'
import { SERIALIZER_PREFIX_FILE_DOWNLOAD } from '../../constants.js'
import { FileDownload } from './DownloadClasses.js'

const fileDownloadReviver: ReviverType<FileDownloadResponseContract, ClientReviverContext> = {
  prefix: SERIALIZER_PREFIX_FILE_DOWNLOAD,
  revive: (metadata, context) => {
    const source = context.receiveStream(metadata)
    const value = new FileDownload(metadata, source)
    return { value, close: source.cancel, abort: source.abort }
  },
}
