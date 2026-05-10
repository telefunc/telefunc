export { fileDownloadReviver }

import type {
  ReviverType,
  FileDownloadResponseContract,
  ClientReviverContext,
  FileDownload,
  DownloadProgress,
} from '../../types.js'
import { SERIALIZER_PREFIX_FILE_DOWNLOAD } from '../../constants.js'

const fileDownloadReviver: ReviverType<FileDownloadResponseContract, ClientReviverContext> = {
  prefix: SERIALIZER_PREFIX_FILE_DOWNLOAD,
  revive: (metadata, context) => {
    const { arrayBuffer, cancel, abort } = context.receiveStreamReader(metadata)
    const callbacks: DownloadProgress[] = []
    let loaded = 0
    const filePromise = arrayBuffer(metadata.size, (chunkSize) => {
      loaded += chunkSize
      for (const cb of callbacks) cb(loaded, metadata.size)
    }).then(
      (buf) =>
        new File([buf], metadata.name, {
          type: metadata.type,
          lastModified: metadata.lastModified,
        }),
    )
    const value: FileDownload = {
      name: metadata.name,
      type: metadata.type,
      size: metadata.size,
      lastModified: metadata.lastModified,
      get loaded() {
        return loaded
      },
      file: filePromise,
      onProgress: (cb) => {
        callbacks.push(cb)
        if (loaded > 0) cb(loaded, metadata.size)
      },
      cancel,
    }
    return { value, close: cancel, abort }
  },
}
