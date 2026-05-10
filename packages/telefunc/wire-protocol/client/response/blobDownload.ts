export { blobDownloadReviver }

import type {
  ReviverType,
  BlobDownloadResponseContract,
  ClientReviverContext,
  BlobDownload,
  DownloadProgress,
} from '../../types.js'
import { SERIALIZER_PREFIX_BLOB_DOWNLOAD } from '../../constants.js'

const blobDownloadReviver: ReviverType<BlobDownloadResponseContract, ClientReviverContext> = {
  prefix: SERIALIZER_PREFIX_BLOB_DOWNLOAD,
  revive: (metadata, context) => {
    const { arrayBuffer, cancel, abort } = context.receiveStreamReader(metadata)
    const callbacks: DownloadProgress[] = []
    let loaded = 0
    const blobPromise = arrayBuffer(metadata.size, (chunkSize) => {
      loaded += chunkSize
      for (const cb of callbacks) cb(loaded, metadata.size)
    }).then((buf) => new Blob([buf], { type: metadata.type }))
    const value: BlobDownload = {
      type: metadata.type,
      size: metadata.size,
      get loaded() {
        return loaded
      },
      blob: blobPromise,
      onProgress: (cb) => {
        callbacks.push(cb)
        if (loaded > 0) cb(loaded, metadata.size)
      },
      cancel,
    }
    return { value, close: cancel, abort }
  },
}
