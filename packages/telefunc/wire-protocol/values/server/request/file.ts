export { fileReviver }

import type { FileRequestContract, ServerReviverContext, ReviverType } from '../../types.js'
import { SERIALIZER_PREFIX_FILE } from '../../../constants.js'
import { LazyFile } from '../../LazyFile.js'

const fileReviver: ReviverType<FileRequestContract, ServerReviverContext> = {
  prefix: SERIALIZER_PREFIX_FILE,
  // The file's bytes are the request body's, read as its stream is read: nothing of the file's own to close.
  revive: (metadata, context) => {
    const getStream = context.registerFile(metadata.index, metadata.size)
    const lazyFile = new LazyFile(metadata.size, metadata.type, metadata.name, metadata.lastModified, getStream)
    return { value: lazyFile, close() {}, abort() {} }
  },
}
