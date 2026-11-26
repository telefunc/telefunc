export { fileReviver }

import type { FileRequestContract, ServerReviverContext, ReviverType } from '../../types.js'
import { SERIALIZER_PREFIX_FILE } from '../../../constants.js'
import { LazyFile } from '../../LazyFile.js'

const fileReviver: ReviverType<FileRequestContract, ServerReviverContext> = {
  prefix: SERIALIZER_PREFIX_FILE,
  // The file's bytes are the request body's, kept for it until it reads them: closing it lets go of them.
  revive: (metadata, context) => {
    const file = context.registerFile(metadata.index, metadata.size)
    const lazyFile = new LazyFile(metadata.size, metadata.type, metadata.name, metadata.lastModified, file.stream)
    return { value: lazyFile, close: file.letGo, abort() {}, onEnd: file.onGone }
  },
}
