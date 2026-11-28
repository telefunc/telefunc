export { fileReviver }

import type { FileRequestContract, ServerReviverContext, ReviverType } from '../../types.js'
import { SERIALIZER_PREFIX_FILE } from '../../../constants.js'
import { LazyFile } from '../../LazyFile.js'

const fileReviver: ReviverType<FileRequestContract, ServerReviverContext> = {
  prefix: SERIALIZER_PREFIX_FILE,
  revive: (metadata, context) => {
    const stream = context.registerFile(metadata.index, metadata.size)
    const lazyFile = new LazyFile(metadata.size, metadata.type, metadata.name, metadata.lastModified, stream)
    return { value: lazyFile, close() {}, abort() {} }
  },
}
