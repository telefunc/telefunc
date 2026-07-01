export { fileReviver }

import type { FileRequestContract, ServerReviverContext, ReviverType } from '../../types.js'
import { SERIALIZER_PREFIX_FILE } from '../../constants.js'
import { LazyFile } from '../../LazyFile.js'

const fileReviver: ReviverType<FileRequestContract, ServerReviverContext> = {
  prefix: SERIALIZER_PREFIX_FILE,
  // Files are fully consumed during request parsing — no ongoing resource to clean up.
  revive: (metadata, context) => {
    context.registerFile(metadata.index, metadata.size)
    const lazyFile = new LazyFile(metadata.size, metadata.type, metadata.name, metadata.lastModified, () =>
      context.consumeFile(metadata.index, metadata.size),
    )
    return { value: lazyFile, close() {}, abort() {} }
  },
}
