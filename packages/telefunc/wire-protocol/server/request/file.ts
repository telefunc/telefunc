export { fileReviver }

import type { FileRequestContract, ServerReviverContext, ReviverType } from '../../types.js'
import { SERIALIZER_PREFIX_FILE } from '../../constants.js'
import { LazyFile } from './LazyFile.js'

const fileReviver: ReviverType<FileRequestContract, ServerReviverContext> = {
  prefix: SERIALIZER_PREFIX_FILE,
  // Files are fully consumed during request parsing — no ongoing resource to clean up.
  createValue: (metadata, reader) => ({ value: new LazyFile(metadata, reader), close() {}, abort() {} }),
}
