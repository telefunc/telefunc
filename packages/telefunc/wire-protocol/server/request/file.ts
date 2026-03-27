export { fileReviver }

import type { FileRequestContract, ServerReviverContext, ReviverType } from '../../types.js'
import { SERIALIZER_PREFIX_FILE } from '../../constants.js'
import { LazyFile } from './LazyFile.js'

const fileReviver: ReviverType<FileRequestContract, ServerReviverContext> = {
  prefix: SERIALIZER_PREFIX_FILE,
  createValue: (metadata, reader) => ({ value: new LazyFile(metadata, reader) }),
}
