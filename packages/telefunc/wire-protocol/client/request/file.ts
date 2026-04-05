export { fileReplacer }

import type { FileRequestContract, ClientReplacerContext, ReplacerType } from '../../types.js'
import { SERIALIZER_PREFIX_FILE } from '../../constants.js'

const fileReplacer: ReplacerType<FileRequestContract, ClientReplacerContext> = {
  prefix: SERIALIZER_PREFIX_FILE,
  detect: (value: unknown): value is File => value instanceof File,
  getMetadata: (value, context) => {
    const index = context.registerFile(value)
    return {
      metadata: { index, name: value.name, size: value.size, type: value.type, lastModified: value.lastModified },
      // Files are fully consumed during request parsing — no ongoing resource to clean up.
      close() {},
      abort() {},
    }
  },
}
