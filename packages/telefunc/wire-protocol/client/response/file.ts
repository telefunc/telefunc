export { fileReviver }

import type { ReviverType, FileResponseContract, ClientReviverContext } from '../../types.js'
import { SERIALIZER_PREFIX_FILE } from '../../constants.js'
import { PendingValue } from './PendingValue.js'

const fileReviver: ReviverType<FileResponseContract, ClientReviverContext> = {
  prefix: SERIALIZER_PREFIX_FILE,
  revive: (metadata, context) => {
    const { bytes, cancel, abort } = context.receiveStream(metadata)
    const filePromise = bytes({ expectedSize: metadata.size }).then(
      (buf) =>
        new File([buf], metadata.name, {
          type: metadata.type,
          lastModified: metadata.lastModified,
        }),
    )
    return { value: new PendingValue(filePromise), close: cancel, abort }
  },
}
