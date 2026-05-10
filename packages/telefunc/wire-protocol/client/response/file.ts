export { fileReviver }

import type { ReviverType, FileResponseContract, ClientReviverContext } from '../../types.js'
import { SERIALIZER_PREFIX_FILE } from '../../constants.js'

const fileReviver: ReviverType<FileResponseContract, ClientReviverContext> = {
  prefix: SERIALIZER_PREFIX_FILE,
  revive: (metadata, context) => {
    const { arrayBuffer, cancel, abort } = context.receiveStreamReader(metadata)
    const filePromise = arrayBuffer(metadata.size).then(
      (buf) =>
        new File([buf], metadata.name, {
          type: metadata.type,
          lastModified: metadata.lastModified,
        }),
    )
    context.waitFor(filePromise)
    return { value: filePromise, close: cancel, abort }
  },
}
