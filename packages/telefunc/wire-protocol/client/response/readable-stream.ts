export { readableStreamReviver }

import type { ReviverType, ReadableStreamContract, ClientReviverContext } from '../../types.js'
import { SERIALIZER_PREFIX_STREAM } from '../../constants.js'

const readableStreamReviver: ReviverType<ReadableStreamContract, ClientReviverContext> = {
  prefix: SERIALIZER_PREFIX_STREAM,
  createValue: (metadata, context) => {
    const { stream, cancel, abort } = context.receiveStream(metadata)
    return {
      value: stream,
      close: cancel,
      abort,
    }
  },
}
