export { readableStreamReviver }

import type { ReviverType, ReadableStreamRequestContract, ServerReviverContext } from '../../types.js'
import { SERIALIZER_PREFIX_STREAM } from '../../constants.js'
import { assertIsNotBrowser } from '../../../utils/assertIsNotBrowser.js'
assertIsNotBrowser()

const readableStreamReviver: ReviverType<ReadableStreamRequestContract, ServerReviverContext> = {
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
