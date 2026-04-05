export { functionReviver }

import type { FunctionContract, ReviverType, ServerReviverContext } from '../../types.js'
import { SERIALIZER_PREFIX_FUNCTION } from '../../constants.js'
import { assertIsNotBrowser } from '../../../utils/assertIsNotBrowser.js'
assertIsNotBrowser()

const functionReviver: ReviverType<FunctionContract, ServerReviverContext> = {
  prefix: SERIALIZER_PREFIX_FUNCTION,
  createValue: ({ channelId }, context) => {
    const channel = context.createChannel({ id: channelId, ack: true })
    return {
      value: (...args) => channel.send(args, { ack: true }),
      async close() {
        await channel.close()
      },
      abort(abortError) {
        channel.abort(abortError.abortValue)
      },
    }
  },
}
