export { functionReviver }

import type { FunctionContract, ReviverType, ServerReviverContext } from '../../types.js'
import { SERIALIZER_PREFIX_FUNCTION } from '../../constants.js'
import { assertIsNotBrowser } from '../../../utils/assertIsNotBrowser.js'
assertIsNotBrowser()

const functionReviver: ReviverType<FunctionContract, ServerReviverContext> = {
  prefix: SERIALIZER_PREFIX_FUNCTION,
  createValue: ({ channelId }, { createChannel, validators }) => {
    const channel = createChannel({ id: channelId, ack: true })
    const validateReturn = validators.get('return')
    const fn = async (...args: unknown[]) => {
      const res = await channel.send(args, { ack: true })
      if (validateReturn) {
        const r = validateReturn(res)
        if (r !== true) throw new Error(r)
      }
      return res
    }
    return {
      value: fn,
      async close() {
        await channel.close()
      },
      abort(abortError) {
        channel.abort(abortError.abortValue)
      },
    }
  },
}
