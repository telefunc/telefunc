export { functionReviver }

import type { FunctionContract, ReviverType, ServerReviverContext } from '../../types.js'
import { SERIALIZER_PREFIX_FUNCTION } from '../../constants.js'
import { ServerChannel } from '../channel.js'
import { assertIsNotBrowser } from '../../../utils/assertIsNotBrowser.js'
assertIsNotBrowser()

const functionReviver: ReviverType<FunctionContract, ServerReviverContext> = {
  prefix: SERIALIZER_PREFIX_FUNCTION,
  createValue: ({ channelId }, context) => {
    const channel = new ServerChannel({ ackMode: true, id: channelId })
    channel._registerChannel()
    context.registerChannel(channel)
    return {
      value: (...args: unknown[]) => channel.send(args, { ack: true }),
    }
  },
}
