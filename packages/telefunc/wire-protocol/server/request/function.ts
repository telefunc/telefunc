export { functionServerRequestType }

import { SERIALIZER_PREFIX_FUNCTION } from '../../constants.js'
import type { PlaceholderReviverType, FunctionContract } from '../../placeholder-types.js'
import { ServerChannel } from '../channel.js'
import { assertIsNotBrowser } from '../../../utils/assertIsNotBrowser.js'
assertIsNotBrowser()

const functionServerRequestType: PlaceholderReviverType<FunctionContract> = {
  prefix: SERIALIZER_PREFIX_FUNCTION,
  createValue: ({ channelId }) => {
    // Client opened its ClientChannel eagerly (at serialization time) with this ID.
    // Server must use the same ID so the WS reconcile links both sides.
    const channel = new ServerChannel(/*ackMode=*/ true, channelId)
    channel._registerChannel()
    return (...args) => channel.send(args, { ack: true })
  },
}
