export { functionServerRequestType }

import { SERIALIZER_PREFIX_FUNCTION } from '../../constants.js'
import type { PlaceholderServerReviverType, FunctionContract } from '../../placeholder-types.js'
import { ServerChannel } from '../channel.js'
import { assertIsNotBrowser } from '../../../utils/assertIsNotBrowser.js'
assertIsNotBrowser()

const functionServerRequestType: PlaceholderServerReviverType<FunctionContract> = {
  prefix: SERIALIZER_PREFIX_FUNCTION,
  createValue: ({ channelId, channelTransport }) => {
    // Client opened its ClientChannel eagerly (at serialization time) with this ID.
    // Server must use the same ID so the WS reconcile links both sides.
    const channel = new ServerChannel({
      ackMode: true,
      id: channelId,
      channelTransport,
    })
    channel._registerChannel()
    return {
      value: (...args: unknown[]) => channel.send(args, { ack: true }),
      close: () => channel.close(),
    }
  },
}
