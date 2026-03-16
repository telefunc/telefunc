export { functionServerPlaceholderType }

import { SERIALIZER_PREFIX_FUNCTION } from '../../constants.js'
import type { PlaceholderReplacerType, FunctionContract } from '../../placeholder-types.js'
import type { ServerResponseContext } from './registry.js'
import { ServerChannel } from '../channel.js'
import { assertIsNotBrowser } from '../../../utils/assertIsNotBrowser.js'
assertIsNotBrowser()

const functionServerPlaceholderType: PlaceholderReplacerType<FunctionContract, ServerResponseContext> = {
  prefix: SERIALIZER_PREFIX_FUNCTION,
  detect(value): value is FunctionContract['value'] {
    return typeof value === 'function'
  },
  getMetadata(fn, { channelTransport, registerChannel }) {
    const channel = new ServerChannel<unknown, readonly unknown[]>({
      ackMode: true,
      channelTransport,
    })
    channel._registerChannel()
    registerChannel(channel)
    channel.listen((args) => fn(...args))
    return { channelId: channel.id, channelTransport }
  },
}
