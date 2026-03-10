export { functionServerPlaceholderType }

import { SERIALIZER_PREFIX_FUNCTION } from '../../constants.js'
import type { PlaceholderReplacerType, FunctionContract } from '../../placeholder-types.js'
import { ServerChannel } from '../channel.js'
import { assertIsNotBrowser } from '../../../utils/assertIsNotBrowser.js'
assertIsNotBrowser()

const functionServerPlaceholderType: PlaceholderReplacerType<FunctionContract> = {
  prefix: SERIALIZER_PREFIX_FUNCTION,
  detect(value): value is FunctionContract['value'] {
    return typeof value === 'function'
  },
  getMetadata(fn) {
    const channel = new ServerChannel(true)
    channel._registerChannel()
    channel.listen((args) => fn(...(args as unknown[])))
    return { channelId: channel.id }
  },
}
