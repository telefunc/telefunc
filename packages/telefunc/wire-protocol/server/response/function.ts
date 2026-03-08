export { functionServerPlaceholderType }

import { SERIALIZER_PREFIX_FUNCTION } from '../../constants.js'
import type { PlaceholderReplacerType, FunctionContract } from '../../placeholder-types.js'
import { createChannel } from '../channel.js'
import { assertIsNotBrowser } from '../../../utils/assertIsNotBrowser.js'
assertIsNotBrowser()

const functionServerPlaceholderType: PlaceholderReplacerType<FunctionContract> = {
  prefix: SERIALIZER_PREFIX_FUNCTION,
  detect(value): value is FunctionContract['value'] {
    return typeof value === 'function'
  },
  getMetadata(fn) {
    const channel = createChannel({ ack: true })
    channel.listen((args) => fn(...(args as unknown[])))
    return { channelId: channel.id }
  },
}
