export { functionReplacer }

import type { FunctionContract, ReplacerType, ServerReplacerContext } from '../../types.js'
import { SERIALIZER_PREFIX_FUNCTION } from '../../constants.js'

import { ServerChannel } from '../channel.js'
import { assertIsNotBrowser } from '../../../utils/assertIsNotBrowser.js'
assertIsNotBrowser()

const functionReplacer: ReplacerType<FunctionContract, ServerReplacerContext> = {
  prefix: SERIALIZER_PREFIX_FUNCTION,
  detect(value): value is FunctionContract['value'] {
    return typeof value === 'function'
  },
  getMetadata(fn, { registerChannel }) {
    const channel = new ServerChannel<unknown, readonly unknown[]>({ ackMode: true })
    channel._registerChannel()
    registerChannel(channel)
    channel.listen((args) => fn(...args))
    return { channelId: channel.id }
  },
}
