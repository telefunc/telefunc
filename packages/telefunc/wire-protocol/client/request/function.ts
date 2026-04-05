export { functionReplacer }

import type { ClientReplacerContext, ReplacerType, FunctionContract } from '../../types.js'
import { SERIALIZER_PREFIX_FUNCTION } from '../../constants.js'

const functionReplacer: ReplacerType<FunctionContract, ClientReplacerContext> = {
  prefix: SERIALIZER_PREFIX_FUNCTION,
  detect: (value): value is FunctionContract['value'] => typeof value === 'function',
  getMetadata: (fn, context) => {
    const channel = context.createChannel<unknown, readonly unknown[]>({ ack: true })
    channel.listen((args) => fn(...args))
    return {
      metadata: { channelId: channel.id },
      async close() {
        await channel.close()
      },
      abort() {
        channel.abort()
      },
    }
  },
}
