export { functionReplacer }

import type { FunctionContract, ReplacerType, ServerReplacerContext } from '../../types.js'
import { SERIALIZER_PREFIX_FUNCTION } from '../../constants.js'

import { assertIsNotBrowser } from '../../../utils/assertIsNotBrowser.js'
assertIsNotBrowser()

const functionReplacer: ReplacerType<FunctionContract, ServerReplacerContext> = {
  prefix: SERIALIZER_PREFIX_FUNCTION,
  detect(value): value is FunctionContract['value'] {
    return typeof value === 'function'
  },
  getMetadata(fn, { createChannel }) {
    const channel = createChannel<unknown, readonly unknown[]>({ ack: true })
    channel.listen((args) => fn(...args))
    return {
      metadata: { channelId: channel.id },
      async close() {
        await channel.close()
      },
      abort(abortError) {
        channel.abort(abortError.abortValue)
      },
    }
  },
}
