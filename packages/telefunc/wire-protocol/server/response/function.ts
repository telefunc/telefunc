export { functionReplacer }

import type { FunctionContract, ReplacerType, ServerReplacerContext } from '../../types.js'
import { SERIALIZER_PREFIX_FUNCTION, FN_SHIELD_ERROR_KEY } from '../../constants.js'

import { assertIsNotBrowser } from '../../../utils/assertIsNotBrowser.js'
assertIsNotBrowser()

const functionReplacer: ReplacerType<FunctionContract, ServerReplacerContext> = {
  prefix: SERIALIZER_PREFIX_FUNCTION,
  detect(value): value is FunctionContract['value'] {
    return typeof value === 'function'
  },
  replace(fn, { createChannel, validators }) {
    const channel = createChannel<readonly unknown[], unknown>({ ack: true })
    const validateArgs = validators.get('args')
    channel.listen((args) => {
      if (validateArgs) {
        // The validator auto-logs the full message server-side via `config.log.shieldErrors`.
        // Return a marker-only payload so the client sees just the canonical generic message.
        if (validateArgs(...args) !== true) return { [FN_SHIELD_ERROR_KEY]: true }
      }
      return fn(...args)
    })
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
