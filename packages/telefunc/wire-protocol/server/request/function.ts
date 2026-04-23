export { functionReviver }

import type { FunctionContract, ReviverType, ServerReviverContext } from '../../types.js'
import { SERIALIZER_PREFIX_FUNCTION } from '../../constants.js'
import { assertIsNotBrowser } from '../../../utils/assertIsNotBrowser.js'
import { ShieldValidationError } from '../../../shared/ShieldValidationError.js'
assertIsNotBrowser()

const functionReviver: ReviverType<FunctionContract, ServerReviverContext> = {
  prefix: SERIALIZER_PREFIX_FUNCTION,
  revive: ({ channelId }, { createChannel, validators }) => {
    const channel = createChannel({ id: channelId, ack: true })
    const validateReturn = validators.get('return')
    const fn = async (...args: unknown[]) => {
      const res = await channel.send(args, { ack: true })
      if (validateReturn) {
        const r = validateReturn(res)
        // The validator auto-logs the full message server-side via `config.log.shieldErrors`;
        // the branded error lets `runTelefunc` surface the canonical 422 so the client sees
        // "Shield Validation Error" rather than a generic bug-error.
        if (r !== true) throw new ShieldValidationError(r)
      }
      return res
    }
    return {
      value: fn,
      async close() {
        await channel.close()
      },
      abort(abortError) {
        channel.abort(abortError.abortValue)
      },
    }
  },
}
