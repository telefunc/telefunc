export { functionReviver }

import type { FunctionContract, ClientReviverContext, ReviverType } from '../../types.js'
import { SERIALIZER_PREFIX_FUNCTION, FN_SHIELD_ERROR_KEY } from '../../constants.js'
import { STATUS_BODY_SHIELD_VALIDATION_ERROR } from '../../../shared/constants.js'
import { hasProp } from '../../../utils/hasProp.js'

/**
 * Reconstructs a server function returned from a telefunction as a callable proxy.
 * Each call is forwarded as an ack message over a persistent `ClientChannel`.
 * The channel closes via `close(fn)` or automatically once the proxy is GC'd.
 *
 * Shield errors travel as a normal ack payload with the `FN_SHIELD_ERROR_KEY` marker —
 * the function layer owns its own error semantics; the channel stays a plain transport.
 */
const functionReviver: ReviverType<FunctionContract, ClientReviverContext> = {
  prefix: SERIALIZER_PREFIX_FUNCTION,
  createValue(metadata, context) {
    const channel = context.createChannel({
      channelId: metadata.channelId,
      ack: true,
    })
    return {
      value: async (...args: unknown[]) => {
        const res = await channel.send(args, { ack: true })
        if (hasProp(res, FN_SHIELD_ERROR_KEY, 'true')) {
          throw new Error(`${STATUS_BODY_SHIELD_VALIDATION_ERROR} — see server logs`)
        }
        return res
      },
      async close() {
        await channel.close()
      },
      abort(abortError) {
        channel.abort(abortError.abortValue, abortError.message)
      },
    }
  },
}
