export { functionReviver }

import type { FunctionContract, ClientReviverContext, ReviverType } from '../../types.js'
import { SERIALIZER_PREFIX_FUNCTION } from '../../constants.js'

/**
 * Reconstructs a server function returned from a telefunction as a callable proxy.
 * Each call is forwarded as an ack message over a persistent `ClientChannel`.
 * The channel closes via `close(fn)` or automatically once the proxy is GC'd.
 */
const functionReviver: ReviverType<FunctionContract, ClientReviverContext> = {
  prefix: SERIALIZER_PREFIX_FUNCTION,
  createValue(metadata, context) {
    const channel = context.createChannel({
      channelId: metadata.channelId,
      ack: true,
    })
    return {
      value: (...args: unknown[]) => channel.send(args, { ack: true }),
      async close() {
        await channel.close()
      },
      abort(abortError) {
        channel._abortWithValue(abortError.abortValue, abortError.message)
      },
    }
  },
}
