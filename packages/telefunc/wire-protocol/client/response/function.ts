export { functionClientPlaceholderType }

import { SERIALIZER_PREFIX_FUNCTION } from '../../constants.js'
import type { PlaceholderReviverType, FunctionContract } from '../../placeholder-types.js'
import { ClientChannel } from '../channel.js'
import { getGlobalObject } from '../../../utils/getGlobalObject.js'

const globalObject = getGlobalObject('wire-protocol/client/response/function.ts', {
  /** Close the channel when the proxy is GC'd. `fn` is weak target — no retention cycle. */
  gcRegistry: new FinalizationRegistry<ClientChannel>((channel) => channel.close()),
})

/**
 * Reconstructs a server function returned from a telefunction as a callable proxy.
 * Each call is forwarded as an ack message over a persistent `ClientChannel`.
 * The channel closes via `close(fn)` or automatically once the proxy is GC'd.
 */
const functionClientPlaceholderType: PlaceholderReviverType<FunctionContract> = {
  prefix: SERIALIZER_PREFIX_FUNCTION,
  createValue(metadata, context) {
    const channel = new ClientChannel(metadata.channelId, /*ackMode=*/ true, context.shard)
    const fn = (...args: unknown[]) => channel.send(args, { ack: true })
    // fn is the weak target; channel is the held value — no cycle, no GC blocker
    globalObject.gcRegistry.register(fn, channel)
    context.registerChannel(channel)
    return {
      value: fn,
      close: () => channel.close(),
    }
  },
}
