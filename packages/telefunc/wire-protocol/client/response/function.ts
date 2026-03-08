export { functionClientPlaceholderType, getFunctionChannel }

import { SERIALIZER_PREFIX_FUNCTION } from '../../constants.js'
import type { PlaceholderReviverType, FunctionContract } from '../../placeholder-types.js'
import { ClientChannel } from '../channel.js'
import { getGlobalObject } from '../../../utils/getGlobalObject.js'

const globalObject = getGlobalObject('wire-protocol/client/response/function.ts', {
  /** Weak map from function proxy → its underlying channel, for `abort(fn)` cleanup. */
  fnChannelMap: new WeakMap<object, ClientChannel>(),
  /** Close the channel when the proxy is GC'd. `fn` is weak target — no retention cycle. */
  gcRegistry: new FinalizationRegistry<ClientChannel>((channel) => channel.close()),
})

/** Returns the underlying channel for a telefunc function proxy, if any. */
function getFunctionChannel(fn: object): ClientChannel | undefined {
  return globalObject.fnChannelMap.get(fn)
}

/**
 * Reconstructs a server function returned from a telefunction as a callable proxy.
 * Each call is forwarded as an ack message over a persistent `ClientChannel`.
 * The channel closes via `abort(fn)` or automatically once the proxy is GC'd.
 */
const functionClientPlaceholderType: PlaceholderReviverType<FunctionContract> = {
  prefix: SERIALIZER_PREFIX_FUNCTION,
  createValue(metadata, shard) {
    const channel = new ClientChannel(metadata.channelId, /*ackMode=*/ true, shard)
    const fn = (...args: unknown[]) => channel.send(args, { ack: true })
    globalObject.fnChannelMap.set(fn, channel)
    // fn is the weak target; channel is the held value — no cycle, no GC blocker
    globalObject.gcRegistry.register(fn, channel)
    return fn
  },
}
