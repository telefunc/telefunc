export { functionClientRequestType }

import { SERIALIZER_PREFIX_FUNCTION } from '../../constants.js'
import type { PlaceholderReplacerType, FunctionContract } from '../../placeholder-types.js'
import { ClientChannel } from '../channel.js'
import { getGlobalObject } from '../../../utils/getGlobalObject.js'

const globalObject = getGlobalObject('wire-protocol/client/request/function.ts', {
  /** Close the channel when the passed-in fn is GC'd — no more calls can arrive. */
  gcRegistry: new FinalizationRegistry<ClientChannel<unknown, unknown[]>>((channel) => channel.close()),
})

const functionClientRequestType: PlaceholderReplacerType<FunctionContract> = {
  prefix: SERIALIZER_PREFIX_FUNCTION,
  detect: (value): value is FunctionContract['value'] => typeof value === 'function',
  getMetadata: (fn) => {
    // Connect eagerly — the server will block reconcile until ServerChannel is created.
    const channel = new ClientChannel<unknown, unknown[]>(
      crypto.randomUUID(),
      /*ackMode=*/ true,
      /*shard=*/ undefined,
      /*defer=*/ true,
    )
    channel.listen((args) => fn(...args))
    globalObject.gcRegistry.register(fn, channel)
    return { channelId: channel.id }
  },
}
