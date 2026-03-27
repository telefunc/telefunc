export { functionReplacer }

import type { ClientReplacerContext, ReplacerType } from '../../types.js'
import { SERIALIZER_PREFIX_FUNCTION } from '../../constants.js'
import { ClientChannel } from '../channel.js'
import { getGlobalObject } from '../../../utils/getGlobalObject.js'

const globalObject = getGlobalObject('wire-protocol/client/request/function.ts', {
  /** Close the channel when the passed-in fn is GC'd — no more calls can arrive. */
  gcRegistry: new FinalizationRegistry<ClientChannel<unknown, readonly unknown[]>>((channel) => channel.close()),
})

type FunctionRequestContract = {
  value: (...args: readonly unknown[]) => unknown
  result: (...args: readonly unknown[]) => Promise<unknown>
  metadata: { channelId: string }
}

const functionReplacer: ReplacerType<FunctionRequestContract, ClientReplacerContext> = {
  prefix: SERIALIZER_PREFIX_FUNCTION,
  detect: (value): value is FunctionRequestContract['value'] => typeof value === 'function',
  getMetadata: (fn, context) => {
    const channel = new ClientChannel<unknown, readonly unknown[]>({
      channelId: crypto.randomUUID(),
      ackMode: true,
      transports: context.channelTransports,
      defer: true,
    })
    channel.listen((args) => fn(...args))
    context.registerChannel(channel)
    globalObject.gcRegistry.register(fn, channel)
    return { channelId: channel.id }
  },
}
