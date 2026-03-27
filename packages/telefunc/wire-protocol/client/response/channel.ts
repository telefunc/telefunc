export { channelReviver }

import type { ChannelContract, ClientReviverContext, ReviverType } from '../../types.js'
import { SERIALIZER_PREFIX_CHANNEL } from '../../constants.js'
import { ClientChannel } from '../channel.js'
import { getGlobalObject } from '../../../utils/getGlobalObject.js'
const globalObject = getGlobalObject('wire-protocol/client/response/channel.ts', {
  gcRegistry: new FinalizationRegistry<ClientChannel>((channel) => channel.close()),
})

const channelReviver: ReviverType<ChannelContract, ClientReviverContext> = {
  prefix: SERIALIZER_PREFIX_CHANNEL,
  createValue(metadata, context) {
    const channel = new ClientChannel({
      channelId: metadata.channelId,
      ackMode: metadata.ack,
      key: metadata.key,
      transports: context.channelTransports,
      sessionToken: context.sessionToken,
    })
    // Proxy so `value` and `channel` are separate objects. This allows
    // FinalizationRegistry to detect when the user drops `value` (the public
    // handle) while `channel` (the internal transport peer) stays alive.
    // If we returned `channel` directly, the GC callback would never fire
    // because `channel` is held by the transport connection.
    const value = new Proxy({} as ClientChannel, {
      get(_target, prop) {
        const property = Reflect.get(channel, prop, channel)
        return typeof property === 'function' ? property.bind(channel) : property
      },
      set(_target, prop, value) {
        return Reflect.set(channel, prop, value, channel)
      },
      has(_target, prop) {
        return Reflect.has(channel, prop)
      },
      ownKeys() {
        return Reflect.ownKeys(channel)
      },
      getOwnPropertyDescriptor(_target, prop) {
        const descriptor = Reflect.getOwnPropertyDescriptor(channel, prop)
        if (!descriptor) return descriptor
        return { ...descriptor, configurable: true }
      },
      getPrototypeOf() {
        return Reflect.getPrototypeOf(channel)
      },
    })
    globalObject.gcRegistry.register(value, channel)
    context.registerChannel(channel)
    return {
      value,
      close: () => channel.close(),
    }
  },
}
