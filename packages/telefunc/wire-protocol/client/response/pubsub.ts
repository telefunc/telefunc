export { pubsubReviver }

import type { PubSubContract, ClientReviverContext, ReviverType } from '../../types.js'
import { SERIALIZER_PREFIX_PUBSUB } from '../../constants.js'
import { ClientPubSub } from '../channel.js'
import { getGlobalObject } from '../../../utils/getGlobalObject.js'
const globalObject = getGlobalObject('wire-protocol/client/response/pubsub.ts', {
  gcRegistry: new FinalizationRegistry<ClientPubSub>((channel) => channel.close()),
})

const pubsubReviver: ReviverType<PubSubContract, ClientReviverContext> = {
  prefix: SERIALIZER_PREFIX_PUBSUB,
  createValue(metadata, context) {
    const channel = new ClientPubSub({
      channelId: metadata.channelId,
      key: metadata.key,
      transports: context.channelTransports,
      sessionToken: context.sessionToken,
    })
    const value = new Proxy({} as ClientPubSub, {
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
