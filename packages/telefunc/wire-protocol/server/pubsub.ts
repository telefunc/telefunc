export { getPubSubTransport, setPubSubTransport }
export type { PubSubTransport, PubSubSubscription }

import { getGlobalObject } from '../../utils/getGlobalObject.js'
import type { ChannelPublishAck } from '../channel.js'

type PubSubSubscription = {
  readonly id: string
  readonly key: string
  readonly selfDelivery: boolean
  onMessage(serialized: string, sourceChannelId: string): void
}

type PubSubTransport = {
  subscribe(subscription: PubSubSubscription): void | Promise<void>
  unsubscribe(subscription: PubSubSubscription): void | Promise<void>
  publish(subscription: PubSubSubscription, serialized: string): ChannelPublishAck | Promise<ChannelPublishAck>
}

class InMemoryPubSubTransport implements PubSubTransport {
  private readonly keySubscriptions = new Map<string, Set<PubSubSubscription>>()
  private readonly keySeqs = new Map<string, number>()

  subscribe(subscription: PubSubSubscription): void {
    let subscriptions = this.keySubscriptions.get(subscription.key)
    if (!subscriptions) {
      subscriptions = new Set()
      this.keySubscriptions.set(subscription.key, subscriptions)
    }
    subscriptions.add(subscription)
  }

  unsubscribe(subscription: PubSubSubscription): void {
    const subscriptions = this.keySubscriptions.get(subscription.key)
    if (!subscriptions) return
    subscriptions.delete(subscription)
    if (subscriptions.size === 0) this.keySubscriptions.delete(subscription.key)
  }

  publish(subscription: PubSubSubscription, serialized: string): ChannelPublishAck {
    const subscriptions = this.keySubscriptions.get(subscription.key)
    const seq = (this.keySeqs.get(subscription.key) ?? 0) + 1
    this.keySeqs.set(subscription.key, seq)
    let delivered = 0

    if (subscriptions) {
      for (const target of subscriptions) {
        if (target.id === subscription.id && !subscription.selfDelivery) continue
        delivered++
        target.onMessage(serialized, subscription.id)
      }
    }

    return { key: subscription.key, seq, ts: Date.now(), meta: { delivered, transport: 'in-memory' } }
  }
}

const globalObject = getGlobalObject('wire-protocol/server/pubsub.ts', {
  transport: new InMemoryPubSubTransport() as PubSubTransport,
})

function getPubSubTransport(): PubSubTransport {
  return globalObject.transport
}

function setPubSubTransport(transport: PubSubTransport): void {
  globalObject.transport = transport
}
