export { MemoryBackend }

// The in-process backend.

import type { BroadcastDriver, BroadcastPayload, BroadcastRoute, PublishResult } from '../broadcast/contract.js'
import type { OrderingInfo } from '../../wire/orderingFrame.js'
import type { BroadcastKind } from '../../wire/payloads.js'
import type { BackendPayload, BackendReceiver, SubscriptionAttempt, SubscriptionDriver } from '../subscription.js'
import { MemoryPubSub, type Subscriptions } from './deliveries.js'

class MemoryBackend implements BroadcastDriver {
  readonly subscriptions: SubscriptionDriver<BroadcastRoute>

  /** The order of each key subscribed here; an unsubscribed key's next publish starts above `broadcastHighest`. */
  private readonly broadcastOrder = new Map<string, OrderingInfo>()
  /** The highest `seq` and `timestamp` a Broadcast publish was given. */
  private readonly broadcastHighest: OrderingInfo = { seq: 0, timestamp: 0 }
  private readonly broadcastSubs: Record<BroadcastKind, Subscriptions> = { text: new Map(), binary: new Map() }
  private readonly pubSub = new MemoryPubSub()

  constructor() {
    this.subscriptions = {
      bind: (source) => ({
        partition: '',
        open: (receiver, localReceiverCount) => this.openSubscription(source, receiver, localReceiverCount),
      }),
      partitionHere: () => '',
    }
  }

  publish(route: BroadcastRoute, payload: BroadcastPayload): PublishResult | Promise<PublishResult> {
    const order = this.broadcastOrder.get(route.key)
    const mark = nextOrderMark(order ?? this.broadcastHighest, Date.now())
    // Only a subscribed key has an order.
    if (order !== undefined || this.broadcastSubscribed(route.key)) this.broadcastOrder.set(route.key, mark)
    const highest = this.broadcastHighest
    if (mark.seq > highest.seq) highest.seq = mark.seq
    if (mark.timestamp > highest.timestamp) highest.timestamp = mark.timestamp
    return this.pubSub.publish(this.broadcastSubs[route.kind].get(route.key) ?? [], payload, mark)
  }

  published(key: string): number {
    return this.broadcastOrder.get(key)?.seq ?? 0
  }

  private broadcastSubscribed(key: string): boolean {
    return this.broadcastSubs.text.has(key) || this.broadcastSubs.binary.has(key)
  }

  private openSubscription(
    { kind, key }: BroadcastRoute,
    receiver: BackendReceiver<BackendPayload>,
    localReceiverCount: () => number,
  ): SubscriptionAttempt {
    return this.pubSub.subscribe({
      subs: this.broadcastSubs[kind],
      key,
      receiver,
      localReceiverCount,
      onDetach: () => {
        if (!this.broadcastSubscribed(key)) this.broadcastOrder.delete(key)
      },
    })
  }
}

/** `seq` strictly increases per key; `timestamp` is this process's time, clamped so it never goes back. */
function nextOrderMark(previous: OrderingInfo | undefined, now: number): OrderingInfo {
  return { seq: (previous?.seq ?? 0) + 1, timestamp: Math.max(now, previous?.timestamp ?? 0) }
}
