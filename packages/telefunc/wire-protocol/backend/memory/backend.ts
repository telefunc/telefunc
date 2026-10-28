export { MemoryBackend }

// The in-process backend.

import type { BroadcastDriver, BroadcastPayload, BroadcastRoute, PublishResult } from '../broadcast/contract.js'
import type { OrderingInfo } from '../../wire/orderingFrame.js'
import type { BroadcastKind } from '../../wire/connectionFrame.js'
import type { BackendPayload, BackendReceiver, SubscriptionDriver } from '../subscription.js'
import {
  MemoryDeliveries,
  MemorySubscriptionAttempt,
  copyBytes,
  removeSubscription,
  sumReceiverCounts,
  type Subscriptions,
} from './deliveries.js'

class MemoryBackend implements BroadcastDriver {
  readonly subscriptions: SubscriptionDriver<BroadcastRoute>

  /** The order of each key subscribed here; an unsubscribed key's next publish starts above `_broadcastHighest`. */
  private readonly _broadcastOrder = new Map<string, OrderingInfo>()
  /** The highest `seq` and `timestamp` a Broadcast publish was given. */
  private readonly _broadcastHighest: OrderingInfo = { seq: 0, timestamp: 0 }
  private readonly _broadcastSubs: Record<BroadcastKind, Subscriptions> = { text: new Map(), binary: new Map() }
  private readonly _deliveries = new MemoryDeliveries()

  constructor() {
    this.subscriptions = {
      bind: (source) => ({
        partition: '',
        open: (receiver, localReceiverCount) => this._openSubscription(source, receiver, localReceiverCount),
      }),
      partitionHere: () => '',
    }
  }

  publish(route: BroadcastRoute, payload: BroadcastPayload): PublishResult | Promise<PublishResult> {
    const order = this._broadcastOrder.get(route.key)
    const mark = nextOrderMark(order ?? this._broadcastHighest, Date.now())
    // Only a subscribed key has an order.
    if (order !== undefined || this._broadcastSubscribed(route.key)) this._broadcastOrder.set(route.key, mark)
    const highest = this._broadcastHighest
    if (mark.seq > highest.seq) highest.seq = mark.seq
    if (mark.timestamp > highest.timestamp) highest.timestamp = mark.timestamp
    const targets = this._broadcastSubs[route.kind].get(route.key) ?? []
    // Counted before delivery, which may unsubscribe or subscribe.
    const receipt = { seq: mark.seq, timestamp: mark.timestamp, receivers: sumReceiverCounts(targets) }
    if (this._deliveries.deliversNow()) {
      this._deliveries.deliverNow(() => {
        for (const target of targets) target.deliver(payload, mark)
      })
      return receipt
    }
    const held = typeof payload === 'string' ? payload : copyBytes(payload)
    // A queued publish resolves as it is delivered, so a publisher awaiting each can't outrun its subscribers.
    return new Promise((resolve) => {
      this._deliveries.deliver(() => {
        for (const target of targets) target.deliver(held, mark)
        resolve(receipt)
      })
    })
  }

  published(key: string): number {
    return this._broadcastOrder.get(key)?.seq ?? 0
  }

  private _broadcastSubscribed(key: string): boolean {
    return this._broadcastSubs.text.has(key) || this._broadcastSubs.binary.has(key)
  }

  private _openSubscription(
    source: BroadcastRoute,
    receiver: BackendReceiver<BackendPayload>,
    localReceiverCount: () => number,
  ): MemorySubscriptionAttempt {
    const subs = this._broadcastSubs[source.kind]
    const key = source.key
    const sub: MemorySubscriptionAttempt = new MemorySubscriptionAttempt({
      receiver,
      localReceiverCount,
      detach: () => {
        removeSubscription(subs, key, sub)
        if (!this._broadcastSubscribed(key)) this._broadcastOrder.delete(key)
      },
    })
    subs.set(key, [...(subs.get(key) ?? []), sub])
    // Ready once what was published before it is delivered, as a network subscriber hears its confirmation after the
    // messages sent before it.
    if (this._deliveries.length === 0) sub.establish()
    else queueMicrotask(() => this._deliveries.deliver(() => sub.establish()))
    return sub
  }
}

/** `seq` strictly increases per key; `timestamp` is this process's time, clamped so it never goes back. */
function nextOrderMark(previous: OrderingInfo | undefined, now: number): OrderingInfo {
  return { seq: (previous?.seq ?? 0) + 1, timestamp: Math.max(now, previous?.timestamp ?? 0) }
}
