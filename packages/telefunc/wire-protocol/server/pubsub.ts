export { getPubSubTransport, setPubSubTransport, setPubSubAdapter, AdaptedPubSubTransport }
export type { PubSubTransport, PubSubSubscription, PubSubAdapter, PubSubAdapterResult }

import { getGlobalObject } from '../../utils/getGlobalObject.js'
import { isPromise } from '../../utils/isPromise.js'
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

// ---------------------------------------------------------------------------
// Public adapter interface — a pure message bus across nodes
// ---------------------------------------------------------------------------

type PubSubAdapterResult = {
  /** Globally monotonic sequence number for the key. */
  seq?: number
  /** Server timestamp. */
  ts?: number
  /** Adapter-specific metadata. */
  meta?: Record<string, unknown>
}

type PubSubAdapter = {
  /**
   * Subscribe to cross-node messages for `key`.
   * Call `deliver(message)` when a message arrives from another node.
   * The message is an opaque string — do not parse or modify it.
   */
  subscribe(key: string, deliver: (message: string) => void): void | Promise<void>
  /** Unsubscribe from `key`. */
  unsubscribe(key: string): void | Promise<void>
  /**
   * Broadcast `message` to all nodes subscribed to `key`.
   * Return `{ seq }` for globally monotonic ordering (e.g. via Redis INCR).
   * If `seq` is omitted, the receipt will not include a sequence number.
   */
  publish(key: string, message: string): PubSubAdapterResult | Promise<PubSubAdapterResult>
}

// ---------------------------------------------------------------------------
// In-memory transport (single-process default)
// ---------------------------------------------------------------------------

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

// ---------------------------------------------------------------------------
// Adapted transport — wraps a user-provided PubSubAdapter
// ---------------------------------------------------------------------------

/** Envelope separator — safe because JSON.stringify escapes literal newlines */
const SEP = '\n'

class AdaptedPubSubTransport implements PubSubTransport {
  private readonly adapter: PubSubAdapter
  private readonly localSubscriptions = new Map<string, Set<PubSubSubscription>>()
  /** Track which keys have an active adapter subscription. */
  private readonly adapterKeys = new Set<string>()

  constructor(adapter: PubSubAdapter) {
    this.adapter = adapter
  }

  subscribe(subscription: PubSubSubscription): void | Promise<void> {
    let subs = this.localSubscriptions.get(subscription.key)
    if (!subs) {
      subs = new Set()
      this.localSubscriptions.set(subscription.key, subs)
    }
    subs.add(subscription)

    if (!this.adapterKeys.has(subscription.key)) {
      this.adapterKeys.add(subscription.key)
      return this.adapter.subscribe(subscription.key, (envelope) => {
        this._onAdapterMessage(subscription.key, envelope)
      })
    }
  }

  unsubscribe(subscription: PubSubSubscription): void | Promise<void> {
    const subs = this.localSubscriptions.get(subscription.key)
    if (!subs) return
    subs.delete(subscription)
    if (subs.size === 0) {
      this.localSubscriptions.delete(subscription.key)
      this.adapterKeys.delete(subscription.key)
      return this.adapter.unsubscribe(subscription.key)
    }
  }

  publish(subscription: PubSubSubscription, serialized: string): ChannelPublishAck | Promise<ChannelPublishAck> {
    // Everything flows through the adapter — local delivery happens when the
    // message comes back via _onAdapterMessage, just like Cloudflare's authority path.
    // This ensures subscribers see messages in global seq order.
    const envelope = subscription.id + SEP + serialized
    const result = this.adapter.publish(subscription.key, envelope)

    if (isPromise(result)) {
      return result.then((r) => this._toAck(subscription.key, r))
    }
    return this._toAck(subscription.key, result)
  }

  private _toAck(key: string, result: PubSubAdapterResult): ChannelPublishAck {
    return {
      key,
      seq: result.seq ?? 0,
      ts: result.ts ?? Date.now(),
      meta: result.meta,
    }
  }

  private _onAdapterMessage(key: string, envelope: string): void {
    // Parse envelope: sourceChannelId\nserialized
    const sep = envelope.indexOf(SEP)
    const sourceChannelId = envelope.slice(0, sep)
    const serialized = envelope.slice(sep + 1)

    const subs = this.localSubscriptions.get(key)
    if (!subs) return
    for (const target of subs) {
      if (target.id === sourceChannelId && !target.selfDelivery) continue
      target.onMessage(serialized, sourceChannelId)
    }
  }
}

// ---------------------------------------------------------------------------
// Global transport state
// ---------------------------------------------------------------------------

const globalObject = getGlobalObject('wire-protocol/server/pubsub.ts', {
  transport: new InMemoryPubSubTransport() as PubSubTransport,
})

function getPubSubTransport(): PubSubTransport {
  return globalObject.transport
}

function setPubSubTransport(transport: PubSubTransport): void {
  globalObject.transport = transport
}

function setPubSubAdapter(adapter: PubSubAdapter): void {
  globalObject.transport = new AdaptedPubSubTransport(adapter)
}
