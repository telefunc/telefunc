export { getPubSubRegistry, setPubSubRegistry, setPubSubAdapter, DefaultPubSubRegistry }
export type {
  PubSubRegistry,
  PubSubSubscriber,
  PubSubSubscription,
  PubSubPublish,
  PubSubPublishResult,
  PubSubOnMessage,
  PubSubAdapter,
  PubSubAdapterResult,
}

import { assertUsage } from '../../utils/assert.js'
import { getGlobalObject } from '../../utils/getGlobalObject.js'
import { isPromise } from '../../utils/isPromise.js'
import type { WirePublishInfo } from '../shared-ws.js'

/** Transport-level publish result — no key, no id. Channel adds those. */
type PubSubPublishResult = WirePublishInfo & { meta?: Record<string, unknown> }

/** Callback for delivering a pub/sub message to a subscriber. */
type PubSubOnMessage = (serialized: string, sourceChannelId: string, info: WirePublishInfo) => void

type PubSubSubscriber = { id: string; key: string; selfDelivery: boolean }
type PubSubSubscription = PubSubSubscriber & { onMessage: PubSubOnMessage }
type PubSubPublish = PubSubSubscriber & { serialized: string }

// ---------------------------------------------------------------------------
// Registry — the interface ServerChannel talks to
// ---------------------------------------------------------------------------

type PubSubRegistry = {
  subscribe(sub: PubSubSubscription): void | Promise<void>
  unsubscribe(id: string, key: string): void | Promise<void>
  publish(msg: PubSubPublish): PubSubPublishResult | Promise<PubSubPublishResult>
}

// ---------------------------------------------------------------------------
// Public adapter interface — a pure message bus across nodes
// ---------------------------------------------------------------------------

type PubSubAdapterResult = {
  /** Per-key counter (1, 2, 3…) for ordering and gap detection. */
  seq?: number
  /** Server timestamp (ms). */
  ts?: number
  /** Adapter-specific metadata (only included in publisher receipt, not delivered to subscribers). */
  meta?: Record<string, unknown>
}

type PubSubAdapter = {
  /**
   * Subscribe to cross-node messages for `key`.
   * Call `onMessage(message, info)` when a message arrives from another node.
   * `message` is an opaque string — do not parse or modify it.
   * `info` carries the ordering metadata assigned at publish time.
   */
  subscribe(key: string, onMessage: (message: string, info: WirePublishInfo) => void): void | Promise<void>
  /** Unsubscribe from `key`. */
  unsubscribe(key: string): void | Promise<void>
  /**
   * Broadcast `message` to all nodes subscribed to `key`.
   * Return `{ seq, ts }` — both optional, defaults applied by the registry.
   */
  publish(key: string, message: string): PubSubAdapterResult | Promise<PubSubAdapterResult>
}

// ---------------------------------------------------------------------------
// Default registry — handles in-memory + adapted (user-provided adapter)
// ---------------------------------------------------------------------------

class DefaultPubSubRegistry implements PubSubRegistry {
  private readonly adapter: PubSubAdapter | null
  /** key → id → subscription */
  private readonly subscriptions = new Map<string, Map<string, PubSubSubscription>>()
  private readonly adapterKeys = new Set<string>()
  /** Per-key seq counter for in-memory mode. */
  private readonly keySeqs = new Map<string, number>()

  constructor(adapter?: PubSubAdapter) {
    this.adapter = adapter ?? null
  }

  subscribe(sub: PubSubSubscription): void | Promise<void> {
    let subs = this.subscriptions.get(sub.key)
    if (!subs) {
      subs = new Map()
      this.subscriptions.set(sub.key, subs)
    }
    subs.set(sub.id, sub)

    if (this.adapter && !this.adapterKeys.has(sub.key)) {
      this.adapterKeys.add(sub.key)
      return this.adapter.subscribe(sub.key, (envelope, info) => {
        this._onAdapterMessage(sub.key, envelope, info)
      })
    }
  }

  unsubscribe(id: string, key: string): void | Promise<void> {
    const subs = this.subscriptions.get(key)
    if (!subs) return
    subs.delete(id)
    if (subs.size === 0) {
      this.subscriptions.delete(key)
      if (this.adapter && this.adapterKeys.has(key)) {
        this.adapterKeys.delete(key)
        return this.adapter.unsubscribe(key)
      }
    }
  }

  publish(msg: PubSubPublish): PubSubPublishResult | Promise<PubSubPublishResult> {
    if (this.adapter) {
      return this._publishViaAdapter(msg.id, msg.key, msg.serialized)
    }
    return this._publishInMemory(msg.id, msg.key, msg.selfDelivery, msg.serialized)
  }

  private _publishInMemory(id: string, key: string, selfDelivery: boolean, serialized: string): PubSubPublishResult {
    const subs = this.subscriptions.get(key)
    const seq = (this.keySeqs.get(key) ?? 0) + 1
    this.keySeqs.set(key, seq)
    const ts = Date.now()
    let delivered = 0

    if (subs) {
      for (const [targetId, target] of subs) {
        if (targetId === id && !selfDelivery) continue
        delivered++
        target.onMessage(serialized, id, { seq, ts })
      }
    }

    return { seq, ts, meta: { delivered, transport: 'in-memory' } }
  }

  private _publishViaAdapter(
    id: string,
    key: string,
    serialized: string,
  ): PubSubPublishResult | Promise<PubSubPublishResult> {
    // Everything flows through the adapter — local delivery happens when the
    // message comes back via _onAdapterMessage, just like Cloudflare's authority path.
    // This ensures subscribers see messages in global seq order.
    const envelope = id + '\n' + serialized
    const result = this.adapter!.publish(key, envelope)

    if (isPromise(result)) {
      return result.then((r) => this._toResult(r))
    }
    return this._toResult(result)
  }

  private _toResult(result: PubSubAdapterResult): PubSubPublishResult {
    const seq = result.seq ?? 0
    const ts = result.ts ?? Date.now()
    assertUsage(Number.isFinite(seq) && Number.isFinite(ts), 'PubSubAdapter.publish() must return finite seq and ts')
    return { seq, ts, meta: result.meta }
  }

  private _onAdapterMessage(key: string, envelope: string, adapterInfo: WirePublishInfo): void {
    assertUsage(
      Number.isFinite(adapterInfo.seq) && Number.isFinite(adapterInfo.ts),
      'PubSubAdapter deliver callback must provide finite seq and ts',
    )
    const nl = envelope.indexOf('\n')
    const sourceChannelId = envelope.slice(0, nl)
    const serialized = envelope.slice(nl + 1)

    const subs = this.subscriptions.get(key)
    if (!subs) return
    for (const [targetId, target] of subs) {
      if (targetId === sourceChannelId && !target.selfDelivery) continue
      target.onMessage(serialized, sourceChannelId, adapterInfo)
    }
  }
}

// ---------------------------------------------------------------------------
// Global registry state
// ---------------------------------------------------------------------------

const globalObject = getGlobalObject('wire-protocol/server/pubsub.ts', {
  registry: new DefaultPubSubRegistry() as PubSubRegistry,
})

function getPubSubRegistry(): PubSubRegistry {
  return globalObject.registry
}

function setPubSubRegistry(registry: PubSubRegistry): void {
  globalObject.registry = registry
}

function setPubSubAdapter(adapter: PubSubAdapter): void {
  globalObject.registry = new DefaultPubSubRegistry(adapter)
}
