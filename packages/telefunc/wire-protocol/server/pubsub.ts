export { getPubSubAdapter, setPubSubAdapter, DefaultPubSubAdapter }
export type { PubSubAdapter, PubSubUnsubscribe, PubSubPublishResult, PubSubOnMessage, PubSubBinaryOnMessage }

import { assertUsage } from '../../utils/assert.js'
import { getGlobalObject } from '../../utils/getGlobalObject.js'
import { isPromise } from '../../utils/isPromise.js'
import type { WirePublishInfo } from '../shared-ws.js'

/** Transport-level publish result. */
type PubSubPublishResult = WirePublishInfo & { meta?: Record<string, unknown> }

/** Callback for delivering a pub/sub message to a subscriber. */
type PubSubOnMessage = (serialized: string, info: WirePublishInfo) => void

type PubSubBinaryOnMessage = (data: Uint8Array, info: WirePublishInfo) => void

type PubSubUnsubscribe = () => void

type PubSubAdapter = {
  subscribe(key: string, onMessage: PubSubOnMessage): PubSubUnsubscribe
  publish(key: string, serialized: string): PubSubPublishResult | Promise<PubSubPublishResult>
  subscribeBinary(key: string, onMessage: PubSubBinaryOnMessage): PubSubUnsubscribe
  publishBinary(key: string, data: Uint8Array): PubSubPublishResult | Promise<PubSubPublishResult>
}

// ---------------------------------------------------------------------------
// Default adapter — handles in-memory + wrapped (user-provided adapter)
// ---------------------------------------------------------------------------

class DefaultPubSubAdapter implements PubSubAdapter {
  private readonly wrappedAdapter: PubSubAdapter | null
  private readonly subscriptions = new Map<string, Set<PubSubOnMessage>>()
  private readonly binarySubscriptions = new Map<string, Set<PubSubBinaryOnMessage>>()
  private readonly adapterUnsubs = new Map<string, () => void>()
  private readonly adapterBinaryUnsubs = new Map<string, () => void>()
  /** Per-key seq counter for in-memory mode. */
  private readonly keySeqs = new Map<string, number>()

  constructor(adapter?: PubSubAdapter) {
    this.wrappedAdapter = adapter ?? null
  }

  subscribe(key: string, onMessage: PubSubOnMessage): PubSubUnsubscribe {
    let subs = this.subscriptions.get(key)
    if (!subs) {
      subs = new Set()
      this.subscriptions.set(key, subs)
    }
    subs.add(onMessage)

    if (this.wrappedAdapter && !this.adapterUnsubs.has(key)) {
      this.adapterUnsubs.set(
        key,
        this.wrappedAdapter.subscribe(key, (message, info) => {
          this._onAdapterMessage(key, message, info)
        }),
      )
    }

    return () => {
      const s = this.subscriptions.get(key)
      if (!s) return
      s.delete(onMessage)
      if (s.size === 0) {
        this.subscriptions.delete(key)
        return this._resolveAdapterUnsub(this.adapterUnsubs, key)
      }
    }
  }

  private _resolveAdapterUnsub(map: Map<string, () => void>, key: string): void {
    const unsub = map.get(key)
    if (!unsub) return
    map.delete(key)
    unsub()
  }

  publish(key: string, serialized: string): PubSubPublishResult | Promise<PubSubPublishResult> {
    if (this.wrappedAdapter) {
      return this._publishViaAdapter(key, serialized)
    }
    return this._publishInMemory(key, serialized)
  }

  private _publishInMemory(key: string, serialized: string): PubSubPublishResult {
    const subs = this.subscriptions.get(key)
    const seq = (this.keySeqs.get(key) ?? 0) + 1
    this.keySeqs.set(key, seq)
    const ts = Date.now()
    let delivered = 0

    if (subs) {
      for (const onMessage of subs.values()) {
        delivered++
        onMessage(serialized, { seq, ts })
      }
    }

    return { seq, ts, meta: { delivered, transport: 'in-memory' } }
  }

  private _publishViaAdapter(key: string, serialized: string): PubSubPublishResult | Promise<PubSubPublishResult> {
    const result = this.wrappedAdapter!.publish(key, serialized)
    if (isPromise(result)) return result.then((r) => this._toResult(r))
    return this._toResult(result)
  }

  private _toResult(result: PubSubPublishResult): PubSubPublishResult {
    const seq = result.seq ?? 0
    const ts = result.ts ?? Date.now()
    assertUsage(Number.isFinite(seq) && Number.isFinite(ts), 'PubSubAdapter.publish() must return finite seq and ts')
    return { seq, ts, meta: result.meta }
  }

  private _onAdapterMessage(key: string, serialized: string, adapterInfo: WirePublishInfo): void {
    assertUsage(
      Number.isFinite(adapterInfo.seq) && Number.isFinite(adapterInfo.ts),
      'PubSubAdapter deliver callback must provide finite seq and ts',
    )
    const subs = this.subscriptions.get(key)
    if (!subs) return
    for (const onMessage of subs.values()) {
      onMessage(serialized, adapterInfo)
    }
  }

  // ── Binary pub/sub ──

  subscribeBinary(key: string, onMessage: PubSubBinaryOnMessage): PubSubUnsubscribe {
    let subs = this.binarySubscriptions.get(key)
    if (!subs) {
      subs = new Set()
      this.binarySubscriptions.set(key, subs)
    }
    subs.add(onMessage)

    if (this.wrappedAdapter && !this.adapterBinaryUnsubs.has(key)) {
      this.adapterBinaryUnsubs.set(
        key,
        this.wrappedAdapter.subscribeBinary(key, (data, info) => {
          this._onAdapterBinaryMessage(key, data, info)
        }),
      )
    }

    return () => {
      const s = this.binarySubscriptions.get(key)
      if (!s) return
      s.delete(onMessage)
      if (s.size === 0) {
        this.binarySubscriptions.delete(key)
        return this._resolveAdapterUnsub(this.adapterBinaryUnsubs, key)
      }
    }
  }

  publishBinary(key: string, data: Uint8Array): PubSubPublishResult | Promise<PubSubPublishResult> {
    if (this.wrappedAdapter) {
      return this._publishBinaryViaAdapter(key, data)
    }
    return this._publishBinaryInMemory(key, data)
  }

  private _publishBinaryInMemory(key: string, data: Uint8Array): PubSubPublishResult {
    const subs = this.binarySubscriptions.get(key)
    const seq = (this.keySeqs.get(key) ?? 0) + 1
    this.keySeqs.set(key, seq)
    const ts = Date.now()
    let delivered = 0

    if (subs) {
      for (const onMessage of subs.values()) {
        delivered++
        onMessage(data, { seq, ts })
      }
    }

    return { seq, ts, meta: { delivered, transport: 'in-memory' } }
  }

  private _publishBinaryViaAdapter(key: string, data: Uint8Array): PubSubPublishResult | Promise<PubSubPublishResult> {
    const result = this.wrappedAdapter!.publishBinary(key, data)
    if (isPromise(result)) return result.then((r) => this._toResult(r))
    return this._toResult(result)
  }

  private _onAdapterBinaryMessage(key: string, data: Uint8Array, adapterInfo: WirePublishInfo): void {
    assertUsage(
      Number.isFinite(adapterInfo.seq) && Number.isFinite(adapterInfo.ts),
      'PubSubAdapter deliver callback must provide finite seq and ts',
    )
    const subs = this.binarySubscriptions.get(key)
    if (!subs) return
    for (const onMessage of subs.values()) {
      onMessage(data, adapterInfo)
    }
  }
}

// ---------------------------------------------------------------------------
// Global adapter state
// ---------------------------------------------------------------------------

const globalObject = getGlobalObject('wire-protocol/server/pubsub.ts', {
  adapter: new DefaultPubSubAdapter() as PubSubAdapter,
})

function getPubSubAdapter(): PubSubAdapter {
  return globalObject.adapter
}

function setPubSubAdapter(adapter: PubSubAdapter): void {
  globalObject.adapter = adapter
}
