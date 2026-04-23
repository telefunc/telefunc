export { getPubSubAdapter, setPubSubAdapter, DefaultPubSubAdapter }
export type {
  PubSubAdapter,
  PubSubTransport,
  PubSubUnsubscribe,
  PubSubPublishResult,
  PubSubOnMessage,
  PubSubBinaryOnMessage,
}

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

/**
 * Minimal interface for a pub/sub transport backend.
 *
 * Implement these 4 methods to get a full PubSubAdapter via `new DefaultPubSubAdapter(transport)`.
 * Subscriber multiplexing and lifecycle are handled for you.
 */
type PubSubTransport = {
  /** Send a text message. Must return the assigned seq and ts. */
  send(key: string, payload: string): { seq: number; ts: number } | Promise<{ seq: number; ts: number }>
  /** Listen for text messages on a key. Called at most once per key. Return an unsubscribe function. */
  listen(key: string, onMessage: (payload: string, info: { seq: number; ts: number }) => void): () => void
  /** Send a binary message. Must return the assigned seq and ts. */
  sendBinary(key: string, payload: Uint8Array): { seq: number; ts: number } | Promise<{ seq: number; ts: number }>
  /** Listen for binary messages on a key. Called at most once per key. Return an unsubscribe function. */
  listenBinary(key: string, onMessage: (payload: Uint8Array, info: { seq: number; ts: number }) => void): () => void
}

// ---------------------------------------------------------------------------
// Default adapter — subscriber multiplexer + in-memory fallback
// ---------------------------------------------------------------------------

class DefaultPubSubAdapter implements PubSubAdapter {
  private readonly transport: PubSubTransport | null
  private readonly subscriptions = new Map<string, Set<PubSubOnMessage>>()
  private readonly binarySubscriptions = new Map<string, Set<PubSubBinaryOnMessage>>()
  private readonly transportUnsubs = new Map<string, () => void>()
  private readonly transportBinaryUnsubs = new Map<string, () => void>()
  /** Per-key seq counter for in-memory mode. */
  private readonly keySeqs = new Map<string, number>()

  constructor(transport?: PubSubTransport) {
    this.transport = transport ?? null
  }

  subscribe(key: string, onMessage: PubSubOnMessage): PubSubUnsubscribe {
    let subs = this.subscriptions.get(key)
    if (!subs) {
      subs = new Set()
      this.subscriptions.set(key, subs)
    }
    subs.add(onMessage)

    if (this.transport && !this.transportUnsubs.has(key)) {
      this.transportUnsubs.set(
        key,
        this.transport.listen(key, (payload, info) => {
          this._deliver(this.subscriptions, key, payload, info)
        }),
      )
    }

    return () => {
      const s = this.subscriptions.get(key)
      if (!s) return
      s.delete(onMessage)
      if (s.size === 0) {
        this.subscriptions.delete(key)
        this._releaseUnsub(this.transportUnsubs, key)
      }
    }
  }

  publish(key: string, serialized: string): PubSubPublishResult | Promise<PubSubPublishResult> {
    if (this.transport) {
      const result = this.transport.send(key, serialized)
      if (isPromise(result)) return result
      return result
    }
    return this._publishInMemory(this.subscriptions, key, serialized)
  }

  subscribeBinary(key: string, onMessage: PubSubBinaryOnMessage): PubSubUnsubscribe {
    let subs = this.binarySubscriptions.get(key)
    if (!subs) {
      subs = new Set()
      this.binarySubscriptions.set(key, subs)
    }
    subs.add(onMessage)

    if (this.transport && !this.transportBinaryUnsubs.has(key)) {
      this.transportBinaryUnsubs.set(
        key,
        this.transport.listenBinary(key, (payload, info) => {
          this._deliver(this.binarySubscriptions, key, payload, info)
        }),
      )
    }

    return () => {
      const s = this.binarySubscriptions.get(key)
      if (!s) return
      s.delete(onMessage)
      if (s.size === 0) {
        this.binarySubscriptions.delete(key)
        this._releaseUnsub(this.transportBinaryUnsubs, key)
      }
    }
  }

  publishBinary(key: string, data: Uint8Array): PubSubPublishResult | Promise<PubSubPublishResult> {
    if (this.transport) {
      const result = this.transport.sendBinary(key, data)
      if (isPromise(result)) return result
      return result
    }
    return this._publishInMemory(this.binarySubscriptions, key, data)
  }

  // ── In-memory ──

  private _publishInMemory<T>(
    subs: Map<string, Set<(data: T, info: WirePublishInfo) => void>>,
    key: string,
    data: T,
  ): PubSubPublishResult {
    const seq = (this.keySeqs.get(key) ?? 0) + 1
    this.keySeqs.set(key, seq)
    const ts = Date.now()
    const info = { seq, ts }
    let delivered = 0
    const set = subs.get(key)
    if (set) {
      for (const onMessage of set) {
        delivered++
        onMessage(data, info)
      }
    }
    return { seq, ts, meta: { delivered, transport: 'in-memory' } }
  }

  // ── Shared ──

  private _deliver<T>(
    subs: Map<string, Set<(data: T, info: WirePublishInfo) => void>>,
    key: string,
    data: T,
    info: WirePublishInfo,
  ): void {
    const set = subs.get(key)
    if (!set) return
    for (const onMessage of set) onMessage(data, info)
  }

  private _releaseUnsub(map: Map<string, () => void>, key: string): void {
    const unsub = map.get(key)
    if (!unsub) return
    map.delete(key)
    unsub()
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
