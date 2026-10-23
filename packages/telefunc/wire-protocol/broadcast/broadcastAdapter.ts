export { getBroadcastAdapter, installBroadcastAdapter, _resetBroadcastAdapterForTesting, DefaultBroadcastAdapter }
export type {
  BroadcastAdapter,
  BroadcastTransport,
  BroadcastUnsubscribe,
  BroadcastPublishResult,
  BroadcastOnMessage,
  BroadcastBinaryOnMessage,
}

import { getGlobalObject } from '../../utils/getGlobalObject.js'
import { isPromise } from '../../utils/isPromise.js'
import { Fifo } from '../../utils/Fifo.js'
import type { WirePublishInfo } from '../wire/connectionFrame.js'

/** Most in-memory deliveries that run before the event loop gets a turn, so listeners answering each other can't starve it. */
const IN_MEMORY_DELIVERY_BURST = 1024

/** Transport-level publish result. */
type BroadcastPublishResult = WirePublishInfo & { meta?: Record<string, unknown> }

/** Callback for delivering a broadcast message to a subscriber. */
type BroadcastOnMessage = (serialized: string, info: WirePublishInfo) => void

type BroadcastBinaryOnMessage = (data: Uint8Array, info: WirePublishInfo) => void

type BroadcastUnsubscribe = () => void

/** `wireRequest`: the request that opened the wire a subscribing page's messages go out on, where one did. */
type BroadcastAdapter = {
  subscribe(key: string, onMessage: BroadcastOnMessage, wireRequest?: Request): BroadcastUnsubscribe
  publish(key: string, serialized: string): BroadcastPublishResult | Promise<BroadcastPublishResult>
  subscribeBinary(key: string, onMessage: BroadcastBinaryOnMessage, wireRequest?: Request): BroadcastUnsubscribe
  publishBinary(key: string, data: Uint8Array): BroadcastPublishResult | Promise<BroadcastPublishResult>
}

/**
 * Minimal interface for a broadcast transport backend.
 *
 * Implement these 4 methods to get a full BroadcastAdapter via `new DefaultBroadcastAdapter(transport)`.
 * Subscriber multiplexing and lifecycle are handled for you.
 */
type BroadcastTransport = {
  /** Send a text message. Must return the assigned seq and timestamp. */
  send(key: string, payload: string): { seq: number; timestamp: number } | Promise<{ seq: number; timestamp: number }>
  /** Listen for text messages on a key. Called at most once per key. Return an unsubscribe function. */
  listen(key: string, onMessage: (payload: string, info: { seq: number; timestamp: number }) => void): () => void
  /** Send a binary message. Must return the assigned seq and timestamp. */
  sendBinary(
    key: string,
    payload: Uint8Array,
  ): { seq: number; timestamp: number } | Promise<{ seq: number; timestamp: number }>
  /** Listen for binary messages on a key. Called at most once per key. Return an unsubscribe function. */
  listenBinary(
    key: string,
    onMessage: (payload: Uint8Array, info: { seq: number; timestamp: number }) => void,
  ): () => void
}

// ---------------------------------------------------------------------------
// Default adapter — subscriber multiplexer + in-memory fallback
// ---------------------------------------------------------------------------

class DefaultBroadcastAdapter implements BroadcastAdapter {
  private readonly transport: BroadcastTransport | null
  private readonly subscriptions = new Map<string, Set<BroadcastOnMessage>>()
  private readonly binarySubscriptions = new Map<string, Set<BroadcastBinaryOnMessage>>()
  private readonly transportUnsubs = new Map<string, () => void>()
  private readonly transportBinaryUnsubs = new Map<string, () => void>()
  /** Per-key seq counter for in-memory mode, while the key has a subscriber. */
  private readonly keySeqs = new Map<string, number>()
  /** The highest seq issued on any key: a key without a counter goes on above it. */
  private highestSeq = 0
  /** In-memory publishes waiting for the ones before them, in publish order. */
  private readonly queued = new Fifo<() => void>()
  private delivering = false
  /** Reset by a macrotask scheduled once half the burst has run: one every turn made each publish 10% slower. */
  private deliveredInBurst = 0

  constructor(transport?: BroadcastTransport) {
    this.transport = transport ?? null
  }

  subscribe(key: string, onMessage: BroadcastOnMessage): BroadcastUnsubscribe {
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
        this._releaseSeq(key)
      }
    }
  }

  publish(key: string, serialized: string): BroadcastPublishResult | Promise<BroadcastPublishResult> {
    if (this.transport) {
      const result = this.transport.send(key, serialized)
      if (isPromise(result)) return result
      return result
    }
    return this._publishInMemory(this.subscriptions, key, serialized)
  }

  subscribeBinary(key: string, onMessage: BroadcastBinaryOnMessage): BroadcastUnsubscribe {
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
        this._releaseSeq(key)
      }
    }
  }

  publishBinary(key: string, data: Uint8Array): BroadcastPublishResult | Promise<BroadcastPublishResult> {
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
  ): BroadcastPublishResult | Promise<BroadcastPublishResult> {
    const seq = (this.keySeqs.get(key) ?? this.highestSeq) + 1
    if (seq > this.highestSeq) this.highestSeq = seq
    if (this.subscriptions.has(key) || this.binarySubscriptions.has(key)) this.keySeqs.set(key, seq)
    const timestamp = Date.now()
    // A publish from a listener reaches subscribers after the message it answers; one past the burst, after the turn.
    if (this.delivering || this.queued.length > 0 || this.deliveredInBurst >= IN_MEMORY_DELIVERY_BURST) {
      // The caller may reuse or transfer its buffer once the publish returns.
      const queuedData = data instanceof Uint8Array ? (new Uint8Array(data) as T) : data
      return new Promise((resolve, reject) => {
        this.queued.push(() => {
          try {
            resolve(this._deliverInMemory(subs, key, queuedData, seq, timestamp))
          } catch (err) {
            reject(err)
          }
        })
      })
    }
    this.delivering = true
    try {
      return this._deliverInMemory(subs, key, data, seq, timestamp)
    } finally {
      this.delivering = false
      if (this.queued.length > 0) this._drainQueued()
    }
  }

  private _deliverInMemory<T>(
    subs: Map<string, Set<(data: T, info: WirePublishInfo) => void>>,
    key: string,
    data: T,
    seq: number,
    timestamp: number,
  ): BroadcastPublishResult {
    if (++this.deliveredInBurst === IN_MEMORY_DELIVERY_BURST / 2) {
      afterEventLoopTurn(() => {
        this.deliveredInBurst = 0
        this._drainQueued()
      })
    }
    const info = { seq, timestamp }
    let delivered = 0
    const set = subs.get(key)
    if (set) {
      for (const onMessage of set) {
        delivered++
        onMessage(data, info)
      }
    }
    return { seq, timestamp, meta: { delivered, transport: 'in-memory' } }
  }

  private _drainQueued(): void {
    while (this.queued.length > 0 && this.deliveredInBurst < IN_MEMORY_DELIVERY_BURST) {
      const deliver = this.queued.shift()!
      this.delivering = true
      deliver()
      this.delivering = false
    }
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

  private _releaseSeq(key: string): void {
    if (!this.subscriptions.has(key) && !this.binarySubscriptions.has(key)) this.keySeqs.delete(key)
  }

  private _releaseUnsub(map: Map<string, () => void>, key: string): void {
    const unsub = map.get(key)
    if (!unsub) return
    map.delete(key)
    unsub()
  }
}

// setImmediate runs after pending I/O, where it exists.
function afterEventLoopTurn(fn: () => void): void {
  if (typeof setImmediate === 'function') setImmediate(fn)
  else setTimeout(fn, 0)
}

// ---------------------------------------------------------------------------
// Global adapter state
// ---------------------------------------------------------------------------

const globalObject = getGlobalObject<{
  adapter: BroadcastAdapter
  installed: boolean
}>('wire-protocol/broadcast/broadcastAdapter.ts', () => ({
  adapter: new DefaultBroadcastAdapter(),
  installed: false,
}))

function getBroadcastAdapter(): BroadcastAdapter {
  return globalObject.adapter
}

/** Per-isolate singleton. The factory runs once: the first caller installs the
 *  adapter, subsequent callers receive the already-installed one. This handles
 *  Vite HMR reloads and bundler quirks that evaluate the user's entry
 *  module more than once in the same isolate. Returns the installed instance so the caller
 *  can keep using the canonical reference. */
function installBroadcastAdapter<T extends BroadcastAdapter>(factory: () => T): T {
  if (!globalObject.installed) {
    globalObject.adapter = factory()
    globalObject.installed = true
  }
  return globalObject.adapter as T
}

/** @internal — test-only escape hatch. */
function _resetBroadcastAdapterForTesting(adapter: BroadcastAdapter): void {
  globalObject.adapter = adapter
  globalObject.installed = true
}
