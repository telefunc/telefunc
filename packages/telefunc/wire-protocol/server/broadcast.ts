export { getBroadcastAdapter, installBroadcastAdapter, _resetBroadcastAdapterForTesting, DefaultBroadcastAdapter }
export type {
  BroadcastAdapter,
  BroadcastTransport,
  BroadcastUnsubscribe,
  BroadcastPublishResult,
  BroadcastOnMessage,
  BroadcastBinaryOnMessage,
}

import { assertUsage } from '../../utils/assert.js'
import { getGlobalObject } from '../../utils/getGlobalObject.js'
import { isPromise } from '../../utils/isPromise.js'
import type { WirePublishInfo } from '../shared-ws.js'

/** Transport-level publish result. */
type BroadcastPublishResult = WirePublishInfo & { meta?: Record<string, unknown> }

/** Callback for delivering a broadcast message to a subscriber. */
type BroadcastOnMessage = (serialized: string, info: WirePublishInfo) => void

type BroadcastBinaryOnMessage = (data: Uint8Array, info: WirePublishInfo) => void

type BroadcastUnsubscribe = () => void

type BroadcastAdapter = {
  subscribe(key: string, onMessage: BroadcastOnMessage): BroadcastUnsubscribe
  publish(key: string, serialized: string): BroadcastPublishResult | Promise<BroadcastPublishResult>
  subscribeBinary(key: string, onMessage: BroadcastBinaryOnMessage): BroadcastUnsubscribe
  publishBinary(key: string, data: Uint8Array): BroadcastPublishResult | Promise<BroadcastPublishResult>
  /** KV — required by `Room`. Reads the value stored at `key`, or `null` if absent. */
  get?(key: string): string | null | Promise<string | null>
  /** KV — required by `Room`. Stores `value` at `key` (upsert). */
  set?(key: string, value: string): void | Promise<void>
  /** KV — required by `Room`. Removes the value stored at `key` (no-op if absent). */
  delete?(key: string): void | Promise<void>
  /** KV — required by `Room`. Lists all stored keys starting with `prefix`. */
  keys?(prefix: string): string[] | Promise<string[]>
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
  /** Optional KV — required by `Room` when a transport is installed. Reads the value at `key`, or `null`. */
  get?(key: string): string | null | Promise<string | null>
  /** Optional KV — required by `Room` when a transport is installed. Stores `value` at `key` (upsert). */
  set?(key: string, value: string): void | Promise<void>
  /** Optional KV — required by `Room` when a transport is installed. Removes the value at `key`. */
  delete?(key: string): void | Promise<void>
  /** Optional KV — required by `Room` when a transport is installed. Lists stored keys starting with `prefix`. */
  keys?(prefix: string): string[] | Promise<string[]>
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
  /** Per-key seq counter for in-memory mode. */
  private readonly keySeqs = new Map<string, number>()
  /** In-memory KV store, used when no transport is installed. */
  private readonly kvStore = new Map<string, string>()

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

  // ── KV ──
  //
  // Without a transport, a plain Map is correct — a single isolate is the only
  // authority. With a transport, state must live where all nodes can see it, so
  // the transport has to bring its own KV; silently falling back to the local Map
  // would split room state across nodes.

  get(key: string): string | null | Promise<string | null> {
    const transport = this.kvTransport('get')
    if (transport) return transport.get!(key)
    return this.kvStore.get(key) ?? null
  }

  set(key: string, value: string): void | Promise<void> {
    const transport = this.kvTransport('set')
    if (transport) return transport.set!(key, value)
    this.kvStore.set(key, value)
  }

  delete(key: string): void | Promise<void> {
    const transport = this.kvTransport('delete')
    if (transport) return transport.delete!(key)
    this.kvStore.delete(key)
  }

  keys(prefix: string): string[] | Promise<string[]> {
    const transport = this.kvTransport('keys')
    if (transport) return transport.keys!(prefix)
    const result: string[] = []
    for (const key of this.kvStore.keys()) {
      if (key.startsWith(prefix)) result.push(key)
    }
    return result
  }

  /** Resolve which KV backend to use: the transport's (multi-node) or the local Map (none). */
  private kvTransport(method: 'get' | 'set' | 'delete' | 'keys'): BroadcastTransport | null {
    if (!this.transport) return null
    assertUsage(
      typeof this.transport[method] === 'function',
      `The installed broadcast transport doesn't implement the KV method \`${method}()\` required by \`Room\` — implement \`get()\`, \`set()\`, \`delete()\`, and \`keys()\` on the transport, backed by a store all server instances can reach.`,
    )
    return this.transport
  }

  // ── In-memory ──

  private _publishInMemory<T>(
    subs: Map<string, Set<(data: T, info: WirePublishInfo) => void>>,
    key: string,
    data: T,
  ): BroadcastPublishResult {
    const seq = (this.keySeqs.get(key) ?? 0) + 1
    this.keySeqs.set(key, seq)
    const timestamp = Date.now()
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

const globalObject = getGlobalObject<{
  adapter: BroadcastAdapter
  installed: boolean
}>('wire-protocol/server/broadcast.ts', () => ({
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
