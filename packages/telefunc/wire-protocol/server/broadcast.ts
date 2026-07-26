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
import type { WirePublishInfo } from '../shared-ws.js'

/** Transport-level publish result. `receivers` is the key's live subscription count at the
 *  transport hop — `0` means nobody anywhere is subscribed (see `ChannelPublishAck.receivers`);
 *  absent when the transport can't count. */
type BroadcastPublishResult = WirePublishInfo & { meta?: Record<string, unknown>; receivers?: number }

/** Callback for delivering a broadcast message to a subscriber. */
type BroadcastOnMessage = (serialized: string, info: WirePublishInfo) => void

type BroadcastBinaryOnMessage = (data: Uint8Array, info: WirePublishInfo) => void

/** Call to unsubscribe. `ready`, when present, resolves once the subscription is actually live at the
 *  backend — the seam a retained replay awaits so a publish racing a just-issued subscribe can't slip
 *  through the gap between subscribing and reading the retained value. A synchronous backend (in-memory)
 *  leaves it absent: the subscription is live the instant `subscribe` returns. `ready` never rejects — a
 *  backend that can't confirm resolves it anyway (degraded; its own reconnect re-establishes the sub). */
type BroadcastUnsubscribe = (() => void) & { ready?: Promise<void> }

type BroadcastAdapter = {
  subscribe(key: string, onMessage: BroadcastOnMessage): BroadcastUnsubscribe
  publish(key: string, serialized: string): BroadcastPublishResult | Promise<BroadcastPublishResult>
  subscribeBinary(key: string, onMessage: BroadcastBinaryOnMessage): BroadcastUnsubscribe
  publishBinary(key: string, data: Uint8Array): BroadcastPublishResult | Promise<BroadcastPublishResult>
}

/**
 * Minimal interface for a broadcast transport backend.
 *
 * Implement these 4 methods to get a full BroadcastAdapter via `new DefaultBroadcastAdapter(transport)`.
 * Subscriber multiplexing and lifecycle are handled for you.
 */
type TransportSendResult = { seq: number; timestamp: number; receivers?: number }

type BroadcastTransport = {
  /** Send a text message. Must return the assigned seq and timestamp; report `receivers` (the
   *  key's subscriber count) when the backend can count — it powers pause-at-0 publishers. */
  send(key: string, payload: string): TransportSendResult | Promise<TransportSendResult>
  /** Listen for text messages on a key. Called at most once per key. Return an unsubscribe function;
   *  when the backend subscribe is asynchronous, attach `.ready` (see `BroadcastUnsubscribe`) so a
   *  consumer can await live-ness before reading retained state. */
  listen(
    key: string,
    onMessage: (payload: string, info: { seq: number; timestamp: number }) => void,
  ): BroadcastUnsubscribe
  /** Send a binary message. Same contract as `send`. */
  sendBinary(key: string, payload: Uint8Array): TransportSendResult | Promise<TransportSendResult>
  /** Listen for binary messages on a key. Called at most once per key. Return an unsubscribe function;
   *  attach `.ready` like `listen`. */
  listenBinary(
    key: string,
    onMessage: (payload: Uint8Array, info: { seq: number; timestamp: number }) => void,
  ): BroadcastUnsubscribe
}

// ---------------------------------------------------------------------------
// Default adapter — subscriber multiplexer + in-memory fallback
// ---------------------------------------------------------------------------

class DefaultBroadcastAdapter implements BroadcastAdapter {
  private readonly transport: BroadcastTransport | null
  private readonly subscriptions = new Map<string, Set<BroadcastOnMessage>>()
  private readonly binarySubscriptions = new Map<string, Set<BroadcastBinaryOnMessage>>()
  private readonly transportUnsubs = new Map<string, BroadcastUnsubscribe>()
  private readonly transportBinaryUnsubs = new Map<string, BroadcastUnsubscribe>()
  /** Per-key seq counter for in-memory mode. */
  private readonly keySeqs = new Map<string, number>()

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

    const unsub: BroadcastUnsubscribe = () => {
      const s = this.subscriptions.get(key)
      if (!s) return
      s.delete(onMessage)
      if (s.size === 0) {
        this.subscriptions.delete(key)
        this._releaseUnsub(this.transportUnsubs, key)
      }
    }
    // Every subscriber to a key shares the one transport listen, so it shares that listen's readiness:
    // an async backend surfaces it here; in-memory leaves it absent (the sub is live on return).
    unsub.ready = this.transportUnsubs.get(key)?.ready
    return unsub
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

    const unsub: BroadcastUnsubscribe = () => {
      const s = this.binarySubscriptions.get(key)
      if (!s) return
      s.delete(onMessage)
      if (s.size === 0) {
        this.binarySubscriptions.delete(key)
        this._releaseUnsub(this.transportBinaryUnsubs, key)
      }
    }
    unsub.ready = this.transportBinaryUnsubs.get(key)?.ready
    return unsub
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
  ): BroadcastPublishResult {
    const seq = (this.keySeqs.get(key) ?? 0) + 1
    this.keySeqs.set(key, seq)
    const timestamp = Date.now()
    const info = { seq, timestamp }
    const set = subs.get(key)
    if (set) for (const onMessage of set) onMessage(data, info)
    return { seq, timestamp, receivers: set?.size ?? 0, meta: { transport: 'in-memory' } }
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

  private _releaseUnsub(map: Map<string, BroadcastUnsubscribe>, key: string): void {
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
