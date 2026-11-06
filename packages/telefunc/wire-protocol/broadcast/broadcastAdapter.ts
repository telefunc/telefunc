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
import { isCloudflareWorkers } from '../../utils/isCloudflareWorkers.js'
import { utf8ByteLength } from '../../utils/utf8ByteLength.js'
import { CHANNEL_HOLD_MAX_BYTES } from '../constants.js'
import { ChannelOverflowError } from '../channel/channelErrors.js'
import { HELD_SEND_OVERHEAD_BYTES } from '../channel/server/ServerChannelBuffer.js'
import type { WirePublishInfo } from '../wire/publishInfo.js'

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

/** After this many in-memory publishes delivered at once, those from the next microtask on wait for the next turn. */
const PUBLISHES_BETWEEN_TURNS = 16_384

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
  /** What `queued` holds: at most `CHANNEL_HOLD_MAX_BYTES` and one message, since a publish outside a listener is never refused. */
  private queuedBytes = 0
  private delivering = false
  private publishesSinceTurn = 0
  private holdingPublishes = false

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
          this.deliver(this.subscriptions, key, payload, info)
        }),
      )
    }

    return () => {
      const s = this.subscriptions.get(key)
      if (!s) return
      s.delete(onMessage)
      if (s.size === 0) {
        this.subscriptions.delete(key)
        this.releaseUnsub(this.transportUnsubs, key)
        this.releaseSeq(key)
      }
    }
  }

  publish(key: string, serialized: string): BroadcastPublishResult | Promise<BroadcastPublishResult> {
    if (this.transport) {
      const result = this.transport.send(key, serialized)
      if (isPromise(result)) return result
      return result
    }
    return this.publishInMemory(this.subscriptions, key, serialized)
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
          this.deliver(this.binarySubscriptions, key, payload, info)
        }),
      )
    }

    return () => {
      const s = this.binarySubscriptions.get(key)
      if (!s) return
      s.delete(onMessage)
      if (s.size === 0) {
        this.binarySubscriptions.delete(key)
        this.releaseUnsub(this.transportBinaryUnsubs, key)
        this.releaseSeq(key)
      }
    }
  }

  publishBinary(key: string, data: Uint8Array): BroadcastPublishResult | Promise<BroadcastPublishResult> {
    if (this.transport) {
      const result = this.transport.sendBinary(key, data)
      if (isPromise(result)) return result
      return result
    }
    return this.publishInMemory(this.binarySubscriptions, key, data)
  }

  // ── In-memory ──

  private publishInMemory<T extends string | Uint8Array>(
    subs: Map<string, Set<(data: T, info: WirePublishInfo) => void>>,
    key: string,
    data: T,
  ): BroadcastPublishResult | Promise<BroadcastPublishResult> {
    const timestamp = Date.now()
    // A publish from a listener reaches subscribers after the message it answers.
    if (this.delivering || this.holdingPublishes || this.queued.length > 0) {
      return this.queueInMemory(subs, key, data, timestamp)
    }
    // A loop delivered at once holds nothing, and only a publish left waiting ends a chain of microtasks. A Worker has
    // no process to starve, its CPU limit ending the request, and its timer would die with that request.
    if (++this.publishesSinceTurn === PUBLISHES_BETWEEN_TURNS && !isCloudflareWorkers()) {
      queueMicrotask(this.holdPublishes)
      // Now: a producer's next turn, which it schedules once its loop ends, finds the hold over.
      afterEventLoopTurn(this.drainQueued)
    }
    this.delivering = true
    try {
      return this.deliverInMemory(subs, key, data, timestamp)
    } finally {
      this.delivering = false
      if (this.queued.length > 0) this.drainQueued()
    }
  }

  private queueInMemory<T extends string | Uint8Array>(
    subs: Map<string, Set<(data: T, info: WirePublishInfo) => void>>,
    key: string,
    data: T,
    timestamp: number,
  ): Promise<BroadcastPublishResult> {
    const isText = typeof data === 'string'
    const size = (isText ? utf8ByteLength(data) : data.byteLength) + HELD_SEND_OVERHEAD_BYTES
    // One message alone goes through whatever its size.
    if (this.queuedBytes > 0 && this.queuedBytes + size > CHANNEL_HOLD_MAX_BYTES) {
      if (this.delivering) {
        return Promise.reject(
          new ChannelOverflowError(
            `Publish rejected: publishes waiting to be delivered exceeded ${CHANNEL_HOLD_MAX_BYTES} bytes; publish less from the listener`,
          ),
        )
      }
      // Outside a listener nothing rejects: what waits goes now.
      this.drainQueued()
    }
    this.queuedBytes += size
    // The caller may reuse or transfer its buffer once the publish returns.
    const queuedData = isText ? data : (new Uint8Array(data as Uint8Array) as T)
    return new Promise((resolve, reject) => {
      this.queued.push(() => {
        this.queuedBytes -= size
        try {
          resolve(this.deliverInMemory(subs, key, queuedData, timestamp))
        } catch (err) {
          reject(err)
        }
      })
    })
  }

  private deliverInMemory<T>(
    subs: Map<string, Set<(data: T, info: WirePublishInfo) => void>>,
    key: string,
    data: T,
    timestamp: number,
  ): BroadcastPublishResult {
    const seq = (this.keySeqs.get(key) ?? this.highestSeq) + 1
    if (seq > this.highestSeq) this.highestSeq = seq
    if (this.subscriptions.has(key) || this.binarySubscriptions.has(key)) this.keySeqs.set(key, seq)
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

  private readonly holdPublishes = (): void => {
    this.holdingPublishes = true
    this.publishesSinceTurn = 0
  }

  /** Delivers what waits now, whatever its size. What its listeners publish waits for the next turn, so a listener
   *  answering itself can't starve the event loop; on a Worker, whose timer would die with its request, it goes now. */
  private readonly drainQueued = (): void => {
    this.holdingPublishes = false
    do {
      for (let waiting = this.queued.length; waiting > 0; waiting--) {
        const deliver = this.queued.shift()!
        this.delivering = true
        deliver()
        this.delivering = false
      }
    } while (this.queued.length > 0 && isCloudflareWorkers())
    if (this.queued.length > 0) afterEventLoopTurn(this.drainQueued)
  }

  // ── Shared ──

  private deliver<T>(
    subs: Map<string, Set<(data: T, info: WirePublishInfo) => void>>,
    key: string,
    data: T,
    info: WirePublishInfo,
  ): void {
    const set = subs.get(key)
    if (!set) return
    for (const onMessage of set) onMessage(data, info)
  }

  private releaseSeq(key: string): void {
    if (!this.subscriptions.has(key) && !this.binarySubscriptions.has(key)) {
      this.keySeqs.delete(key)
    }
  }

  private releaseUnsub(map: Map<string, () => void>, key: string): void {
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
