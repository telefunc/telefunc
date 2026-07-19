export {
  getBroadcastAdapter,
  installBroadcastAdapter,
  _resetBroadcastAdapterForTesting,
  DefaultBroadcastAdapter,
  KV_KEEP,
}
export type {
  BroadcastAdapter,
  BroadcastTransport,
  BroadcastUnsubscribe,
  BroadcastPublishResult,
  BroadcastOnMessage,
  BroadcastBinaryOnMessage,
  KvMutate,
  KvReadOptions,
  KvWriteOptions,
  RoomFrameCommit,
  RoomFrameCommitResult,
}

import { assertUsage } from '../../utils/assert.js'
import { getGlobalObject } from '../../utils/getGlobalObject.js'
import { isPromise } from '../../utils/isPromise.js'
import { encodePublishText, type WirePublishInfo } from '../shared-ws.js'

/** Sentinel an `update` mutator returns to leave the key untouched (no write). Distinct from
 *  `null`, which deletes. `Symbol.for` (not `Symbol`) so every copy of this module shares one
 *  identity: a dev server loads two SSR module graphs, so the mutator (one graph) and the adapter
 *  that compares its result (the other, since the adapter is a cross-graph singleton) hold different
 *  bindings — a plain `Symbol` would mismatch, and the raw sentinel would be written into the store
 *  instead of kept, corrupting it. */
const KV_KEEP: unique symbol = Symbol.for('telefunc.kv.keep')

/** An `update` mutator: given the current raw value (`null` if absent), return the next raw value,
 *  `null` to delete, or `KV_KEEP` to leave it unchanged. Runs inside the store's linearization
 *  point, so no other writer to the key interleaves between the read it saw and the store — but it
 *  MUST be pure and re-runnable: a compare-and-set backend may invoke it more than once (retry on
 *  a concurrent write). It may throw to abort the update (the error propagates to the caller). */
type KvMutate = (current: string | null) => string | null | typeof KV_KEEP

/** `partitionKey` groups keys that must share a home so a backend can co-locate and linearize them.
 *  `Room` tags all of one room's keys with the room's control-lane key, so a sharded backend
 *  (Cloudflare) routes them to the single Durable Object that already sequences that room — same
 *  optimal location, one authority per room. Backends that don't shard (in-memory, Redis) ignore it.
 *
 *  `consistent: true` pins the operation to the strongly-consistent authority rather than an
 *  eventually-consistent read replica: a read observes the latest write (read-your-writes) and a
 *  write is kept off the replica. `Room` reads its directory state (config, roster, markers) from the
 *  replica — fast, global, and self-healing through the live event stream — and flags `consistent`
 *  only where staleness would break a guarantee: retained-message replay (its late-subscriber
 *  delivery needs read-your-writes) and the retained writes behind it. A single-tier backend
 *  (in-memory, Redis) is already strongly consistent and ignores it; only a two-tier backend
 *  (Cloudflare: Durable Object authority + Workers KV replica) acts on it. */
type KvReadOptions = { partitionKey?: string; consistent?: boolean }
type KvWriteOptions = { ttlMs?: number; partitionKey?: string; consistent?: boolean }

/** Transport-level publish result. `receivers` is the key's live subscription count at the
 *  transport hop — `0` means nobody anywhere is subscribed (see `ChannelPublishAck.receivers`);
 *  absent when the transport can't count. */
type BroadcastPublishResult = WirePublishInfo & { meta?: Record<string, unknown>; receivers?: number }

/** One atomic room-frame commit: the fused assign-order + retain + publish that gives a room's
 *  semantic timeline (`publish()` text and `Room.announce()`) one gap-free order, even across nodes.
 *  The adapter compares every fence (each `key` must still equal `expected`, else the caller's
 *  incarnation is stale), advances the clamped monotonic clock at `orderKey`, frames `payload` with
 *  the assigned order, optionally stores that frame at `retainKey` (MQTT-style retained), and
 *  publishes it on `channelKey` — the order rides the transport frame, so a receiver reads it there,
 *  never out of the payload. It all linearizes against `partitionKey` (a room's keys share one home)
 *  so no later commit overtakes. Every field is opaque to the adapter: it never parses the payload,
 *  a config, or an identity — only compares fences, advances a clock, and moves bytes. */
type RoomFrameCommit = {
  partitionKey: string
  fences: readonly { key: string; expected: string }[]
  orderKey: string
  orderTtlMs?: number
  channelKey: string
  payload: string
  retainKey?: string
}

/** The outcome of `commitFrame`: the assigned order and live receiver count on success, or a
 *  `stale-fence` refusal when a fence no longer holds (a closed or superseded incarnation) — on
 *  refusal nothing was ordered, retained, or published. */
type RoomFrameCommitResult =
  | { ok: true; seq: number; timestamp: number; receivers?: number }
  | { ok: false; reason: 'stale-fence' }

/** Advance a room's clamped monotonic clock. `seq` resets to 1 when wall time passes the last
 *  `timestamp`, else it clamps that timestamp and increments `seq`, so `(timestamp, seq)` is strictly
 *  increasing whatever the node's clock skew. The shared shape every `commitFrame` realizes — this
 *  one in-memory, the Redis Lua and the Cloudflare Durable Object each in their own runtime. */
function advanceRoomFrameOrder(prev: WirePublishInfo | null, now: number): WirePublishInfo {
  const base = prev?.timestamp ?? 0
  return now > base ? { seq: 1, timestamp: now } : { seq: (prev?.seq ?? 0) + 1, timestamp: base }
}

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
  /** KV — required by `Room`. Reads the value stored at `key`, or `null` if absent. */
  get?(key: string, options?: KvReadOptions): string | null | Promise<string | null>
  /** KV — required by `Room`. Stores `value` at `key` (upsert). `ttlMs` asks the store to
   *  expire the record on its own — the backstop that bounds crash leftovers even when no
   *  reader ever runs. Best-effort: stores without native expiry may ignore it. */
  set?(key: string, value: string, options?: KvWriteOptions): void | Promise<void>
  /** KV — required by `Room`. Removes the value stored at `key` (no-op if absent). */
  delete?(key: string, options?: KvReadOptions): void | Promise<void>
  /** KV — required by `Room`. Lists all stored keys starting with `prefix`. */
  keys?(prefix: string, options?: KvReadOptions): string[] | Promise<string[]>
  /** KV — required by `Room`. Atomic create-if-absent: stores `value` only if `key` is absent,
   *  returning `true` when this call wrote (won the create) and `false` when the key already held a
   *  value. The race-free primitive behind `Room.create`. */
  setIfAbsent?(key: string, value: string, options?: KvWriteOptions): boolean | Promise<boolean>
  /** KV — required by `Room`. Linearizable read-modify-write on one key: `mutate` sees the current
   *  value and returns the next (`null` deletes, `KV_KEEP` leaves it), with no other writer
   *  interleaving. Returns the stored value (`null` if deleted or kept-absent). The race-free
   *  primitive behind every room-state mutation (config, member meta, tracks, heartbeat). */
  update?(key: string, mutate: KvMutate, options?: KvWriteOptions): string | null | Promise<string | null>
  /** KV+pub/sub — required by `Room`. The fused, linearizable assign-order + retain + publish behind a
   *  room's semantic timeline (see `RoomFrameCommit`). One authority op so a later frame can't overtake
   *  and a crash can't leave the order advanced with nothing published. */
  commitFrame?(input: RoomFrameCommit): RoomFrameCommitResult | Promise<RoomFrameCommitResult>
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
  /** Optional KV — required by `Room` when a transport is installed. Reads the value at `key`, or `null`. */
  get?(key: string, options?: KvReadOptions): string | null | Promise<string | null>
  /** Optional KV — required by `Room` when a transport is installed. Stores `value` at `key`
   *  (upsert), expiring it after `ttlMs` when the backing store supports native expiry. */
  set?(key: string, value: string, options?: KvWriteOptions): void | Promise<void>
  /** Optional KV — required by `Room` when a transport is installed. Removes the value at `key`. */
  delete?(key: string, options?: KvReadOptions): void | Promise<void>
  /** Optional KV — required by `Room` when a transport is installed. Lists stored keys starting with `prefix`. */
  keys?(prefix: string, options?: KvReadOptions): string[] | Promise<string[]>
  /** Optional KV — atomic create-if-absent (see `BroadcastAdapter.setIfAbsent`). A transport that
   *  backs `Room` state across nodes must implement this for race-free room creation. */
  setIfAbsent?(key: string, value: string, options?: KvWriteOptions): boolean | Promise<boolean>
  /** Optional KV — linearizable read-modify-write (see `BroadcastAdapter.update`). A transport that
   *  backs `Room` state across nodes must implement this so concurrent writers converge without loss. */
  update?(key: string, mutate: KvMutate, options?: KvWriteOptions): string | null | Promise<string | null>
  /** Optional — the fused assign-order + retain + publish (see `BroadcastAdapter.commitFrame`). A
   *  transport that backs `Room` across nodes must implement this atomically (one Lua script, one
   *  Durable Object turn) so a room's timeline stays gap-free and ordered under concurrency; there is
   *  no safe way to compose it from separate KV+publish calls, so `Room` requires it rather than
   *  falling back. */
  commitFrame?(input: RoomFrameCommit): RoomFrameCommitResult | Promise<RoomFrameCommitResult>
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
  /** In-memory KV store, used when no transport is installed. Entries expire lazily on read. */
  private readonly kvStore = new Map<string, { value: string; expiresAt: number | null }>()

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

  // ── KV ──
  //
  // Without a transport, a plain Map is correct — a single isolate is the only
  // authority. With a transport, state must live where all nodes can see it, so
  // the transport has to bring its own KV; silently falling back to the local Map
  // would split room state across nodes.

  get(key: string, options?: KvReadOptions): string | null | Promise<string | null> {
    const transport = this.kvTransport('get')
    if (transport) return transport.get!(key, options)
    return this._readInMemory(key)
  }

  set(key: string, value: string, options?: KvWriteOptions): void | Promise<void> {
    const transport = this.kvTransport('set')
    if (transport) return transport.set!(key, value, options)
    this._writeInMemory(key, value, options)
  }

  delete(key: string, options?: KvReadOptions): void | Promise<void> {
    const transport = this.kvTransport('delete')
    if (transport) return transport.delete!(key, options)
    this.kvStore.delete(key)
  }

  keys(prefix: string, options?: KvReadOptions): string[] | Promise<string[]> {
    const transport = this.kvTransport('keys')
    if (transport) return transport.keys!(prefix, options)
    const now = Date.now()
    const result: string[] = []
    for (const [key, entry] of this.kvStore) {
      if (entry.expiresAt !== null && entry.expiresAt <= now) {
        this.kvStore.delete(key)
        continue
      }
      if (key.startsWith(prefix)) result.push(key)
    }
    return result
  }

  setIfAbsent(key: string, value: string, options?: KvWriteOptions): boolean | Promise<boolean> {
    const transport = this.kvTransport('setIfAbsent')
    if (transport) return transport.setIfAbsent!(key, value, options)
    // Single isolate: read-then-write with no await between is atomic.
    if (this._readInMemory(key) !== null) return false
    this._writeInMemory(key, value, options)
    return true
  }

  update(key: string, mutate: KvMutate, options?: KvWriteOptions): string | null | Promise<string | null> {
    const transport = this.kvTransport('update')
    if (transport) return transport.update!(key, mutate, options)
    // Single isolate: read-mutate-write with no await between is atomic, so `mutate` runs exactly once.
    const current = this._readInMemory(key)
    const next = mutate(current)
    if (next === KV_KEEP) return current
    if (next === null) {
      this.kvStore.delete(key)
      return null
    }
    this._writeInMemory(key, next, options)
    return next
  }

  commitFrame(input: RoomFrameCommit): RoomFrameCommitResult | Promise<RoomFrameCommitResult> {
    if (this.transport) {
      assertUsage(
        typeof this.transport.commitFrame === 'function',
        "The installed broadcast transport doesn't implement `commitFrame()`, the atomic assign-order + retain + publish `Room` needs to keep a room's timeline ordered across nodes. Implement it on the transport (one Lua script, one Durable Object turn) — there's no safe way to compose it from separate KV and publish calls.",
      )
      return this.transport.commitFrame!(input)
    }
    // In-memory: one isolate, no `await` between steps, so the whole commit is atomic and no later
    // frame overtakes — the fence check, clock advance, retain, and publish linearize for free.
    for (const { key, expected } of input.fences) {
      if (this._readInMemory(key) !== expected) return { ok: false, reason: 'stale-fence' }
    }
    const prev = this._readInMemory(input.orderKey)
    const ord = advanceRoomFrameOrder(prev === null ? null : (JSON.parse(prev) as WirePublishInfo), Date.now())
    this._writeInMemory(input.orderKey, JSON.stringify(ord), { ttlMs: input.orderTtlMs })
    // Retain the self-framed bytes (order prefix + payload), so a replay re-emits the message in its
    // real place in the order, not a fresh stamp — the room decodes it back on the retained read.
    if (input.retainKey !== undefined) this._writeInMemory(input.retainKey, encodePublishText(input.payload, ord))
    const set = this.subscriptions.get(input.channelKey)
    if (set) for (const onMessage of set) onMessage(input.payload, ord)
    return { ok: true, seq: ord.seq, timestamp: ord.timestamp, receivers: set?.size ?? 0 }
  }

  /** In-memory read with lazy expiry (used when no transport is installed). */
  private _readInMemory(key: string): string | null {
    const entry = this.kvStore.get(key)
    if (!entry) return null
    if (entry.expiresAt !== null && entry.expiresAt <= Date.now()) {
      this.kvStore.delete(key)
      return null
    }
    return entry.value
  }

  private _writeInMemory(key: string, value: string, options?: { ttlMs?: number }): void {
    this.kvStore.set(key, { value, expiresAt: options?.ttlMs === undefined ? null : Date.now() + options.ttlMs })
  }

  /** Resolve which KV backend to use: the transport's (multi-node) or the local Map (none). */
  private kvTransport(method: 'get' | 'set' | 'delete' | 'keys' | 'setIfAbsent' | 'update'): BroadcastTransport | null {
    if (!this.transport) return null
    assertUsage(
      typeof this.transport[method] === 'function',
      `The installed broadcast transport doesn't implement the KV method \`${method}()\` required by \`Room\` — implement \`get()\`, \`set()\`, \`delete()\`, \`keys()\`, \`setIfAbsent()\`, and \`update()\` on the transport, backed by a store all server instances can reach.`,
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
