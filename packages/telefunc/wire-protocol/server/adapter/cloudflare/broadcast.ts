/// <reference types="@cloudflare/workers-types" />
export { CloudflareBroadcastTransport, CloudflareBroadcastAuthorityState }
export type { BroadcastDeliverRequest, BroadcastPublishRequest, TelefuncDurableObjectStub, RoomFrameCommit }

import { CHANNEL_BUFFER_LIMIT_BYTES } from '../../../constants.js'
import { KNOWN_BROADCAST_BUCKETS, getBucketCoordinatorShardIndices, getDeterministicKeyBucketIndex } from './routing.js'
import { assert, assertUsage } from '../../../../utils/assert.js'
import { utf8ByteLength } from '../../../../utils/utf8ByteLength.js'
import { KV_KEEP } from '../../broadcast.js'
import type {
  BroadcastBinaryOnMessage,
  BroadcastOnMessage,
  BroadcastPublishResult,
  BroadcastAdapter,
  BroadcastUnsubscribe,
  KvMutate,
  KvReadOptions,
  KvWriteOptions,
  RoomFrameCommit,
  RoomFrameCommitResult,
} from '../../broadcast.js'
import { encodePublishText, type WirePublishInfo } from '../../../shared-ws.js'
import type { CloudflareScale, LocationBucket } from './routing.js'

const PRESENCE_TTL_SECONDS = 90
const PRESENCE_REFRESH_INTERVAL_MS = 30_000
/** Namespace of the room-state read replica within the Workers KV binding. Directory reads (`Room.get`,
 *  `Room.list`) and the cross-room room index are served from here — Workers KV is globally distributed,
 *  so these reads are fast from any region and enumeration is a plain KV list. The strongly-consistent
 *  copy lives in each room's authority Durable Object (see `CloudflareBroadcastAuthorityState`), which
 *  mirrors replicated writes through to this replica; reads that need read-your-writes (`consistent`)
 *  bypass it and hit the authority. */
const KV_STORE_PREFIX = 'tfkv:'

/** Workers KV expirations are whole seconds with a 60s floor — round up, never down, so a record never
 *  outlives its TTL by rounding yet never expires early. */
function replicaKvTtl(ttlMs: number | undefined): { expirationTtl: number } | undefined {
  return ttlMs === undefined ? undefined : { expirationTtl: Math.max(60, Math.ceil(ttlMs / 1000)) }
}

/** Read-replica mutators over the Workers KV namespace, shared by the transport (cross-room index) and
 *  the authority (write-through mirror of a room's replicated state) so both speak one key scheme. */
function replicaKvPut(kv: KVNamespace, key: string, value: string, ttlMs?: number): Promise<void> {
  return kv.put(KV_STORE_PREFIX + key, value, replicaKvTtl(ttlMs))
}

function replicaKvDelete(kv: KVNamespace, key: string): Promise<void> {
  return kv.delete(KV_STORE_PREFIX + key)
}

function replicaKvGet(kv: KVNamespace, key: string): Promise<string | null> {
  return kv.get(KV_STORE_PREFIX + key)
}

async function replicaKvList(kv: KVNamespace, prefix: string): Promise<string[]> {
  const fullPrefix = KV_STORE_PREFIX + prefix
  const keys: string[] = []
  let cursor: string | undefined
  do {
    const list = await kv.list({ prefix: fullPrefix, cursor })
    for (const entry of list.keys) keys.push(entry.name.slice(KV_STORE_PREFIX.length))
    cursor = list.list_complete ? undefined : list.cursor
  } while (cursor)
  return keys
}

/** DO-storage prefix for room-state entries, isolating them from the authority's broadcast bookkeeping
 *  (`broadcast:*`) in the same Durable Object. */
const ROOM_STATE_STORAGE_PREFIX = 'rs:'
/** How often a room-state authority sweeps expired entries. DO storage has no native TTL, so this
 *  alarm does what Workers KV expiry did before — bounding a crashed owner's leftover member records. */
const ROOM_STATE_REAP_INTERVAL_MS = 60_000

/** One room-state entry in DO storage: the raw value and its absolute expiry (`null` = never). */
type RoomStateEntry = { v: string; e: number | null }

/** Unwrap Cloudflare DO RPC proxy into a plain object.
 *  RPC properties are lazy stubs that must be awaited to resolve their values. */
async function unwrapRpcResult(rpc: Promise<BroadcastPublishResult>): Promise<BroadcastPublishResult> {
  const r = await rpc
  // `receivers` (the authority's live subscriber count) rides back like `meta` — without this it
  // was dropped, blinding `publishBinary().receivers` / `onDemand` on Cloudflare.
  const [seq, timestamp, meta, receivers] = await Promise.all([r.seq, r.timestamp, r.meta, r.receivers])
  return {
    seq,
    timestamp,
    ...(meta ? { meta } : undefined),
    ...(receivers === undefined ? undefined : { receivers }),
  }
}

type BroadcastPublishRequest = {
  key: string
  locationBucket: LocationBucket
  forwarded?: boolean
  doNames?: string[]
  info?: WirePublishInfo
  serialized?: string
  binaryData?: Uint8Array
}

type BroadcastDeliverRequest = {
  key: string
  info: WirePublishInfo
  serialized?: string
  binaryData?: Uint8Array
}

/** A stub to a Telefunc Durable Object. One class implements every RPC surface — broadcast fan-out and,
 *  for a room's authority, its room-state store — so one stub type carries both (`serve/cloudflare.ts`). */
type TelefuncDurableObjectStub = DurableObjectStub & {
  telefuncBroadcastPublish(request: BroadcastPublishRequest): Promise<BroadcastPublishResult>
  telefuncBroadcastDeliver(request: BroadcastDeliverRequest): Promise<void>
  telefuncRoomStateGet(key: string): Promise<string | null>
  telefuncRoomStateKeys(prefix: string): Promise<string[]>
  telefuncRoomStateSet(key: string, value: string, ttlMs?: number, replicate?: boolean): Promise<void>
  telefuncRoomStateDelete(key: string, replicate?: boolean): Promise<void>
  telefuncRoomStateSetIfAbsent(key: string, value: string, ttlMs?: number): Promise<boolean>
  telefuncRoomStateCompareAndSet(
    key: string,
    expected: string | null,
    next: string | null,
    ttlMs?: number,
    replicate?: boolean,
  ): Promise<boolean>
  telefuncRoomCommitFrame(input: RoomFrameCommit): Promise<RoomFrameCommitResult>
}

type PendingBucketPublish = {
  resolve: (ack: BroadcastPublishResult) => void
  reject: (err: Error) => void
}

type BufferedPublish = {
  send: () => Promise<BroadcastPublishResult>
  bytes: number
  pending: PendingBucketPublish
}

/**
 * Hard-capped ring buffer for bucket publishes that arrive before the source bucket setup finishes.
 * This bounds cold-path memory while preserving a contiguous FIFO suffix of pending publishes.
 * Payload-agnostic — each entry carries its own send function (works for both text and binary).
 */
class CloudflareBucketPublishBuffer {
  private entries: BufferedPublish[] = []
  private head = 0
  private totalBytes = 0
  private readonly maxBytes: number
  private _flushing = false

  constructor(maxBytes: number) {
    assert(maxBytes > 0, 'Cloudflare bucket publish buffer size must be > 0.')
    this.maxBytes = maxBytes
  }

  push(entry: BufferedPublish): void {
    if (entry.bytes > this.maxBytes) {
      entry.pending.reject(
        new Error('Keyed publish exceeded the Cloudflare bucket buffer limit during cold-path setup'),
      )
      this.clear()
      return
    }

    this.entries.push(entry)
    this.totalBytes += entry.bytes
    this.evict()
  }

  get flushing(): boolean {
    return this._flushing
  }

  async flush(): Promise<void> {
    if (this._flushing) return
    this._flushing = true
    try {
      while (this.head < this.entries.length) {
        const entry = this.entries[this.head]!
        try {
          const ack = await entry.send()
          entry.pending.resolve(ack)
        } catch (err) {
          entry.pending.reject(err instanceof Error ? err : new Error(String(err)))
        }
        this.totalBytes -= entry.bytes
        this.head++
      }
      this.compact()
    } finally {
      this._flushing = false
    }
  }

  clear(): void {
    for (let i = this.head; i < this.entries.length; i++) {
      this.entries[i]!.pending.reject(new Error('Keyed publish was dropped from the Cloudflare bucket buffer'))
    }
    this.entries.length = 0
    this.head = 0
    this.totalBytes = 0
  }

  private evict(): void {
    while (this.totalBytes > this.maxBytes && this.head < this.entries.length) {
      this.entries[this.head]!.pending.reject(
        new Error('Keyed publish was evicted from the Cloudflare bucket buffer before bucket setup completed'),
      )
      this.totalBytes -= this.entries[this.head]!.bytes
      this.head++
    }
    this.compact()
  }

  private compact(): void {
    if (this.head === 0) return
    if (this.head < this.entries.length - this.head) return
    this.entries = this.entries.slice(this.head)
    this.head = 0
  }
}

class MemberBucketState {
  readonly pendingPublishes: CloudflareBucketPublishBuffer
  setupInFlight = true
  teardownRequested = false
  refreshTimer: ReturnType<typeof setInterval> | null = null
  /** Resolves once presence is first registered in KV — the point a cross-region publisher's
   *  `listPresenceByBucket` can see this subscriber, so a retained replay awaiting it (see
   *  `BroadcastUnsubscribe.ready`) reads the retained copy only after live frames can reach here. */
  readonly ready: Promise<void>
  private markReady!: () => void
  private readonly authority: TelefuncDurableObjectStub
  private readonly key: string
  private readonly locationBucket: LocationBucket

  constructor(key: string, locationBucket: LocationBucket, authority: TelefuncDurableObjectStub) {
    this.key = key
    this.locationBucket = locationBucket
    this.authority = authority
    this.pendingPublishes = new CloudflareBucketPublishBuffer(CHANNEL_BUFFER_LIMIT_BYTES)
    this.ready = new Promise((resolve) => {
      this.markReady = resolve
    })
  }

  /** Called by the transport once presence registration settles (success or failure). Idempotent —
   *  resolving an already-resolved promise is a no-op — so a presence refresh never re-arms it. */
  markPresenceRegistered(): void {
    this.markReady()
  }

  publish(serialized: string): Promise<BroadcastPublishResult> {
    return unwrapRpcResult(
      this.authority.telefuncBroadcastPublish({
        key: this.key,
        locationBucket: this.locationBucket,
        serialized,
      }),
    )
  }

  publishBinary(data: Uint8Array): Promise<BroadcastPublishResult> {
    return unwrapRpcResult(
      this.authority.telefuncBroadcastPublish({
        key: this.key,
        locationBucket: this.locationBucket,
        binaryData: data,
      }),
    )
  }

  stopRefresh(): void {
    if (this.refreshTimer) {
      clearInterval(this.refreshTimer)
      this.refreshTimer = null
    }
  }
}

// ---------------------------------------------------------------------------
// CloudflareBroadcastAuthorityState — wraps DurableObjectState for seq counters
// and authority bucket persistence. One per DO instance, passed to publishToSubscribers.
// ---------------------------------------------------------------------------

class CloudflareBroadcastAuthorityState {
  private readonly state: DurableObjectState
  private readonly kv: KVNamespace | null
  private authorityPublishChain: Promise<void> = Promise.resolve()
  private readonly keySeqCache = new Map<string, number>()
  private readonly authorityBucketCache = new Map<string, LocationBucket>()

  constructor(state: DurableObjectState, kv?: KVNamespace) {
    this.state = state
    this.kv = kv ?? null
  }

  private requireReplicaKv(): KVNamespace {
    assert(this.kv, 'Cloudflare KV binding is not attached — room state needs a KV namespace for its read replica.')
    return this.kv
  }

  async getNextKeySeq(key: string): Promise<number> {
    const cachedSeq = this.keySeqCache.get(key)
    const currentSeq = cachedSeq ?? (await this.state.storage.get<number>(`broadcast:${key}:sequence`)) ?? 0
    const nextSeq = currentSeq + 1
    this.keySeqCache.set(key, nextSeq)
    await this.state.storage.put(`broadcast:${key}:sequence`, nextSeq)
    return nextSeq
  }

  async runInAuthorityChain<T>(fn: () => Promise<T>): Promise<T> {
    const previous = this.authorityPublishChain
    let release!: () => void
    this.authorityPublishChain = new Promise<void>((resolve) => {
      release = resolve
    })
    await previous
    try {
      return await fn()
    } finally {
      release()
    }
  }

  async getOrInitAuthorityBucket(key: string, preferredBucket: LocationBucket): Promise<LocationBucket> {
    const cachedBucket = this.authorityBucketCache.get(key)
    if (cachedBucket) return cachedBucket

    const storageKey = `broadcast:${key}:authority-bucket`
    const authorityBucket = (await this.state.storage.get<LocationBucket>(storageKey)) ?? preferredBucket

    this.authorityBucketCache.set(key, authorityBucket)
    await this.state.storage.put(storageKey, authorityBucket)
    return authorityBucket
  }

  // --- Room state (backs `Room` on Cloudflare) ---
  //
  // A room's whole state lives here, in the same Durable Object that already sequences the room's
  // control lane — one strongly-consistent authority per room, at the room's optimal location. The
  // atomic operations run through `runInAuthorityChain`, so they linearize against each other and
  // against the publish path. Entries carry the caller's TTL; reads drop expired ones lazily and
  // `reapExpiredRoomState` (the alarm) sweeps the rest — standing in for the native expiry DO storage
  // lacks.
  //
  // Replicated writes also mirror through to the Workers KV read replica (`replicate`), so directory
  // reads (`Room.get`, `Room.list`) and enumeration are fast global KV reads instead of a round-trip
  // to this region-pinned authority; the mirror rides inside the serialized write, so the replica
  // converges in authority order. Retained state is written `replicate: false` — it is read only
  // through the authority (`consistent`), so a replica copy would be dead weight and, on a hot lane,
  // would breach the replica's per-key write ceiling.

  private roomStateStorageKey(key: string): string {
    return ROOM_STATE_STORAGE_PREFIX + key
  }

  private liveRoomStateValue(entry: RoomStateEntry | undefined, now: number): string | null {
    if (!entry) return null
    return entry.e !== null && entry.e <= now ? null : entry.v
  }

  private async putRoomState(key: string, value: string, ttlMs: number | undefined): Promise<void> {
    await this.state.storage.put<RoomStateEntry>(this.roomStateStorageKey(key), {
      v: value,
      e: ttlMs === undefined ? null : Date.now() + ttlMs,
    })
    // A TTL entry (a member record) needs sweeping even if the room is never read again — arm the
    // alarm once. Entries without a TTL (config, retained) don't, so they never start the sweep.
    if (ttlMs !== undefined && (await this.state.storage.getAlarm()) === null) {
      await this.state.storage.setAlarm(Date.now() + ROOM_STATE_REAP_INTERVAL_MS)
    }
  }

  async roomStateGet(key: string): Promise<string | null> {
    return this.liveRoomStateValue(
      await this.state.storage.get<RoomStateEntry>(this.roomStateStorageKey(key)),
      Date.now(),
    )
  }

  async roomStateKeys(prefix: string): Promise<string[]> {
    const entries = await this.state.storage.list<RoomStateEntry>({ prefix: this.roomStateStorageKey(prefix) })
    const now = Date.now()
    const keys: string[] = []
    for (const [storageKey, entry] of entries) {
      if (this.liveRoomStateValue(entry, now) !== null) keys.push(storageKey.slice(ROOM_STATE_STORAGE_PREFIX.length))
    }
    return keys
  }

  /** Store to the authority and, when `replicate`, mirror to the KV read replica in the same step. */
  private async writeRoomState(
    key: string,
    value: string,
    ttlMs: number | undefined,
    replicate: boolean,
  ): Promise<void> {
    await this.putRoomState(key, value, ttlMs)
    if (replicate) await replicaKvPut(this.requireReplicaKv(), key, value, ttlMs)
  }

  /** Delete from the authority and, when `replicate`, from the KV read replica in the same step. */
  private async removeRoomState(key: string, replicate: boolean): Promise<void> {
    await this.state.storage.delete(this.roomStateStorageKey(key))
    if (replicate) await replicaKvDelete(this.requireReplicaKv(), key)
  }

  roomStateSet(key: string, value: string, ttlMs?: number, replicate = true): Promise<void> {
    return this.writeRoomState(key, value, ttlMs, replicate)
  }

  roomStateDelete(key: string, replicate = true): Promise<void> {
    return this.removeRoomState(key, replicate)
  }

  roomStateSetIfAbsent(key: string, value: string, ttlMs?: number): Promise<boolean> {
    return this.runInAuthorityChain(async () => {
      const current = this.liveRoomStateValue(
        await this.state.storage.get<RoomStateEntry>(this.roomStateStorageKey(key)),
        Date.now(),
      )
      if (current !== null) return false
      // The create-claim is always directory state (a room's config) — replicate it.
      await this.writeRoomState(key, value, ttlMs, true)
      return true
    })
  }

  roomStateCompareAndSet(
    key: string,
    expected: string | null,
    next: string | null,
    ttlMs?: number,
    replicate = true,
  ): Promise<boolean> {
    return this.runInAuthorityChain(async () => {
      const current = this.liveRoomStateValue(
        await this.state.storage.get<RoomStateEntry>(this.roomStateStorageKey(key)),
        Date.now(),
      )
      if (current !== expected) return false
      // Directory CAS (config, member records) mirrors to the KV read replica; an authority-only lane —
      // the `:o` order watermark, written `consistent` on every publish — passes `replicate: false` so it
      // stays off the replica (dead weight there, and a per-key write-ceiling risk on a hot room).
      if (next === null) await this.removeRoomState(key, replicate)
      else await this.writeRoomState(key, next, ttlMs, replicate)
      return true
    })
  }

  /** Alarm handler: drop every expired room-state entry, and re-arm while un-expired TTL entries
   *  remain so a busy room keeps getting swept and an emptied one lets the alarm lapse. */
  async reapExpiredRoomState(): Promise<void> {
    const entries = await this.state.storage.list<RoomStateEntry>({ prefix: ROOM_STATE_STORAGE_PREFIX })
    const now = Date.now()
    const expired: string[] = []
    let hasLiveTtl = false
    for (const [storageKey, entry] of entries) {
      if (entry.e === null) continue
      if (entry.e <= now) expired.push(storageKey)
      else hasLiveTtl = true
    }
    if (expired.length > 0) await this.state.storage.delete(expired)
    if (hasLiveTtl) await this.state.storage.setAlarm(Date.now() + ROOM_STATE_REAP_INTERVAL_MS)
  }
}

// ---------------------------------------------------------------------------
// CloudflareBroadcastTransport — per-isolate broadcast transport.
//
// A single CloudflareBroadcastTransport instance exists per worker isolate.
// Multiple Durable Object instances in the same isolate share this transport.
//
// State stored here:
//   - textSubs/binarySubs: local subscriber callbacks by key
//   - locationBucket: set once on first attachIsolateInfo call
//   - representativeDOName: the first shard DO name, used for RPC delivery
// ---------------------------------------------------------------------------

class CloudflareBroadcastTransport implements BroadcastAdapter {
  private readonly baseInstanceName: string
  private readonly scale: CloudflareScale | undefined
  private bindingName: string | null = null
  private binding: DurableObjectNamespace | null = null
  private kv: KVNamespace | null = null
  private locationBucket: LocationBucket | null = null
  private representativeDOName: string | null = null
  private readonly memberStates = new Map<string, MemberBucketState>()
  private readonly textSubs = new Map<string, Set<BroadcastOnMessage>>()
  private readonly binarySubs = new Map<string, Set<BroadcastBinaryOnMessage>>()

  constructor({ baseInstanceName, scale }: { baseInstanceName: string; scale?: CloudflareScale }) {
    this.baseInstanceName = baseInstanceName
    this.scale = scale
  }

  attachBinding(binding: DurableObjectNamespace, bindingName: string): void {
    this.binding = binding
    this.bindingName = bindingName
  }

  attachKV(kv: KVNamespace): void {
    this.kv = kv
  }

  attachIsolateInfo(doName: string, locationBucket: LocationBucket): void {
    assert(
      KNOWN_BROADCAST_BUCKETS.has(locationBucket),
      `attachIsolateInfo received invalid locationBucket "${locationBucket}".`,
    )
    if (!this.locationBucket) {
      this.locationBucket = locationBucket
      this.representativeDOName = doName
    }
  }

  // --- KV presence ---

  private requireKV(): KVNamespace {
    assert(this.kv, 'Cloudflare KV binding is not attached. Broadcast requires a KV namespace.')
    return this.kv
  }

  private requireLocationBucket(): LocationBucket {
    assert(this.locationBucket, 'Expected attachIsolateInfo() to be called before subscribe/publish.')
    return this.locationBucket
  }

  private requireRepresentativeDOName(): string {
    assert(this.representativeDOName, 'Expected attachIsolateInfo() to be called before subscribe/publish.')
    return this.representativeDOName
  }

  private getPresenceKey(key: string): string {
    return `tfps:${encodeURIComponent(key)}:${this.locationBucket}:${this.representativeDOName}`
  }

  private getPresencePrefix(key: string): string {
    return `tfps:${encodeURIComponent(key)}:`
  }

  private async putPresence(key: string): Promise<void> {
    const doName = this.requireRepresentativeDOName()
    await this.requireKV().put(this.getPresenceKey(key), doName, {
      expirationTtl: PRESENCE_TTL_SECONDS,
    })
  }

  private async deletePresence(key: string): Promise<void> {
    await this.requireKV().delete(this.getPresenceKey(key))
  }

  private async listPresenceByBucket(key: string): Promise<Map<LocationBucket, string[]>> {
    const kv = this.requireKV()
    const prefix = this.getPresencePrefix(key)
    const result = new Map<LocationBucket, string[]>()
    let cursor: string | undefined

    do {
      const list = await kv.list({ prefix, cursor })
      for (const entry of list.keys) {
        const suffix = entry.name.slice(prefix.length)
        const sepIdx = suffix.indexOf(':')
        if (sepIdx === -1) continue
        const bucket = suffix.slice(0, sepIdx) as LocationBucket
        const doName = suffix.slice(sepIdx + 1)
        let doNames = result.get(bucket)
        if (!doNames) {
          doNames = []
          result.set(bucket, doNames)
        }
        doNames.push(doName)
      }
      cursor = list.list_complete ? undefined : list.cursor
    } while (cursor)

    return result
  }

  // --- KV (backs `Room` state) ---
  //
  // A room's state has two tiers. The authority is the Durable Object that already sequences the room
  // (routed by `partitionKey`, the control-lane key) — strongly consistent, at the room's optimal
  // location. The replica is the globally-distributed Workers KV namespace, which the authority mirrors
  // its replicated writes to. Reads default to the replica so directory reads (`Room.get`, `Room.list`)
  // and enumeration are fast from any region; `consistent` reads bypass it and hit the authority for
  // read-your-writes. The one unpartitioned key is the cross-room room index — replica-only, since it
  // has no single home; enumeration tolerates its eventual consistency.

  async get(key: string, options?: KvReadOptions): Promise<string | null> {
    if (options?.consistent) return await this.roomStateStub(this.requirePartition(options)).telefuncRoomStateGet(key)
    return await replicaKvGet(this.requireKV(), key)
  }

  async set(key: string, value: string, options?: KvWriteOptions): Promise<void> {
    const partitionKey = options?.partitionKey
    // Unpartitioned: the cross-room room index — replica-only (no authority owns it).
    if (partitionKey === undefined) return await replicaKvPut(this.requireKV(), key, value, options?.ttlMs)
    // Partitioned: the room's authority owns it and (unless `consistent`) mirrors it to the replica.
    await this.roomStateStub(partitionKey).telefuncRoomStateSet(key, value, options?.ttlMs, !options?.consistent)
  }

  async delete(key: string, options?: KvReadOptions): Promise<void> {
    const partitionKey = options?.partitionKey
    if (partitionKey === undefined) return await replicaKvDelete(this.requireKV(), key)
    await this.roomStateStub(partitionKey).telefuncRoomStateDelete(key, !options?.consistent)
  }

  async keys(prefix: string, options?: KvReadOptions): Promise<string[]> {
    if (options?.consistent)
      return await this.roomStateStub(this.requirePartition(options)).telefuncRoomStateKeys(prefix)
    return await replicaKvList(this.requireKV(), prefix)
  }

  async setIfAbsent(key: string, value: string, options?: KvWriteOptions): Promise<boolean> {
    const stub = this.roomStateStub(this.requirePartition(options))
    return await stub.telefuncRoomStateSetIfAbsent(key, value, options?.ttlMs)
  }

  async update(key: string, mutate: KvMutate, options?: KvWriteOptions): Promise<string | null> {
    const stub = this.roomStateStub(this.requirePartition(options))
    // Optimistic compare-and-set loop against the authority: `mutate` runs on the authority's current
    // value (read-your-writes, so the CAS is meaningful), the DO applies only if it's unchanged and
    // mirrors the winner to the replica, and a concurrent writer re-runs `mutate` instead of losing it.
    for (;;) {
      const current = await stub.telefuncRoomStateGet(key)
      const next = mutate(current)
      if (next === KV_KEEP) return current
      if (await stub.telefuncRoomStateCompareAndSet(key, current, next, options?.ttlMs, !options?.consistent))
        return next
    }
  }

  async commitFrame(input: RoomFrameCommit): Promise<RoomFrameCommitResult> {
    // The whole commit runs in one turn on the room's partition authority DO (`commitFrameOnAuthority`);
    // this is the client hop to it, routed by `partitionKey` like every other authority-tier op. We only
    // materialize its lazy RPC result onto a plain object (see `unwrapRpcResult`), awaiting each field so
    // none is dropped in transit — `ok` discriminates, so it's read first.
    const r = (await this.roomStateStub(input.partitionKey).telefuncRoomCommitFrame(input)) as {
      ok: boolean
      seq?: number
      timestamp?: number
      receivers?: number
    }
    if (!(await r.ok)) return { ok: false, reason: 'stale-fence' }
    const [seq, timestamp, receivers] = await Promise.all([r.seq, r.timestamp, r.receivers])
    return { ok: true, seq: seq!, timestamp: timestamp!, ...(receivers === undefined ? undefined : { receivers }) }
  }

  private requirePartition(options: KvReadOptions | undefined): string {
    assert(options?.partitionKey !== undefined, 'A Room authority-tier KV op must carry a partition key.')
    return options.partitionKey
  }

  /** The room-state authority stub for a partition key (a room's control-lane key): the very Durable
   *  Object that sequences that room, provisioned at its optimal location. */
  private roomStateStub(partitionKey: string): TelefuncDurableObjectStub {
    return this.getAuthorityStub(partitionKey, this.locationBucket ?? undefined)
  }

  // --- Local subscriber tracking ---

  private hasLocalCallbacks(key: string): boolean {
    const text = this.textSubs.get(key)
    if (text && text.size > 0) return true
    const binary = this.binarySubs.get(key)
    return binary !== undefined && binary.size > 0
  }

  private deliverLocal({ key, serialized, binaryData, info }: BroadcastDeliverRequest): void {
    if (serialized !== undefined) {
      const subs = this.textSubs.get(key)
      if (subs) for (const cb of subs) cb(serialized, info)
    }
    if (binaryData !== undefined) {
      const subs = this.binarySubs.get(key)
      if (subs) for (const cb of subs) cb(binaryData, info)
    }
  }

  // --- BroadcastAdapter interface ---

  subscribe(key: string, onMessage: BroadcastOnMessage): BroadcastUnsubscribe {
    const locationBucket = this.requireLocationBucket()
    let subs = this.textSubs.get(key)
    if (!subs) {
      subs = new Set()
      this.textSubs.set(key, subs)
    }
    subs.add(onMessage)
    this.ensurePresence(key, locationBucket)
    const unsub: BroadcastUnsubscribe = () => {
      const s = this.textSubs.get(key)
      if (s) {
        s.delete(onMessage)
        if (s.size === 0) this.textSubs.delete(key)
      }
      this.teardownPresenceIfEmpty(key)
    }
    // Every subscriber to a key shares the one presence registration, so it shares that registration's
    // readiness (see `BroadcastUnsubscribe.ready` and `MemberBucketState.ready`).
    unsub.ready = this.memberStates.get(key)?.ready
    return unsub
  }

  subscribeBinary(key: string, onMessage: BroadcastBinaryOnMessage): BroadcastUnsubscribe {
    const locationBucket = this.requireLocationBucket()
    let subs = this.binarySubs.get(key)
    if (!subs) {
      subs = new Set()
      this.binarySubs.set(key, subs)
    }
    subs.add(onMessage)
    this.ensurePresence(key, locationBucket)
    const unsub: BroadcastUnsubscribe = () => {
      const s = this.binarySubs.get(key)
      if (s) {
        s.delete(onMessage)
        if (s.size === 0) this.binarySubs.delete(key)
      }
      this.teardownPresenceIfEmpty(key)
    }
    unsub.ready = this.memberStates.get(key)?.ready
    return unsub
  }

  publish(key: string, serialized: string): Promise<BroadcastPublishResult> {
    const memberState = this.memberStates.get(key)

    if (memberState) {
      if (memberState.setupInFlight || memberState.pendingPublishes.flushing) {
        return new Promise<BroadcastPublishResult>((resolve, reject) => {
          memberState.pendingPublishes.push({
            send: () => memberState.publish(serialized),
            bytes: utf8ByteLength(serialized),
            pending: { resolve, reject },
          })
        })
      }
      return memberState.publish(serialized)
    }

    const locationBucket = this.requireLocationBucket()
    const authority = this.getAuthorityStub(key, locationBucket)
    return unwrapRpcResult(authority.telefuncBroadcastPublish({ key, locationBucket, serialized }))
  }

  publishBinary(key: string, data: Uint8Array): Promise<BroadcastPublishResult> {
    const memberState = this.memberStates.get(key)

    if (memberState) {
      if (memberState.setupInFlight || memberState.pendingPublishes.flushing) {
        return new Promise<BroadcastPublishResult>((resolve, reject) => {
          memberState.pendingPublishes.push({
            send: () => memberState.publishBinary(data),
            bytes: data.byteLength,
            pending: { resolve, reject },
          })
        })
      }
      return memberState.publishBinary(data)
    }

    const locationBucket = this.requireLocationBucket()
    const authority = this.getAuthorityStub(key, locationBucket)
    return unwrapRpcResult(authority.telefuncBroadcastPublish({ key, locationBucket, binaryData: data }))
  }

  /**
   * Fans out one keyed publish from either a key authority or a bucket coordinator.
   *
   * Non-forwarded: reads KV presence, sequences through authority, forwards to bucket coordinators.
   * Forwarded: delivers to listed DO names without reading KV again.
   */
  async publishToSubscribers(
    authorityState: CloudflareBroadcastAuthorityState,
    request: BroadcastPublishRequest,
  ): Promise<BroadcastPublishResult> {
    const { key, locationBucket, serialized, binaryData, forwarded = false } = request

    if (forwarded) {
      assert(request.info, 'Forwarded publish must include info')
      const info = request.info
      const doNames = request.doNames ?? []
      await Promise.all(
        doNames.map((doName) =>
          this.getBoundStub(doName).telefuncBroadcastDeliver({ key, serialized, binaryData, info }),
        ),
      )
      return { seq: info.seq, timestamp: info.timestamp }
    }

    const { authorityBucket, seq, presenceByBucket } = await authorityState.runInAuthorityChain(async () => ({
      authorityBucket: await authorityState.getOrInitAuthorityBucket(key, locationBucket),
      seq: await authorityState.getNextKeySeq(key),
      presenceByBucket: await this.listPresenceByBucket(key),
    }))

    const info = { seq, timestamp: Date.now() }
    // `receivers` counts subscribed DOs (presence entries) — the same want-driven zero
    // semantics as the other transports: 0 ⟺ no subscriber anywhere.
    const { receivers, fanoutBuckets } = await this.fanoutFrame(key, { serialized, binaryData }, info, presenceByBucket)
    return { seq: info.seq, timestamp: info.timestamp, receivers, meta: { authorityBucket, fanoutBuckets } }
  }

  /** Forward one already-ordered frame to every populated bucket coordinator, each of which delivers it
   *  to the subscribed DOs in its bucket. The seq/timestamp ride on `info` — the single source of a
   *  frame's place in the order — so `publishToSubscribers` (a fresh per-key seq) and
   *  `commitFrameOnAuthority` (the committed room order) share one ordered-outbound fanout. Returns the
   *  live receiver count and the buckets reached. */
  private async fanoutFrame(
    key: string,
    payload: { serialized?: string; binaryData?: Uint8Array },
    info: WirePublishInfo,
    presenceByBucket: Map<LocationBucket, string[]>,
  ): Promise<{ receivers: number; fanoutBuckets: LocationBucket[] }> {
    const fanoutBuckets = Array.from(presenceByBucket.keys())
    let receivers = 0
    for (const doNames of presenceByBucket.values()) receivers += doNames.length
    await Promise.all(
      fanoutBuckets.map((activeBucket) =>
        this.getBucketCoordinatorStub(key, activeBucket).telefuncBroadcastPublish({
          key,
          serialized: payload.serialized,
          binaryData: payload.binaryData,
          forwarded: true,
          locationBucket: activeBucket,
          doNames: presenceByBucket.get(activeBucket)!,
          info,
        }),
      ),
    )
    return { receivers, fanoutBuckets }
  }

  /**
   * Delivers a publish to local subscribers. Called via RPC on the representative DO for this isolate.
   */
  deliverToLocal(request: BroadcastDeliverRequest): void {
    this.deliverLocal(request)
  }

  /**
   * The authority half of `commitFrame` (see `BroadcastAdapter.commitFrame`), run via RPC on the room's
   * partition Durable Object — the one that already sequences the room's control lane. One
   * `runInAuthorityChain` turn does the whole atomic core: it compares every fence (each `key` must still
   * equal `expected`, else the caller's incarnation is stale and nothing else happens), advances the
   * clamped monotonic room clock at `orderKey`, and — authority-only (`replicate: false`), like the `:o`
   * watermark — retains the self-framed bytes at `retainKey`. Presence is snapshotted in that same turn so
   * no later commit slips its order in ahead of this one's fanout targets. The ordered fanout then runs
   * outside the chain (as `publishToSubscribers` does, so a slow remote hop can't stall the next commit's
   * ordering), carrying the committed `(seq,timestamp)` on the wire — so every local and remote receiver
   * reads this frame's real place in the room order, never a fresh per-key seq.
   *
   * NOTE (certification): the atomic authority turn and the presence-registration readiness
   * (`MemberBucketState.ready`) run only on real Durable Objects, which need a workerd runtime, not the
   * in-memory fakes the unit suite uses. Certify them in the `test/@cloudflare_vite-plugin` playground
   * (the same real-DO harness the "Cloudflare" CI job runs): drive a `Room` across two isolates and
   * assert a committed frame reaches a subscriber that joined mid-commit in order, and that a stale-fence
   * commit is rejected. That harness needs the workerd dev server, so it is certified there, not here.
   */
  async commitFrameOnAuthority(
    authorityState: CloudflareBroadcastAuthorityState,
    input: RoomFrameCommit,
  ): Promise<RoomFrameCommitResult> {
    const committed = await authorityState.runInAuthorityChain(async () => {
      for (const { key, expected } of input.fences) {
        if ((await authorityState.roomStateGet(key)) !== expected) return null
      }
      const prevRaw = await authorityState.roomStateGet(input.orderKey)
      const prev = prevRaw === null ? null : (JSON.parse(prevRaw) as WirePublishInfo)
      // The clamped advance (mirrors `advanceRoomFrameOrder`): `seq` resets to 1 when wall time passes the
      // last timestamp, else it clamps that timestamp and increments — so `(timestamp, seq)` is strictly
      // increasing whatever this authority's clock skew.
      const now = Date.now()
      const prevTs = prev?.timestamp ?? 0
      const info: WirePublishInfo =
        now > prevTs ? { seq: 1, timestamp: now } : { seq: (prev?.seq ?? 0) + 1, timestamp: prevTs }
      // Order + retain are authority-only — read back through `consistent`, so a replica copy is dead
      // weight (and a per-key write-ceiling risk on a hot room).
      await authorityState.roomStateSet(input.orderKey, JSON.stringify(info), input.orderTtlMs, false)
      if (input.retainKey !== undefined)
        await authorityState.roomStateSet(input.retainKey, encodePublishText(input.payload, info), undefined, false)
      return { info, presenceByBucket: await this.listPresenceByBucket(input.channelKey) }
    })
    if (committed === null) return { ok: false, reason: 'stale-fence' }
    const { receivers } = await this.fanoutFrame(
      input.channelKey,
      { serialized: input.payload },
      committed.info,
      committed.presenceByBucket,
    )
    return { ok: true, seq: committed.info.seq, timestamp: committed.info.timestamp, receivers }
  }

  // --- Private ---

  private ensurePresence(key: string, locationBucket: LocationBucket): void {
    if (this.memberStates.has(key)) return
    const memberState = new MemberBucketState(key, locationBucket, this.getAuthorityStub(key, locationBucket))
    this.memberStates.set(key, memberState)
    void this.initializePresence(key, memberState)
  }

  private teardownPresenceIfEmpty(key: string): void {
    if (this.hasLocalCallbacks(key)) return
    const memberState = this.memberStates.get(key)
    if (!memberState) return

    if (memberState.setupInFlight || memberState.pendingPublishes.flushing) {
      memberState.teardownRequested = true
      return
    }

    memberState.stopRefresh()
    this.memberStates.delete(key)
    void this.deletePresence(key)
  }

  private async initializePresence(key: string, memberState: MemberBucketState): Promise<void> {
    try {
      await this.putPresence(key)
    } finally {
      memberState.setupInFlight = false
      // Presence is now registered (or the write threw and its own refresh/reconnect will retry) — the
      // subscription is as live as it gets, so release any retained replay waiting on its readiness.
      memberState.markPresenceRegistered()
    }

    memberState.refreshTimer = setInterval(() => {
      void this.putPresence(key)
    }, PRESENCE_REFRESH_INTERVAL_MS)

    await memberState.pendingPublishes.flush()

    if (memberState.teardownRequested) {
      memberState.stopRefresh()
      await this.deletePresence(key)
      this.memberStates.delete(key)
    }
  }

  private getBucketCoordinatorStub(key: string, locationBucket: LocationBucket): TelefuncDurableObjectStub {
    const bucketShardCount = getBucketCoordinatorShardIndices(this.scale, locationBucket).length
    const bucketShardOrdinal = getDeterministicKeyBucketIndex(key, bucketShardCount)
    return this.getBoundStub(
      `${this.baseInstanceName}:broadcast:${locationBucket}:${bucketShardOrdinal}`,
      locationBucket,
    )
  }

  private getAuthorityStub(key: string, locationHint?: DurableObjectLocationHint): TelefuncDurableObjectStub {
    return this.getBoundStub(`${this.baseInstanceName}:broadcast:authority:${key}`, locationHint)
  }

  private getBoundStub(instanceName: string, locationHint?: DurableObjectLocationHint): TelefuncDurableObjectStub {
    assert(this.binding, `Missing Cloudflare Durable Object binding "${this.bindingName ?? 'unknown'}".`)
    return this.binding.get(
      this.binding.idFromName(instanceName),
      locationHint ? { locationHint } : undefined,
    ) as TelefuncDurableObjectStub
  }
}
