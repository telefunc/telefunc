/// <reference types="@cloudflare/workers-types" />
export { CloudflareBroadcastTransport, CloudflareBroadcastAuthorityState }
export type { BroadcastDeliverRequest, BroadcastPublishRequest, TelefuncDurableObjectStub }

import { CHANNEL_BUFFER_LIMIT_BYTES } from '../../../constants.js'
import { KNOWN_BROADCAST_BUCKETS, getBucketCoordinatorShardIndices, getDeterministicKeyBucketIndex } from './routing.js'
import { assert, assertUsage } from '../../../../utils/assert.js'
import { utf8ByteLength } from '../../../../utils/utf8ByteLength.js'
import type {
  BroadcastBinaryOnMessage,
  BroadcastOnMessage,
  BroadcastPublishResult,
  BroadcastAdapter,
  BroadcastUnsubscribe,
} from '../../broadcast.js'
import type { WirePublishInfo } from '../../../shared-ws.js'
import type { CloudflareScale, LocationBucket } from './routing.js'

const PRESENCE_TTL_SECONDS = 90
const PRESENCE_REFRESH_INTERVAL_MS = 30_000

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

/** A stub to a Telefunc Durable Object carrying the generic broadcast fan-out RPC surface. */
type TelefuncDurableObjectStub = DurableObjectStub & {
  telefuncBroadcastPublish(request: BroadcastPublishRequest): Promise<BroadcastPublishResult>
  telefuncBroadcastDeliver(request: BroadcastDeliverRequest): Promise<void>
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
  private authorityPublishChain: Promise<void> = Promise.resolve()
  private readonly keySeqCache = new Map<string, number>()
  private readonly authorityBucketCache = new Map<string, LocationBucket>()

  constructor(state: DurableObjectState) {
    this.state = state
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

  /** Forward one already-ordered generic broadcast frame to every populated bucket coordinator, each
   *  of which delivers it to the subscribed DOs in its bucket. Returns the live receiver count and the
   *  buckets reached. */
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
      // Release any retained replay waiting on this subscription's readiness. Readiness is marked on
      // failure too: if the put threw, the error propagates from here — the refresh timer is never
      // installed, the pending publishes are never flushed, and nothing re-attempts this initial
      // registration, so the subscription is live locally with no presence entry behind it.
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
