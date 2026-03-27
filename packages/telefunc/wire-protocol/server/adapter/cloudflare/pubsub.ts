/// <reference types="@cloudflare/workers-types" />
export { CloudflarePubSubTransport, CloudflarePubSubRegistry }
export type { PubSubDeliverRequest, PubSubPublishRequest, TelefuncPubSubStub }

import { CHANNEL_BUFFER_LIMIT_BYTES } from '../../../constants.js'
import {
  KNOWN_PUBSUB_BUCKETS,
  TELEFUNC_PUBSUB_BUCKET_HEADER,
  TELEFUNC_SHARD_HEADER,
  getBucketCoordinatorShardIndices,
  getDeterministicKeyBucketIndex,
} from './routing.js'
import { assert, assertUsage } from '../../../../utils/assert.js'
import { utf8ByteLength } from '../../../../utils/utf8ByteLength.js'
import type {
  PubSubBinaryOnMessage,
  PubSubBinaryPublish,
  PubSubBinarySubscription,
  PubSubOnMessage,
  PubSubPublish,
  PubSubPublishResult,
  PubSubRegistry,
  PubSubSubscription,
} from '../../pubsub.js'
import type { WirePublishInfo } from '../../../shared-ws.js'
import type { CloudflareScale, LocationBucket } from './routing.js'

const PRESENCE_TTL_SECONDS = 90
const PRESENCE_REFRESH_INTERVAL_MS = 30_000

/** Unwrap Cloudflare DO RPC proxy into a plain object.
 *  RPC properties are lazy stubs that must be awaited to resolve their values. */
async function unwrapRpcResult(rpc: Promise<PubSubPublishResult>): Promise<PubSubPublishResult> {
  const r = await rpc
  const [seq, ts, meta] = await Promise.all([r.seq, r.ts, r.meta])
  return { seq, ts, ...(meta ? { meta } : undefined) }
}

type CloudflarePubSubTransportOptions = {
  baseInstanceName: string
  scale?: CloudflareScale
}

type PubSubPublishRequest = {
  key: string
  locationBucket: LocationBucket
  sourceChannelId: string
  forwarded?: boolean
  sessionShardNames?: string[]
  info?: WirePublishInfo
  // Exactly one of these is set per publish
  serialized?: string
  binaryData?: Uint8Array
}

type PubSubDeliverRequest = {
  key: string
  locationBucket: LocationBucket
  sourceChannelId: string
  info: WirePublishInfo
  serialized?: string
  binaryData?: Uint8Array
}

type TelefuncPubSubStub = DurableObjectStub & {
  telefuncPubSubPublish(request: PubSubPublishRequest): Promise<PubSubPublishResult>
  telefuncPubSubDeliver(request: PubSubDeliverRequest): Promise<void>
}

type MemberLocation = {
  sessionInstanceName: string
  locationBucket: LocationBucket
}

type PendingBucketPublish = {
  resolve: (ack: PubSubPublishResult) => void
  reject: (err: Error) => void
}

type BufferedPublish = {
  send: () => Promise<PubSubPublishResult>
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
  readonly location: MemberLocation
  readonly key: string
  readonly pendingPublishes: CloudflareBucketPublishBuffer
  setupInFlight = true
  teardownRequested = false
  refreshTimer: ReturnType<typeof setInterval> | null = null
  private readonly publishRequestBase: Pick<PubSubPublishRequest, 'key' | 'locationBucket' | 'sourceChannelId'>
  private readonly authority: TelefuncPubSubStub

  constructor(key: string, sourceChannelId: string, location: MemberLocation, authority: TelefuncPubSubStub) {
    this.key = key
    this.location = location
    this.publishRequestBase = {
      key,
      locationBucket: location.locationBucket,
      sourceChannelId,
    }
    this.authority = authority
    this.pendingPublishes = new CloudflareBucketPublishBuffer(CHANNEL_BUFFER_LIMIT_BYTES)
  }

  publish(serialized: string): Promise<PubSubPublishResult> {
    return unwrapRpcResult(
      this.authority.telefuncPubSubPublish({
        ...this.publishRequestBase,
        serialized,
      }),
    )
  }

  publishBinary(data: Uint8Array): Promise<PubSubPublishResult> {
    return unwrapRpcResult(
      this.authority.telefuncPubSubPublish({
        ...this.publishRequestBase,
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

type LocalSubscriber = {
  selfDelivery: boolean
  onMessage?: PubSubOnMessage
  onBinaryMessage?: PubSubBinaryOnMessage
}

class CloudflarePubSubRegistry {
  private readonly state: DurableObjectState
  private readonly localKeyBuckets = new Map<string, Map<LocationBucket, Map<string, LocalSubscriber>>>()
  private authorityPublishChain: Promise<void> = Promise.resolve()
  private readonly keySeqCache = new Map<string, number>()
  private readonly authorityBucketCache = new Map<string, LocationBucket>()

  constructor(state: DurableObjectState) {
    this.state = state
  }

  /** Records a local text subscriber in memory only. */
  registerLocal(sub: PubSubSubscription, locationBucket: LocationBucket): void {
    const entry = this.getOrCreateEntry(sub.id, sub.key, sub.selfDelivery, locationBucket)
    entry.onMessage = sub.onMessage
  }

  /** Records a local binary subscriber in memory only. */
  registerBinaryLocal(sub: PubSubBinarySubscription, locationBucket: LocationBucket): void {
    const entry = this.getOrCreateEntry(sub.id, sub.key, sub.selfDelivery, locationBucket)
    entry.onBinaryMessage = sub.onMessage
  }

  /** Removes a local text subscriber callback, cleaning up if both callbacks are gone. */
  unregisterLocal(id: string, key: string, locationBucket: LocationBucket): void {
    const entry = this.getEntry(id, key, locationBucket)
    if (!entry) return
    entry.onMessage = undefined
    if (!entry.onBinaryMessage) this.removeEntry(id, key, locationBucket)
  }

  /** Removes a local binary subscriber callback, cleaning up if both callbacks are gone. */
  unregisterBinaryLocal(id: string, key: string, locationBucket: LocationBucket): void {
    const entry = this.getEntry(id, key, locationBucket)
    if (!entry) return
    entry.onBinaryMessage = undefined
    if (!entry.onMessage) this.removeEntry(id, key, locationBucket)
  }

  /** Returns true if the subscriber entry still has at least one callback. */
  hasLocalCallbacks(id: string, key: string, locationBucket: LocationBucket): boolean {
    const entry = this.getEntry(id, key, locationBucket)
    return entry !== undefined && (entry.onMessage !== undefined || entry.onBinaryMessage !== undefined)
  }

  /** Delivers to only the subscribers that are both local to this DO and in the same key-location-bucket. */
  deliverLocal({ key, locationBucket, serialized, binaryData, sourceChannelId, info }: PubSubDeliverRequest): void {
    const members = this.localKeyBuckets.get(key)?.get(locationBucket)
    if (!members) return
    for (const [id, member] of members) {
      if (id === sourceChannelId && !member.selfDelivery) continue
      if (serialized !== undefined && member.onMessage) {
        member.onMessage(serialized, sourceChannelId, info)
      }
      if (binaryData !== undefined && member.onBinaryMessage) {
        member.onBinaryMessage(binaryData, sourceChannelId, info)
      }
    }
  }

  private getOrCreateEntry(
    id: string,
    key: string,
    selfDelivery: boolean,
    locationBucket: LocationBucket,
  ): LocalSubscriber {
    let keyBuckets = this.localKeyBuckets.get(key)
    if (!keyBuckets) {
      keyBuckets = new Map()
      this.localKeyBuckets.set(key, keyBuckets)
    }
    let members = keyBuckets.get(locationBucket)
    if (!members) {
      members = new Map()
      keyBuckets.set(locationBucket, members)
    }
    let entry = members.get(id)
    if (!entry) {
      entry = { selfDelivery }
      members.set(id, entry)
    }
    return entry
  }

  private getEntry(id: string, key: string, locationBucket: LocationBucket): LocalSubscriber | undefined {
    return this.localKeyBuckets.get(key)?.get(locationBucket)?.get(id)
  }

  private removeEntry(id: string, key: string, locationBucket: LocationBucket): void {
    const keyBuckets = this.localKeyBuckets.get(key)
    if (!keyBuckets) return
    const members = keyBuckets.get(locationBucket)
    if (!members) return
    members.delete(id)
    if (members.size !== 0) return
    keyBuckets.delete(locationBucket)
    if (keyBuckets.size === 0) this.localKeyBuckets.delete(key)
  }

  /** Returns the next globally ordered sequence for this key from this authority instance. */
  async getNextKeySeq(key: string): Promise<number> {
    const cachedSeq = this.keySeqCache.get(key)
    const currentSeq = cachedSeq ?? (await this.state.storage.get<number>(`pubsub:${key}:sequence`)) ?? 0
    const nextSeq = currentSeq + 1
    this.keySeqCache.set(key, nextSeq)
    await this.state.storage.put(`pubsub:${key}:sequence`, nextSeq)
    return nextSeq
  }

  /** Serializes authority dispatch for one key. The callback runs exclusively — no two dispatches for the same key overlap. */
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

  /** Returns the authority bucket chosen on first touch and persists it for later publishes. */
  async getOrInitAuthorityBucket(key: string, preferredBucket: LocationBucket): Promise<LocationBucket> {
    const cachedBucket = this.authorityBucketCache.get(key)
    if (cachedBucket) return cachedBucket

    const storageKey = `pubsub:${key}:authority-bucket`
    const authorityBucket = (await this.state.storage.get<LocationBucket>(storageKey)) ?? preferredBucket

    this.authorityBucketCache.set(key, authorityBucket)
    await this.state.storage.put(storageKey, authorityBucket)
    return authorityBucket
  }
}

class CloudflarePubSubTransport implements PubSubRegistry {
  private readonly baseInstanceName: string
  private readonly scale: CloudflareScale | undefined
  private bindingName: string | null = null
  private binding: DurableObjectNamespace | null = null
  private kv: KVNamespace | null = null
  private readonly localRegistries = new Map<string, CloudflarePubSubRegistry>()
  private readonly memberStates = new Map<string, MemberBucketState>()

  constructor({ baseInstanceName, scale }: CloudflarePubSubTransportOptions) {
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

  attachSessionRegistry(sessionInstanceName: string, registry: CloudflarePubSubRegistry): void {
    this.localRegistries.set(sessionInstanceName, registry)
  }

  // --- KV presence ---

  private requireKV(): KVNamespace {
    assert(this.kv, 'Cloudflare KV binding is not attached. Keyed pub/sub channels require a KV namespace.')
    return this.kv
  }

  private getPresenceKey(key: string, locationBucket: LocationBucket, sessionInstanceName: string): string {
    return `tfps:${encodeURIComponent(key)}:${locationBucket}:${sessionInstanceName}`
  }

  private getPresencePrefix(key: string): string {
    return `tfps:${encodeURIComponent(key)}:`
  }

  private async putPresence(key: string, locationBucket: LocationBucket, sessionInstanceName: string): Promise<void> {
    await this.requireKV().put(this.getPresenceKey(key, locationBucket, sessionInstanceName), '1', {
      expirationTtl: PRESENCE_TTL_SECONDS,
    })
  }

  private async deletePresence(
    key: string,
    locationBucket: LocationBucket,
    sessionInstanceName: string,
  ): Promise<void> {
    await this.requireKV().delete(this.getPresenceKey(key, locationBucket, sessionInstanceName))
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
        const locationBucket = suffix.slice(0, sepIdx) as LocationBucket
        const sessionInstanceName = suffix.slice(sepIdx + 1)
        let shards = result.get(locationBucket)
        if (!shards) {
          shards = []
          result.set(locationBucket, shards)
        }
        shards.push(sessionInstanceName)
      }
      cursor = list.list_complete ? undefined : list.cursor
    } while (cursor)

    return result
  }

  // --- PubSubRegistry interface ---

  subscribe(sub: PubSubSubscription): void {
    const memberLocation = this.resolveMemberLocation(sub.headers)
    const registry = this.requireRegistry(memberLocation.sessionInstanceName)
    registry.registerLocal(sub, memberLocation.locationBucket)
    this.ensurePresence(sub.id, sub.key, memberLocation)
  }

  subscribeBinary(sub: PubSubBinarySubscription): void {
    const memberLocation = this.resolveMemberLocation(sub.headers)
    const registry = this.requireRegistry(memberLocation.sessionInstanceName)
    registry.registerBinaryLocal(sub, memberLocation.locationBucket)
    this.ensurePresence(sub.id, sub.key, memberLocation)
  }

  publish({ id, key, serialized, headers }: PubSubPublish): Promise<PubSubPublishResult> {
    const memberState = this.memberStates.get(id)

    // Subscribed channel — route through memberState (respects presence setup buffering)
    if (memberState) {
      if (memberState.setupInFlight || memberState.pendingPublishes.flushing) {
        return new Promise<PubSubPublishResult>((resolve, reject) => {
          memberState.pendingPublishes.push({
            send: () => memberState.publish(serialized),
            bytes: utf8ByteLength(serialized),
            pending: { resolve, reject },
          })
        })
      }
      return memberState.publish(serialized)
    }

    // Publish-only channel — send directly to authority without presence registration
    const location = this.resolveMemberLocation(headers)
    const authority = this.getAuthorityStub(key, location.locationBucket)
    return unwrapRpcResult(
      authority.telefuncPubSubPublish({
        key,
        locationBucket: location.locationBucket,
        serialized,
        sourceChannelId: id,
      }),
    )
  }

  publishBinary({ id, key, data, headers }: PubSubBinaryPublish): Promise<PubSubPublishResult> {
    const memberState = this.memberStates.get(id)

    // Subscribed channel — route through memberState (respects presence setup buffering)
    if (memberState) {
      if (memberState.setupInFlight || memberState.pendingPublishes.flushing) {
        return new Promise<PubSubPublishResult>((resolve, reject) => {
          memberState.pendingPublishes.push({
            send: () => memberState.publishBinary(data),
            bytes: data.byteLength,
            pending: { resolve, reject },
          })
        })
      }
      return memberState.publishBinary(data)
    }

    // Publish-only channel — send directly to authority without presence registration
    const location = this.resolveMemberLocation(headers)
    const authority = this.getAuthorityStub(key, location.locationBucket)
    return unwrapRpcResult(
      authority.telefuncPubSubPublish({
        key,
        locationBucket: location.locationBucket,
        binaryData: data,
        sourceChannelId: id,
      }),
    )
  }

  unsubscribe(id: string, key: string): void {
    const memberState = this.memberStates.get(id)
    assert(
      memberState,
      'Cloudflare keyed channel unsubscribe was called before the channel was registered. Expected ServerChannel._registerChannel() to run first.',
    )
    const registry = this.localRegistries.get(memberState.location.sessionInstanceName)
    registry?.unregisterLocal(id, key, memberState.location.locationBucket)
    this.teardownPresenceIfEmpty(id, key, memberState)
  }

  unsubscribeBinary(id: string, key: string): void {
    const memberState = this.memberStates.get(id)
    assert(
      memberState,
      'Cloudflare keyed channel unsubscribeBinary was called before the channel was registered. Expected ServerChannel._registerChannel() to run first.',
    )
    const registry = this.localRegistries.get(memberState.location.sessionInstanceName)
    registry?.unregisterBinaryLocal(id, key, memberState.location.locationBucket)
    this.teardownPresenceIfEmpty(id, key, memberState)
  }

  /**
   * Fans out one keyed publish from either a key authority or a bucket coordinator.
   *
   * The non-forwarded path reads active presence from KV, sequences the publish through the authority,
   * and forwards to each populated bucket coordinator with the session shard list included.
   * Forwarded calls deliver to the listed session shards without reading KV again.
   */
  async publishToSubscribers(
    registry: CloudflarePubSubRegistry,
    request: PubSubPublishRequest,
  ): Promise<PubSubPublishResult> {
    const { key, locationBucket, serialized, binaryData, sourceChannelId, forwarded = false } = request

    if (forwarded) {
      assert(request.info, 'Forwarded publish must include info')
      const info = request.info
      const sessionShardNames = request.sessionShardNames ?? []
      await Promise.all(
        sessionShardNames.map((sessionInstanceName) =>
          this.getBoundStub(sessionInstanceName).telefuncPubSubDeliver({
            key,
            locationBucket,
            serialized,
            binaryData,
            sourceChannelId,
            info,
          }),
        ),
      )
      return { seq: info.seq, ts: info.ts }
    }

    const { authorityBucket, seq, presenceByBucket } = await registry.runInAuthorityChain(async () => ({
      authorityBucket: await registry.getOrInitAuthorityBucket(key, locationBucket),
      seq: await registry.getNextKeySeq(key),
      presenceByBucket: await this.listPresenceByBucket(key),
    }))

    const ts = Date.now()
    const info = { seq, ts }
    const activeBuckets = Array.from(presenceByBucket.keys())
    await Promise.all(
      activeBuckets.map((activeBucket) =>
        this.getBucketCoordinatorStub(key, activeBucket).telefuncPubSubPublish({
          key,
          serialized,
          binaryData,
          sourceChannelId,
          forwarded: true,
          locationBucket: activeBucket,
          sessionShardNames: presenceByBucket.get(activeBucket)!,
          info,
        }),
      ),
    )
    return { seq, ts, meta: { authorityBucket, fanoutBuckets: activeBuckets } }
  }

  // --- Private ---

  private requireRegistry(sessionInstanceName: string): CloudflarePubSubRegistry {
    const registry = this.localRegistries.get(sessionInstanceName)
    assert(registry, `No Cloudflare pub/sub registry found for session instance "${sessionInstanceName}".`)
    return registry
  }

  private ensurePresence(id: string, key: string, memberLocation: MemberLocation): void {
    if (this.memberStates.has(id)) return
    const memberState = new MemberBucketState(
      key,
      id,
      memberLocation,
      this.getAuthorityStub(key, memberLocation.locationBucket),
    )
    this.memberStates.set(id, memberState)
    void this.initializePresence(id, memberState)
  }

  private teardownPresenceIfEmpty(id: string, key: string, memberState: MemberBucketState): void {
    const registry = this.localRegistries.get(memberState.location.sessionInstanceName)
    if (registry?.hasLocalCallbacks(id, key, memberState.location.locationBucket)) return

    if (memberState.setupInFlight || memberState.pendingPublishes.flushing) {
      memberState.teardownRequested = true
      return
    }

    memberState.stopRefresh()
    this.memberStates.delete(id)
    void this.deletePresence(key, memberState.location.locationBucket, memberState.location.sessionInstanceName)
  }

  private async initializePresence(id: string, memberState: MemberBucketState): Promise<void> {
    try {
      await this.putPresence(
        memberState.key,
        memberState.location.locationBucket,
        memberState.location.sessionInstanceName,
      )
    } finally {
      memberState.setupInFlight = false
    }

    memberState.refreshTimer = setInterval(() => {
      void this.putPresence(
        memberState.key,
        memberState.location.locationBucket,
        memberState.location.sessionInstanceName,
      )
    }, PRESENCE_REFRESH_INTERVAL_MS)

    await memberState.pendingPublishes.flush()

    if (memberState.teardownRequested) {
      memberState.stopRefresh()
      await this.deletePresence(
        memberState.key,
        memberState.location.locationBucket,
        memberState.location.sessionInstanceName,
      )
      this.memberStates.delete(id)
    }
  }

  private resolveMemberLocation(headers?: Headers): MemberLocation {
    assertUsage(
      headers,
      'Ensure createChannel() is called before any `await` in the telefunc, or enable `"nodejs_als"` in your compatibility flags. See https://telefunc.com/getContext#access',
    )

    const locationBucket = headers.get(TELEFUNC_PUBSUB_BUCKET_HEADER)
    assert(
      locationBucket && KNOWN_PUBSUB_BUCKETS.has(locationBucket),
      'Cloudflare keyed pub/sub requires a valid forwarded bucket header. Expected handleTelefunc() to set x-telefunc-pubsub-bucket before Durable Object dispatch.',
    )

    const sessionInstanceName = headers.get(TELEFUNC_SHARD_HEADER)
    assert(
      sessionInstanceName && sessionInstanceName.startsWith(`${this.baseInstanceName}-shard-`),
      'Cloudflare keyed pub/sub registration is missing a valid shard header.',
    )

    return { sessionInstanceName, locationBucket: locationBucket as LocationBucket }
  }

  private getBucketCoordinatorStub(key: string, locationBucket: LocationBucket): TelefuncPubSubStub {
    const bucketShardCount = getBucketCoordinatorShardIndices(this.scale, locationBucket).length
    const bucketShardOrdinal = getDeterministicKeyBucketIndex(key, bucketShardCount)
    return this.getBoundStub(`${this.baseInstanceName}:pubsub:${locationBucket}:${bucketShardOrdinal}`, locationBucket)
  }

  private getAuthorityStub(key: string, locationHint?: DurableObjectLocationHint): TelefuncPubSubStub {
    return this.getBoundStub(`${this.baseInstanceName}:pubsub:authority:${key}`, locationHint)
  }

  private getBoundStub(instanceName: string, locationHint?: DurableObjectLocationHint): TelefuncPubSubStub {
    assert(this.binding, `Missing Cloudflare Durable Object binding "${this.bindingName ?? 'unknown'}".`)
    return this.binding.get(
      this.binding.idFromName(instanceName),
      locationHint ? { locationHint } : undefined,
    ) as TelefuncPubSubStub
  }
}
