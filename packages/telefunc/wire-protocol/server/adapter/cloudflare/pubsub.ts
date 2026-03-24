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
import { getRequestContext } from '../../../../node/server/requestContext.js'
import { assert } from '../../../../utils/assert.js'
import { utf8ByteLength } from '../../../../utils/utf8ByteLength.js'
import type { ChannelPublishAck } from '../../../channel.js'
import type { PubSubSubscription, PubSubTransport } from '../../pubsub.js'
import type { CloudflareScale, LocationBucket } from './routing.js'

const PRESENCE_TTL_SECONDS = 90
const PRESENCE_REFRESH_INTERVAL_MS = 30_000

type CloudflarePubSubTransportOptions = {
  baseInstanceName: string
  scale?: CloudflareScale
}

type PubSubPublishRequest = {
  key: string
  locationBucket: LocationBucket
  serialized: string
  sourceChannelId: string
  sourceSessionInstanceName: string | null
  forwarded?: boolean
  sessionShardNames?: string[]
}

type PubSubDeliverRequest = {
  key: string
  locationBucket: LocationBucket
  serialized: string
  sourceChannelId: string
}

type TelefuncPubSubStub = DurableObjectStub & {
  telefuncPubSubPublish(request: PubSubPublishRequest): Promise<ChannelPublishAck>
  telefuncPubSubDeliver(request: PubSubDeliverRequest): Promise<void>
}

type MemberLocation = {
  sessionInstanceName: string
  locationBucket: LocationBucket
}

type PendingBucketPublish = {
  resolve: (ack: ChannelPublishAck) => void
  reject: (err: Error) => void
}

/**
 * Hard-capped ring buffer for bucket publishes that arrive before the source bucket setup finishes.
 * This bounds cold-path memory while preserving a contiguous FIFO suffix of pending publishes.
 */
class CloudflareBucketPublishBuffer {
  private data: string[] = []
  private sizes: number[] = []
  private pendingPublishes: Array<PendingBucketPublish> = []
  private head = 0
  private totalBytes = 0
  private readonly maxBytes: number
  private readonly sendPublish: (serialized: string) => Promise<ChannelPublishAck>
  private _flushing = false

  constructor(maxBytes: number, sendPublish: (serialized: string) => Promise<ChannelPublishAck>) {
    assert(maxBytes > 0, 'Cloudflare bucket publish buffer size must be > 0.')
    this.maxBytes = maxBytes
    this.sendPublish = sendPublish
  }

  push(serialized: string, pendingPublish: PendingBucketPublish): void {
    const bytes = utf8ByteLength(serialized)

    if (bytes > this.maxBytes) {
      pendingPublish.reject(
        new Error('Keyed publish exceeded the Cloudflare bucket buffer limit during cold-path setup'),
      )
      this.clear()
      return
    }

    this.data.push(serialized)
    this.sizes.push(bytes)
    this.pendingPublishes.push(pendingPublish)
    this.totalBytes += bytes
    this.evict()
  }

  get flushing(): boolean {
    return this._flushing
  }

  async flush(): Promise<void> {
    if (this._flushing) return
    this._flushing = true
    try {
      while (this.head < this.data.length) {
        const pendingPublish = this.pendingPublishes[this.head]!
        try {
          const ack = await this.sendPublish(this.data[this.head]!)
          pendingPublish.resolve(ack)
        } catch (err) {
          pendingPublish.reject(err instanceof Error ? err : new Error(String(err)))
        }
        this.totalBytes -= this.sizes[this.head]!
        this.head++
      }
      this.compact()
    } finally {
      this._flushing = false
    }
  }

  clear(): void {
    this.rejectRange(
      this.head,
      this.pendingPublishes.length,
      'Keyed publish was dropped from the Cloudflare bucket buffer',
    )
    this.data.length = 0
    this.sizes.length = 0
    this.pendingPublishes.length = 0
    this.head = 0
    this.totalBytes = 0
  }

  private evict(): void {
    while (this.totalBytes > this.maxBytes && this.head < this.data.length) {
      this.pendingPublishes[this.head]!.reject(
        new Error('Keyed publish was evicted from the Cloudflare bucket buffer before bucket setup completed'),
      )
      this.totalBytes -= this.sizes[this.head]!
      this.head++
    }
    this.compact()
  }

  private compact(): void {
    if (this.head === 0) return
    if (this.head < this.data.length - this.head) return
    this.data = this.data.slice(this.head)
    this.sizes = this.sizes.slice(this.head)
    this.pendingPublishes = this.pendingPublishes.slice(this.head)
    this.head = 0
  }

  private rejectRange(start: number, end: number, message: string): void {
    for (let index = start; index < end; index++) {
      this.pendingPublishes[index]!.reject(new Error(message))
    }
  }
}

class MemberBucketState {
  readonly location: MemberLocation
  readonly key: string
  readonly pendingPublishes: CloudflareBucketPublishBuffer
  setupInFlight = true
  teardownRequested = false
  refreshTimer: ReturnType<typeof setInterval> | null = null
  private readonly publishRequestBase: Omit<PubSubPublishRequest, 'serialized' | 'forwarded' | 'sessionShardNames'>
  private readonly authority: TelefuncPubSubStub

  constructor(key: string, sourceChannelId: string, location: MemberLocation, authority: TelefuncPubSubStub) {
    this.key = key
    this.location = location
    this.publishRequestBase = {
      key,
      locationBucket: location.locationBucket,
      sourceChannelId,
      sourceSessionInstanceName: location.sessionInstanceName,
    }
    this.authority = authority
    this.pendingPublishes = new CloudflareBucketPublishBuffer(CHANNEL_BUFFER_LIMIT_BYTES, (s) => this.publish(s))
  }

  publish(serialized: string): Promise<ChannelPublishAck> {
    return this.authority.telefuncPubSubPublish({
      ...this.publishRequestBase,
      serialized,
    })
  }

  stopRefresh(): void {
    if (this.refreshTimer) {
      clearInterval(this.refreshTimer)
      this.refreshTimer = null
    }
  }
}

class CloudflarePubSubRegistry {
  private readonly state: DurableObjectState
  private readonly localKeyBuckets = new Map<string, Map<LocationBucket, Set<PubSubSubscription>>>()
  private authorityPublishChain: Promise<void> = Promise.resolve()
  private readonly keySeqCache = new Map<string, number>()
  private readonly authorityBucketCache = new Map<string, LocationBucket>()

  constructor(state: DurableObjectState) {
    this.state = state
  }

  /** Records a local subscriber in memory only. */
  registerLocal(subscription: PubSubSubscription, locationBucket: LocationBucket): void {
    const key = subscription.key
    if (!key) return
    let keyBuckets = this.localKeyBuckets.get(key)
    if (!keyBuckets) {
      keyBuckets = new Map()
      this.localKeyBuckets.set(key, keyBuckets)
    }
    let members = keyBuckets.get(locationBucket)
    if (!members) {
      members = new Set()
      keyBuckets.set(locationBucket, members)
    }
    members.add(subscription)
  }

  /** Removes a local subscriber from the in-memory key index. */
  unregisterLocal(subscription: PubSubSubscription, locationBucket: LocationBucket): void {
    const key = subscription.key
    if (!key) return
    const keyBuckets = this.localKeyBuckets.get(key)
    if (!keyBuckets) return
    const members = keyBuckets.get(locationBucket)
    if (!members) return
    members.delete(subscription)
    if (members.size !== 0) return
    keyBuckets.delete(locationBucket)
    if (keyBuckets.size === 0) this.localKeyBuckets.delete(key)
  }

  /** Delivers to only the subscribers that are both local to this DO and in the same key-location-bucket. */
  deliverLocal({ key, locationBucket, serialized, sourceChannelId }: PubSubDeliverRequest): void {
    const members = this.localKeyBuckets.get(key)?.get(locationBucket)
    if (!members) return
    for (const subscription of members) {
      if (subscription.id === sourceChannelId) continue
      subscription.onMessage(serialized, sourceChannelId)
    }
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

class CloudflarePubSubTransport implements PubSubTransport {
  private readonly baseInstanceName: string
  private readonly scale: CloudflareScale | undefined
  private bindingName: string | null = null
  private binding: DurableObjectNamespace | null = null
  private kv: KVNamespace | null = null
  private readonly localRegistries = new Map<string, CloudflarePubSubRegistry>()
  private readonly memberStates = new WeakMap<PubSubSubscription, MemberBucketState>()

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

  // --- PubSubTransport interface ---

  subscribe(subscription: PubSubSubscription): void {
    const memberLocation = this.resolveCurrentMemberLocation()
    const registry = this.localRegistries.get(memberLocation.sessionInstanceName)
    assert(
      registry,
      `No Cloudflare pub/sub registry found for session instance "${memberLocation.sessionInstanceName}".`,
    )
    registry.registerLocal(subscription, memberLocation.locationBucket)

    const memberState = new MemberBucketState(
      subscription.key,
      subscription.id,
      memberLocation,
      this.getAuthorityStub(subscription.key, memberLocation.locationBucket),
    )
    this.memberStates.set(subscription, memberState)
    void this.initializePresence(subscription, memberState)
  }

  publish(subscription: PubSubSubscription, serialized: string): Promise<ChannelPublishAck> {
    const memberState = this.memberStates.get(subscription)
    assert(
      memberState,
      'Cloudflare keyed channel publish was called before the channel was registered. Expected ServerChannel._registerChannel() to run first.',
    )

    if (memberState.setupInFlight || memberState.pendingPublishes.flushing) {
      return new Promise<ChannelPublishAck>((resolve, reject) => {
        memberState.pendingPublishes.push(serialized, { resolve, reject })
      })
    }

    return memberState.publish(serialized)
  }

  unsubscribe(subscription: PubSubSubscription): void {
    const memberState = this.memberStates.get(subscription)
    assert(
      memberState,
      'Cloudflare keyed channel unsubscribe was called before the channel was registered. Expected ServerChannel._registerChannel() to run first.',
    )
    this.localRegistries
      .get(memberState.location.sessionInstanceName)
      ?.unregisterLocal(subscription, memberState.location.locationBucket)

    if (memberState.setupInFlight || memberState.pendingPublishes.flushing) {
      memberState.teardownRequested = true
      return
    }

    memberState.stopRefresh()
    this.memberStates.delete(subscription)
    void this.deletePresence(
      subscription.key,
      memberState.location.locationBucket,
      memberState.location.sessionInstanceName,
    )
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
  ): Promise<ChannelPublishAck> {
    const { key, locationBucket, serialized, sourceChannelId, sourceSessionInstanceName, forwarded = false } = request

    if (forwarded) {
      const sessionShardNames = request.sessionShardNames ?? []
      await Promise.all(
        sessionShardNames.map((sessionInstanceName) =>
          this.getBoundStub(sessionInstanceName).telefuncPubSubDeliver({
            key,
            locationBucket,
            serialized,
            sourceChannelId,
          }),
        ),
      )
      return { key, seq: 0, ts: Date.now() }
    }

    const { authorityBucket, seq, presenceByBucket } = await registry.runInAuthorityChain(async () => ({
      authorityBucket: await registry.getOrInitAuthorityBucket(key, locationBucket),
      seq: await registry.getNextKeySeq(key),
      presenceByBucket: await this.listPresenceByBucket(key),
    }))

    const ts = Date.now()
    const activeBuckets = Array.from(presenceByBucket.keys())
    await Promise.all(
      activeBuckets.map((activeBucket) =>
        this.getBucketCoordinatorStub(key, activeBucket).telefuncPubSubPublish({
          key,
          serialized,
          sourceChannelId,
          sourceSessionInstanceName,
          forwarded: true,
          locationBucket: activeBucket,
          sessionShardNames: presenceByBucket.get(activeBucket)!,
        }),
      ),
    )
    return { key, seq, ts, meta: { authorityBucket, fanoutBuckets: activeBuckets } }
  }

  // --- Private ---

  private async initializePresence(subscription: PubSubSubscription, memberState: MemberBucketState): Promise<void> {
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
      this.memberStates.delete(subscription)
    }
  }

  private resolveCurrentMemberLocation(): MemberLocation {
    const request = getRequestContext()?.request ?? null
    assert(
      request,
      'Cloudflare keyed pub/sub requires request context. Enable Workers AsyncLocalStorage with compatibility_flags: ["nodejs_als"].',
    )

    const locationBucket = request.headers.get(TELEFUNC_PUBSUB_BUCKET_HEADER)
    assert(
      locationBucket && KNOWN_PUBSUB_BUCKETS.has(locationBucket),
      'Cloudflare keyed pub/sub requires a valid forwarded bucket header. Expected handleTelefunc() to set x-telefunc-pubsub-bucket before Durable Object dispatch.',
    )

    const sessionInstanceName = request.headers.get(TELEFUNC_SHARD_HEADER)
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
