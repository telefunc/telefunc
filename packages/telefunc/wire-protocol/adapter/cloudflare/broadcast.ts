/// <reference types="@cloudflare/workers-types" />
export { CloudflareBroadcastTransport, CloudflareBroadcastAuthorityState }
export type { BroadcastDeliverRequest, BroadcastPublishRequest, TelefuncBroadcastStub }

import { CHANNEL_BUFFER_LIMIT_BYTES } from '../../constants.js'
import { KNOWN_BROADCAST_BUCKETS, getBucketCoordinatorShardIndices, getDeterministicKeyBucketIndex } from './routing.js'
import { assert, assertUsage, assertWarning } from '../../../utils/assert.js'
import { utf8ByteLength } from '../../../utils/utf8ByteLength.js'
import type {
  BroadcastBinaryOnMessage,
  BroadcastOnMessage,
  BroadcastPublishResult,
  BroadcastAdapter,
  BroadcastUnsubscribe,
} from '../../broadcast/broadcastAdapter.js'
import type { WirePublishInfo } from '../../wire/connectionFrame.js'
import type { CloudflareScale, LocationBucket } from './routing.js'

const PRESENCE_TTL_SECONDS = 90
const PRESENCE_REFRESH_INTERVAL_MS = 30_000

/** Unwrap Cloudflare DO RPC proxy into a plain object.
 *  RPC properties are lazy stubs that must be awaited to resolve their values. */
async function unwrapRpcResult(rpc: Promise<BroadcastPublishResult>): Promise<BroadcastPublishResult> {
  const r = await rpc
  const [seq, timestamp, meta] = await Promise.all([r.seq, r.timestamp, r.meta])
  return { seq, timestamp, ...(meta ? { meta } : undefined) }
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
  /** The Durable Object it goes to: it delivers to its own subscribers only. */
  doName: string
  info: WirePublishInfo
  serialized?: string
  binaryData?: Uint8Array
}

type TelefuncBroadcastStub = DurableObjectStub & {
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

/** KV takes one write a second per key, so a write it refuses is expected. */
async function warnRefusedWrite(write: Promise<void>, outcome: string): Promise<void> {
  try {
    await write
  } catch (err) {
    assertWarning(false, `A Broadcast presence couldn't be ${outcome}: ${String(err)}`, { onlyOnce: false })
  }
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
    if (this.admit(entry.bytes, entry.pending)) this.append(entry)
  }

  /** Holds a copy of `data` for `send`, made only once the buffer admits it. */
  pushCopy(
    data: Uint8Array,
    send: (data: Uint8Array) => Promise<BroadcastPublishResult>,
    pending: PendingBucketPublish,
  ): void {
    if (!this.admit(data.byteLength, pending)) return
    const copy = new Uint8Array(data)
    this.append({ send: () => send(copy), bytes: copy.byteLength, pending })
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

  /** Whether an entry of `bytes` fits; one that can't clears the buffer and rejects `pending`. */
  private admit(bytes: number, pending: PendingBucketPublish): boolean {
    if (bytes <= this.maxBytes) return true
    pending.reject(new Error('Keyed publish exceeded the Cloudflare bucket buffer limit during cold-path setup'))
    this.clear()
    return false
  }

  private append(entry: BufferedPublish): void {
    this.entries.push(entry)
    this.totalBytes += entry.bytes
    this.evict()
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
  /** Presences being written: publishes from here wait for them, so their own subscribers get them. */
  setupsInFlight = 0
  /** Each Durable Object with subscribers on the key here, with how many: each has a presence of its own. */
  readonly owners = new Map<string, number>()
  refreshTimer: ReturnType<typeof setInterval> | null = null
  /** A stub can only be used by the Durable Object that got it, so each publish gets its own. */
  private readonly authority: () => TelefuncBroadcastStub
  private readonly key: string
  private readonly locationBucket: LocationBucket

  constructor(key: string, locationBucket: LocationBucket, authority: () => TelefuncBroadcastStub) {
    this.key = key
    this.locationBucket = locationBucket
    this.authority = authority
    this.pendingPublishes = new CloudflareBucketPublishBuffer(CHANNEL_BUFFER_LIMIT_BYTES)
  }

  publish(serialized: string): Promise<BroadcastPublishResult> {
    return unwrapRpcResult(
      this.authority().telefuncBroadcastPublish({
        key: this.key,
        locationBucket: this.locationBucket,
        serialized,
      }),
    )
  }

  publishBinary(data: Uint8Array): Promise<BroadcastPublishResult> {
    return unwrapRpcResult(
      this.authority().telefuncBroadcastPublish({
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
//   - textSubs/binarySubs: local subscriber callbacks by key, each with the DO that owns it
//   - memberStates: per key, the presences of the DOs with subscribers here and the publishes waiting on them
//   - requestOwners: each request a DO received, with its name
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
  /** Each subscriber with the Durable Object whose context may write to its page. */
  private readonly textSubs = new Map<string, Map<BroadcastOnMessage, string>>()
  private readonly binarySubs = new Map<string, Map<BroadcastBinaryOnMessage, string>>()
  /** Each request a Durable Object received, with its name: a wire it opens is that Durable Object's. */
  private readonly requestOwners = new WeakMap<Request, string>()

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

  attachRequest(request: Request, doName: string): void {
    this.requestOwners.set(request, doName)
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

  private getPresenceKey(key: string, owner: string): string {
    return `tfps:${encodeURIComponent(key)}:${this.locationBucket}:${owner}`
  }

  private getPresencePrefix(key: string): string {
    return `tfps:${encodeURIComponent(key)}:`
  }

  private async putPresence(key: string, owner: string): Promise<void> {
    const written = this.requireKV().put(this.getPresenceKey(key, owner), owner, {
      expirationTtl: PRESENCE_TTL_SECONDS,
    })
    await warnRefusedWrite(
      written,
      `written, and is written again at its next refresh, within ${PRESENCE_REFRESH_INTERVAL_MS / 1000} s`,
    )
  }

  private async deletePresence(key: string, owner: string): Promise<void> {
    await warnRefusedWrite(
      this.requireKV().delete(this.getPresenceKey(key, owner)),
      `deleted, and expires within ${PRESENCE_TTL_SECONDS} s`,
    )
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

  private deliverLocal({ key, doName, serialized, binaryData, info }: BroadcastDeliverRequest): void {
    if (serialized !== undefined) {
      const subs = this.textSubs.get(key)
      if (subs) for (const [cb, owner] of subs) if (owner === doName) cb(serialized, info)
    }
    if (binaryData !== undefined) {
      const subs = this.binarySubs.get(key)
      if (subs) for (const [cb, owner] of subs) if (owner === doName) cb(binaryData, info)
    }
  }

  /** The Durable Object that may write to a subscriber's page; the server's own subscribers go with the isolate's first. */
  private ownerOf(wireRequest: Request | undefined): string {
    if (!wireRequest) return this.requireRepresentativeDOName()
    const owner = this.requestOwners.get(wireRequest)
    assert(owner, 'a wire opened by a request no Durable Object received')
    return owner
  }

  /** Adds `onMessage` to `subsByKey` under `key`, with its owner's presence; the returned function removes both. */
  private addSubscriber<OnMessage>(
    subsByKey: Map<string, Map<OnMessage, string>>,
    key: string,
    onMessage: OnMessage,
    wireRequest: Request | undefined,
  ): BroadcastUnsubscribe {
    const locationBucket = this.requireLocationBucket()
    const owner = this.ownerOf(wireRequest)
    let subs = subsByKey.get(key)
    if (!subs) {
      subs = new Map()
      subsByKey.set(key, subs)
    }
    // A function subscribed again keeps its one entry and its one presence count, and either unsubscribe removes it.
    if (!subs.has(onMessage)) {
      subs.set(onMessage, owner)
      this.ensurePresence(key, locationBucket, owner)
    }
    return () => {
      const current = subsByKey.get(key)
      const entryOwner = current?.get(onMessage)
      if (!current || entryOwner === undefined) return
      current.delete(onMessage)
      if (current.size === 0) subsByKey.delete(key)
      this.releasePresence(key, entryOwner)
    }
  }

  // --- BroadcastAdapter interface ---

  subscribe(key: string, onMessage: BroadcastOnMessage, wireRequest?: Request): BroadcastUnsubscribe {
    return this.addSubscriber(this.textSubs, key, onMessage, wireRequest)
  }

  subscribeBinary(key: string, onMessage: BroadcastBinaryOnMessage, wireRequest?: Request): BroadcastUnsubscribe {
    return this.addSubscriber(this.binarySubs, key, onMessage, wireRequest)
  }

  publish(key: string, serialized: string): Promise<BroadcastPublishResult> {
    const memberState = this.memberStates.get(key)

    if (memberState) {
      if (memberState.setupsInFlight > 0 || memberState.pendingPublishes.flushing) {
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
      if (memberState.setupsInFlight > 0 || memberState.pendingPublishes.flushing) {
        // The caller may reuse or transfer its buffer once the publish returns.
        return new Promise<BroadcastPublishResult>((resolve, reject) => {
          memberState.pendingPublishes.pushCopy(data, (queuedData) => memberState.publishBinary(queuedData), {
            resolve,
            reject,
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
          this.getBoundStub(doName).telefuncBroadcastDeliver({ key, doName, serialized, binaryData, info }),
        ),
      )
      return { seq: info.seq, timestamp: info.timestamp }
    }

    const { authorityBucket, seq, presenceByBucket } = await authorityState.runInAuthorityChain(async () => ({
      authorityBucket: await authorityState.getOrInitAuthorityBucket(key, locationBucket),
      seq: await authorityState.getNextKeySeq(key),
      presenceByBucket: await this.listPresenceByBucket(key),
    }))

    const timestamp = Date.now()
    const info = { seq, timestamp }
    const activeBuckets = Array.from(presenceByBucket.keys())
    await Promise.all(
      activeBuckets.map((activeBucket) =>
        this.getBucketCoordinatorStub(key, activeBucket).telefuncBroadcastPublish({
          key,
          serialized,
          binaryData,
          forwarded: true,
          locationBucket: activeBucket,
          doNames: presenceByBucket.get(activeBucket)!,
          info,
        }),
      ),
    )
    return { seq, timestamp, meta: { authorityBucket, fanoutBuckets: activeBuckets } }
  }

  /**
   * Delivers a publish to local subscribers. Called via RPC on the representative DO for this isolate.
   */
  deliverToLocal(request: BroadcastDeliverRequest): void {
    this.deliverLocal(request)
  }

  // --- Private ---

  private ensurePresence(key: string, locationBucket: LocationBucket, owner: string): void {
    let memberState = this.memberStates.get(key)
    if (!memberState) {
      memberState = new MemberBucketState(key, locationBucket, () => this.getAuthorityStub(key, locationBucket))
      this.memberStates.set(key, memberState)
    }
    const count = memberState.owners.get(owner) ?? 0
    memberState.owners.set(owner, count + 1)
    if (count === 0) void this.initializePresence(key, memberState, owner)
  }

  private releasePresence(key: string, owner: string): void {
    const memberState = this.memberStates.get(key)
    // Its subscriber was still there, so the key's state is too.
    assert(memberState)
    const count = memberState.owners.get(owner)! - 1
    if (count > 0) memberState.owners.set(owner, count)
    else {
      memberState.owners.delete(owner)
      void this.deletePresence(key, owner)
    }
    this.teardownPresenceIfEmpty(key)
  }

  private teardownPresenceIfEmpty(key: string): void {
    if (this.hasLocalCallbacks(key)) return
    const memberState = this.memberStates.get(key)
    if (!memberState) return

    // The setup in flight tears it down once done, if no subscriber came meanwhile.
    if (memberState.setupsInFlight > 0 || memberState.pendingPublishes.flushing) return

    memberState.stopRefresh()
    this.memberStates.delete(key)
  }

  private async initializePresence(key: string, memberState: MemberBucketState, owner: string): Promise<void> {
    memberState.setupsInFlight++
    try {
      await this.putPresence(key, owner)
    } finally {
      memberState.setupsInFlight--
    }
    // Its release may have deleted the presence before the put above wrote it.
    if (!memberState.owners.has(owner)) void this.deletePresence(key, owner)
    if (memberState.setupsInFlight > 0) return

    memberState.refreshTimer ??= setInterval(() => {
      for (const current of memberState.owners.keys()) void this.putPresence(key, current)
    }, PRESENCE_REFRESH_INTERVAL_MS)

    await memberState.pendingPublishes.flush()

    this.teardownPresenceIfEmpty(key)
  }

  private getBucketCoordinatorStub(key: string, locationBucket: LocationBucket): TelefuncBroadcastStub {
    const bucketShardCount = getBucketCoordinatorShardIndices(this.scale, locationBucket).length
    const bucketShardOrdinal = getDeterministicKeyBucketIndex(key, bucketShardCount)
    return this.getBoundStub(
      `${this.baseInstanceName}:broadcast:${locationBucket}:${bucketShardOrdinal}`,
      locationBucket,
    )
  }

  private getAuthorityStub(key: string, locationHint?: DurableObjectLocationHint): TelefuncBroadcastStub {
    return this.getBoundStub(`${this.baseInstanceName}:broadcast:authority:${key}`, locationHint)
  }

  private getBoundStub(instanceName: string, locationHint?: DurableObjectLocationHint): TelefuncBroadcastStub {
    assert(this.binding, `Missing Cloudflare Durable Object binding "${this.bindingName ?? 'unknown'}".`)
    return this.binding.get(
      this.binding.idFromName(instanceName),
      locationHint ? { locationHint } : undefined,
    ) as TelefuncBroadcastStub
  }
}
