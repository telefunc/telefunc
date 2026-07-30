/// <reference types="@cloudflare/workers-types" />
export { CloudflareBroadcastTransport, CloudflareBroadcastAuthorityState }
export type { BroadcastDeliverRequest, BroadcastPublishRequest, TelefuncDurableObjectStub }

import { KNOWN_BROADCAST_BUCKETS, getBucketCoordinatorShardIndices, getDeterministicKeyBucketIndex } from './routing.js'
import { assert } from '../../../../utils/assert.js'
import type {
  BackendReceiver,
  BroadcastLane,
  PublishResult,
  SubscriptionAttempt,
  SubscriptionState,
} from '../../../backend/spi.js'
import { decodeOrderingFrame, encodeOrderingFrame, type OrderingInfo } from '../../../ordering-frame.js'
import type { CloudflareScale, LocationBucket } from './routing.js'

const PRESENCE_TTL_SECONDS = 90
const PRESENCE_REFRESH_INTERVAL_MS = 30_000
const textEncoder = new TextEncoder()
const textDecoder = new TextDecoder()
const laneKey = (lane: BroadcastLane) => `${lane.kind}:${lane.key}`

/** Unwrap Cloudflare DO RPC proxy into a plain object.
 *  RPC properties are lazy stubs that must be awaited to resolve their values. */
async function unwrapRpcResult(rpc: Promise<PublishResult>): Promise<PublishResult> {
  const r = await rpc
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
  info?: OrderingInfo
  serialized?: string
  binaryData?: Uint8Array
}

type BroadcastDeliverRequest = {
  key: string
  kind: BroadcastLane['kind']
  frame: Uint8Array
}

type TelefuncDurableObjectStub = DurableObjectStub & {
  telefuncBroadcastPublish(request: BroadcastPublishRequest): Promise<PublishResult>
  telefuncBroadcastDeliver(request: BroadcastDeliverRequest): Promise<void>
}

class MemberBucketState {
  setupInFlight = true
  teardownRequested = false
  refreshTimer: ReturnType<typeof setInterval> | null = null
  readonly ready: Promise<void>
  private settleReady!: { resolve: () => void; reject: (error: unknown) => void }
  private readonly authority: TelefuncDurableObjectStub
  private readonly key: string
  private readonly locationBucket: LocationBucket
  readonly #presenceListeners = new Set<(state: 'ready' | 'lost') => void>()
  #presenceState: 'establishing' | 'ready' | 'lost' = 'establishing'

  constructor(key: string, locationBucket: LocationBucket, authority: TelefuncDurableObjectStub) {
    this.key = key
    this.locationBucket = locationBucket
    this.authority = authority
    this.ready = new Promise((resolve, reject) => {
      this.settleReady = { resolve, reject }
    })
    void this.ready.catch(() => {})
  }

  acknowledgePresence(): void {
    const recovered = this.#presenceState === 'lost'
    this.#presenceState = 'ready'
    this.settleReady.resolve()
    if (recovered) this.#notifyPresenceState('ready')
  }

  rejectPresence(error: unknown): void {
    this.settleReady.reject(error)
  }

  losePresence(): void {
    if (this.#presenceState !== 'ready') return
    this.#presenceState = 'lost'
    this.#notifyPresenceState('lost')
  }

  onPresenceStateChange(cb: (state: 'ready' | 'lost') => void): () => void {
    this.#presenceListeners.add(cb)
    return () => this.#presenceListeners.delete(cb)
  }

  publish(serialized: string): Promise<PublishResult> {
    return unwrapRpcResult(
      this.authority.telefuncBroadcastPublish({
        key: this.key,
        locationBucket: this.locationBucket,
        serialized,
      }),
    )
  }

  publishBinary(data: Uint8Array): Promise<PublishResult> {
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

  #notifyPresenceState(state: 'ready' | 'lost'): void {
    for (const listener of [...this.#presenceListeners]) listener(state)
  }
}

// ---------------------------------------------------------------------------
// CloudflareBroadcastAuthorityState — wraps DurableObjectState for seq counters
// and authority bucket persistence. One per DO instance, passed to publishToSubscribers.
// ---------------------------------------------------------------------------

class CloudflareBroadcastSubscriptionAttempt implements SubscriptionAttempt {
  readonly ready: Promise<void>
  readonly #receiver: BackendReceiver
  readonly #detach: () => Promise<void>
  readonly #listeners = new Set<(state: SubscriptionState) => void>()
  readonly #stopPresenceObservation: () => void
  #state: SubscriptionState = 'establishing'
  #unsubscribed = false

  constructor(member: MemberBucketState, receiver: BackendReceiver, detach: () => Promise<void>) {
    this.#receiver = receiver
    this.#detach = detach
    this.#stopPresenceObservation = member.onPresenceStateChange((state) => {
      if (!this.#unsubscribed && this.#state !== 'closed') this.#transition(state)
    })
    this.ready = member.ready.then(
      () => {
        if (this.#state !== 'closed') this.#transition('ready')
      },
      (error) => {
        this.#stopPresenceObservation()
        this.#transition('closed')
        throw error
      },
    )
    void this.ready.catch(() => {})
  }

  state(): SubscriptionState {
    return this.#state
  }

  onStateChange(cb: (state: SubscriptionState) => void): () => void {
    this.#listeners.add(cb)
    return () => this.#listeners.delete(cb)
  }

  async deliver(payload: Uint8Array, info: OrderingInfo): Promise<void> {
    if (this.#state !== 'ready') return
    await (this.#receiver(payload, info) as unknown)
  }

  async unsubscribe(): Promise<void> {
    if (this.#unsubscribed) return
    this.#unsubscribed = true
    this.#stopPresenceObservation()
    this.#transition('closed')
    await this.#detach()
  }

  #transition(state: SubscriptionState): void {
    if (this.#state === state) return
    this.#state = state
    for (const listener of [...this.#listeners]) listener(state)
  }
}

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
    assert(
      Number.isSafeInteger(currentSeq) && currentSeq >= 0 && currentSeq < Number.MAX_SAFE_INTEGER,
      'Cloudflare Broadcast sequence exhausted for the ordering domain',
    )
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

class CloudflareBroadcastTransport {
  private readonly baseInstanceName: string
  private readonly scale: CloudflareScale | undefined
  private bindingName: string | null = null
  private binding: DurableObjectNamespace | null = null
  private kv: KVNamespace | null = null
  private locationBucket: LocationBucket | null = null
  private representativeDOName: string | null = null
  private readonly memberStates = new Map<string, MemberBucketState>()
  private readonly textSubs = new Map<string, CloudflareBroadcastSubscriptionAttempt>()
  private readonly binarySubs = new Map<string, CloudflareBroadcastSubscriptionAttempt>()

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

  publish(lane: BroadcastLane, payload: Uint8Array): Promise<PublishResult> {
    return lane.kind === 'text'
      ? this.publishText(lane.key, textDecoder.decode(payload))
      : this.publishBinary(lane.key, payload)
  }

  private publishText(key: string, serialized: string): Promise<PublishResult> {
    const memberState = this.memberStates.get(laneKey({ key, kind: 'text' }))

    if (memberState) return memberState.publish(serialized)

    const locationBucket = this.requireLocationBucket()
    const authority = this.getAuthorityStub(key, locationBucket)
    return unwrapRpcResult(authority.telefuncBroadcastPublish({ key, locationBucket, serialized }))
  }

  private publishBinary(key: string, data: Uint8Array): Promise<PublishResult> {
    const memberState = this.memberStates.get(laneKey({ key, kind: 'binary' }))

    if (memberState) return memberState.publishBinary(data)

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
  ): Promise<PublishResult> {
    const { key, locationBucket, serialized, binaryData, forwarded = false } = request
    const kind = serialized === undefined ? 'binary' : 'text'
    if (forwarded) {
      assert(request.info, 'Forwarded publish must include info')
      const info = request.info
      const doNames = request.doNames ?? []
      const payload = serialized === undefined ? binaryData : textEncoder.encode(serialized)
      assert(payload !== undefined, 'Forwarded publish must include a payload')
      const frame = encodeOrderingFrame(payload, info)
      await Promise.all(
        doNames.map((doName) => this.getBoundStub(doName).telefuncBroadcastDeliver({ key, kind, frame })),
      )
      return { seq: info.seq, timestamp: info.timestamp }
    }

    const { authorityBucket, seq, presenceByBucket } = await authorityState.runInAuthorityChain(async () => ({
      authorityBucket: await authorityState.getOrInitAuthorityBucket(key, locationBucket),
      seq: await authorityState.getNextKeySeq(key),
      presenceByBucket: await this.listPresenceByBucket(laneKey({ key, kind })),
    }))

    const info = { seq, timestamp: Date.now() }
    const fanoutBuckets = Array.from(presenceByBucket.keys())
    let receivers = 0
    for (const doNames of presenceByBucket.values()) receivers += doNames.length
    await Promise.all(
      fanoutBuckets.map((activeBucket) =>
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
    return { seq: info.seq, timestamp: info.timestamp, receivers, meta: { authorityBucket, fanoutBuckets } }
  }

  /**
   * Delivers a publish to local subscribers. Called via RPC on the representative DO for this isolate.
   */
  async deliverToLocal(request: BroadcastDeliverRequest): Promise<void> {
    const { payload, info } = decodeOrderingFrame(request.frame)
    const attempt = (request.kind === 'text' ? this.textSubs : this.binarySubs).get(request.key)
    await attempt?.deliver(payload, info)
  }

  // --- Private ---

  openSubscription(lane: BroadcastLane, receiver: BackendReceiver): CloudflareBroadcastSubscriptionAttempt {
    const locationBucket = this.requireLocationBucket()
    const member = this.ensurePresence(lane, locationBucket)
    const routes = lane.kind === 'text' ? this.textSubs : this.binarySubs
    let attempt!: CloudflareBroadcastSubscriptionAttempt
    attempt = new CloudflareBroadcastSubscriptionAttempt(member, receiver, async () => {
      if (routes.get(lane.key) === attempt) routes.delete(lane.key)
      await this.teardownPresenceIfEmpty(lane)
    })
    routes.set(lane.key, attempt)
    return attempt
  }

  private ensurePresence(lane: BroadcastLane, locationBucket: LocationBucket): MemberBucketState {
    const key = laneKey(lane)
    const existing = this.memberStates.get(key)
    if (existing !== undefined) return existing
    const memberState = new MemberBucketState(lane.key, locationBucket, this.getAuthorityStub(lane.key, locationBucket))
    this.memberStates.set(key, memberState)
    void this.initializePresence(key, memberState).catch(() => {})
    return memberState
  }

  private async teardownPresenceIfEmpty(lane: BroadcastLane): Promise<void> {
    if ((lane.kind === 'text' ? this.textSubs : this.binarySubs).has(lane.key)) return
    const key = laneKey(lane)
    const memberState = this.memberStates.get(key)
    if (!memberState) return

    if (memberState.setupInFlight) {
      memberState.teardownRequested = true
      return
    }

    memberState.stopRefresh()
    this.memberStates.delete(key)
    await this.deletePresence(key)
  }

  private async initializePresence(key: string, memberState: MemberBucketState): Promise<void> {
    try {
      await this.putPresence(key)
      memberState.setupInFlight = false
      memberState.acknowledgePresence()
    } catch (error) {
      memberState.setupInFlight = false
      memberState.rejectPresence(error)
      if (this.memberStates.get(key) === memberState) this.memberStates.delete(key)
      throw error
    }

    memberState.refreshTimer = setInterval(() => {
      void this.putPresence(key).then(
        () => memberState.acknowledgePresence(),
        () => memberState.losePresence(),
      )
    }, PRESENCE_REFRESH_INTERVAL_MS)

    if (memberState.teardownRequested) {
      memberState.stopRefresh()
      await this.deletePresence(key)
      if (this.memberStates.get(key) === memberState) this.memberStates.delete(key)
    }
  }

  async dispose(): Promise<void> {
    const attempts = [...this.textSubs.values(), ...this.binarySubs.values()]
    await Promise.allSettled(attempts.map((attempt) => attempt.unsubscribe()))
    const states = [...this.memberStates.entries()]
    this.memberStates.clear()
    this.textSubs.clear()
    this.binarySubs.clear()
    await Promise.allSettled(
      states.map(async ([key, state]) => {
        state.stopRefresh()
        await this.deletePresence(key)
      }),
    )
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
