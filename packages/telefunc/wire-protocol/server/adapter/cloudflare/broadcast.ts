/// <reference types="@cloudflare/workers-types" />
export { CloudflareBroadcastTransport, CloudflareBroadcastAuthorityState }
export type { BroadcastDeliverRequest, BroadcastForwardRequest, BroadcastPublishRequest, TelefuncDurableObjectStub }

import { KNOWN_BROADCAST_BUCKETS, getBucketCoordinatorShardIndices, getDeterministicKeyBucketIndex } from './routing.js'
import { assert } from '../../../../utils/assert.js'
import type { BroadcastLane, PublishResult } from '../../../backend/broadcast/contract.js'
import { broadcastRouteKey } from '../../../backend/broadcast/route-key.js'
import type { BackendReceiver } from '../../../backend/subscription.js'
import { DriverAttempt } from '../../../backend/attempt.js'
import { createDeferred } from '../../../../utils/createDeferred.js'
import type { OrderingInfo } from '../../../ordering-frame.js'
import type { CloudflareScale, LocationBucket } from './routing.js'

const PRESENCE_TTL_SECONDS = 90
const PRESENCE_REFRESH_INTERVAL_MS = 30_000

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
  kind: BroadcastLane['kind']
  locationBucket: LocationBucket
  payload: Uint8Array
}

/** The authority's sequenced publish, handed to one bucket coordinator for the member DOs it names. */
type BroadcastForwardRequest = {
  key: string
  kind: BroadcastLane['kind']
  payload: Uint8Array
  info: OrderingInfo
  doNames: string[]
}

type BroadcastDeliverRequest = {
  key: string
  kind: BroadcastLane['kind']
  payload: Uint8Array
  info: OrderingInfo
}

type TelefuncDurableObjectStub = DurableObjectStub & {
  telefuncBroadcastPublish(request: BroadcastPublishRequest): Promise<PublishResult>
  telefuncBroadcastForward(request: BroadcastForwardRequest): Promise<void>
  telefuncBroadcastDeliver(request: BroadcastDeliverRequest): Promise<void>
}

/** One lane's KV presence for this isolate's representative DO. */
class MemberBucketState {
  state: 'establishing' | 'ready' | 'lost' = 'establishing'
  teardownRequested = false
  refreshTimer: ReturnType<typeof setInterval> | null = null
  /** Publishes reuse this stub: calls through one stub arrive in order, calls through fresh stubs don't. */
  readonly authority: TelefuncDurableObjectStub
  readonly #setup = createDeferred()
  readonly #presenceListeners = new Set<(state: 'ready' | 'lost') => void>()

  constructor(authority: TelefuncDurableObjectStub) {
    this.authority = authority
    void this.#setup.promise.catch(() => {})
  }

  /** Settles with the first presence write. */
  get ready(): Promise<void> {
    return this.#setup.promise
  }

  acknowledgePresence(): void {
    const recovered = this.state === 'lost'
    this.state = 'ready'
    this.#setup.resolve()
    if (recovered) this.#notifyPresenceState('ready')
  }

  rejectPresence(error: unknown): void {
    this.#setup.reject(error)
  }

  losePresence(): void {
    if (this.state !== 'ready') return
    this.state = 'lost'
    this.#notifyPresenceState('lost')
  }

  onPresenceStateChange(cb: (state: 'ready' | 'lost') => void): () => void {
    this.#presenceListeners.add(cb)
    return () => this.#presenceListeners.delete(cb)
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

/** Follows its lane's presence: ready once presence is written, lost while a refresh fails. */
class CloudflareBroadcastSubscriptionAttempt extends DriverAttempt {
  readonly #receiver: BackendReceiver
  readonly #detach: () => Promise<void>
  readonly #stopPresenceObservation: () => void
  #unsubscribed = false

  constructor(member: MemberBucketState, receiver: BackendReceiver, detach: () => Promise<void>) {
    super()
    this.#receiver = receiver
    this.#detach = detach
    this.#stopPresenceObservation = member.onPresenceStateChange((state) => this.transition(state))
    member.ready.then(
      () => this.transition('ready'),
      (error: unknown) => {
        this.#stopPresenceObservation()
        this.transition('closed', error)
      },
    )
  }

  async deliver(payload: Uint8Array, info: OrderingInfo): Promise<void> {
    if (this.state() !== 'ready') return
    await this.#receiver(payload, info)
  }

  async unsubscribe(): Promise<void> {
    if (this.#unsubscribed) return
    this.#unsubscribed = true
    this.#stopPresenceObservation()
    this.transition('closed')
    await this.#detach()
  }
}

/** A key authority DO's sequence counters and first-touch authority buckets. */
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

class CloudflareBroadcastTransport {
  private readonly baseInstanceName: string
  private readonly scale: CloudflareScale | undefined
  private bindingName: string | null = null
  private binding: DurableObjectNamespace | null = null
  private kv: KVNamespace | null = null
  private locationBucket: LocationBucket | null = null
  private representativeDOName: string | null = null
  private readonly presenceMutationChains = new Map<string, Promise<void>>()
  private readonly memberStates = new Map<string, MemberBucketState>()
  private readonly subscriptions = new Map<string, CloudflareBroadcastSubscriptionAttempt>()

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

  private getPresenceKey(routeKey: string): string {
    return `${this.getPresencePrefix(routeKey)}${this.locationBucket}:${this.representativeDOName}`
  }

  private getPresencePrefix(routeKey: string): string {
    return `tfps:${routeKey}:`
  }

  private async putPresence(routeKey: string): Promise<void> {
    await this.mutatePresence(routeKey, () => {
      const doName = this.requireRepresentativeDOName()
      return this.requireKV().put(this.getPresenceKey(routeKey), doName, {
        expirationTtl: PRESENCE_TTL_SECONDS,
      })
    })
  }

  private async deletePresence(routeKey: string): Promise<void> {
    await this.mutatePresence(routeKey, () => this.requireKV().delete(this.getPresenceKey(routeKey)))
  }

  private async mutatePresence(routeKey: string, mutation: () => Promise<void>): Promise<void> {
    const current = (this.presenceMutationChains.get(routeKey) ?? Promise.resolve()).catch(() => {}).then(mutation)
    this.presenceMutationChains.set(routeKey, current)
    try {
      await current
    } finally {
      if (this.presenceMutationChains.get(routeKey) === current) this.presenceMutationChains.delete(routeKey)
    }
  }

  private async listPresenceByBucket(routeKey: string): Promise<Map<LocationBucket, string[]>> {
    const kv = this.requireKV()
    const prefix = this.getPresencePrefix(routeKey)
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

  publish(lane: BroadcastLane, payload: Uint8Array): Promise<PublishResult> {
    const locationBucket = this.requireLocationBucket()
    const authority =
      this.memberStates.get(broadcastRouteKey(lane))?.authority ?? this.getAuthorityStub(lane.key, locationBucket)
    return unwrapRpcResult(
      authority.telefuncBroadcastPublish({ key: lane.key, kind: lane.kind, locationBucket, payload }),
    )
  }

  /** At the key's authority: sequences the publish, reads KV presence and forwards once per populated bucket. */
  async publishToSubscribers(
    authorityState: CloudflareBroadcastAuthorityState,
    request: BroadcastPublishRequest,
  ): Promise<PublishResult> {
    const { key, kind, locationBucket, payload } = request
    const { authorityBucket, seq, presenceByBucket } = await authorityState.runInAuthorityChain(async () => ({
      authorityBucket: await authorityState.getOrInitAuthorityBucket(key, locationBucket),
      seq: await authorityState.getNextKeySeq(key),
      presenceByBucket: await this.listPresenceByBucket(broadcastRouteKey({ key, kind })),
    }))

    const info = { seq, timestamp: Date.now() }
    const fanoutBuckets = Array.from(presenceByBucket.keys())
    let receivers = 0
    for (const doNames of presenceByBucket.values()) receivers += doNames.length
    await Promise.all(
      fanoutBuckets.map((activeBucket) =>
        this.getBucketCoordinatorStub(key, activeBucket).telefuncBroadcastForward({
          key,
          kind,
          payload,
          info,
          doNames: presenceByBucket.get(activeBucket)!,
        }),
      ),
    )
    return { seq: info.seq, timestamp: info.timestamp, receivers, meta: { authorityBucket, fanoutBuckets } }
  }

  /** At a bucket coordinator: delivers the authority's sequenced publish to the named member DOs. */
  async forwardToBucket(request: BroadcastForwardRequest): Promise<void> {
    const { doNames, ...delivery } = request
    await Promise.all(doNames.map((doName) => this.getBoundStub(doName).telefuncBroadcastDeliver(delivery)))
  }

  /** Delivers a publish to this isolate's subscription. Called via RPC on its representative DO. */
  async deliverToLocal(request: BroadcastDeliverRequest): Promise<void> {
    await this.subscriptions.get(broadcastRouteKey(request))?.deliver(request.payload, request.info)
  }

  openSubscription(lane: BroadcastLane, receiver: BackendReceiver): CloudflareBroadcastSubscriptionAttempt {
    const routeKey = broadcastRouteKey(lane)
    const member = this.ensurePresence(lane, routeKey)
    const attempt: CloudflareBroadcastSubscriptionAttempt = new CloudflareBroadcastSubscriptionAttempt(
      member,
      receiver,
      async () => {
        if (this.subscriptions.get(routeKey) === attempt) this.subscriptions.delete(routeKey)
        await this.teardownPresenceIfEmpty(routeKey)
      },
    )
    this.subscriptions.set(routeKey, attempt)
    return attempt
  }

  async dispose(): Promise<void> {
    await Promise.allSettled([...this.subscriptions.values()].map((attempt) => attempt.unsubscribe()))
    this.subscriptions.clear()
    await Promise.allSettled([...this.memberStates].map(([routeKey, member]) => this.releasePresence(routeKey, member)))
  }

  private ensurePresence(lane: BroadcastLane, routeKey: string): MemberBucketState {
    const existing = this.memberStates.get(routeKey)
    if (existing !== undefined) {
      existing.teardownRequested = false
      return existing
    }
    const member = new MemberBucketState(this.getAuthorityStub(lane.key, this.requireLocationBucket()))
    this.memberStates.set(routeKey, member)
    // Nobody awaits a deferred teardown; a failed presence delete lapses with the presence TTL.
    void this.initializePresence(routeKey, member).catch(() => {})
    return member
  }

  private async initializePresence(routeKey: string, member: MemberBucketState): Promise<void> {
    try {
      await this.putPresence(routeKey)
    } catch (error) {
      member.rejectPresence(error)
      if (this.memberStates.get(routeKey) === member) this.memberStates.delete(routeKey)
      return
    }
    member.acknowledgePresence()
    if (member.teardownRequested) return this.releasePresence(routeKey, member)
    member.refreshTimer = setInterval(() => {
      void this.putPresence(routeKey).then(
        () => member.acknowledgePresence(),
        () => member.losePresence(),
      )
    }, PRESENCE_REFRESH_INTERVAL_MS)
  }

  private async teardownPresenceIfEmpty(routeKey: string): Promise<void> {
    const member = this.memberStates.get(routeKey)
    if (member === undefined || this.subscriptions.has(routeKey)) return
    if (member.state === 'establishing') {
      member.teardownRequested = true
      return
    }
    await this.releasePresence(routeKey, member)
  }

  private async releasePresence(routeKey: string, member: MemberBucketState): Promise<void> {
    member.stopRefresh()
    if (this.memberStates.get(routeKey) === member) this.memberStates.delete(routeKey)
    await this.deletePresence(routeKey)
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
