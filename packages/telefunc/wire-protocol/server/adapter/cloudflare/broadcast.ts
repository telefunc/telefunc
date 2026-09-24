/// <reference types="@cloudflare/workers-types" />
export { CloudflareBroadcastTransport, CloudflareBroadcastAuthorityState }
export type {
  BroadcastCalls,
  BroadcastDeliverRequest,
  BroadcastForwardRequest,
  BroadcastPresenceRequest,
  BroadcastPublishRequest,
  TelefuncDurableObjectStub,
}

import { KNOWN_BROADCAST_BUCKETS, getBucketCoordinatorShardIndices, getDeterministicKeyBucketIndex } from './routing.js'
import type { OrderedStubs } from './ordered-stubs.js'
import { assert } from '../../../../utils/assert.js'
import type { BroadcastLane, PublishResult } from '../../../backend/broadcast/contract.js'
import { broadcastRouteKey } from '../../../backend/broadcast/route-key.js'
import type { BackendReceiver } from '../../../backend/subscription.js'
import { DriverAttempt } from '../../../backend/attempt.js'
import { createDeferred } from '../../../../utils/createDeferred.js'
import type { OrderingInfo } from '../../../ordering-frame.js'
import type { CloudflareScale, LocationBucket } from './routing.js'

const PRESENCE_TTL_MS = 90_000
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

/** A member DO's presence on a lane, at the key's authority; `bucket: null` withdraws it. */
type BroadcastPresenceRequest = {
  key: string
  kind: BroadcastLane['kind']
  member: string
  bucket: LocationBucket | null
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
  telefuncBroadcastPresence(request: BroadcastPresenceRequest): Promise<void>
}

/** One DO's outgoing Broadcast calls. */
type BroadcastCalls = OrderedStubs<TelefuncDurableObjectStub>

/** One lane's presence, at the key's authority, for this isolate's representative DO. */
class MemberBucketState {
  state: 'establishing' | 'ready' | 'lost' = 'establishing'
  teardownRequested = false
  refreshTimer: ReturnType<typeof setInterval> | null = null
  readonly lane: BroadcastLane
  /** Publishes reuse this stub: calls through one stub arrive in order, calls through fresh stubs don't. */
  readonly authority: TelefuncDurableObjectStub
  readonly #setup = createDeferred()
  readonly #presenceListeners = new Set<(state: 'ready' | 'lost') => void>()

  constructor(lane: BroadcastLane, authority: TelefuncDurableObjectStub) {
    this.lane = lane
    this.authority = authority
    void this.#setup.promise.catch(() => {})
  }

  /** Settles once the authority first holds this presence. */
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

/** Follows its lane's presence: ready once the authority holds it, lost while a refresh fails. */
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

const AUTHORITY_SCHEMA = `
  CREATE TABLE IF NOT EXISTS broadcast_key (key TEXT PRIMARY KEY, seq INTEGER NOT NULL, authority_bucket TEXT NOT NULL);
  CREATE TABLE IF NOT EXISTS broadcast_presence
    (route_key TEXT NOT NULL, member TEXT NOT NULL, bucket TEXT NOT NULL, expires_at INTEGER NOT NULL, PRIMARY KEY (route_key, member));
`

/** A key authority DO's Broadcast state, in its SQLite storage: each key's `seq` and first-touch bucket, and each
 *  lane's member presence. The tables are created on first use, so a DO in another role never has them. */
class CloudflareBroadcastAuthorityState {
  readonly #storage: DurableObjectStorage
  #schema = false

  constructor(state: DurableObjectState) {
    this.#storage = state.storage
  }

  /** The key's next `seq`; the key's first publish fixes its authority bucket. */
  sequence(key: string, preferredBucket: LocationBucket): { seq: number; authorityBucket: LocationBucket } {
    const sql = this.#sql()
    return this.#storage.transactionSync(() => {
      const row = sql
        .exec<{ seq: number; authority_bucket: LocationBucket }>(
          'SELECT seq, authority_bucket FROM broadcast_key WHERE key = ?',
          key,
        )
        .toArray()[0]
      const current = row?.seq ?? 0
      assert(current < Number.MAX_SAFE_INTEGER, 'Cloudflare Broadcast sequence exhausted for the ordering domain')
      const authorityBucket = row?.authority_bucket ?? preferredBucket
      sql.exec(
        'INSERT OR REPLACE INTO broadcast_key (key, seq, authority_bucket) VALUES (?, ?, ?)',
        key,
        current + 1,
        authorityBucket,
      )
      return { seq: current + 1, authorityBucket }
    })
  }

  /** Records or withdraws a member DO, dropping the lane's lapsed entries; the RPC reply waits for the write. */
  setPresence({ key, kind, member, bucket }: BroadcastPresenceRequest): void {
    const sql = this.#sql()
    const routeKey = broadcastRouteKey({ key, kind })
    const now = Date.now()
    this.#storage.transactionSync(() => {
      sql.exec(
        'DELETE FROM broadcast_presence WHERE route_key = ? AND (member = ? OR expires_at <= ?)',
        routeKey,
        member,
        now,
      )
      if (bucket === null) return
      sql.exec(
        'INSERT INTO broadcast_presence (route_key, member, bucket, expires_at) VALUES (?, ?, ?, ?)',
        routeKey,
        member,
        bucket,
        now + PRESENCE_TTL_MS,
      )
    })
  }

  /** The lane's unexpired member DOs by bucket. */
  livePresence(routeKey: string, now: number): Map<LocationBucket, string[]> {
    const byBucket = new Map<LocationBucket, string[]>()
    const rows = this.#sql()
      .exec<{ member: string; bucket: LocationBucket }>(
        'SELECT member, bucket FROM broadcast_presence WHERE route_key = ? AND expires_at > ?',
        routeKey,
        now,
      )
      .toArray()
    for (const { member, bucket } of rows) {
      const doNames = byBucket.get(bucket)
      if (doNames === undefined) byBucket.set(bucket, [member])
      else doNames.push(member)
    }
    return byBucket
  }

  #sql(): SqlStorage {
    if (!this.#schema) {
      this.#storage.sql.exec(AUTHORITY_SCHEMA)
      this.#schema = true
    }
    return this.#storage.sql
  }
}

class CloudflareBroadcastTransport {
  private readonly baseInstanceName: string
  private readonly scale: CloudflareScale | undefined
  private bindingName: string | null = null
  private binding: DurableObjectNamespace | null = null
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

  private requireLocationBucket(): LocationBucket {
    assert(this.locationBucket, 'Expected attachIsolateInfo() to be called before subscribe/publish.')
    return this.locationBucket
  }

  private requireRepresentativeDOName(): string {
    assert(this.representativeDOName, 'Expected attachIsolateInfo() to be called before subscribe/publish.')
    return this.representativeDOName
  }

  private async putPresence(lane: BroadcastLane): Promise<void> {
    await this.mutatePresence(lane, this.requireLocationBucket())
  }

  private async deletePresence(lane: BroadcastLane): Promise<void> {
    await this.mutatePresence(lane, null)
  }

  /** One lane's presence writes go one at a time, each through a fresh stub, which belongs to whichever DO calls. */
  private async mutatePresence(lane: BroadcastLane, bucket: LocationBucket | null): Promise<void> {
    const routeKey = broadcastRouteKey(lane)
    const request = { key: lane.key, kind: lane.kind, member: this.requireRepresentativeDOName(), bucket }
    const current = (this.presenceMutationChains.get(routeKey) ?? Promise.resolve())
      .catch(() => {})
      .then(() => this.getAuthorityStub(lane.key, this.requireLocationBucket()).telefuncBroadcastPresence(request))
    this.presenceMutationChains.set(routeKey, current)
    try {
      await current
    } finally {
      if (this.presenceMutationChains.get(routeKey) === current) this.presenceMutationChains.delete(routeKey)
    }
  }

  publish(lane: BroadcastLane, payload: Uint8Array): Promise<PublishResult> {
    const locationBucket = this.requireLocationBucket()
    const authority =
      this.memberStates.get(broadcastRouteKey(lane))?.authority ?? this.getAuthorityStub(lane.key, locationBucket)
    return unwrapRpcResult(
      authority.telefuncBroadcastPublish({ key: lane.key, kind: lane.kind, locationBucket, payload }),
    )
  }

  /** At the key's authority: sequences the publish, reads its presence and forwards once per populated bucket, all in
   *  one synchronous turn, so forwards leave through this DO's ordered stubs in `seq` order. */
  async publishToSubscribers(
    authorityState: CloudflareBroadcastAuthorityState,
    calls: BroadcastCalls,
    request: BroadcastPublishRequest,
  ): Promise<PublishResult> {
    const { key, kind, locationBucket, payload } = request
    const { seq, authorityBucket } = authorityState.sequence(key, locationBucket)
    const info = { seq, timestamp: Date.now() }
    const presenceByBucket = authorityState.livePresence(broadcastRouteKey({ key, kind }), info.timestamp)
    const fanoutBuckets = Array.from(presenceByBucket.keys())
    let receivers = 0
    for (const doNames of presenceByBucket.values()) receivers += doNames.length
    await Promise.all(
      fanoutBuckets.map((bucket) =>
        this.call(calls, this.getBucketCoordinatorName(key, bucket), bucket, (coordinator) =>
          coordinator.telefuncBroadcastForward({ key, kind, payload, info, doNames: presenceByBucket.get(bucket)! }),
        ),
      ),
    )
    return { ...info, receivers, meta: { authorityBucket, fanoutBuckets } }
  }

  /** At a bucket coordinator: delivers the authority's sequenced publish to the named member DOs, in arrival order. */
  async forwardToBucket(calls: BroadcastCalls, request: BroadcastForwardRequest): Promise<void> {
    const { doNames, ...delivery } = request
    await Promise.all(
      doNames.map((doName) =>
        this.call(calls, doName, undefined, (member) => member.telefuncBroadcastDeliver(delivery)),
      ),
    )
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
    const member = new MemberBucketState(lane, this.getAuthorityStub(lane.key, this.requireLocationBucket()))
    this.memberStates.set(routeKey, member)
    // Nobody awaits a deferred teardown; a failed presence delete lapses with the presence TTL.
    void this.initializePresence(routeKey, member).catch(() => {})
    return member
  }

  private async initializePresence(routeKey: string, member: MemberBucketState): Promise<void> {
    try {
      await this.putPresence(member.lane)
    } catch (error) {
      member.rejectPresence(error)
      if (this.memberStates.get(routeKey) === member) this.memberStates.delete(routeKey)
      return
    }
    member.acknowledgePresence()
    if (member.teardownRequested) return this.releasePresence(routeKey, member)
    member.refreshTimer = setInterval(() => {
      void this.putPresence(member.lane).then(
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
    await this.deletePresence(member.lane)
  }

  /** A call from the DO owning `calls`, through its stub for `instanceName`. */
  private call<T>(
    calls: BroadcastCalls,
    instanceName: string,
    locationHint: DurableObjectLocationHint | undefined,
    invoke: (stub: TelefuncDurableObjectStub) => Promise<T>,
  ): Promise<T> {
    return calls.call(instanceName, () => this.getBoundStub(instanceName, locationHint), invoke)
  }

  private getBucketCoordinatorName(key: string, locationBucket: LocationBucket): string {
    const bucketShardCount = getBucketCoordinatorShardIndices(this.scale, locationBucket).length
    const bucketShardOrdinal = getDeterministicKeyBucketIndex(key, bucketShardCount)
    return `${this.baseInstanceName}:broadcast:${locationBucket}:${bucketShardOrdinal}`
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
