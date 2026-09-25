/// <reference types="@cloudflare/workers-types" />
export { CloudflareBroadcastTransport, CloudflareBroadcastAuthorityState, CloudflareBroadcastMember }
export type {
  BroadcastCalls,
  BroadcastDeliverRequest,
  BroadcastForwardRequest,
  BroadcastPresenceRequest,
  BroadcastPublishRequest,
  TelefuncBroadcastStub,
}

import {
  KNOWN_BROADCAST_BUCKETS,
  getBucketCoordinatorShardIndices,
  getDeterministicKeyBucketIndex,
  getScaleCountForBucket,
} from './routing.js'
import { reportLostDeliveries, type OrderedStubs } from './ordered-stubs.js'
import { assert } from '../../../../utils/assert.js'
import type { BroadcastRoute, PublishResult } from '../../../backend/broadcast/contract.js'
import { broadcastRouteKey } from '../../../backend/broadcast/route-key.js'
import type { BackendReceiver } from '../../../backend/subscription.js'
import { DriverAttempt } from '../../../backend/attempt.js'
import { createDeferred } from '../../../../utils/createDeferred.js'
import type { OrderingInfo } from '../../../ordering-frame.js'
import type { CloudflareScale, LocationBucket } from './routing.js'
import { currentCloudflareSession } from './session.js'

const PRESENCE_TTL_MS = 90_000
const PRESENCE_REFRESH_INTERVAL_MS = 30_000

/** `locationBucket` is the publishing session's; a publish from outside a session, as from a cron trigger, has none. */
type BroadcastPublishRequest = {
  key: string
  kind: BroadcastRoute['kind']
  locationBucket: LocationBucket | null
  payload: Uint8Array
}

/** The authority's sequenced publish, handed to one bucket coordinator for the member DOs it names by id. */
type BroadcastForwardRequest = {
  key: string
  kind: BroadcastRoute['kind']
  payload: Uint8Array
  info: OrderingInfo
  members: string[]
}

/** A member DO's presence on a route, at the key's authority, by DO id; `bucket: null` withdraws it. */
type BroadcastPresenceRequest = {
  key: string
  kind: BroadcastRoute['kind']
  member: string
  bucket: LocationBucket | null
}

type BroadcastDeliverRequest = {
  key: string
  kind: BroadcastRoute['kind']
  payload: Uint8Array
  info: OrderingInfo
}

type TelefuncBroadcastStub = DurableObjectStub & {
  telefuncBroadcastPublish(request: BroadcastPublishRequest): Promise<PublishResult>
  telefuncBroadcastForward(request: BroadcastForwardRequest): Promise<void>
  telefuncBroadcastDeliver(request: BroadcastDeliverRequest): Promise<void>
  telefuncBroadcastPresence(request: BroadcastPresenceRequest): Promise<void>
}

/** One DO's outgoing Broadcast calls. */
type BroadcastCalls = OrderedStubs<TelefuncBroadcastStub>

/** One route's presence at the key's authority, for one member DO. */
class MemberRoute {
  state: 'establishing' | 'ready' | 'lost' = 'establishing'
  teardownRequested = false
  refreshTimer: ReturnType<typeof setInterval> | null = null
  readonly route: BroadcastRoute
  readonly #setup = createDeferred()
  readonly #presenceListeners = new Set<(state: 'ready' | 'lost') => void>()

  constructor(route: BroadcastRoute) {
    this.route = route
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

/** Follows its route's presence: ready once the authority holds it, lost while a refresh fails. */
class CloudflareBroadcastSubscriptionAttempt extends DriverAttempt {
  readonly #receiver: BackendReceiver
  readonly #detach: () => Promise<void>
  readonly #stopPresenceObservation: () => void
  #unsubscribed = false

  constructor(member: MemberRoute, receiver: BackendReceiver, detach: () => Promise<void>) {
    super()
    this.#receiver = receiver
    this.#detach = detach
    this.#stopPresenceObservation = member.onPresenceStateChange((state) => this.transition(state))
    member.ready.then(
      // Joining a route whose presence is lost waits for its recovery, which the observer reports.
      () => {
        if (member.state === 'ready') this.transition('ready')
      },
      (error: unknown) => {
        this.#stopPresenceObservation()
        this.transition('closed', error)
      },
    )
  }

  // The authority forwards only to an unexpired record, so what arrives is owed, lost route or not.
  deliver(payload: Uint8Array, info: OrderingInfo): void {
    if (this.state() === 'closed') return
    this.#receiver(payload, info)
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
  CREATE TABLE IF NOT EXISTS broadcast_key (key TEXT PRIMARY KEY, seq INTEGER NOT NULL, authority_bucket TEXT);
  CREATE TABLE IF NOT EXISTS broadcast_presence
    (route_key TEXT NOT NULL, member TEXT NOT NULL, bucket TEXT NOT NULL, expires_at INTEGER NOT NULL, PRIMARY KEY (route_key, member));
`

/** A key authority DO's Broadcast state, in its SQLite storage: each key's `seq` and first-touch bucket, and each
 *  route's member presence. The tables are created on first use, so a DO in another role never has them. */
class CloudflareBroadcastAuthorityState {
  readonly #storage: DurableObjectStorage
  #schema = false

  constructor(state: DurableObjectState) {
    this.#storage = state.storage
  }

  /** The key's next `seq`; the key's first publish from a session fixes its authority bucket. */
  sequence(
    key: string,
    preferredBucket: LocationBucket | null,
  ): { seq: number; authorityBucket: LocationBucket | null } {
    const sql = this.#sql()
    return this.#storage.transactionSync(() => {
      const row = sql
        .exec<{ seq: number; authority_bucket: LocationBucket | null }>(
          'SELECT seq, authority_bucket FROM broadcast_key WHERE key = ?',
          key,
        )
        .toArray()[0]
      const current = row?.seq ?? 0
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

  /** Records or withdraws a member DO, dropping the route's lapsed entries; the RPC reply waits for the write. */
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

  /** The route's unexpired member DOs by bucket. */
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

/** A session DO's Broadcast membership: its routes' presence at their key authorities, its subscriptions, and the
 *  deliveries to them. It is addressed by the DO's id and sends through the DO's own ordered stubs. */
class CloudflareBroadcastMember {
  /** Subscriptions share an attempt only within one session DO. */
  readonly partition = crypto.randomUUID()
  readonly calls: BroadcastCalls
  readonly #transport: CloudflareBroadcastTransport
  readonly #id: string
  #bucket: LocationBucket | null = null
  readonly #routes = new Map<string, MemberRoute>()
  readonly #subscriptions = new Map<string, CloudflareBroadcastSubscriptionAttempt>()

  constructor(transport: CloudflareBroadcastTransport, id: string, calls: BroadcastCalls) {
    this.#transport = transport
    this.#id = id
    this.calls = calls
  }

  get bucket(): LocationBucket | null {
    return this.#bucket
  }

  /** The bucket the session was placed in, from the request that reached it. */
  locate(bucket: LocationBucket): void {
    assert(KNOWN_BROADCAST_BUCKETS.has(bucket), `Invalid Broadcast location bucket "${bucket}".`)
    this.#bucket = bucket
  }

  openSubscription(route: BroadcastRoute, receiver: BackendReceiver): CloudflareBroadcastSubscriptionAttempt {
    const routeKey = broadcastRouteKey(route)
    const memberRoute = this.#ensureRoute(route, routeKey)
    const attempt: CloudflareBroadcastSubscriptionAttempt = new CloudflareBroadcastSubscriptionAttempt(
      memberRoute,
      receiver,
      async () => {
        if (this.#subscriptions.get(routeKey) === attempt) this.#subscriptions.delete(routeKey)
        await this.#teardownIfEmpty(routeKey)
      },
    )
    this.#subscriptions.set(routeKey, attempt)
    return attempt
  }

  /** A coordinator's delivery to this DO's subscription. */
  deliver(request: BroadcastDeliverRequest): void {
    this.#subscriptions.get(broadcastRouteKey(request))?.deliver(request.payload, request.info)
  }

  #ensureRoute(route: BroadcastRoute, routeKey: string): MemberRoute {
    const existing = this.#routes.get(routeKey)
    if (existing !== undefined) {
      existing.teardownRequested = false
      return existing
    }
    const memberRoute = new MemberRoute(route)
    this.#routes.set(routeKey, memberRoute)
    // Nobody awaits a deferred teardown; a failed withdrawal lapses with the presence TTL.
    void this.#initializeRoute(routeKey, memberRoute).catch(() => {})
    return memberRoute
  }

  async #initializeRoute(routeKey: string, memberRoute: MemberRoute): Promise<void> {
    try {
      await this.#writePresence(memberRoute.route, true)
    } catch (error) {
      memberRoute.rejectPresence(error)
      if (this.#routes.get(routeKey) === memberRoute) this.#routes.delete(routeKey)
      return
    }
    memberRoute.acknowledgePresence()
    if (memberRoute.teardownRequested) return this.#release(routeKey, memberRoute)
    memberRoute.refreshTimer = setInterval(() => {
      void this.#writePresence(memberRoute.route, true).then(
        () => memberRoute.acknowledgePresence(),
        () => memberRoute.losePresence(),
      )
    }, PRESENCE_REFRESH_INTERVAL_MS)
  }

  async #teardownIfEmpty(routeKey: string): Promise<void> {
    const memberRoute = this.#routes.get(routeKey)
    if (memberRoute === undefined || this.#subscriptions.has(routeKey)) return
    if (memberRoute.state === 'establishing') {
      memberRoute.teardownRequested = true
      return
    }
    await this.#release(routeKey, memberRoute)
  }

  async #release(routeKey: string, memberRoute: MemberRoute): Promise<void> {
    memberRoute.stopRefresh()
    if (this.#routes.get(routeKey) === memberRoute) this.#routes.delete(routeKey)
    await this.#writePresence(memberRoute.route, false)
  }

  /** Through the DO's ordered stubs, so one route's writes reach its authority in the order they were made. */
  #writePresence(route: BroadcastRoute, present: boolean): Promise<void> {
    assert(this.#bucket, 'A Broadcast member registers from a session that knows its bucket')
    const request = { key: route.key, kind: route.kind, member: this.#id, bucket: present ? this.#bucket : null }
    return this.#transport.sendPresence(this.calls, request)
  }
}

/** The isolate's Cloudflare Broadcast driver: where each key's authority and each bucket's coordinators live, and
 *  their RPC handlers. Each session DO subscribes through its own `CloudflareBroadcastMember`. */
class CloudflareBroadcastTransport {
  private readonly baseInstanceName: string
  private readonly scale: CloudflareScale | undefined
  private readonly locationFallback: LocationBucket
  private readonly namespace: () => DurableObjectNamespace

  constructor({
    baseInstanceName,
    scale,
    locationFallback,
    namespace,
  }: {
    baseInstanceName: string
    scale?: CloudflareScale
    locationFallback: LocationBucket
    namespace: () => DurableObjectNamespace
  }) {
    this.baseInstanceName = baseInstanceName
    this.scale = scale
    this.locationFallback = locationFallback
    this.namespace = namespace
  }

  member(id: string, calls: BroadcastCalls): CloudflareBroadcastMember {
    return new CloudflareBroadcastMember(this, id, calls)
  }

  /** From a session DO, through its ordered stubs; from elsewhere, as a cron trigger, through a fresh stub. Async, so
   *  the caller gets a native promise: a stub's RpcPromise is callable, which `isPromise` doesn't take for a promise. */
  async publish(route: BroadcastRoute, payload: Uint8Array): Promise<PublishResult> {
    const member = currentCloudflareSession()?.broadcast()
    const locationBucket = member?.bucket ?? null
    const request = { key: route.key, kind: route.kind, locationBucket, payload }
    const send = (authority: TelefuncBroadcastStub) => authority.telefuncBroadcastPublish(request)
    const name = this.authorityName(route.key)
    return member === undefined
      ? send(this.stubByName(name, locationBucket))
      : this.callByName(member.calls, name, locationBucket, send)
  }

  sendPresence(calls: BroadcastCalls, request: BroadcastPresenceRequest): Promise<void> {
    return this.callByName(calls, this.authorityName(request.key), request.bucket, (authority) =>
      authority.telefuncBroadcastPresence(request),
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
    // Presence written before a redeploy dropped its region forwards through the fallback region's coordinators.
    const membersByCoordinatorBucket = new Map<LocationBucket, string[]>()
    for (const [bucket, members] of presenceByBucket) {
      receivers += members.length
      const via = getScaleCountForBucket(this.scale, bucket) > 0 ? bucket : this.locationFallback
      membersByCoordinatorBucket.set(via, [...(membersByCoordinatorBucket.get(via) ?? []), ...members])
    }
    const forwards = await Promise.allSettled(
      Array.from(membersByCoordinatorBucket, ([bucket, members]) =>
        this.callByName(calls, this.coordinatorName(key, bucket), bucket, (coordinator) =>
          coordinator.telefuncBroadcastForward({ key, kind, payload, info, members }),
        ),
      ),
    )
    reportLostDeliveries(`Cloudflare Broadcast delivery of '${key}'`, forwards)
    return { ...info, receivers, meta: { authorityBucket, fanoutBuckets } }
  }

  /** At a bucket coordinator: delivers the authority's sequenced publish to the named member DOs, in arrival order. */
  async forwardToBucket(calls: BroadcastCalls, request: BroadcastForwardRequest): Promise<void> {
    const { members, ...delivery } = request
    const deliveries = await Promise.allSettled(
      members.map((member) =>
        calls.call(
          member,
          () => this.stubById(member),
          (stub) => stub.telefuncBroadcastDeliver(delivery),
        ),
      ),
    )
    reportLostDeliveries(`Cloudflare Broadcast delivery of '${delivery.key}'`, deliveries)
  }

  /** A call from the DO owning `calls`, through its stub for the named instance. */
  private callByName<T>(
    calls: BroadcastCalls,
    name: string,
    locationHint: LocationBucket | null,
    invoke: (stub: TelefuncBroadcastStub) => Promise<T>,
  ): Promise<T> {
    return calls.call(name, () => this.stubByName(name, locationHint), invoke)
  }

  private authorityName(key: string): string {
    return `${this.baseInstanceName}:broadcast:authority:${key}`
  }

  private coordinatorName(key: string, locationBucket: LocationBucket): string {
    const bucketShardCount = getBucketCoordinatorShardIndices(this.scale, locationBucket).length
    const bucketShardOrdinal = getDeterministicKeyBucketIndex(key, bucketShardCount)
    return `${this.baseInstanceName}:broadcast:${locationBucket}:${bucketShardOrdinal}`
  }

  private stubByName(name: string, locationHint: LocationBucket | null): TelefuncBroadcastStub {
    const namespace = this.namespace()
    return namespace.get(
      namespace.idFromName(name),
      locationHint === null ? undefined : { locationHint },
    ) as TelefuncBroadcastStub
  }

  private stubById(id: string): TelefuncBroadcastStub {
    const namespace = this.namespace()
    return namespace.get(namespace.idFromString(id)) as TelefuncBroadcastStub
  }
}
