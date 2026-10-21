/// <reference types="@cloudflare/workers-types" />
export { CloudflareBroadcast, CloudflareBroadcastAuthorityState, CloudflareBroadcastMember }
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
import { reportLostDeliveries, type OrderedStubs } from '../../server/adapter/cloudflare/ordered-stubs.js'
import { assert } from '../../../utils/assert.js'
import type { BroadcastPayload, BroadcastRoute, PublishResult } from '../../backend/broadcast/contract.js'
import { broadcastRouteKey } from '../../backend/broadcast/route-key.js'
import type { BackendPayload, BackendReceiver } from '../../backend/subscription.js'
import { DriverAttempt } from '../../backend/attempt.js'
import { createDeferred } from '../../../utils/createDeferred.js'
import { getOrCreate } from '../../../utils/getOrCreate.js'
import { Listeners } from '../../../utils/Listeners.js'
import type { OrderingInfo } from '../../wire/orderingFrame.js'
import type { CloudflareScale, LocationBucket } from './routing.js'
import { currentCloudflareSession } from '../../server/adapter/cloudflare/session.js'

const PRESENCE_TTL_MS = 90_000
const PRESENCE_REFRESH_INTERVAL_MS = 30_000

type BroadcastPublishRequest = {
  key: string
  kind: BroadcastRoute['kind']
  payload: BroadcastPayload
}

type BroadcastDeliverRequest = BroadcastPublishRequest & { info: OrderingInfo }

/** The authority's sequenced publish, handed to one bucket coordinator for the member DOs it names by id. */
type BroadcastForwardRequest = BroadcastDeliverRequest & { members: string[] }

/** A member DO's presence on a route, at the key's authority, by DO id; `bucket: null` withdraws it. */
type BroadcastPresenceRequest = {
  key: string
  kind: BroadcastRoute['kind']
  member: string
  bucket: LocationBucket | null
}

type TelefuncBroadcastStub = {
  telefuncBroadcastPublish(request: BroadcastPublishRequest): Promise<PublishResult>
  telefuncBroadcastForward(request: BroadcastForwardRequest): Promise<void>
  telefuncBroadcastDeliver(request: BroadcastDeliverRequest): Promise<void>
  telefuncBroadcastPresence(request: BroadcastPresenceRequest): Promise<void>
}

/** One DO's outgoing Broadcast calls. */
type BroadcastCalls = OrderedStubs<TelefuncBroadcastStub>

type BroadcastNamespace = {
  idFromName(name: string): unknown
  idFromString(id: string): unknown
  get(id: unknown, options?: { locationHint: LocationBucket }): TelefuncBroadcastStub
}

/** One route's presence at the key's authority, for one member DO. */
class MemberRoute {
  state: 'establishing' | 'ready' | 'lost' = 'establishing'
  readonly route: BroadcastRoute
  readonly #setup = createDeferred()
  readonly #presenceListeners = new Listeners<(state: 'ready' | 'lost') => void>()
  #refreshTimer: ReturnType<typeof setInterval> | undefined

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

  losePresence(error: unknown): void {
    if (this.state !== 'ready') return
    this.state = 'lost'
    console.error(new Error(`Cloudflare Broadcast presence for '${this.route.key}' lost`, { cause: error }))
    this.#notifyPresenceState('lost')
  }

  onPresenceStateChange(cb: (state: 'ready' | 'lost') => void): () => void {
    return this.#presenceListeners.add(cb)
  }

  startRefresh(refresh: () => Promise<void>): void {
    this.#refreshTimer = setInterval(() => {
      void refresh().then(
        () => this.acknowledgePresence(),
        (error: unknown) => this.losePresence(error),
      )
    }, PRESENCE_REFRESH_INTERVAL_MS)
  }

  stopRefresh(): void {
    clearInterval(this.#refreshTimer)
  }

  #notifyPresenceState(state: 'ready' | 'lost'): void {
    for (const listener of this.#presenceListeners.list()) listener(state)
  }
}

/** Follows its route's presence: ready once the authority holds it, lost while a refresh fails. */
class CloudflareBroadcastSubscriptionAttempt extends DriverAttempt {
  readonly #receiver: BackendReceiver<BackendPayload>
  readonly #detach: () => Promise<void>
  readonly #stopPresenceObservation: () => void

  constructor(memberRoute: MemberRoute, receiver: BackendReceiver<BackendPayload>, detach: () => Promise<void>) {
    super()
    this.#receiver = receiver
    this.#detach = detach
    this.#stopPresenceObservation = memberRoute.onPresenceStateChange((state) => this.transition(state))
    memberRoute.ready.then(
      () => this.transition('ready'),
      (error: unknown) => {
        this.#stopPresenceObservation()
        this.transition('closed', error)
      },
    )
  }

  /** The authority forwards only to an unexpired record, so what arrives is owed, lost route or not. */
  deliver(payload: BroadcastPayload, info: OrderingInfo): void {
    this.#receiver(payload, info)
  }

  async unsubscribe(): Promise<void> {
    this.#stopPresenceObservation()
    this.transition('closed')
    await this.#detach()
  }
}

const AUTHORITY_SCHEMA = `
  CREATE TABLE IF NOT EXISTS broadcast_key (key TEXT PRIMARY KEY, seq INTEGER NOT NULL);
  CREATE TABLE IF NOT EXISTS broadcast_presence
    (route_key TEXT NOT NULL, member TEXT NOT NULL, bucket TEXT NOT NULL, expires_at INTEGER NOT NULL, PRIMARY KEY (route_key, member));
`

/** A key authority DO's Broadcast state, in its SQLite storage: each key's `seq` and each route's member presence.
 *  The tables are created on first use, so a DO in another role never has them. */
class CloudflareBroadcastAuthorityState {
  readonly #storage: DurableObjectStorage
  #schema = false

  constructor(state: DurableObjectState) {
    this.#storage = state.storage
  }

  nextSequence(key: string): number {
    const sql = this.#sql()
    return this.#storage.transactionSync(() => {
      const row = sql.exec<{ seq: number }>('SELECT seq FROM broadcast_key WHERE key = ?', key).toArray()[0]
      const seq = (row?.seq ?? 0) + 1
      sql.exec('INSERT OR REPLACE INTO broadcast_key (key, seq) VALUES (?, ?)', key, seq)
      return seq
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

  /** The route's unexpired member DOs, each with its bucket. */
  livePresence(routeKey: string, now: number): Array<{ member: string; bucket: LocationBucket }> {
    return this.#sql()
      .exec<{ member: string; bucket: LocationBucket }>(
        'SELECT member, bucket FROM broadcast_presence WHERE route_key = ? AND expires_at > ?',
        routeKey,
        now,
      )
      .toArray()
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
  readonly #broadcast: CloudflareBroadcast
  readonly #id: string
  #bucket: LocationBucket | null = null
  readonly #routes = new Map<string, MemberRoute>()
  readonly #subscriptions = new Map<string, CloudflareBroadcastSubscriptionAttempt>()

  constructor(broadcast: CloudflareBroadcast, id: string, calls: BroadcastCalls) {
    this.#broadcast = broadcast
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

  openSubscription(
    route: BroadcastRoute,
    receiver: BackendReceiver<BackendPayload>,
  ): CloudflareBroadcastSubscriptionAttempt {
    const routeKey = broadcastRouteKey(route)
    const memberRoute = this.#ensureRoute(route, routeKey)
    const attempt = new CloudflareBroadcastSubscriptionAttempt(memberRoute, receiver, async () => {
      this.#subscriptions.delete(routeKey)
      // An establishing route is released once its presence is recorded, if no subscription remains.
      if (memberRoute.state !== 'establishing') await this.#release(routeKey, memberRoute)
    })
    this.#subscriptions.set(routeKey, attempt)
    return attempt
  }

  /** A coordinator's delivery to this DO's subscription. */
  deliver(request: BroadcastDeliverRequest): void {
    this.#subscriptions.get(broadcastRouteKey(request))?.deliver(request.payload, request.info)
  }

  #ensureRoute(route: BroadcastRoute, routeKey: string): MemberRoute {
    const existing = this.#routes.get(routeKey)
    if (existing !== undefined) return existing
    const memberRoute = new MemberRoute(route)
    this.#routes.set(routeKey, memberRoute)
    // Nobody awaits a deferred teardown; a failed withdrawal lapses with the presence TTL.
    void this.#initializeRoute(routeKey, memberRoute).catch(() => {})
    return memberRoute
  }

  async #initializeRoute(routeKey: string, memberRoute: MemberRoute): Promise<void> {
    try {
      await this.#recordPresence(memberRoute.route)
    } catch (error) {
      memberRoute.rejectPresence(error)
      this.#routes.delete(routeKey)
      return
    }
    memberRoute.acknowledgePresence()
    if (!this.#subscriptions.has(routeKey)) return this.#release(routeKey, memberRoute)
    memberRoute.startRefresh(() => this.#recordPresence(memberRoute.route))
  }

  async #release(routeKey: string, memberRoute: MemberRoute): Promise<void> {
    memberRoute.stopRefresh()
    this.#routes.delete(routeKey)
    await this.#withdrawPresence(memberRoute.route)
  }

  /** Through the DO's ordered stubs, so one route's writes reach its authority in the order they were made. */
  #recordPresence(route: BroadcastRoute): Promise<void> {
    assert(this.#bucket, 'A Broadcast member registers from a session that knows its bucket')
    const request = { key: route.key, kind: route.kind, member: this.#id, bucket: this.#bucket }
    return this.#broadcast.sendPresence(this.calls, request)
  }

  #withdrawPresence(route: BroadcastRoute): Promise<void> {
    const request = { key: route.key, kind: route.kind, member: this.#id, bucket: null }
    return this.#broadcast.sendPresence(this.calls, request)
  }
}

/** The isolate's Cloudflare Broadcast driver: where each key's authority and each bucket's coordinators live, and
 *  their RPC handlers. Each session DO subscribes through its own `CloudflareBroadcastMember`. */
class CloudflareBroadcast {
  readonly #baseInstanceName: string
  readonly #scale: CloudflareScale | undefined
  readonly #locationFallback: LocationBucket
  readonly #namespace: () => BroadcastNamespace

  constructor({
    baseInstanceName,
    scale,
    locationFallback,
    namespace,
  }: {
    baseInstanceName: string
    scale?: CloudflareScale
    locationFallback: LocationBucket
    namespace: () => BroadcastNamespace
  }) {
    this.#baseInstanceName = baseInstanceName
    this.#scale = scale
    this.#locationFallback = locationFallback
    this.#namespace = namespace
  }

  member(id: string, calls: BroadcastCalls): CloudflareBroadcastMember {
    return new CloudflareBroadcastMember(this, id, calls)
  }

  /** From a session DO, through its ordered stubs; from elsewhere, as a cron trigger, through a fresh stub. Async, so
   *  the caller gets a native promise: a stub's RpcPromise is callable, which `isPromise` doesn't take for a
   *  promise. */
  async publish(route: BroadcastRoute, payload: BroadcastPayload): Promise<PublishResult> {
    const name = this.#authorityName(route.key)
    const member = currentCloudflareSession()?.broadcast
    // An RPC serializes its arguments as it is made, so only a call that waits copies the caller's bytes.
    const waits = member?.calls.waits(name) === true
    const request = {
      key: route.key,
      kind: route.kind,
      payload: waits && typeof payload !== 'string' ? new Uint8Array(payload) : payload,
    }
    const send = (authority: TelefuncBroadcastStub) => authority.telefuncBroadcastPublish(request)
    if (member === undefined) return send(this.#stubByName(name, null))
    return this.#callByName(member.calls, name, member.bucket, send)
  }

  sendPresence(calls: BroadcastCalls, request: BroadcastPresenceRequest): Promise<void> {
    return this.#callByName(calls, this.#authorityName(request.key), request.bucket, (authority) =>
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
    const { key, kind, payload } = request
    const info = { seq: authorityState.nextSequence(key), timestamp: Date.now() }
    const presence = authorityState.livePresence(broadcastRouteKey({ key, kind }), info.timestamp)
    // Presence written before a redeploy dropped its region forwards through the fallback region's coordinators.
    const membersByCoordinatorBucket = new Map<LocationBucket, string[]>()
    for (const { member, bucket } of presence) {
      const via = getScaleCountForBucket(this.#scale, bucket) > 0 ? bucket : this.#locationFallback
      getOrCreate(membersByCoordinatorBucket, via, () => []).push(member)
    }
    const forwards = await Promise.allSettled(
      Array.from(membersByCoordinatorBucket, ([bucket, members]) =>
        this.#callByName(calls, this.#coordinatorName(key, bucket), bucket, (coordinator) =>
          coordinator.telefuncBroadcastForward({ key, kind, payload, info, members }),
        ),
      ),
    )
    reportLostDeliveries(`Cloudflare Broadcast delivery of '${key}'`, forwards)
    return { seq: info.seq, timestamp: info.timestamp, receivers: presence.length }
  }

  /** At a bucket coordinator: delivers the authority's sequenced publish to the named member DOs, in arrival order. */
  async forwardToBucket(calls: BroadcastCalls, request: BroadcastForwardRequest): Promise<void> {
    const delivery = { key: request.key, kind: request.kind, payload: request.payload, info: request.info }
    const deliveries = await Promise.allSettled(
      request.members.map((member) =>
        calls.call(
          member,
          () => this.#stubById(member),
          (stub) => stub.telefuncBroadcastDeliver(delivery),
        ),
      ),
    )
    reportLostDeliveries(`Cloudflare Broadcast delivery of '${delivery.key}'`, deliveries)
  }

  /** A call from the DO owning `calls`, through its stub for the named instance. */
  #callByName<T>(
    calls: BroadcastCalls,
    name: string,
    locationHint: LocationBucket | null,
    invoke: (stub: TelefuncBroadcastStub) => Promise<T>,
  ): Promise<T> {
    return calls.call(name, () => this.#stubByName(name, locationHint), invoke)
  }

  #authorityName(key: string): string {
    return `${this.#baseInstanceName}:broadcast:authority:${key}`
  }

  #coordinatorName(key: string, locationBucket: LocationBucket): string {
    const bucketShardCount = getBucketCoordinatorShardIndices(this.#scale, locationBucket).length
    const bucketShardOrdinal = getDeterministicKeyBucketIndex(key, bucketShardCount)
    return `${this.#baseInstanceName}:broadcast:${locationBucket}:${bucketShardOrdinal}`
  }

  #stubByName(name: string, locationHint: LocationBucket | null): TelefuncBroadcastStub {
    const namespace = this.#namespace()
    return namespace.get(namespace.idFromName(name), locationHint === null ? undefined : { locationHint })
  }

  #stubById(id: string): TelefuncBroadcastStub {
    const namespace = this.#namespace()
    return namespace.get(namespace.idFromString(id))
  }
}
