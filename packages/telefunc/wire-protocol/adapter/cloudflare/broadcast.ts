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
  getBucketCoordinatorScale,
  getDeterministicKeyBucketIndex,
  getScaleCountForBucket,
} from './routing.js'
import { reportLostDeliveries, type OrderedStubs } from './OrderedStubs.js'
import { assert } from '../../../utils/assert.js'
import type { BroadcastPayload, BroadcastRoute, PublishResult, Subscriber, Unsubscribe } from '../../backend/driver.js'
import { broadcastRouteKey } from '../../backend/broadcast/routeKey.js'
import { getOrCreate } from '../../../utils/getOrCreate.js'
import type { OrderingInfo } from '../../wire/orderingFrame.js'
import type { CloudflareScale, LocationBucket } from './routing.js'
import { currentCloudflareSession } from './session.js'

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

/** One route's presence at its key's authority, for one member DO, and the subscriber it serves. */
class MemberRoute {
  state: 'establishing' | 'ready' | 'lost' = 'establishing'
  /** Null once it unsubscribed: an establishing route is released once its presence is recorded. */
  subscriber: Subscriber | null
  readonly route: BroadcastRoute
  private refreshTimer: ReturnType<typeof setInterval> | undefined

  constructor({ route, subscriber }: { route: BroadcastRoute; subscriber: Subscriber }) {
    this.route = route
    this.subscriber = subscriber
  }

  presenceRecorded(): void {
    if (this.state === 'ready') return
    this.state = 'ready'
    this.subscriber?.ready()
  }

  presenceLost(error: unknown): void {
    if (this.state !== 'ready') return
    this.state = 'lost'
    console.error(new Error(`Cloudflare Broadcast presence for '${this.route.key}' lost`, { cause: error }))
    this.subscriber?.lost()
  }

  startRefresh(refresh: () => Promise<void>): void {
    this.refreshTimer = setInterval(() => {
      void refresh().then(
        () => this.presenceRecorded(),
        (error: unknown) => this.presenceLost(error),
      )
    }, PRESENCE_REFRESH_INTERVAL_MS)
  }

  stopRefresh(): void {
    clearInterval(this.refreshTimer)
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
  private readonly storage: DurableObjectStorage
  private schema = false

  constructor(state: DurableObjectState) {
    this.storage = state.storage
  }

  nextSequence(key: string): number {
    const sql = this.sql()
    return this.storage.transactionSync(() => {
      const row = sql.exec<{ seq: number }>('SELECT seq FROM broadcast_key WHERE key = ?', key).toArray()[0]
      const seq = (row?.seq ?? 0) + 1
      sql.exec('INSERT OR REPLACE INTO broadcast_key (key, seq) VALUES (?, ?)', key, seq)
      return seq
    })
  }

  /** Records or withdraws a member DO, dropping the route's lapsed entries; the RPC reply waits for the write. */
  setPresence({ key, kind, member, bucket }: BroadcastPresenceRequest): void {
    const sql = this.sql()
    const routeKey = broadcastRouteKey({ key, kind })
    const now = Date.now()
    this.storage.transactionSync(() => {
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
    return this.sql()
      .exec<{
        member: string
        bucket: LocationBucket
      }>('SELECT member, bucket FROM broadcast_presence WHERE route_key = ? AND expires_at > ?', routeKey, now)
      .toArray()
  }

  private sql(): SqlStorage {
    if (!this.schema) {
      this.storage.sql.exec(AUTHORITY_SCHEMA)
      this.schema = true
    }
    return this.storage.sql
  }
}

/** A session DO's Broadcast membership: its routes' presence at their key authorities, its subscriptions, and the
 *  deliveries to them. It is addressed by the DO's id and sends through the DO's own ordered stubs. */
class CloudflareBroadcastMember {
  readonly calls: BroadcastCalls
  private readonly broadcast: CloudflareBroadcast
  private readonly id: string
  private locatedBucket: LocationBucket | null = null
  private readonly routes = new Map<string, MemberRoute>()

  constructor({ broadcast, id, calls }: { broadcast: CloudflareBroadcast; id: string; calls: BroadcastCalls }) {
    this.broadcast = broadcast
    this.id = id
    this.calls = calls
  }

  get bucket(): LocationBucket | null {
    return this.locatedBucket
  }

  /** The bucket the session was placed in, from the request that reached it. */
  locate(bucket: LocationBucket): void {
    assert(KNOWN_BROADCAST_BUCKETS.has(bucket), `Invalid Broadcast location bucket "${bucket}".`)
    this.locatedBucket = bucket
  }

  subscribe(route: BroadcastRoute, subscriber: Subscriber): Unsubscribe {
    const routeKey = broadcastRouteKey(route)
    let memberRoute = this.routes.get(routeKey)
    if (memberRoute === undefined) {
      memberRoute = new MemberRoute({ route, subscriber })
      this.routes.set(routeKey, memberRoute)
      // Nobody awaits a deferred teardown; a failed withdrawal lapses with the presence TTL.
      void this.recordFirstPresence(routeKey, memberRoute).catch(() => {})
    } else {
      // Unsubscribed while establishing, and subscribed again before its presence was recorded.
      memberRoute.subscriber = subscriber
    }
    const subscribed = memberRoute
    return () => this.unsubscribe(routeKey, subscribed)
  }

  /** A coordinator's delivery to this DO. The authority forwards only to an unexpired record, so what arrives is
   *  owed, lost route or not. */
  deliver(request: BroadcastDeliverRequest): void {
    this.routes.get(broadcastRouteKey(request))?.subscriber?.message(request.payload, request.info)
  }

  private async unsubscribe(routeKey: string, memberRoute: MemberRoute): Promise<void> {
    memberRoute.subscriber = null
    // An establishing route is released once its presence is recorded.
    if (memberRoute.state !== 'establishing') await this.release(routeKey, memberRoute)
  }

  private async recordFirstPresence(routeKey: string, memberRoute: MemberRoute): Promise<void> {
    try {
      await this.recordPresence(memberRoute.route)
    } catch (error) {
      this.routes.delete(routeKey)
      memberRoute.subscriber?.failed(error)
      return
    }
    memberRoute.presenceRecorded()
    if (memberRoute.subscriber === null) return this.release(routeKey, memberRoute)
    memberRoute.startRefresh(() => this.recordPresence(memberRoute.route))
  }

  private async release(routeKey: string, memberRoute: MemberRoute): Promise<void> {
    memberRoute.stopRefresh()
    this.routes.delete(routeKey)
    await this.withdrawPresence(memberRoute.route)
  }

  /** Through the DO's ordered stubs, so one route's writes reach its authority in the order they were made. */
  private recordPresence(route: BroadcastRoute): Promise<void> {
    assert(this.locatedBucket, 'A Broadcast member registers from a session that knows its bucket')
    const request = { key: route.key, kind: route.kind, member: this.id, bucket: this.locatedBucket }
    return this.broadcast.sendPresence(this.calls, request)
  }

  private withdrawPresence(route: BroadcastRoute): Promise<void> {
    const request = { key: route.key, kind: route.kind, member: this.id, bucket: null }
    return this.broadcast.sendPresence(this.calls, request)
  }
}

/** The isolate's Cloudflare Broadcast driver: where each key's authority and each bucket's coordinators live, and
 *  their RPC handlers. Each session DO subscribes through its own `CloudflareBroadcastMember`. */
class CloudflareBroadcast {
  private readonly baseInstanceName: string
  private readonly scale: CloudflareScale | undefined
  private readonly locationFallback: LocationBucket
  private readonly getNamespace: () => BroadcastNamespace

  constructor({
    baseInstanceName,
    scale,
    locationFallback,
    getNamespace,
  }: {
    baseInstanceName: string
    scale?: CloudflareScale
    locationFallback: LocationBucket
    getNamespace: () => BroadcastNamespace
  }) {
    this.baseInstanceName = baseInstanceName
    this.scale = scale
    this.locationFallback = locationFallback
    this.getNamespace = getNamespace
  }

  member(id: string, calls: BroadcastCalls): CloudflareBroadcastMember {
    return new CloudflareBroadcastMember({ broadcast: this, id, calls })
  }

  /** From a session DO, through its ordered stubs; from elsewhere, as a cron trigger, through a fresh stub. Async, so
   *  the caller gets a native promise: a stub's RpcPromise is callable, which `isPromise` doesn't take for a
   *  promise. */
  async publish(route: BroadcastRoute, payload: BroadcastPayload): Promise<PublishResult> {
    const name = this.authorityName(route.key)
    const member = currentCloudflareSession()?.broadcast
    const request = {
      key: route.key,
      kind: route.kind,
      payload: typeof payload === 'string' ? payload : new Uint8Array(payload),
    }
    const send = (authority: TelefuncBroadcastStub) => authority.telefuncBroadcastPublish(request)
    if (member === undefined) return send(this.stubByName(name, null))
    return this.callByName(member.calls, name, member.bucket, send)
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
    const { key, kind, payload } = request
    const info = { seq: authorityState.nextSequence(key), timestamp: Date.now() }
    const presence = authorityState.livePresence(broadcastRouteKey({ key, kind }), info.timestamp)
    // Presence written before a redeploy dropped its region forwards through the fallback region's coordinators.
    const membersByCoordinatorBucket = new Map<LocationBucket, string[]>()
    for (const { member, bucket } of presence) {
      const via = getScaleCountForBucket(this.scale, bucket) > 0 ? bucket : this.locationFallback
      getOrCreate(membersByCoordinatorBucket, via, () => []).push(member)
    }
    const forwards = await Promise.allSettled(
      Array.from(membersByCoordinatorBucket, ([bucket, members]) =>
        this.callByName(calls, this.coordinatorName(key, bucket), bucket, (coordinator) =>
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
    const bucketShardCount = getBucketCoordinatorScale(this.scale, locationBucket)
    const bucketShardOrdinal = getDeterministicKeyBucketIndex(key, bucketShardCount)
    return `${this.baseInstanceName}:broadcast:${locationBucket}:${bucketShardOrdinal}`
  }

  private stubByName(name: string, locationHint: LocationBucket | null): TelefuncBroadcastStub {
    const namespace = this.getNamespace()
    return namespace.get(namespace.idFromName(name), locationHint === null ? undefined : { locationHint })
  }

  private stubById(id: string): TelefuncBroadcastStub {
    const namespace = this.getNamespace()
    return namespace.get(namespace.idFromString(id))
  }
}
