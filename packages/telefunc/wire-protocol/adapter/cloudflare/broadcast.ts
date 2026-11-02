/// <reference types="@cloudflare/workers-types" />
export { CloudflareBroadcast, CloudflareBroadcastAuthorityState, CloudflareBroadcastMember }
export type {
  BroadcastCalls,
  BroadcastForwardRequest,
  BroadcastPresenceRequest,
  BroadcastPublishRequest,
  MemberPlacement,
  TelefuncBroadcastStub,
}

import { KNOWN_BROADCAST_BUCKETS, getBucketCoordinatorScale, getDeterministicKeyBucketIndex } from './routing.js'
import type { OrderedStubs } from './OrderedStubs.js'
import { assert } from '../../../utils/assert.js'
import {
  DriverAttempt,
  type BackendReceiver,
  type BroadcastPayload,
  type BroadcastRoute,
  type PublishResult,
} from '../../backend/driver.js'
import { broadcastRouteKey } from '../../backend/broadcast/routeKey.js'
import { getOrCreate } from '../../../utils/getOrCreate.js'
import type { OrderingInfo } from '../../wire/orderingFrame.js'
import type { CloudflareScale, LocationBucket } from './routing.js'
import { currentCloudflareSession } from './session.js'
import type { BroadcastDelivery } from './extensions.js'

const PRESENCE_TTL_MS = 90_000
const PRESENCE_REFRESH_INTERVAL_MS = 30_000

type BroadcastPublishRequest = {
  key: string
  kind: BroadcastRoute['kind']
  payload: BroadcastPayload
}

/** A delivery, handed to one bucket coordinator for the member DOs it names by id. */
type BroadcastForwardRequest = BroadcastDelivery & { members: string[] }

/** A member DO, by id, and the bucket its session was placed in. */
type MemberPlacement = { member: string; bucket: LocationBucket }

/** A member DO's presence on a route, at the key's authority, by DO id; `bucket: null` withdraws it. */
type BroadcastPresenceRequest = {
  key: string
  kind: BroadcastRoute['kind']
  member: string
  bucket: LocationBucket | null
}

type TelefuncBroadcastStub = {
  telefuncBroadcastPublish(request: BroadcastPublishRequest): Promise<PublishResult>
  /** Resolves to how many of the member DOs it names failed to take the delivery twice. */
  telefuncBroadcastForward(request: BroadcastForwardRequest): Promise<number>
  telefuncBroadcastDeliver(delivery: BroadcastDelivery): Promise<void>
  telefuncBroadcastPresence(request: BroadcastPresenceRequest): Promise<void>
}

/** One DO's outgoing Broadcast calls. */
type BroadcastCalls = OrderedStubs<TelefuncBroadcastStub>

type BroadcastNamespace = {
  idFromName(name: string): unknown
  idFromString(id: string): unknown
  get(id: unknown, options?: { locationHint: LocationBucket }): TelefuncBroadcastStub
}

type MemberRouteOptions = { route: BroadcastRoute }

/** One route's presence at its key's authority, for one member DO, and the attempt it serves. */
class MemberRoute {
  state: 'establishing' | 'ready' | 'lost' = 'establishing'
  /** Null once it unsubscribed: an establishing route is released once its presence is recorded. */
  attempt: MemberAttempt | null = null
  readonly route: BroadcastRoute
  /** The last seq it took, by the key that sequenced it: its own. */
  readonly taken = new Map<string, number>()
  private refreshTimer: ReturnType<typeof setInterval> | undefined

  constructor({ route }: MemberRouteOptions) {
    this.route = route
  }

  presenceRecorded(): void {
    if (this.state === 'ready') return
    this.state = 'ready'
    this.attempt?.presence('ready')
  }

  presenceLost(error: unknown): void {
    if (this.state !== 'ready') return
    this.state = 'lost'
    console.error(new Error(`Cloudflare Broadcast presence for '${this.route.key}' lost`, { cause: error }))
    this.attempt?.presence('lost')
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

type MemberAttemptOptions = {
  member: CloudflareBroadcastMember
  memberRoute: MemberRoute
  receiver: BackendReceiver
}

/** A subscription on a member DO's route: ready while the route's presence is recorded. */
class MemberAttempt extends DriverAttempt {
  readonly receiver: BackendReceiver
  private readonly member: CloudflareBroadcastMember
  private readonly memberRoute: MemberRoute

  constructor({ member, memberRoute, receiver }: MemberAttemptOptions) {
    super()
    this.member = member
    this.memberRoute = memberRoute
    this.receiver = receiver
  }

  /** Its route's presence was recorded, lost, or failed to record with `error`. */
  presence(state: 'ready' | 'lost' | 'closed', error?: unknown): void {
    this.transition(state, error)
  }

  unsubscribe(): Promise<void> {
    this.transition('closed')
    return this.member.unsubscribe(this.memberRoute)
  }
}

const AUTHORITY_SCHEMA = `
  CREATE TABLE IF NOT EXISTS broadcast_key (key TEXT PRIMARY KEY, seq INTEGER NOT NULL);
  CREATE TABLE IF NOT EXISTS broadcast_presence
    (route_key TEXT NOT NULL, member TEXT NOT NULL, bucket TEXT NOT NULL, expires_at INTEGER NOT NULL, PRIMARY KEY (route_key, member));
`

/** The Broadcast state of a key authority DO, in its SQLite storage: each key's `seq` and each route's member presence.
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

type CloudflareBroadcastMemberOptions = { broadcast: CloudflareBroadcast; id: string; calls: BroadcastCalls }

/** A session DO's Broadcast membership: its routes' presence at their key authorities, its subscriptions, and the
 *  deliveries to them. It is addressed by the DO's id and sends through the DO's own ordered stubs. */
class CloudflareBroadcastMember {
  readonly calls: BroadcastCalls
  private readonly broadcast: CloudflareBroadcast
  private readonly id: string
  private locatedBucket: LocationBucket | null = null
  /** This incarnation of the DO: a slot an evicted one left is never shared with the next. */
  readonly hostKey = crypto.randomUUID()
  private readonly routes = new Map<string, MemberRoute>()
  /** The receivers of routes whose sender keeps its own list of receivers, by route key, each with the last seq it
   *  took of each lane that sequenced a delivery to it. */
  private readonly localReceivers = new Map<string, { receiver: BackendReceiver; taken: Map<string, number> }>()

  constructor({ broadcast, id, calls }: CloudflareBroadcastMemberOptions) {
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

  openSubscription(route: BroadcastRoute, receiver: BackendReceiver): DriverAttempt {
    const routeKey = broadcastRouteKey(route)
    let memberRoute = this.routes.get(routeKey)
    if (memberRoute === undefined) {
      memberRoute = new MemberRoute({ route })
      this.routes.set(routeKey, memberRoute)
      // Nobody awaits a deferred teardown; a failed withdrawal lapses with the presence TTL.
      void this.recordFirstPresence(routeKey, memberRoute).catch(() => {})
    }
    // Else it was unsubscribed while establishing, and is subscribed again before its presence was recorded.
    const attempt = new MemberAttempt({ member: this, memberRoute, receiver })
    memberRoute.attempt = attempt
    return attempt
  }

  /** Takes `route`'s deliveries for `receiver` with no presence at the key's authority: for a sender that keeps its
   *  own list of receivers, as a room authority does. A later `subscribeLocal()` to the route replaces it. */
  subscribeLocal(route: BroadcastRoute, receiver: BackendReceiver): () => void {
    const routeKey = broadcastRouteKey(route)
    const local = { receiver, taken: new Map<string, number>() }
    this.localReceivers.set(routeKey, local)
    return () => {
      if (this.localReceivers.get(routeKey) === local) this.localReceivers.delete(routeKey)
    }
  }

  /** A coordinator's delivery to this DO. The authority forwards only to an unexpired record, so what arrives is
   *  owed, lost route or not. A delivery whose reply was lost comes again: a receiver drops a seq it already took of
   *  the delivery's first route, the key or lane that sequenced it. */
  deliver({ routes, payload, info }: BroadcastDelivery): void {
    const origin = broadcastRouteKey(routes[0]!)
    for (const route of routes) {
      const routeKey = broadcastRouteKey(route)
      const memberRoute = this.routes.get(routeKey)
      if (memberRoute?.attempt) {
        if (takeOnce(memberRoute.taken, origin, info.seq)) memberRoute.attempt.receiver.deliver(payload, info)
        continue
      }
      const local = this.localReceivers.get(routeKey)
      if (local !== undefined && takeOnce(local.taken, origin, info.seq)) local.receiver.deliver(payload, info)
    }
  }

  async unsubscribe(memberRoute: MemberRoute): Promise<void> {
    memberRoute.attempt = null
    // An establishing route is released once its presence is recorded.
    if (memberRoute.state !== 'establishing') await this.release(broadcastRouteKey(memberRoute.route), memberRoute)
  }

  private async recordFirstPresence(routeKey: string, memberRoute: MemberRoute): Promise<void> {
    try {
      await this.recordPresence(memberRoute.route)
    } catch (error) {
      this.routes.delete(routeKey)
      memberRoute.attempt?.presence('closed', error)
      return
    }
    memberRoute.presenceRecorded()
    if (memberRoute.attempt === null) return this.release(routeKey, memberRoute)
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

type CloudflareBroadcastOptions = {
  baseInstanceName: string
  scale?: CloudflareScale
  locationFallback: LocationBucket
  getNamespace: () => BroadcastNamespace
}

/** The Cloudflare Broadcast driver of the isolate: where each key's authority and each bucket's coordinators live, and
 *  their RPC handlers. Each session DO subscribes through its own `CloudflareBroadcastMember`. */
class CloudflareBroadcast {
  private readonly baseInstanceName: string
  private readonly scale: CloudflareScale | undefined
  private readonly locationFallback: LocationBucket
  private readonly getNamespace: () => BroadcastNamespace

  constructor({ baseInstanceName, scale, locationFallback, getNamespace }: CloudflareBroadcastOptions) {
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

  /** At the key's authority: sequences the publish, reads its presence and forwards it, all in one synchronous turn,
   *  so forwards leave through this DO's ordered stubs in `seq` order. */
  async publishToSubscribers(
    authorityState: CloudflareBroadcastAuthorityState,
    calls: BroadcastCalls,
    request: BroadcastPublishRequest,
  ): Promise<PublishResult> {
    const { key, kind, payload } = request
    const info = { seq: authorityState.nextSequence(key), timestamp: Date.now() }
    const presence = authorityState.livePresence(broadcastRouteKey({ key, kind }), info.timestamp)
    await this.forward(calls, key, { routes: [{ key, kind }], payload, info }, presence)
    return info
  }

  /** Sends `delivery` across to each bucket once, to the coordinator `key` picks there, which hands it to the bucket's
   *  `members`; resolves once every member DO took it, and rejects once one failed to take it twice. The forwards leave
   *  through `calls` before this returns, so a DO's forwards for one key reach each coordinator in the order it made
   *  them. */
  async forward(
    calls: BroadcastCalls,
    key: string,
    delivery: BroadcastDelivery,
    members: Iterable<MemberPlacement>,
  ): Promise<void> {
    const membersByBucket = new Map<LocationBucket, string[]>()
    for (const { member, bucket } of members) getOrCreate(membersByBucket, bucket, () => []).push(member)
    let total = 0
    let lost = 0
    let reason: unknown
    await Promise.all(
      Array.from(membersByBucket, ([bucket, members]) => {
        total += members.length
        const send = () =>
          this.callByName(calls, this.coordinatorName(key, bucket), bucket, (coordinator) =>
            coordinator.telefuncBroadcastForward({ ...delivery, members }),
          )
        // A coordinator that failed to take it gets it once more; it gave each member DO a second try itself.
        return send()
          .catch(() => send())
          .then(
            (lostThere) => {
              lost += lostThere
            },
            (error: unknown) => {
              lost += members.length
              reason ??= error
            },
          )
      }),
    )
    if (lost === 0) return
    throw new Error(`Cloudflare Broadcast delivery of '${key}' lost to ${lost}/${total} Durable Objects`, {
      cause: reason,
    })
  }

  /** At a bucket coordinator: delivers a forwarded delivery to the named member DOs, in arrival order, and once more to
   *  each that failed to take it. Returns how many failed again. */
  async forwardToBucket(calls: BroadcastCalls, { members, ...delivery }: BroadcastForwardRequest): Promise<number> {
    const deliver = (member: string) =>
      calls.call(
        member,
        () => this.stubById(member),
        (stub) => stub.telefuncBroadcastDeliver(delivery),
      )
    const deliveries = await Promise.allSettled(members.map((member) => deliver(member).catch(() => deliver(member))))
    const keys = delivery.routes.map((route) => route.key).join("', '")
    return reportLostDeliveries(`Cloudflare Broadcast delivery of '${keys}'`, deliveries)
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

  /** Hinted with `locationFallback` outside a session: Cloudflare places a DO on its first `get()`, for good. */
  private stubByName(name: string, locationHint: LocationBucket | null): TelefuncBroadcastStub {
    const namespace = this.getNamespace()
    return namespace.get(namespace.idFromName(name), { locationHint: locationHint ?? this.locationFallback })
  }

  private stubById(id: string): TelefuncBroadcastStub {
    const namespace = this.getNamespace()
    return namespace.get(namespace.idFromString(id))
  }
}

/** Logs the deliveries that failed twice, with the first one's reason; returns how many. */
function reportLostDeliveries(delivery: string, outcomes: PromiseSettledResult<unknown>[]): number {
  const failed = outcomes.filter((outcome) => outcome.status === 'rejected')
  if (failed.length > 0) {
    console.error(`${delivery} lost to ${failed.length}/${outcomes.length} Durable Objects: ${failed[0]!.reason}`)
  }
  return failed.length
}

/** Whether a receiver that took `taken` takes `seq` of `origin`, which it then records. */
function takeOnce(taken: Map<string, number>, origin: string, seq: number): boolean {
  if (seq <= (taken.get(origin) ?? 0)) return false
  taken.set(origin, seq)
  return true
}
