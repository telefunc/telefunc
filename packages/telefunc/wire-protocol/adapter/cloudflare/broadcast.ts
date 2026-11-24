/// <reference types="@cloudflare/workers-types" />
export { CloudflareBroadcast, CloudflareBroadcastAuthorityState, CloudflareBroadcastMember }
export type {
  BroadcastCalls,
  BroadcastForwardRequest,
  BroadcastHoldRequest,
  BroadcastLeaseRequest,
  BroadcastNamespace,
  BroadcastPublishRequest,
  BroadcastReleaseRequest,
  ForwardLoss,
  MemberPlacement,
  TelefuncBroadcastStub,
}

import {
  KNOWN_BROADCAST_BUCKETS,
  getBucketCoordinatorScale,
  getDeterministicKeyBucketIndex,
  readLocationBucket,
} from './routing.js'
import { CallNotSent, type CallOptions, type Invoke, type OrderedStubs } from './OrderedStubs.js'
import { integerColumn, type DurableStorage, type SqlStore } from './sqlStorage.js'
import { TransientRetry } from './transientRetry.js'
import { assert } from '../../../utils/assert.js'
import {
  DriverAttempt,
  type BackendReceiver,
  type BroadcastPayload,
  type BroadcastRoute,
  type PublishResult,
} from '../../backend/driver.js'
import { broadcastRouteKey } from '../../backend/broadcast/routeKey.js'
import { reportBroadcastDeliveryLost } from '../../backend/broadcast/reportBroadcastDeliveryLost.js'
import { getOrCreate } from '../../../utils/getOrCreate.js'
import type { OrderingInfo } from '../../backend/driverOrdering.js'
import type { CloudflareScale, LocationBucket } from './routing.js'
import { currentCloudflareSession } from './session.js'
import type { BroadcastDelivery } from './extensions.js'
import {
  HolderGrants,
  LEASE_CALL_DEADLINE_MS,
  LEASE_RENEW_EVERY_MS,
  grantHolder,
  initLeaseSchema,
  leaseRegistered,
  liveLeases,
  nextLeaseSweep,
  registerLease,
  removeLease,
  renewLease,
  renewsAfterPause,
  sweepLeases,
  type LeaseOrder,
  type LeaseRegistered,
  type LeaseRemoved,
  type LeaseRenewed,
} from './leases.js'

type BroadcastPublishRequest = {
  key: string
  kind: BroadcastRoute['kind']
  payload: BroadcastPayload
}

/** A delivery, handed to one bucket coordinator for the member DOs it names by id. */
type BroadcastForwardRequest = BroadcastDelivery & { members: string[] }
/** What a bucket coordinator lost of a forward: how many member DOs failed to take it, with the routes an owed one had
 *  no receiver for, and why: the first failure's error, else what had no receiver. */
type ForwardLoss = { lost: number; reason: unknown }

/** A member DO, by id, and the bucket its session was placed in, as it was stored or sent: `forward()` checks each
 *  bucket once. */
type MemberPlacement = { member: string; bucket: string }

/** A member DO's presence on a route, at the key's authority, by DO id and the lease of its subscription there. */
type BroadcastLeaseRequest = { key: string; kind: BroadcastRoute['kind']; member: string; lease: string }
/** Records the presence, in the bucket its member DO was placed in. */
type BroadcastHoldRequest = BroadcastLeaseRequest & { bucket: LocationBucket; order: LeaseOrder }
type BroadcastReleaseRequest = BroadcastLeaseRequest & { order: LeaseOrder }

type TelefuncBroadcastStub = {
  telefuncBroadcastPublish(request: BroadcastPublishRequest): Promise<PublishResult>
  /** Resolves to what of the delivery the member DOs it names didn't take, or null where they took it all. */
  telefuncBroadcastForward(request: BroadcastForwardRequest): Promise<ForwardLoss | null>
  /** How many of an owed delivery's routes it had no receiver for. */
  telefuncBroadcastDeliver(delivery: BroadcastDelivery): Promise<number>
  /** A new grant for the member DO, for its writes of presence at this key's authority (`leases.ts`). */
  telefuncBroadcastGrant(member: string): Promise<number>
  telefuncBroadcastHold(request: BroadcastHoldRequest): Promise<LeaseRegistered>
  telefuncBroadcastRenew(request: BroadcastLeaseRequest): Promise<LeaseRenewed>
  telefuncBroadcastRelease(request: BroadcastReleaseRequest): Promise<LeaseRemoved>
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
  readonly lease = crypto.randomUUID()
  /** Its presence lapsed and was reported, and is recorded again before it is renewed. */
  private lapsed = false
  /** When the write its key's authority last confirmed its presence by went out, by this DO's clock: the authority's
   *  lease runs from no earlier. */
  private confirmedAt = 0
  /** When its last hold went out, by this DO's clock. */
  private heldAt = 0
  private refreshTimer: ReturnType<typeof setInterval> | undefined
  /** A refresh its key's authority's overload pause kept from being sent, made once the pause ends. */
  private refreshAfterPause: ReturnType<typeof setTimeout> | undefined

  constructor({ route }: MemberRouteOptions) {
    this.route = route
  }

  /** A hold of its presence goes out. */
  holding(): void {
    this.heldAt = Date.now()
  }

  /** Its key's authority recorded the hold that went out last. */
  presenceRecorded(): void {
    this.confirmed(this.heldAt)
  }

  /** Its key's authority confirmed its presence by a write that went out at `sentAt`: ready. */
  private confirmed(sentAt: number): void {
    this.confirmedAt = sentAt
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

  startRefresh(
    renew: () => Promise<LeaseRenewed>,
    hold: () => Promise<LeaseRegistered>,
    pausedFor: () => number,
  ): void {
    this.refreshTimer = setInterval(() => void this.refreshPresence(renew, hold, pausedFor), LEASE_RENEW_EVERY_MS)
  }

  /** A lapse its key's authority answers is reported once, however many renewals it answers it to, until the presence
   *  is recorded again. */
  private async refreshPresence(
    renew: () => Promise<LeaseRenewed>,
    hold: () => Promise<LeaseRegistered>,
    pausedFor: () => number,
  ): Promise<void> {
    clearTimeout(this.refreshAfterPause)
    this.refreshAfterPause = undefined
    const sentAt = Date.now()
    try {
      if (!this.lapsed) {
        if ((await renew()) === 'renewed') return this.confirmed(sentAt)
        this.lapsed = true
        const reason = `this Durable Object's presence lapsed, so what was published since didn't reach it`
        reportBroadcastDeliveryLost(this.route.key, reason)
      }
      // Else a later write of this DO's on the route came first: its release.
      if ((await hold()) !== 'registered') return
    } catch (error) {
      if (!this.lapsed && renewsAfterPause(error, this.confirmedAt)) {
        this.refreshAfterPause = setTimeout(() => void this.refreshPresence(renew, hold, pausedFor), pausedFor())
        return
      }
      return this.presenceLost(error)
    }
    this.lapsed = false
    this.presenceRecorded()
  }

  stopRefresh(): void {
    clearInterval(this.refreshTimer)
    clearTimeout(this.refreshAfterPause)
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

const AUTHORITY_SCHEMA = 'CREATE TABLE IF NOT EXISTS broadcast_key (key TEXT PRIMARY KEY, seq INTEGER NOT NULL)'

/** The Broadcast state of a key authority DO, in its SQLite storage: each key's `seq` and each route's member presence.
 *  The tables are created on first use, so a DO in another role never has them. */
class CloudflareBroadcastAuthorityState {
  private readonly storage: DurableStorage
  private schema = false
  /** The alarm this run of the object set, null for none; undefined until it first sets or clears it. */
  private alarmAt: number | null | undefined = undefined

  constructor(state: { storage: DurableStorage }) {
    this.storage = state.storage
  }

  nextSequence(key: string): number {
    const sql = this.sql()
    return this.storage.transactionSync(() => {
      const row = sql.exec('SELECT seq FROM broadcast_key WHERE key = ?', key).toArray()[0]
      const seq = (row === undefined ? 0 : integerColumn(row, 'seq')) + 1
      sql.exec('INSERT OR REPLACE INTO broadcast_key (key, seq) VALUES (?, ?)', key, seq)
      return seq
    })
  }

  /** A grant for a member DO's writes of presence here; a member DO that holds no presence lapses with its grant. */
  async grant(member: string): Promise<number> {
    const now = Date.now()
    const grant = this.transaction((sql) => {
      sweepLeases(sql, now)
      return grantHolder(sql, member, now)
    })
    await this.scheduleSweep()
    return grant
  }

  /** The RPC reply waits for the write. A hold that came before is answered as it was. */
  async hold({ key, kind, member, lease, bucket, order }: BroadcastHoldRequest): Promise<LeaseRegistered> {
    const scope = broadcastRouteKey({ key, kind })
    const held = this.transaction((sql) =>
      registerLease(sql, { scope, holder: member, lease, bucket }, order, Date.now()),
    )
    await this.scheduleSweep()
    return leaseRegistered(held)
  }

  renew({ key, kind, member, lease }: BroadcastLeaseRequest): LeaseRenewed {
    const scope = broadcastRouteKey({ key, kind })
    return this.transaction((sql) => renewLease(sql, scope, member, lease, Date.now()))
  }

  async release({ key, kind, member, lease, order }: BroadcastReleaseRequest): Promise<LeaseRemoved> {
    const scope = broadcastRouteKey({ key, kind })
    const removed = this.transaction((sql) => removeLease(sql, scope, member, lease, order, Date.now()))
    await this.scheduleSweep()
    return removed
  }

  /** Drops the holders that went idle, so a key nobody holds stores no lease row once `LEASE_TTL_MS` passed. */
  async alarm(): Promise<void> {
    this.alarmAt = null
    this.transaction((sql) => sweepLeases(sql, Date.now()))
    await this.scheduleSweep()
  }

  /** The route's member DOs whose presence is unexpired, each with its bucket. */
  livePresence(routeKey: string, now: number): MemberPlacement[] {
    return liveLeases(this.sql(), [routeKey], now, (member, bucket) => ({ member, bucket }))
  }

  private transaction<T>(fn: (sql: SqlStore) => T): T {
    const sql = this.sql()
    return this.storage.transactionSync(() => fn(sql))
  }

  /** Sets the alarm to when a holder may next go idle in the turn of the write that changed it, with no `await`
   *  between them, so both are stored or neither. An earlier alarm sets the next one when it fires. */
  private scheduleSweep(): Promise<void> {
    const next = nextLeaseSweep(this.sql())
    const current = this.alarmAt
    if (next === null) {
      if (current === null) return Promise.resolve()
      this.alarmAt = null
      return this.storage.deleteAlarm()
    }
    if (current !== undefined && current !== null && current <= next) return Promise.resolve()
    this.alarmAt = next
    return this.storage.setAlarm(next)
  }

  private sql(): SqlStore {
    if (!this.schema) {
      this.storage.sql.exec(AUTHORITY_SCHEMA)
      initLeaseSchema(this.storage.sql)
      this.schema = true
    }
    return this.storage.sql
  }
}

/** The receiver of a local route released on purpose: what still comes for it is taken, and dropped. */
const RELEASED: BackendReceiver = { deliver() {} }

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
  private readonly grants = new HolderGrants()
  /** The receivers of routes whose sender keeps its own list of receivers, by route key. */
  private readonly localReceivers = new Map<string, BackendReceiver>()

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
    const existing = this.routes.get(routeKey)
    // Else it was unsubscribed while establishing, and is subscribed again before its presence was recorded.
    const memberRoute = existing ?? new MemberRoute({ route })
    const attempt = new MemberAttempt({ member: this, memberRoute, receiver })
    memberRoute.attempt = attempt
    if (existing === undefined) {
      this.routes.set(routeKey, memberRoute)
      void this.recordFirstPresence(routeKey, memberRoute)
    }
    return attempt
  }

  /** Takes `route`'s deliveries for `receiver` with no presence at the key's authority: for a sender that keeps its
   *  own list of receivers, as a room authority does. A later `subscribeLocal()` to the route replaces it. Released, the
   *  route takes what still comes for it and drops it, as released on purpose, until it is subscribed again. */
  subscribeLocal(route: BroadcastRoute, receiver: BackendReceiver): () => void {
    const routeKey = broadcastRouteKey(route)
    this.localReceivers.set(routeKey, receiver)
    return () => {
      if (this.localReceivers.get(routeKey) === receiver) this.localReceivers.set(routeKey, RELEASED)
    }
  }

  /** A coordinator's delivery to this DO. The authority forwards only to an unexpired record, so what arrives is
   *  owed, lost route or not. Returns how many of an owed delivery's routes this DO has no receiver for, as after it
   *  restarted. */
  deliver({ routes, payload, info, owed }: BroadcastDelivery): number {
    let missing = 0
    for (const route of routes) {
      const routeKey = broadcastRouteKey(route)
      const receiver = this.routes.get(routeKey)?.attempt?.receiver ?? this.localReceivers.get(routeKey)
      if (receiver === undefined) {
        if (owed) missing++
        continue
      }
      // A receiver's bug is no lost delivery, and the routes after it still get theirs.
      try {
        receiver.deliver(payload, info)
      } catch (error) {
        console.error(error)
      }
    }
    return missing
  }

  async unsubscribe(memberRoute: MemberRoute): Promise<void> {
    memberRoute.attempt = null
    // An establishing route is released once its presence is recorded.
    if (memberRoute.state !== 'establishing') await this.release(broadcastRouteKey(memberRoute.route), memberRoute)
  }

  /** Written again after a failure Cloudflare calls transient, a few times (`TransientRetry`), unless nobody wants the
   *  route by then; after any other failure, or the last, its attempt closes with the error, or with the overload that
   *  kept it from being sent. A write that may have been stored is then released. */
  private async recordFirstPresence(routeKey: string, memberRoute: MemberRoute): Promise<void> {
    const retry = new TransientRetry()
    let sent = false
    for (;;) {
      try {
        const held = await this.hold(memberRoute, retry)
        sent = true
        // A write of this DO's on the route that was sent later came first.
        if (held === 'stale')
          throw new Error(`Cloudflare Broadcast presence for '${memberRoute.route.key}' was overtaken`)
        break
      } catch (error) {
        if (!(error instanceof CallNotSent)) sent = true
        const reason = retry.reasonFor(error)
        const delay = memberRoute.attempt === null ? null : retry.delayAfter(error)
        if (delay === null) {
          this.routes.delete(routeKey)
          if (sent) void this.removeUntilDone(memberRoute)
          memberRoute.attempt?.presence('closed', reason)
          return
        }
        await new Promise((resolve) => setTimeout(resolve, delay))
      }
    }
    memberRoute.presenceRecorded()
    if (memberRoute.attempt === null) return this.release(routeKey, memberRoute)
    const name = this.broadcast.authorityName(memberRoute.route.key)
    memberRoute.startRefresh(
      () => this.renew(memberRoute),
      () => this.hold(memberRoute),
      () => this.calls.pausedFor(name),
    )
  }

  private release(routeKey: string, memberRoute: MemberRoute): Promise<void> {
    memberRoute.stopRefresh()
    this.routes.delete(routeKey)
    return this.removeUntilDone(memberRoute)
  }

  /** Releases the route's presence as many times as it takes, whoever waits for it. */
  private removeUntilDone(memberRoute: MemberRoute): Promise<void> {
    const request = this.leaseRequest(memberRoute)
    const release = () =>
      this.write(memberRoute.route, (authority, order) => authority.telefuncBroadcastRelease({ ...request, order }), {
        deadline: Date.now() + LEASE_CALL_DEADLINE_MS,
        paused: true,
      })
    const name = this.broadcast.authorityName(memberRoute.route.key)
    return this.grants.removeUntilDone(release, () => this.calls.pausedFor(name))
  }

  /** While the route is wanted, in its time, and its key's authority isn't paused: a first one's attempt is made
   *  through `retry`, within its window, with the grant it may ask for first. */
  private hold(memberRoute: MemberRoute, retry?: TransientRetry): Promise<LeaseRegistered> {
    assert(this.locatedBucket, 'A Broadcast member registers from a session that knows its bucket')
    const request = { ...this.leaseRequest(memberRoute), bucket: this.locatedBucket }
    const options = {
      wanted: () =>
        retry === undefined
          ? this.routes.get(broadcastRouteKey(memberRoute.route)) === memberRoute
          : memberRoute.attempt !== null,
      deadline: retry === undefined ? Date.now() + LEASE_CALL_DEADLINE_MS : retry.deadline,
      paused: true,
    }
    return this.write(
      memberRoute.route,
      (authority, order) => {
        memberRoute.holding()
        return retry === undefined
          ? authority.telefuncBroadcastHold({ ...request, order })
          : retry.attempt(() => authority.telefuncBroadcastHold({ ...request, order }))
      },
      options,
    )
  }

  private async renew(memberRoute: MemberRoute): Promise<LeaseRenewed> {
    const request = this.leaseRequest(memberRoute)
    const sentAt = Date.now()
    const renewed = await this.broadcast.callAuthority(
      this.calls,
      memberRoute.route.key,
      this.locatedBucket,
      (authority) => authority.telefuncBroadcastRenew(request),
      { deadline: sentAt + LEASE_RENEW_EVERY_MS, paused: true },
    )
    if (renewed === 'renewed') this.grants.renewed(this.broadcast.authorityName(memberRoute.route.key), sentAt)
    return renewed
  }

  /** Through the DO's ordered stubs, with its grant at the key's authority and its next order there. */
  private write<T>(
    route: BroadcastRoute,
    write: (authority: TelefuncBroadcastStub, order: LeaseOrder) => Promise<T | 'regrant'>,
    options: CallOptions,
  ): Promise<T> {
    const call = <R>(invoke: Invoke<TelefuncBroadcastStub, R> | Promise<Invoke<TelefuncBroadcastStub, R>>) =>
      this.broadcast.callAuthority(this.calls, route.key, this.locatedBucket, invoke, options)
    return this.grants.send(
      this.broadcast.authorityName(route.key),
      () => call((authority) => authority.telefuncBroadcastGrant(this.id)),
      (order) => call(order.then((order) => (authority: TelefuncBroadcastStub) => write(authority, order))),
    )
  }

  private leaseRequest({ route, lease }: MemberRoute): BroadcastLeaseRequest {
    return { key: route.key, kind: route.kind, member: this.id, lease }
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

  /** Through `calls`, to the authority of `key`. */
  callAuthority<T>(
    calls: BroadcastCalls,
    key: string,
    locationHint: LocationBucket | null,
    invoke: Invoke<TelefuncBroadcastStub, T> | Promise<Invoke<TelefuncBroadcastStub, T>>,
    options?: CallOptions,
  ): Promise<T> {
    return calls.call(
      this.authorityName(key),
      () => this.stubByName(this.authorityName(key), locationHint),
      invoke,
      options,
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
   *  `members`; resolves once every member DO took it, and rejects once one failed to take it. The forwards leave
   *  through `calls` before this returns, so a DO's forwards for one key reach each coordinator in the order it made
   *  them. */
  async forward(
    calls: BroadcastCalls,
    key: string,
    delivery: BroadcastDelivery,
    members: Iterable<MemberPlacement>,
  ): Promise<void> {
    const membersByBucket = new Map<string, string[]>()
    let total = 0
    for (const { member, bucket } of members) {
      getOrCreate(membersByBucket, bucket, () => []).push(member)
      total++
    }
    let lost = 0
    let reason: unknown
    const forwardThrough = async ([name, members]: [string, string[]]) => {
      const bucket = readLocationBucket(name)
      try {
        const loss = await this.callByName(calls, this.coordinatorName(key, bucket), bucket, (coordinator) =>
          coordinator.telefuncBroadcastForward({ ...delivery, members }),
        )
        if (loss === null) return
        lost += loss.lost
        reason ??= loss.reason
      } catch (error) {
        lost += members.length
        reason ??= error
      }
    }
    await Promise.all(Array.from(membersByBucket, forwardThrough))
    if (lost === 0) return
    throw new Error(`Cloudflare Broadcast delivery of '${key}' lost to ${lost}/${total} Durable Objects`, {
      cause: reason,
    })
  }

  /** At a bucket coordinator: delivers a forwarded delivery to the named member DOs, in arrival order. Returns what they
   *  lost of it, or null where none lost any. */
  async forwardToBucket(
    calls: BroadcastCalls,
    { members, ...delivery }: BroadcastForwardRequest,
  ): Promise<ForwardLoss | null> {
    const deliveries = await Promise.allSettled(
      members.map((member) =>
        calls.call(
          member,
          () => this.stubById(member),
          (stub) => stub.telefuncBroadcastDeliver(delivery),
        ),
      ),
    )
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

  authorityName(key: string): string {
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

/** Logs the deliveries that failed, with the first one's reason; returns how many were lost, with the routes a member
 *  DO had no receiver for, and why, or null where none was. */
function reportLostDeliveries(delivery: string, outcomes: PromiseSettledResult<number>[]): ForwardLoss | null {
  let failed = 0
  let missing = 0
  let reason: unknown
  for (const outcome of outcomes) {
    if (outcome.status === 'fulfilled') {
      missing += outcome.value
    } else {
      if (failed === 0) reason = outcome.reason
      failed++
    }
  }
  if (failed > 0) console.error(`${delivery} lost to ${failed}/${outcomes.length} Durable Objects: ${reason}`)
  const lost = failed + missing
  if (lost === 0) return null
  return {
    lost,
    reason: failed > 0 ? reason : new Error(`${delivery} had no receiver for ${missing} of its owed routes`),
  }
}
