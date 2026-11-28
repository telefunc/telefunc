/// <reference types="@cloudflare/workers-types" />
export { CloudflareBroadcastMember }

import type { LocationBucket } from './routing.js'
import { CallNotSent, type CallOptions, type Invoke } from './OrderedStubs.js'
import { TransientRetry } from './transientRetry.js'
import { assert } from '../../../utils/assert.js'
import { DriverAttempt, type BackendReceiver, type BroadcastRoute } from '../../backend/driver.js'
import { broadcastRouteKey } from '../../backend/broadcast/routeKey.js'
import { reportBroadcastDeliveryLost } from '../../backend/broadcast/reportBroadcastDeliveryLost.js'
import type { BroadcastDelivery } from './extensions.js'
import {
  HolderGrants,
  LEASE_CALL_DEADLINE_MS,
  LEASE_RENEW_EVERY_MS,
  renewsAfterPause,
  type LeaseOrder,
  type LeaseRegistered,
  type LeaseRenewed,
} from './leases.js'
import type { BroadcastLeaseRequest, TelefuncBroadcastStub, BroadcastCalls } from './broadcastRpc.js'
import type { CloudflareBroadcast } from './CloudflareBroadcast.js'

type MemberRouteOptions = { route: BroadcastRoute }

/** One route's presence at its key's authority, for one member DO, and the attempt it serves, which holds whether the
 *  presence is recorded. */
class MemberRoute {
  /** Null once it unsubscribed: an establishing route is released once its presence is recorded. */
  attempt: MemberAttempt | null = null
  readonly route: BroadcastRoute
  readonly lease = crypto.randomUUID()
  /** Its presence lapsed and was reported, and is recorded again before it is renewed. */
  lapsed = false
  /** When the write its key's authority last confirmed its presence by went out, by this DO's clock: the authority's
   *  lease runs from no earlier. */
  private confirmedAt = 0
  /** When its last hold went out, by this DO's clock. */
  private heldAt = 0
  /** Its presence's next refresh, which the member sets after each one, while the route is wanted. */
  refreshTimer: ReturnType<typeof setTimeout> | undefined

  constructor({ route }: MemberRouteOptions) {
    this.route = route
  }

  /** A hold of its presence goes out. */
  holding(): void {
    this.heldAt = Date.now()
  }

  /** Its key's authority recorded the hold that went out last: no longer lapsed. */
  presenceRecorded(): void {
    this.lapsed = false
    this.confirmed(this.heldAt)
  }

  /** Its key's authority confirmed its presence by a write that went out at `sentAt`: ready. */
  confirmed(sentAt: number): void {
    this.confirmedAt = sentAt
    this.attempt?.recorded()
  }

  /** Whether a renewal `error` kept from being sent is sent again once its authority's overload pause ends: while the
   *  presence it renews is still live there. */
  renewsAfterPause(error: unknown): boolean {
    return !this.lapsed && renewsAfterPause(error, this.confirmedAt)
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

  /** Its route's presence was recorded. */
  recorded(): void {
    this.transition('ready')
  }

  /** Its route's presence may have lapsed: a refresh failed with `error`, which is reported where it was ready. */
  lost(error: unknown): void {
    if (this.state() !== 'ready') return
    console.error(new Error(`Cloudflare Broadcast presence for '${this.memberRoute.route.key}' lost`, { cause: error }))
    this.transition('lost')
  }

  /** Its route's presence failed to record, with `reason`. */
  failed(reason: unknown): void {
    this.transition('closed', reason)
  }

  unsubscribe(): Promise<void> {
    const recorded = this.state() !== 'establishing'
    this.transition('closed')
    return this.member.unsubscribe(this.memberRoute, recorded)
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
    this.locatedBucket = bucket
  }

  openSubscription(route: BroadcastRoute, receiver: BackendReceiver): DriverAttempt {
    const routeKey = broadcastRouteKey(route)
    // One that is still here was unsubscribed while establishing: it is subscribed again before its presence was
    // recorded, and that presence serves this attempt.
    const existing = this.routes.get(routeKey)
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

  /** `recorded`: whether the route's presence was recorded; an establishing route is released once it is. */
  async unsubscribe(memberRoute: MemberRoute, recorded: boolean): Promise<void> {
    memberRoute.attempt = null
    if (recorded) await this.release(broadcastRouteKey(memberRoute.route), memberRoute)
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
          memberRoute.attempt?.failed(reason)
          return
        }
        await new Promise((resolve) => setTimeout(resolve, delay))
      }
    }
    memberRoute.presenceRecorded()
    if (memberRoute.attempt === null) return this.release(routeKey, memberRoute)
    this.refreshIn(memberRoute, LEASE_RENEW_EVERY_MS)
  }

  /** The route's presence is refreshed `delay` from now, while it is still the route's. */
  private refreshIn(memberRoute: MemberRoute, delay: number): void {
    if (this.routes.get(broadcastRouteKey(memberRoute.route)) !== memberRoute) return
    memberRoute.refreshTimer = setTimeout(() => void this.refreshPresence(memberRoute), delay)
  }

  /** Renewed, or recorded again once it lapsed, then refreshed again: 30 s after this one went out, or once its key's
   *  authority's overload pause ends where that pause kept the renewal from being sent. A lapse its key's authority
   *  answers is reported once, however many renewals it answers it to, until the presence is recorded again. */
  private async refreshPresence(memberRoute: MemberRoute): Promise<void> {
    const sentAt = Date.now()
    try {
      if (!memberRoute.lapsed) {
        if ((await this.renew(memberRoute)) === 'renewed') {
          memberRoute.confirmed(sentAt)
          return this.refreshIn(memberRoute, renewalDue(sentAt))
        }
        memberRoute.lapsed = true
        const reason = `this Durable Object's presence lapsed, so what was published since didn't reach it`
        reportBroadcastDeliveryLost(memberRoute.route.key, reason)
      }
      // A hold that isn't registered was overtaken by a later write of this DO's on the route: its release.
      if ((await this.hold(memberRoute)) === 'registered') memberRoute.presenceRecorded()
    } catch (error) {
      if (memberRoute.renewsAfterPause(error))
        return this.refreshIn(memberRoute, this.calls.pausedFor(this.broadcast.authorityName(memberRoute.route.key)))
      memberRoute.attempt?.lost(error)
    }
    this.refreshIn(memberRoute, renewalDue(sentAt))
  }

  private release(routeKey: string, memberRoute: MemberRoute): Promise<void> {
    clearTimeout(memberRoute.refreshTimer)
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
    if (renewed === 'renewed') this.grants.counted(this.broadcast.authorityName(memberRoute.route.key), sentAt)
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

/** How long from now until the refresh that is due 30 s after one that went out at `sentAt`. */
function renewalDue(sentAt: number): number {
  return Math.max(0, sentAt + LEASE_RENEW_EVERY_MS - Date.now())
}
