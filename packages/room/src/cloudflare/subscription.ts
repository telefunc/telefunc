export { RoomSessionSubscriptions, currentRoomSessionSubscriptions, getRoomSessionSubscriptions, deliverToRoomSession }
export type { RoomSessionDeliveryRequest }

import { type OrderingInfo, type Subscriber, type Unsubscribe, OrderedStubs, hostLocal } from 'telefunc/__internal'
import type { RoomSubscriptionSource } from '../backend/contract.js'
import { encodeLaneKey } from '../backend/laneKey.js'
import { assertUsage } from '../utils/assert.js'
import { ROUTE_RENEW_EVERY_MS, type RouteInstallation } from './routes.js'
import type { CloudflareRoomAuthorityStub, RegisterWire, RenewWire } from './do.js'
import type { CloudflareRoomDriver } from './CloudflareRoomDriver.js'

type RoomSessionDeliveryRequest = {
  route: RouteInstallation
  payload: Uint8Array
  info: OrderingInfo
}

const ROOM_SESSION = Symbol.for('telefunc.room.cloudflareSession')

/** A session DO's Room part: its subscriptions, made by the first subscribe or lane commit made in it. */
type RoomSession = { readonly id: string; subscriptions: RoomSessionSubscriptions | null }

/** The session DO whose call this is, null outside a session DO's call. */
function currentRoomSession(): RoomSession | null {
  return hostLocal(ROOM_SESSION, (host) => (host.id === null ? null : { id: host.id, subscriptions: null }))
}

/** The Room subscriptions of the session DO whose call this is, null outside a session DO's call. */
function currentRoomSessionSubscriptions(driver: CloudflareRoomDriver): RoomSessionSubscriptions | null {
  const session = currentRoomSession()
  if (session === null) return null
  return (session.subscriptions ??= new RoomSessionSubscriptions({ sessionId: session.id, driver }))
}

function getRoomSessionSubscriptions(driver: CloudflareRoomDriver): RoomSessionSubscriptions {
  const subscriptions = currentRoomSessionSubscriptions(driver)
  assertUsage(
    subscriptions !== null,
    'A Cloudflare subscription delivers to a Telefunc session: subscribe from a telefunction or a channel handler, not from outside a request.',
  )
  return subscriptions
}

/** A room authority's delivery to this session DO, which has no subscription to take it before its first one. */
function deliverToRoomSession(request: RoomSessionDeliveryRequest): void {
  currentRoomSession()?.subscriptions?.deliver(request)
}

function entryKey(route: Pick<RouteInstallation, 'roomId' | 'inc' | 'laneKey'>): string {
  return JSON.stringify([route.roomId, route.inc, route.laneKey])
}

type RoomSessionSubscriptionsOptions = { sessionId: string; driver: CloudflareRoomDriver }

/** A session DO's Room subscriptions, and its calls to room authorities. */
class RoomSessionSubscriptions {
  private readonly authorityCalls = new OrderedStubs<CloudflareRoomAuthorityStub>()
  private readonly id: string
  private readonly driver: CloudflareRoomDriver
  private readonly entries = new Map<string, CloudflareRoomSubscription>()

  constructor({ sessionId, driver }: RoomSessionSubscriptionsOptions) {
    this.id = sessionId
    this.driver = driver
  }

  subscribe({ roomId, inc, lane }: RoomSubscriptionSource, subscriber: Subscriber): Unsubscribe {
    const route = { roomId, inc, laneKey: encodeLaneKey(lane), sessionDoId: this.id, leaseId: crypto.randomUUID() }
    const subscription = new CloudflareRoomSubscription({ owner: this, route, subscriber })
    this.entries.set(entryKey(route), subscription)
    void subscription.register()
    return () => subscription.unsubscribe()
  }

  /** A call to a room's authority through this session's ordered stub for it, so a room's commits and route calls
   *  reach its authority in the order they were made. A line opens a fresh stub: a stub that rejected may be broken. */
  callAuthority<T>(roomId: string, invoke: (authority: CloudflareRoomAuthorityStub) => Promise<T>): Promise<T> {
    return this.authorityCalls.call(roomId, () => this.driver.stub(roomId), invoke)
  }

  /** `route`'s subscription ended: a delivery to it is dropped from now on. */
  routeClosed(route: RouteInstallation): void {
    const key = entryKey(route)
    if (this.entries.get(key)?.route === route) this.entries.delete(key)
  }

  /** A delivery to a lease this session no longer holds, as after a restart, is dropped: delivery is at-most-once, and
   *  the authority's route lapses with the lease. */
  deliver(request: RoomSessionDeliveryRequest): void {
    const entry = this.entries.get(entryKey(request.route))
    if (entry?.route.leaseId !== request.route.leaseId) return
    entry.deliver(request.payload, request.info)
  }
}

type CloudflareRoomSubscriptionOptions = {
  owner: RoomSessionSubscriptions
  route: RouteInstallation
  subscriber: Subscriber
}

/** Ready once the authority has durably registered this subscription's exact route; Room owns retry and
 *  replacement. */
class CloudflareRoomSubscription {
  readonly route: RouteInstallation
  private readonly owner: RoomSessionSubscriptions
  private readonly subscriber: Subscriber
  private ended = false
  private renewalTimer: ReturnType<typeof setTimeout> | undefined

  constructor({ owner, route, subscriber }: CloudflareRoomSubscriptionOptions) {
    this.owner = owner
    this.route = route
    this.subscriber = subscriber
  }

  deliver(payload: Uint8Array, info: OrderingInfo): void {
    this.subscriber.message(payload, info)
  }

  async register(): Promise<void> {
    let registration: RegisterWire
    try {
      registration = await this.callAuthority((authority) => authority.registerRoute(this.route))
    } catch (error) {
      return this.fail(error)
    }
    // Unsubscribed while it registered: its release already went out.
    if (this.ended) return
    if (registration === 'dropped') return this.end()
    this.subscriber.ready()
    this.scheduleRenewal()
  }

  async unsubscribe(): Promise<void> {
    this.stop()
    await this.callAuthority((authority) => authority.unsubscribeRoute(this.route))
  }

  private scheduleRenewal(): void {
    this.renewalTimer = setTimeout(() => void this.renew(), ROUTE_RENEW_EVERY_MS)
  }

  private async renew(): Promise<void> {
    let renewal: RenewWire
    try {
      renewal = await this.callAuthority((authority) => authority.renewRoute(this.route))
    } catch (error) {
      return this.fail(error)
    }
    if (this.ended) return
    if (renewal === 'renewed') return this.scheduleRenewal()
    // A route that went with its dropped incarnation ends on purpose; a lapsed one is a failure.
    if (renewal === 'dropped') this.end()
    else this.fail(new Error(`Cloudflare route '${this.route.laneKey}' lapsed`))
  }

  /** Its incarnation is no longer open. */
  private end(): void {
    this.stop()
    this.subscriber.ended()
  }

  private fail(error: unknown): void {
    if (this.ended) return
    this.stop()
    this.subscriber.failed(error)
  }

  private stop(): void {
    this.ended = true
    clearTimeout(this.renewalTimer)
    this.owner.routeClosed(this.route)
  }

  private callAuthority<T>(invoke: (authority: CloudflareRoomAuthorityStub) => Promise<T>): Promise<T> {
    return this.owner.callAuthority(this.route.roomId, invoke)
  }
}
