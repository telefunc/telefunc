export { CloudflareRoomSessionManager, currentRoomSessionManager, getRoomSessionManager }
export type { RoomSessionDeliveryRequest }

import { type OrderingInfo, type Subscriber, type Unsubscribe, OrderedStubs, hostLocal } from 'telefunc/__internal'
import type { RoomSubscriptionSource } from '../backend/contract.js'
import { encodeLaneKey } from '../backend/laneKey.js'
import { assertUsage } from '../utils/assert.js'
import { ROUTE_RENEW_EVERY_MS, type RouteInstallation } from './routes.js'
import type { CloudflareRoomAuthorityStub, RegisterWire, RenewWire } from './do.js'
import type { CloudflareRoomDriver } from './driver.js'

type RoomSessionDeliveryRequest = {
  route: RouteInstallation
  payload: Uint8Array
  info: OrderingInfo
}

const ROOM_SESSION_MANAGER = Symbol.for('telefunc.room.cloudflareSessionManager')

/** The Room manager of the session DO whose call this is, null outside a session DO's call. */
function currentRoomSessionManager(): CloudflareRoomSessionManager | null {
  return hostLocal(ROOM_SESSION_MANAGER, (host) =>
    host.id === null ? null : new CloudflareRoomSessionManager(host.id),
  )
}

function getRoomSessionManager(): CloudflareRoomSessionManager {
  const manager = currentRoomSessionManager()
  assertUsage(
    manager !== null,
    'A Cloudflare subscription delivers to a Telefunc session: subscribe from a telefunction or a channel handler, not from outside a request.',
  )
  return manager
}

function entryKey(route: Pick<RouteInstallation, 'roomId' | 'inc' | 'laneKey'>): string {
  return JSON.stringify([route.roomId, route.inc, route.laneKey])
}

class CloudflareRoomSessionManager {
  private readonly authorityCalls = new OrderedStubs<CloudflareRoomAuthorityStub>()
  private readonly id: string
  private readonly entries = new Map<string, CloudflareRoomSubscription>()

  constructor(sessionId: string) {
    this.id = sessionId
  }

  subscribe(
    { roomId, inc, lane }: RoomSubscriptionSource,
    subscriber: Subscriber,
    driver: CloudflareRoomDriver,
  ): Unsubscribe {
    const route = { roomId, inc, laneKey: encodeLaneKey(lane), sessionDoId: this.id, leaseId: crypto.randomUUID() }
    const subscription = new CloudflareRoomSubscription({ session: this, route, driver, subscriber })
    this.entries.set(entryKey(route), subscription)
    void subscription.register()
    return () => subscription.unsubscribe()
  }

  /** A call to a room's authority through this session's ordered stub for it, so a room's commits and route calls
   *  reach its authority in the order they were made. A line opens a fresh stub: a stub that rejected may be broken. */
  callAuthority<T>(
    driver: CloudflareRoomDriver,
    roomId: string,
    invoke: (authority: CloudflareRoomAuthorityStub) => Promise<T>,
  ): Promise<T> {
    return this.authorityCalls.call(roomId, () => driver.stub(roomId), invoke)
  }

  forget(subscription: CloudflareRoomSubscription): void {
    const key = entryKey(subscription.route)
    if (this.entries.get(key) === subscription) this.entries.delete(key)
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
  session: CloudflareRoomSessionManager
  route: RouteInstallation
  driver: CloudflareRoomDriver
  subscriber: Subscriber
}

/** Ready once the authority has durably registered this subscription's exact route; Room owns retry and
 *  replacement. */
class CloudflareRoomSubscription {
  readonly route: RouteInstallation
  private readonly session: CloudflareRoomSessionManager
  private readonly driver: CloudflareRoomDriver
  private readonly subscriber: Subscriber
  private ended = false
  private renewalTimer: ReturnType<typeof setTimeout> | undefined

  constructor({ session, route, driver, subscriber }: CloudflareRoomSubscriptionOptions) {
    this.session = session
    this.route = route
    this.driver = driver
    this.subscriber = subscriber
  }

  deliver(payload: Uint8Array, info: OrderingInfo): void {
    this.subscriber.message(payload, info)
  }

  async register(): Promise<void> {
    let registered: RegisterWire
    try {
      registered = await this.callAuthority((authority) => authority.registerRoute(this.route))
    } catch (error) {
      return this.fail(error)
    }
    if (!('ok' in registered)) return this.fail(new Error(registered.reason))
    // Unsubscribed while it registered: its release already went out.
    if (this.ended) return
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
    this.stop()
    // A route that went with its dropped incarnation ends on purpose; a lapsed one is a failure.
    if (renewal === 'dropped') this.subscriber.ended()
    else this.subscriber.failed(new Error(`Cloudflare route '${this.route.laneKey}' lapsed`))
  }

  private fail(error: unknown): void {
    if (this.ended) return
    this.stop()
    this.subscriber.failed(error)
  }

  private stop(): void {
    this.ended = true
    clearTimeout(this.renewalTimer)
    this.session.forget(this)
  }

  private callAuthority<T>(invoke: (authority: CloudflareRoomAuthorityStub) => Promise<T>): Promise<T> {
    return this.session.callAuthority(this.driver, this.route.roomId, invoke)
  }
}
