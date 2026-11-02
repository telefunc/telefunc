export { RoomSessionSubscriptions, currentRoomSessionSubscriptions, getRoomSessionSubscriptions }

import {
  type BackendReceiver,
  type CloudflareMember,
  DriverAttempt,
  OrderedStubs,
  currentCloudflareMember,
  hostLocal,
} from 'telefunc/__internal'
import type { RoomSubscriptionSource } from '../backend/contract.js'
import { encodeLaneKey } from '../backend/laneKey.js'
import { assert, assertUsage } from '../utils/assert.js'
import { ROUTE_RENEW_EVERY_MS, laneRoute, type RouteInstallation } from './routes.js'
import type { CloudflareRoomAuthorityStub, RegisterWire, RenewWire } from './RoomAuthority.js'
import type { CloudflareRoomDriver } from './CloudflareRoomDriver.js'

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
  const member = currentCloudflareMember()
  assert(member !== undefined) // a session DO's call runs in its Cloudflare session
  return (session.subscriptions ??= new RoomSessionSubscriptions({ sessionId: session.id, member, driver }))
}

function getRoomSessionSubscriptions(driver: CloudflareRoomDriver): RoomSessionSubscriptions {
  const subscriptions = currentRoomSessionSubscriptions(driver)
  assertUsage(
    subscriptions !== null,
    'A Cloudflare subscription delivers to a Telefunc session: subscribe from a telefunction or a channel handler, not from outside a request.',
  )
  return subscriptions
}

type RoomSessionSubscriptionsOptions = { sessionId: string; member: CloudflareMember; driver: CloudflareRoomDriver }

/** A session DO's Room subscriptions, and its calls to room authorities. */
class RoomSessionSubscriptions {
  private readonly authorityCalls = new OrderedStubs<CloudflareRoomAuthorityStub>()
  private readonly id: string
  private readonly member: CloudflareMember
  private readonly driver: CloudflareRoomDriver

  constructor({ sessionId, member, driver }: RoomSessionSubscriptionsOptions) {
    this.id = sessionId
    this.member = member
    this.driver = driver
  }

  /** The lane's frames reach `receiver` from the Broadcast coordinator in this session's bucket, which the route it
   *  registers names. A later subscription to the lane takes them over, those committed under this one's lease too. */
  subscribe({ roomId, inc, lane }: RoomSubscriptionSource, receiver: BackendReceiver): DriverAttempt {
    const { bucket } = this.member
    assert(bucket !== null, 'A Room subscription registers from a session that knows its bucket')
    const laneKey = encodeLaneKey(lane)
    const route = { roomId, inc, laneKey, sessionDoId: this.id, leaseId: crypto.randomUUID(), bucket }
    const stopDeliveries = this.member.subscribeLocal(laneRoute(inc, laneKey), receiver)
    const subscription = new CloudflareRoomAttempt({ owner: this, route, stopDeliveries })
    void subscription.register()
    return subscription
  }

  /** Calls the room's authority through the ordered stub this session keeps for it, so a room's commits and route calls
   *  reach its authority in the order they were made. A line opens a fresh stub: a stub that rejected may be broken. */
  callAuthority<T>(roomId: string, invoke: (authority: CloudflareRoomAuthorityStub) => Promise<T>): Promise<T> {
    return this.authorityCalls.call(roomId, () => this.driver.stub(roomId), invoke)
  }
}

type CloudflareRoomAttemptOptions = {
  owner: RoomSessionSubscriptions
  route: RouteInstallation
  /** Stops the lane's frames reaching its receiver. */
  stopDeliveries: () => void
}

/** Ready once the authority has durably registered this subscription's exact route; Room owns retry and
 *  replacement. */
class CloudflareRoomAttempt extends DriverAttempt {
  readonly route: RouteInstallation
  private readonly owner: RoomSessionSubscriptions
  private readonly stopDeliveries: () => void
  private renewalTimer: ReturnType<typeof setTimeout> | undefined

  constructor({ owner, route, stopDeliveries }: CloudflareRoomAttemptOptions) {
    super()
    this.owner = owner
    this.route = route
    this.stopDeliveries = stopDeliveries
  }

  async register(): Promise<void> {
    let registration: RegisterWire
    try {
      registration = await this.callAuthority((authority) => authority.registerRoute(this.route))
    } catch (error) {
      return this.fail(error)
    }
    // Unsubscribed while it registered: its release already went out.
    if (this.state() === 'closed') return
    if (registration === 'dropped') return this.end()
    this.transition('ready')
    this.scheduleRenewal()
  }

  async unsubscribe(): Promise<void> {
    this.stop()
    this.transition('closed')
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
    if (this.state() === 'closed') return
    if (renewal === 'renewed') return this.scheduleRenewal()
    // A route that went with its dropped incarnation ends on purpose; a lapsed one is a failure.
    if (renewal === 'dropped') this.end()
    else this.fail(new Error(`Cloudflare route '${this.route.laneKey}' lapsed`))
  }

  /** Its incarnation is no longer open: closed on purpose, with no reason. */
  private end(): void {
    this.stop()
    this.transition('closed')
  }

  private fail(error: unknown): void {
    if (this.state() === 'closed') return
    this.stop()
    this.transition('closed', error)
  }

  private stop(): void {
    clearTimeout(this.renewalTimer)
    this.stopDeliveries()
  }

  private callAuthority<T>(invoke: (authority: CloudflareRoomAuthorityStub) => Promise<T>): Promise<T> {
    return this.owner.callAuthority(this.route.roomId, invoke)
  }
}
