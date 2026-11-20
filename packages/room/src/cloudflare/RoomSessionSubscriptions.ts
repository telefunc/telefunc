export { RoomSessionSubscriptions, currentRoomSessionSubscriptions }

import {
  type BackendReceiver,
  type CloudflareMember,
  DriverAttempt,
  OrderedStubs,
  currentCloudflareMember,
  getOrCreate,
  hostLocal,
  CallNotSent,
  HolderGrants,
  LEASE_CALL_DEADLINE_MS,
  LEASE_RENEW_EVERY_MS,
  TransientRetry,
  type CallOptions,
  type Invoke,
  type LeaseOrder,
} from 'telefunc/__internal'
import type { RoomSubscriptionSource } from '../backend/contract.js'
import { encodeLaneKey } from '../backend/laneKey.js'
import { assert } from '../utils/assert.js'
import { laneRoute, type RouteInstallation } from './routes.js'
import type { RegisterWire, RenewWire } from './RoomAuthority.js'
import type { CloudflareRoomAuthorityStub } from './objects.js'
import type { CloudflareRoomDriver } from './CloudflareRoomDriver.js'

const ROOM_SESSION = Symbol.for('telefunc.room.cloudflareSession')
const REGISTER_RETRY_MIN_MS = 100
const REGISTER_RETRY_MAX_MS = 5_000

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

/** The attempts a room's next renewal renews, once its timer fires. */
type RoomRenewal = { attempts: Set<CloudflareRoomAttempt>; timer: ReturnType<typeof setTimeout> }

type RoomSessionSubscriptionsOptions = { sessionId: string; member: CloudflareMember; driver: CloudflareRoomDriver }

/** A session DO's Room subscriptions, and its calls to room authorities. */
class RoomSessionSubscriptions {
  private readonly authorityCalls = new OrderedStubs<CloudflareRoomAuthorityStub>()
  private readonly grants = new HolderGrants()
  /** By room: a session's lanes of a room renew in one call, which the first of them to register times. */
  private readonly renewals = new Map<string, RoomRenewal>()
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
    const attempt = new CloudflareRoomAttempt({ owner: this, route, stopDeliveries })
    void attempt.register()
    return attempt
  }

  /** Calls the room's authority through the ordered stub this session keeps for it, so a room's commits and route calls
   *  reach its authority in the order they were made. A line opens a fresh stub: a stub that rejected may be broken. */
  callAuthority<T>(
    roomId: string,
    invoke: Invoke<CloudflareRoomAuthorityStub, T> | Promise<Invoke<CloudflareRoomAuthorityStub, T>>,
    options?: CallOptions,
  ): Promise<T> {
    return this.authorityCalls.call(roomId, () => this.driver.stub(roomId), invoke, options)
  }

  /** How long the room's authority gets no registration of this session's, after it answered overloaded. */
  pausedFor(roomId: string): number {
    return this.authorityCalls.pausedFor(roomId)
  }

  /** A registration or removal of a route at the room's authority, through its ordered stub, with this session's grant
   *  there and its next order. */
  writeRoute<T>(
    roomId: string,
    write: (authority: CloudflareRoomAuthorityStub, order: LeaseOrder) => Promise<T | 'regrant'>,
    options: CallOptions,
  ): Promise<T> {
    return this.grants.send(
      roomId,
      () => this.callAuthority(roomId, (authority) => authority.grantHolder(this.id), options),
      (order) =>
        this.callAuthority(
          roomId,
          order.then((order) => (authority: CloudflareRoomAuthorityStub) => write(authority, order)),
          options,
        ),
    )
  }

  /** Removes `route` at its room's authority, as many times as it takes, whoever waits for it. */
  removeRoute(route: RouteInstallation): Promise<void> {
    const remove = () =>
      this.writeRoute(route.roomId, (authority, order) => authority.unsubscribeRoute(route, order), {
        deadline: Date.now() + LEASE_CALL_DEADLINE_MS,
        paused: true,
      })
    return this.grants.removeUntilDone(remove, () => this.pausedFor(route.roomId))
  }

  renewWithRoom(attempt: CloudflareRoomAttempt): void {
    const { roomId } = attempt.route
    const renewal = getOrCreate(this.renewals, roomId, () => ({
      attempts: new Set<CloudflareRoomAttempt>(),
      timer: setTimeout(() => void this.renewRoutes(roomId), LEASE_RENEW_EVERY_MS),
    }))
    renewal.attempts.add(attempt)
  }

  stopRenewing(attempt: CloudflareRoomAttempt): void {
    const { roomId } = attempt.route
    const renewal = this.renewals.get(roomId)
    if (renewal === undefined || !renewal.attempts.delete(attempt) || renewal.attempts.size > 0) return
    clearTimeout(renewal.timer)
    this.renewals.delete(roomId)
  }

  private async renewRoutes(roomId: string): Promise<void> {
    const attempts = [...this.renewals.get(roomId)!.attempts]
    this.renewals.delete(roomId)
    let results: RenewWire[]
    try {
      results = await this.callAuthority(roomId, (authority) => authority.renewRoutes(attempts.map((a) => a.route)), {
        deadline: Date.now() + LEASE_RENEW_EVERY_MS,
        paused: true,
      })
    } catch (error) {
      for (const attempt of attempts) attempt.lose(error)
      return
    }
    attempts.forEach((attempt, index) => attempt.onRenewal(results[index]!))
  }
}

type CloudflareRoomAttemptOptions = {
  owner: RoomSessionSubscriptions
  route: RouteInstallation
  /** Stops the lane's frames reaching its receiver. */
  stopDeliveries: () => void
}

/** Ready once the authority has durably registered this attempt's exact route: a member's join waits for its inbox's,
 *  as a join committed after a registration that failed would let a private message reach no route. Its first
 *  registration that fails is made again only as Cloudflare says to (`TransientRetry`), and otherwise closes it with
 *  the error. Once registered, a failed call to the authority, or a lapsed route, holds it back until its route is
 *  registered again. */
class CloudflareRoomAttempt extends DriverAttempt {
  readonly route: RouteInstallation
  private readonly owner: RoomSessionSubscriptions
  private readonly stopDeliveries: () => void
  private retry: ReturnType<typeof setTimeout> | null = null
  private retryDelay = REGISTER_RETRY_MIN_MS
  /** Whether a registration of its route may have reached the authority, which then removes it should it close. */
  private sent = false
  /** How its first registration is made again, until its route was registered. */
  private firstRegistration: TransientRetry | null = new TransientRetry()

  constructor({ owner, route, stopDeliveries }: CloudflareRoomAttemptOptions) {
    super()
    this.owner = owner
    this.route = route
    this.stopDeliveries = stopDeliveries
  }

  async register(): Promise<void> {
    let registration: RegisterWire
    try {
      registration = await this.sendRegistration()
    } catch (error) {
      if (!(error instanceof CallNotSent)) this.sent = true
      return this.firstRegistration === null ? this.lose(error) : this.failFirst(this.firstRegistration, error)
    }
    this.sent = true
    // Unsubscribed while it registered: its release already went out.
    if (this.state() === 'closed') return
    if (registration === 'dropped') return this.end()
    // A write of this session's on the lane, sent later, came first: registered again, behind it.
    if (registration === 'stale') return this.lose(new Error(`Cloudflare route '${this.route.laneKey}' was overtaken`))
    this.firstRegistration = null
    this.retryDelay = REGISTER_RETRY_MIN_MS
    this.transition('ready')
    this.owner.renewWithRoom(this)
  }

  unsubscribe(): Promise<void> {
    this.stop()
    this.transition('closed')
    return this.owner.removeRoute(this.route)
  }

  onRenewal(renewal: RenewWire): void {
    if (this.state() === 'closed') return
    if (renewal === 'renewed') return this.owner.renewWithRoom(this)
    // A route that went with its dropped incarnation ends on purpose; a lapsed one is registered again.
    if (renewal === 'dropped') this.end()
    else this.lose(new Error(`Cloudflare route '${this.route.laneKey}' lapsed`))
  }

  /** What is committed until its route is registered again may miss it: lost if it was ready, then registered again
   *  after a wait that doubles while the authority fails, and lasts its overload's pause at least. The first failure of
   *  a run is reported. */
  lose(error: unknown): void {
    if (this.state() === 'closed') return
    this.owner.stopRenewing(this)
    if (this.state() === 'ready') this.transition('lost')
    if (this.retryDelay === REGISTER_RETRY_MIN_MS) console.error(error)
    this.retry = setTimeout(
      () => {
        this.retry = null
        void this.register()
      },
      Math.max(this.retryDelay, this.owner.pausedFor(this.route.roomId)),
    )
    this.retryDelay = Math.min(this.retryDelay * 2, REGISTER_RETRY_MAX_MS)
  }

  /** Its first registration failed: made again after a wait where Cloudflare says to, else closed with `error`, or the
   *  overload that kept it from being sent, which core reports; a registration that may have been stored is removed. */
  private failFirst(retry: TransientRetry, error: unknown): void {
    if (this.state() === 'closed') return
    const reason = retry.reasonFor(error)
    const delay = retry.delayAfter(error)
    if (delay === null) {
      this.stop()
      if (this.sent) void this.owner.removeRoute(this.route)
      return this.transition('closed', reason)
    }
    this.retry = setTimeout(() => {
      this.retry = null
      void this.register()
    }, delay)
  }

  /** Its incarnation is no longer open: closed on purpose, with no reason. */
  private end(): void {
    this.stop()
    this.transition('closed')
  }

  private stop(): void {
    if (this.retry !== null) clearTimeout(this.retry)
    this.retry = null
    this.owner.stopRenewing(this)
    this.stopDeliveries()
  }

  /** Sent behind what this session sent the room before, with the grant it may ask for first, while it is wanted and
   *  in its time: a first registration's is its retries' window. */
  private sendRegistration(): Promise<Exclude<RegisterWire, 'regrant'>> {
    const first = this.firstRegistration
    const options = {
      wanted: () => this.state() !== 'closed',
      deadline: first === null ? Date.now() + LEASE_CALL_DEADLINE_MS : first.deadline,
      paused: true,
    }
    return this.owner.writeRoute(
      this.route.roomId,
      (authority, order) =>
        first === null
          ? authority.registerRoute(this.route, order)
          : first.attempt(() => authority.registerRoute(this.route, order)),
      options,
    )
  }
}
