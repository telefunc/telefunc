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
  renewsAfterPause,
  type CallOptions,
  type Invoke,
  type LeaseOrder,
} from 'telefunc/__internal'
import type { LaneId, RoomSubscriptionSource } from '../backend/contract.js'
import { encodeLaneKey } from '../backend/laneKey.js'
import { assert } from '../utils/assert.js'
import { laneRoute, routeScope, type RouteInstallation } from './routes.js'
import type { OperationWire, RegisterWire, RenewWire } from './RoomAuthority.js'
import type { OperationArgs, OperationValue } from '../server/store/roomOperations.js'
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
  /** Its attempts by their route's scope, each until it closes. */
  private readonly attempts = new Map<string, CloudflareRoomAttempt>()
  private readonly id: string
  private readonly member: CloudflareMember
  private readonly driver: CloudflareRoomDriver

  constructor({ sessionId, member, driver }: RoomSessionSubscriptionsOptions) {
    this.id = sessionId
    this.member = member
    this.driver = driver
  }

  /** The lane's frames reach `receiver` from the Broadcast coordinator in this session's bucket, which the route it
   *  registers names, or the operation that carries it does. A later subscription to the lane takes them over, those
   *  committed under this one's lease too. */
  subscribe({ roomId, inc, lane, registeredBy }: RoomSubscriptionSource, receiver: BackendReceiver): DriverAttempt {
    const { bucket } = this.member
    assert(bucket !== null, 'A Room subscription registers from a session that knows its bucket')
    const laneKey = encodeLaneKey(lane)
    const route = { roomId, inc, laneKey, sessionDoId: this.id, leaseId: crypto.randomUUID(), bucket }
    const stopTaking = this.member.subscribeLocal(laneRoute(inc, laneKey), takeOnce(lane, receiver))
    const scope = routeScope(inc, laneKey)
    const attempt = new CloudflareRoomAttempt({
      owner: this,
      route,
      registeredByOperation: registeredBy === 'operation',
      stopDeliveries: () => {
        stopTaking()
        if (this.attempts.get(scope) === attempt) this.attempts.delete(scope)
      },
    })
    this.attempts.set(scope, attempt)
    if (!attempt.registeredByOperation) void attempt.register()
    return attempt
  }

  /** This session's open attempt on the lane, if any. */
  attemptOf(roomId: string, inc: string, lane: LaneId): CloudflareRoomAttempt | undefined {
    const attempt = this.attempts.get(routeScope(inc, encodeLaneKey(lane)))
    return attempt?.route.roomId === roomId ? attempt : undefined
  }

  /** Runs a join of `args` at the room's authority with `attempt`'s exact route registered in the same transaction,
   *  under this session's next order there, behind what it sent the room before; `attempt`, which the join registers,
   *  is ready once that is answered registered. Obeys the room's overload pause, as a first registration does. */
  async runCarrying(
    attempt: CloudflareRoomAttempt,
    args: OperationArgs['join'],
    options: Pick<CallOptions, 'deadline' | 'wanted'>,
  ): Promise<OperationWire<OperationValue['join']>> {
    const { roomId } = attempt.route
    const callOptions = {
      ...options,
      wanted: () => attempt.state() !== 'closed' && options.wanted?.() !== false,
      paused: true,
    }
    const wire = await this.grants.send(
      roomId,
      () => this.grantHere(roomId, callOptions),
      (order) => this.carry(attempt, args, order, callOptions),
    )
    if (wire.kind === 'done' && wire.registered === true) attempt.registeredBy()
    return wire
  }

  /** Sends the operation that carries `attempt`'s route under `order`, and again under the same order where a sent one
   *  failed as Cloudflare says to retry (`TransientRetry`): the authority answers a copy as it answered the operation,
   *  if that came, and applies it once, so an operation whose answer was lost learns what it did. */
  private async carry(
    attempt: CloudflareRoomAttempt,
    args: OperationArgs['join'],
    order: Promise<LeaseOrder>,
    options: CallOptions,
  ): Promise<OperationWire<OperationValue['join']> | 'regrant'> {
    const { roomId, inc } = attempt.route
    const retry = new TransientRetry()
    for (;;) {
      let sent = false
      const send = order.then((order) => (authority: CloudflareRoomAuthorityStub) => {
        sent = true
        attempt.sending()
        return authority.runJoinWithInbox(inc, args, { route: attempt.route, order })
      })
      try {
        return await this.callAuthority(roomId, send, options)
      } catch (error) {
        const reason = retry.reasonFor(error)
        const delay = sent ? retry.delayAfter(error) : null
        if (delay === null) throw reason
        await new Promise((resolve) => setTimeout(resolve, delay))
      }
    }
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
      () => this.grantHere(roomId, options),
      (order) =>
        this.callAuthority(
          roomId,
          order.then((order) => (authority: CloudflareRoomAuthorityStub) => write(authority, order)),
          options,
        ),
    )
  }

  /** A grant for this session's writes of routes at the room's authority. */
  private grantHere(roomId: string, options: CallOptions): Promise<number> {
    return this.callAuthority(roomId, (authority) => authority.grantHolder(this.id), options)
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

  /** `attempts`' renewal, which the room's overload pause kept from being sent, goes once the pause ends, with whatever
   *  else of the room is due by then. */
  private renewAfterPause(roomId: string, attempts: CloudflareRoomAttempt[]): void {
    const renewal = this.renewals.get(roomId)
    if (renewal !== undefined) clearTimeout(renewal.timer)
    const next = {
      attempts: renewal?.attempts ?? new Set<CloudflareRoomAttempt>(),
      timer: setTimeout(() => void this.renewRoutes(roomId), this.pausedFor(roomId)),
    }
    for (const attempt of attempts) next.attempts.add(attempt)
    this.renewals.set(roomId, next)
  }

  stopRenewing(attempt: CloudflareRoomAttempt): void {
    const { roomId } = attempt.route
    const renewal = this.renewals.get(roomId)
    if (renewal === undefined || !renewal.attempts.delete(attempt) || renewal.attempts.size > 0) return
    clearTimeout(renewal.timer)
    this.renewals.delete(roomId)
  }

  private async renewRoutes(roomId: string): Promise<void> {
    const renewal = this.renewals.get(roomId)
    assert(renewal !== undefined)
    const attempts = [...renewal.attempts]
    this.renewals.delete(roomId)
    const sentAt = Date.now()
    let results: RenewWire[]
    try {
      results = await this.callAuthority(roomId, (authority) => authority.renewRoutes(attempts.map((a) => a.route)), {
        deadline: Date.now() + LEASE_RENEW_EVERY_MS,
        paused: true,
      })
    } catch (error) {
      const live = attempts.filter((attempt) => attempt.state() !== 'closed')
      const again = live.filter((attempt) => attempt.renewsAfterPause(error))
      for (const attempt of live) if (!again.includes(attempt)) attempt.lose(error)
      if (again.length > 0) this.renewAfterPause(roomId, again)
      return
    }
    // The room counts a renewal it renewed as this session's activity, which keeps its grant there.
    if (results.includes('renewed')) this.grants.counted(roomId, sentAt)
    // The authority answers each route in turn.
    attempts.forEach((attempt, index) => {
      const result = results[index]
      assert(result !== undefined)
      attempt.onRenewal(result, sentAt)
    })
  }
}

type CloudflareRoomAttemptOptions = {
  owner: RoomSessionSubscriptions
  route: RouteInstallation
  /** Whether the operation that carries its route (a member's join) registers it first, instead of itself. */
  registeredByOperation: boolean
  /** Stops the lane's frames reaching its receiver. */
  stopDeliveries: () => void
}

/** Ready once the authority has durably registered this attempt's exact route: a member's join registers its inbox's
 *  in its own write, as a join committed after a registration that failed would let a private message reach no route.
 *  Its first registration of its own that fails is made again only as Cloudflare says to (`TransientRetry`), and
 *  otherwise closes it with the error. Once registered, a failed call to the authority, or a lapsed route, holds it
 *  back until its route is registered again. */
class CloudflareRoomAttempt extends DriverAttempt {
  readonly route: RouteInstallation
  readonly registeredByOperation: boolean
  private readonly owner: RoomSessionSubscriptions
  private readonly stopDeliveries: () => void
  private retry: ReturnType<typeof setTimeout> | null = null
  private retryDelay = REGISTER_RETRY_MIN_MS
  /** When the last write of its route went out, by this session's clock; null before the first. One that went out
   *  removes it should it close. */
  private sentAt: number | null = null
  /** How its first registration is made again, until its route was registered. */
  private firstRegistration: TransientRetry | null = new TransientRetry()
  /** When the write the authority last confirmed its route by went out, by this session's clock: the authority's lease
   *  runs from no earlier. */
  private confirmedAt = 0

  constructor({ owner, route, registeredByOperation, stopDeliveries }: CloudflareRoomAttemptOptions) {
    super()
    this.owner = owner
    this.route = route
    this.registeredByOperation = registeredByOperation
    this.stopDeliveries = stopDeliveries
  }

  async register(): Promise<void> {
    let registration: RegisterWire
    try {
      registration = await this.sendRegistration()
    } catch (error) {
      return this.firstRegistration === null ? this.lose(error) : this.failFirst(this.firstRegistration, error)
    }
    // Unsubscribed while it registered: its release already went out.
    if (this.state() === 'closed') return
    if (registration === 'dropped') return this.end()
    // A write of this session's on the lane, sent later, came first: registered again, behind it.
    if (registration === 'stale') return this.lose(new Error(`Cloudflare route '${this.route.laneKey}' was overtaken`))
    this.firstRegistration = null
    this.retryDelay = REGISTER_RETRY_MIN_MS
    this.confirm()
    this.transition('ready')
    this.owner.renewWithRoom(this)
  }

  /** A write of its route goes out. */
  sending(): void {
    this.sentAt = Date.now()
  }

  /** Its route was registered by the operation that carried it (a member's join): ready, as if its own registration
   *  had been answered. */
  registeredBy(): void {
    if (this.state() === 'closed') return
    this.firstRegistration = null
    this.confirm()
    this.transition('ready')
    this.owner.renewWithRoom(this)
  }

  unsubscribe(): Promise<void> {
    this.stop()
    this.transition('closed')
    // A write still in its line isn't sent once it is closed.
    return this.sentAt !== null ? this.owner.removeRoute(this.route) : Promise.resolve()
  }

  onRenewal(renewal: RenewWire, sentAt: number): void {
    if (this.state() === 'closed') return
    if (renewal === 'renewed') {
      this.confirmedAt = sentAt
      return this.owner.renewWithRoom(this)
    }
    // A route that went with its dropped incarnation ends on purpose; a lapsed one is registered again.
    if (renewal === 'dropped') this.end()
    else this.lose(new Error(`Cloudflare route '${this.route.laneKey}' lapsed`))
  }

  /** The authority answered the write of its route that went out last: registered. */
  private confirm(): void {
    assert(this.sentAt !== null)
    this.confirmedAt = this.sentAt
  }

  /** Whether its renewal, which failed with `error`, is made again once its room's overload pause ends. */
  renewsAfterPause(error: unknown): boolean {
    return renewsAfterPause(error, this.confirmedAt)
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
      if (this.sentAt !== null) void this.owner.removeRoute(this.route)
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
      (authority, order) => {
        this.sending()
        return first === null
          ? authority.registerRoute(this.route, order)
          : first.attempt(() => authority.registerRoute(this.route, order))
      },
      options,
    )
  }
}

/** Its receiver takes a control frame once: the room's authority hands a stored commit on again after a restart, which
 *  may have been handed on before. A control lane's frames come in its own seq order. */
function takeOnce(lane: RoomSubscriptionSource['lane'], receiver: BackendReceiver): BackendReceiver {
  if (lane.kind !== 'control') return receiver
  let last = 0
  return {
    deliver(payload, info) {
      if (info.seq <= last) return
      last = info.seq
      receiver.deliver(payload, info)
    },
  }
}
