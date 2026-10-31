export {
  CloudflareRoomSessionManager,
  CloudflareRoomSubscriptionAttempt,
  currentRoomSessionManager,
  getRoomSessionManager,
}
export type { RoomSessionDeliveryRequest }

import {
  type BackendReceiver,
  type OrderingInfo,
  DriverAttempt,
  OrderedStubs,
  SourceDroppedError,
  hostLocal,
} from 'telefunc/__internal'
import type { RoomSubscriptionSource } from '../backend/contract.js'
import { encodeLaneKey } from '../backend/laneKey.js'
import { assertUsage } from '../utils/assert.js'
import { ROUTE_RENEW_EVERY_MS, type RouteInstallation } from './routes.js'
import type { CloudflareRoomAuthorityStub, RegisterWire, RenewWire } from './do.js'

type RoomSessionDeliveryRequest = {
  route: RouteInstallation
  payload: Uint8Array
  info: OrderingInfo
}

/** A call to the room's authority through the session's ordered stub for it. */
type AuthorityCall = <T>(invoke: (authority: CloudflareRoomAuthorityStub) => Promise<T>) => Promise<T>

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
  /** Subscriptions share an attempt only within one session DO. */
  readonly partition = crypto.randomUUID()
  private readonly authorityCalls = new OrderedStubs<CloudflareRoomAuthorityStub>()
  private readonly id: string
  private readonly entries = new Map<string, CloudflareRoomSubscriptionAttempt>()

  constructor(sessionId: string) {
    this.id = sessionId
  }

  openSubscription(
    { roomId, inc, lane }: RoomSubscriptionSource,
    openAuthority: () => CloudflareRoomAuthorityStub,
    receiver: BackendReceiver,
  ): CloudflareRoomSubscriptionAttempt {
    const route = { roomId, inc, laneKey: encodeLaneKey(lane), sessionDoId: this.id, leaseId: crypto.randomUUID() }
    const callAuthority: AuthorityCall = (invoke) => this.callAuthority(roomId, openAuthority, invoke)
    const key = entryKey(route)
    const attempt = new CloudflareRoomSubscriptionAttempt({
      route,
      callAuthority,
      receiver,
      onClosed: () => this.entries.delete(key),
    })
    this.entries.set(key, attempt)
    attempt.start()
    return attempt
  }

  /** A call to a room's authority through this session's ordered stub for it, so a room's commits and route calls
   *  reach its authority in the order they were made. */
  callAuthority<T>(
    roomId: string,
    open: () => CloudflareRoomAuthorityStub,
    invoke: (authority: CloudflareRoomAuthorityStub) => Promise<T>,
  ): Promise<T> {
    return this.authorityCalls.call(roomId, open, invoke)
  }

  /** A delivery to a lease this session no longer holds, as after a restart, is dropped: delivery is at-most-once, and
   *  the authority's route lapses with the lease. */
  deliver(request: RoomSessionDeliveryRequest): void {
    const entry = this.entries.get(entryKey(request.route))
    if (entry?.leaseId !== request.route.leaseId) return
    entry.deliver(request.payload, request.info)
  }
}

/** Ready once the authority has durably registered this attempt's exact route; Room owns retry and replacement. */
class CloudflareRoomSubscriptionAttempt extends DriverAttempt {
  private readonly callAuthority: AuthorityCall
  private readonly route: RouteInstallation
  private readonly receiver: BackendReceiver
  private readonly onClosed: () => void
  private renewalTimer: ReturnType<typeof setTimeout> | undefined

  constructor({
    route,
    callAuthority,
    receiver,
    onClosed,
  }: {
    route: RouteInstallation
    callAuthority: AuthorityCall
    receiver: BackendReceiver
    onClosed: () => void
  }) {
    super()
    this.callAuthority = callAuthority
    this.route = route
    this.receiver = receiver
    this.onClosed = onClosed
  }

  start(): void {
    void this.establish()
  }

  get leaseId(): string {
    return this.route.leaseId
  }

  deliver(payload: Uint8Array, info: OrderingInfo): void {
    this.receiver(payload, info)
  }

  async unsubscribe(): Promise<void> {
    this.finish()
    await this.callAuthority((authority) => authority.unsubscribeRoute(this.route))
  }

  private async establish(): Promise<void> {
    let registered: RegisterWire
    try {
      registered = await this.callAuthority((authority) => authority.registerRoute(this.route))
    } catch (error) {
      this.finish(error)
      return
    }
    if (!('ok' in registered)) {
      this.finish(new Error(registered.reason))
      return
    }
    // Unsubscribed while it registered: its release already went out.
    if (this.ended) return
    this.transition('ready')
    this.scheduleRenewal()
  }

  private scheduleRenewal(): void {
    this.renewalTimer = setTimeout(() => void this.renew(), ROUTE_RENEW_EVERY_MS)
  }

  private async renew(): Promise<void> {
    let renewal: RenewWire
    try {
      renewal = await this.callAuthority((authority) => authority.renewRoute(this.route))
    } catch (error) {
      this.finish(error)
      return
    }
    if (this.state() !== 'ready') return
    if (renewal === 'renewed') this.scheduleRenewal()
    else if (renewal === 'dropped') this.finish(new SourceDroppedError('Cloudflare route went with its generation'))
    else this.finish()
  }

  private finish(error?: unknown): void {
    if (this.ended) return
    clearTimeout(this.renewalTimer)
    this.transition('closed', error)
    this.onClosed()
  }
}
