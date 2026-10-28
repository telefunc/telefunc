export { CloudflareRoomSessionManager, CloudflareRoomSubscriptionAttempt, getRoomSessionManager }
export type { RoomSessionDeliveryRequest }

import type { BackendReceiver } from '../../../backend/subscription.js'
import type { OrderingInfo } from '../../../wire/orderingFrame.js'
import type { RoomSubscriptionSource } from '../../../backend/room/contract.js'
import { encodeLaneKey } from '../../../backend/room/laneKey.js'
import { DriverAttempt } from '../../../backend/attempt.js'
import { OrderedStubs } from '../OrderedStubs.js'
import { hostLocal } from '../../../../node/server/__internal.js'
import { assert } from '../../../../utils/assert.js'
import { ROUTE_RENEW_EVERY_MS, type RouteInstallation } from './routes.js'
import type { CloudflareRoomAuthorityStub, RegisterWire } from './do.js'

type RoomSessionDeliveryRequest = {
  route: RouteInstallation
  payload: Uint8Array
  info: OrderingInfo
}

/** A call to the room's authority through the session's ordered stub for it. */
type AuthorityCall = <T>(invoke: (authority: CloudflareRoomAuthorityStub) => Promise<T>) => Promise<T>

const ROOM_SESSION_MANAGER = Symbol.for('telefunc.room.cloudflareSessionManager')

/** The Room manager of the session DO whose call this is. */
function getRoomSessionManager(): CloudflareRoomSessionManager {
  return hostLocal(ROOM_SESSION_MANAGER, (host) => {
    assert(host.id !== null)
    return new CloudflareRoomSessionManager(host.id)
  })
}

function entryKey(route: Pick<RouteInstallation, 'roomId' | 'inc' | 'laneKey'>): string {
  return JSON.stringify([route.roomId, route.inc, route.laneKey])
}

class CloudflareRoomSessionManager {
  /** Subscriptions share an attempt only within one session DO. */
  readonly partition = crypto.randomUUID()
  readonly #authorityCalls = new OrderedStubs<CloudflareRoomAuthorityStub>()
  readonly #id: string
  readonly #entries = new Map<string, CloudflareRoomSubscriptionAttempt>()

  constructor(sessionId: string) {
    this.#id = sessionId
  }

  openSubscription(
    { roomId, inc, lane }: RoomSubscriptionSource,
    openAuthority: () => CloudflareRoomAuthorityStub,
    receiver: BackendReceiver,
  ): CloudflareRoomSubscriptionAttempt {
    const route = { roomId, inc, laneKey: encodeLaneKey(lane), sessionDoId: this.#id, leaseId: crypto.randomUUID() }
    const callAuthority: AuthorityCall = (invoke) => this.callAuthority(roomId, openAuthority, invoke)
    const key = entryKey(route)
    const attempt = new CloudflareRoomSubscriptionAttempt({
      route,
      callAuthority,
      receiver,
      onClosed: () => this.#entries.delete(key),
    })
    this.#entries.set(key, attempt)
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
    return this.#authorityCalls.call(roomId, open, invoke)
  }

  /** A delivery to a lease this session no longer holds, as after a restart, is dropped: delivery is at-most-once, and
   *  the authority's route lapses with the lease. */
  deliver(request: RoomSessionDeliveryRequest): void {
    const entry = this.#entries.get(entryKey(request.route))
    if (entry?.leaseId !== request.route.leaseId) return
    entry.deliver(request.payload, request.info)
  }
}

/** Ready once the authority has durably registered this attempt's exact route; Room owns retry and replacement. */
class CloudflareRoomSubscriptionAttempt extends DriverAttempt {
  readonly #callAuthority: AuthorityCall
  readonly #route: RouteInstallation
  readonly #receiver: BackendReceiver
  readonly #onClosed: () => void
  #renewalTimer: ReturnType<typeof setTimeout> | undefined

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
    this.#callAuthority = callAuthority
    this.#route = route
    this.#receiver = receiver
    this.#onClosed = onClosed
  }

  start(): void {
    void this.#establish()
  }

  get leaseId(): string {
    return this.#route.leaseId
  }

  deliver(payload: Uint8Array, info: OrderingInfo): void {
    this.#receiver(payload, info)
  }

  async unsubscribe(): Promise<void> {
    this.#finish()
    await this.#callAuthority((authority) => authority.unsubscribeRoute(this.#route))
  }

  async #establish(): Promise<void> {
    let registered: RegisterWire
    try {
      registered = await this.#callAuthority((authority) => authority.registerRoute(this.#route))
    } catch (error) {
      this.#finish(error)
      return
    }
    if (!('ok' in registered)) {
      this.#finish(new Error(registered.reason))
      return
    }
    // Unsubscribed while it registered: its release already went out.
    if (this.ended) return
    this.transition('ready')
    this.#scheduleRenewal()
  }

  #scheduleRenewal(): void {
    this.#renewalTimer = setTimeout(() => void this.#renew(), ROUTE_RENEW_EVERY_MS)
  }

  async #renew(): Promise<void> {
    let renewed: boolean
    try {
      renewed = await this.#callAuthority((authority) => authority.renewRoute(this.#route))
    } catch (error) {
      this.#finish(error)
      return
    }
    if (this.state() !== 'ready') return
    if (renewed) this.#scheduleRenewal()
    else this.#finish()
  }

  #finish(error?: unknown): void {
    if (this.ended) return
    clearTimeout(this.#renewalTimer)
    this.transition('closed', error)
    this.#onClosed()
  }
}
