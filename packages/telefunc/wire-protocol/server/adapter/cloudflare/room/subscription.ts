export { CloudflareRoomSessionManager, CloudflareRoomSubscriptionAttempt }
export type { RoomSessionDeliveryRequest }

import type { BackendReceiver } from '../../../../backend/subscription.js'
import type { RoomSubscriptionSource } from '../../../../backend/room/contract.js'
import { encodeLaneKey } from '../../../../backend/room/lane-key.js'
import { DriverAttempt } from '../../../../backend/attempt.js'
import { OrderedStubs } from '../ordered-stubs.js'
import { ROUTE_RENEW_EVERY_MS, type RouteInstallation } from './routes.js'
import type { CloudflareRoomAuthorityStub } from './backend.js'
import type { RegisterWire } from './do.js'

type RoomSessionDeliveryRequest = RouteInstallation & {
  payload: Uint8Array
  seq: number
  timestamp: number
}

const entryKey = (route: Pick<RouteInstallation, 'roomId' | 'inc' | 'laneKey'>) =>
  JSON.stringify([route.roomId, route.inc, route.laneKey])

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
    const callAuthority: AuthorityCall = (invoke) => this.callAuthority(roomId, openAuthority, invoke)
    const source = { roomId, inc, laneKey: encodeLaneKey(lane), sessionDoId: this.#id, callAuthority }
    const key = entryKey(source)
    const attempt: CloudflareRoomSubscriptionAttempt = new CloudflareRoomSubscriptionAttempt(source, receiver, {
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
    const entry = this.#entries.get(entryKey(request))
    if (entry?.leaseId !== request.leaseId) return
    entry.deliver(request.payload, request.seq, request.timestamp)
  }
}

/** A call to the room's authority through the session's ordered stub for it. */
type AuthorityCall = <T>(invoke: (authority: CloudflareRoomAuthorityStub) => Promise<T>) => Promise<T>

type CloudflareRoomSubscriptionSource = Omit<RouteInstallation, 'leaseId'> & { callAuthority: AuthorityCall }

type CloudflareRoomSubscriptionOptions = {
  onClosed(): void
}

/** Ready once the authority has durably registered this attempt's exact route; Room owns retry and replacement. */
class CloudflareRoomSubscriptionAttempt extends DriverAttempt {
  readonly #callAuthority: AuthorityCall
  readonly #route: RouteInstallation
  readonly #receiver: BackendReceiver
  readonly #onClosed: () => void
  #cancelRenewal: (() => void) | null = null

  constructor(
    source: CloudflareRoomSubscriptionSource,
    receiver: BackendReceiver,
    options: CloudflareRoomSubscriptionOptions,
  ) {
    super()
    const { callAuthority, ...route } = source
    this.#callAuthority = callAuthority
    this.#route = { ...route, leaseId: crypto.randomUUID() }
    this.#receiver = receiver
    this.#onClosed = options.onClosed
  }

  start(): void {
    void this.#establish()
  }

  get leaseId(): string {
    return this.#route.leaseId
  }

  deliver(payload: Uint8Array, seq: number, timestamp: number): void {
    this.#receiver(new Uint8Array(payload), { seq, timestamp })
  }

  async unsubscribe(): Promise<void> {
    this.#finish()
    await this.#release()
  }

  async #establish(): Promise<void> {
    let registered: RegisterWire
    try {
      registered = await this.#callAuthority((authority) => authority.registerRoute(this.#route))
    } catch (error) {
      return this.#finish(error)
    }
    if (!('ok' in registered)) {
      return this.#finish(new Error(registered.reason))
    }
    // Unsubscribed while it registered: its release already went out.
    if (this.ended) return
    this.transition('ready')
    this.#scheduleRenewal()
  }

  #scheduleRenewal(): void {
    const handle = setTimeout(() => void this.#renew(), ROUTE_RENEW_EVERY_MS)
    this.#cancelRenewal = () => clearTimeout(handle)
  }

  async #renew(): Promise<void> {
    this.#cancelRenewal = null
    let renewed: boolean
    try {
      renewed = await this.#callAuthority((authority) => authority.renewRoute(this.#route))
    } catch (error) {
      return this.#finish(error)
    }
    if (this.state() !== 'ready') return
    if (renewed) this.#scheduleRenewal()
    else this.#finish()
  }

  async #release(): Promise<void> {
    await this.#callAuthority((authority) => authority.unsubscribeRoute(this.#route))
  }

  #finish(error?: unknown): void {
    if (this.ended) return
    this.#cancelRenewal?.()
    this.#cancelRenewal = null
    this.transition('closed', error)
    this.#onClosed()
  }
}
