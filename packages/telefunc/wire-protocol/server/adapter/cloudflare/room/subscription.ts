export { CloudflareRoomSubscriptionAttempt }

import type { BackendReceiver } from '../../../../backend/subscription.js'
import { DriverAttempt } from '../../../../backend/attempt.js'
import { ROUTE_RENEW_EVERY_MS, type RouteInstallation } from './routes.js'
import type { CloudflareRoomAuthorityStub } from './backend.js'
import type { RegisterWire } from './do.js'

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
  /** The route's removal from the authority was requested. */
  #released = false

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
    if (this.state() !== 'ready') return
    this.#receiver(new Uint8Array(payload), { seq, timestamp })
  }

  async unsubscribe(): Promise<void> {
    if (this.#released) return
    this.#released = true
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
