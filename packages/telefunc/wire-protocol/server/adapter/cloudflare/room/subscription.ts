import type { BackendReceiver } from '../../../../backend/subscription.js'
import { DriverAttempt } from '../../../../backend/attempt.js'
import { ROUTE_RENEW_EVERY_MS, type RouteInstallation } from './routes.js'
import type { CloudflareRoomAuthorityStub } from './backend.js'
import type { RegisterWire } from './do.js'

export type CloudflareRoomSubscriptionSource = Omit<RouteInstallation, 'leaseId'> & {
  authority: CloudflareRoomAuthorityStub
}

type CloudflareRoomSubscriptionOptions = {
  onClosed(): void
}

/** Ready once the authority has durably registered this attempt's exact route; Room owns retry and replacement. */
export class CloudflareRoomSubscriptionAttempt extends DriverAttempt {
  readonly #authority: CloudflareRoomAuthorityStub
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
    const { authority, ...route } = source
    this.#authority = authority
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

  async deliver(payload: Uint8Array, seq: number, timestamp: number): Promise<void> {
    if (this.state() !== 'ready') return
    await this.#receiver(new Uint8Array(payload), { seq, timestamp })
  }

  invalidate(): void {
    this.#finish()
  }

  terminate(): void {
    if (this.#released) return
    this.#released = true
    this.#finish()
    void this.#release().catch(console.error)
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
      registered = await this.#authority.registerRoute(this.#route)
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
    if (this.state() !== 'ready') return
    this.#cancelRenewal?.()
    const handle = setTimeout(() => void this.#renew(), ROUTE_RENEW_EVERY_MS)
    this.#cancelRenewal = () => clearTimeout(handle)
  }

  async #renew(): Promise<void> {
    this.#cancelRenewal = null
    if (this.state() !== 'ready') return
    let renewed: { ok: boolean; terminal?: boolean }
    try {
      renewed = await this.#authority.renewRoute(this.#route)
    } catch {
      return this.#finish()
    }
    if (this.state() !== 'ready') return
    if (renewed.ok) this.#scheduleRenewal()
    else if (renewed.terminal === true) this.terminate()
    else this.#finish()
  }

  async #release(): Promise<void> {
    await this.#authority.unsubscribeRoute(this.#route)
  }

  #finish(error?: unknown): void {
    if (this.ended) return
    this.#cancelRenewal?.()
    this.#cancelRenewal = null
    this.transition('closed', error)
    this.#onClosed()
  }
}
