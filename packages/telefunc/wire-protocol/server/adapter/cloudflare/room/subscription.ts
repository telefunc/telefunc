import type {
  BackendReceiver,
  SubscriptionAttempt,
  SubscriptionAttemptState,
} from '../../../../backend/subscription.js'
import { ROUTE_RENEW_EVERY_MS, type RouteInstallation } from './routes.js'
import type { CloudflareRoomAuthorityStub } from './backend.js'

export type CloudflareRoomSubscriptionSource = Omit<RouteInstallation, 'leaseId'> & {
  authority: CloudflareRoomAuthorityStub
}

type CloudflareRoomSubscriptionOptions = {
  onClosed(): void
}

/** Cloudflare's raw driver edge. It acknowledges only after the authority has durably registered the
 * exact route. Room owns retry/replacement policy; shared subscription code owns readiness and local fan-out. */
export class CloudflareRoomSubscriptionAttempt implements SubscriptionAttempt {
  readonly ready: Promise<void>
  readonly #authority: CloudflareRoomAuthorityStub
  readonly #route: RouteInstallation
  readonly #receiver: BackendReceiver
  readonly #onClosed: () => void
  readonly #listeners = new Set<(state: SubscriptionAttemptState) => void>()
  #state: SubscriptionAttemptState = 'establishing'
  #settleReady!: { resolve: () => void; reject: (error: unknown) => void }
  #cancelRenewal: (() => void) | null = null
  #started = false
  #unsubscribed = false

  constructor(
    source: CloudflareRoomSubscriptionSource,
    receiver: BackendReceiver,
    options: CloudflareRoomSubscriptionOptions,
  ) {
    const { authority, ...route } = source
    this.#authority = authority
    this.#route = { ...route, leaseId: crypto.randomUUID() }
    this.#receiver = receiver
    this.#onClosed = options.onClosed
    this.ready = new Promise<void>((resolve, reject) => {
      this.#settleReady = { resolve, reject }
    })
    void this.ready.catch(() => {})
  }

  start(): void {
    if (this.#started || this.#isClosed()) return
    this.#started = true
    void this.#establish()
  }

  state(): SubscriptionAttemptState {
    return this.#state
  }

  onStateChange(cb: (state: SubscriptionAttemptState) => void): () => void {
    this.#listeners.add(cb)
    return () => this.#listeners.delete(cb)
  }

  get leaseId(): string {
    return this.#route.leaseId
  }

  async deliver(frame: Uint8Array, seq: number, timestamp: number): Promise<void> {
    if (this.#state !== 'ready') throw new Error('Cloudflare Room delivery lease is not installed')
    await (this.#receiver(new Uint8Array(frame), { seq, timestamp }) as unknown)
  }

  invalidate(): void {
    this.#finish('closed')
  }

  terminate(): void {
    if (this.#unsubscribed) return
    this.#unsubscribed = true
    this.#finish('terminated')
    void this.#teardown().catch(console.error)
  }

  async unsubscribe(): Promise<void> {
    if (this.#unsubscribed) return
    this.#unsubscribed = true
    this.#settleReadiness()
    this.#finish('closed')
    await this.#teardown()
  }

  async #establish(): Promise<void> {
    try {
      const registered = await this.#authority.registerRoute(this.#route)
      if (this.#isClosed()) return
      if (!('ok' in registered)) {
        const error = new Error(registered.reason)
        if (registered.terminal === true) {
          this.#settleReadiness(error)
          this.#finish('terminated')
          return
        }
        throw error
      }
      this.#transition('ready')
      this.#settleReadiness()
      this.#scheduleRenewal()
    } catch (error) {
      if (this.#isClosed()) return
      this.#settleReadiness(error)
      this.#finish('closed')
    }
  }

  #scheduleRenewal(): void {
    if (this.#state !== 'ready') return
    this.#cancelRenewal?.()
    const handle = setTimeout(() => void this.#renew(), ROUTE_RENEW_EVERY_MS)
    this.#cancelRenewal = () => clearTimeout(handle)
  }

  async #renew(): Promise<void> {
    this.#cancelRenewal = null
    if (this.#state !== 'ready') return
    try {
      const renewed = await this.#authority.renewRoute(this.#route)
      if (this.#state !== 'ready') return
      if (!renewed.ok) {
        if (renewed.terminal === true) this.terminate()
        else this.#finish('closed')
        return
      }
      this.#scheduleRenewal()
    } catch {
      this.#finish('closed')
    }
  }

  async #teardown(): Promise<void> {
    await this.#authority.unsubscribeRoute(this.#route)
  }

  #finish(state: 'closed' | 'terminated'): void {
    if (this.#isClosed()) return
    this.#cancelRenewal?.()
    this.#cancelRenewal = null
    // A no-op once the attempt was acknowledged.
    this.#settleReady.reject(new Error(`Cloudflare Room subscription ${state} before acknowledgement`))
    this.#transition(state)
    this.#onClosed()
  }

  #isClosed(): boolean {
    return this.#state === 'closed' || this.#state === 'terminated'
  }

  #settleReadiness(error?: unknown): void {
    if (error === undefined) this.#settleReady.resolve()
    else this.#settleReady.reject(error)
  }

  #transition(state: SubscriptionAttemptState): void {
    if (this.#state === state) return
    this.#state = state
    for (const listener of [...this.#listeners]) listener(state)
  }
}
