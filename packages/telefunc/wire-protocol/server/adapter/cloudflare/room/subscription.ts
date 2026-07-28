import type { BackendReceiver, SubscriptionAttempt, SubscriptionAttemptState } from '../../../../backend/spi.js'
import { ROUTE_RENEW_EVERY_MS } from './routes.js'
import type { CloudflareRoomAuthorityStub, RoomShardInvalidationRequest } from './backend.js'

export type SubscriptionScheduler = {
  schedule(delayMs: number, task: () => Promise<void>): () => void
}

export const realSubscriptionScheduler: SubscriptionScheduler = {
  schedule(delayMs, task) {
    const handle = setTimeout(() => void task(), delayMs)
    return () => clearTimeout(handle)
  },
}

export type CloudflareRoomSubscriptionSource = {
  roomId: string
  inc: string
  laneKey: string
  subscriberDoId: string
  authority: CloudflareRoomAuthorityStub
}

export type CloudflareRoomSubscriptionOptions = {
  scheduler?: SubscriptionScheduler
  now?: () => number
  onClosed(): void
}

/** Cloudflare's raw driver edge. It acknowledges only after the authority has durably registered the
 * exact route. Retry, replacement, readiness generations and local fan-out all live in SubscriptionManager. */
export class CloudflareRoomSubscriptionAttempt implements SubscriptionAttempt {
  readonly ready: Promise<void>
  readonly #source: CloudflareRoomSubscriptionSource
  readonly #receiver: BackendReceiver
  readonly #scheduler: SubscriptionScheduler
  readonly #onClosed: () => void
  readonly #attemptId = crypto.randomUUID()
  readonly #createdAt: number
  readonly #leaseId = crypto.randomUUID()
  readonly #listeners = new Set<(state: SubscriptionAttemptState) => void>()
  #state: SubscriptionAttemptState = 'establishing'
  #generationToken = ''
  #settleReady!: { resolve: () => void; reject: (error: unknown) => void }
  #readySettled = false
  #cancelRenewal: (() => void) | null = null
  #establishment: Promise<void> | null = null
  #establishmentSettled = false
  #unsubscribed = false

  constructor(
    source: CloudflareRoomSubscriptionSource,
    receiver: BackendReceiver,
    options: CloudflareRoomSubscriptionOptions,
  ) {
    this.#source = source
    this.#receiver = receiver
    this.#scheduler = options.scheduler ?? realSubscriptionScheduler
    this.#createdAt = (options.now ?? Date.now)()
    this.#onClosed = options.onClosed
    this.ready = new Promise<void>((resolve, reject) => {
      this.#settleReady = { resolve, reject }
    })
    void this.ready.catch(() => {})
  }

  start(): void {
    if (this.#establishment !== null || this.#isClosed()) return
    const establishment = this.#establish()
    this.#establishment = establishment
    void establishment
      .finally(() => {
        this.#establishmentSettled = true
      })
      .catch(() => {})
  }

  state(): SubscriptionAttemptState {
    return this.#state
  }

  onStateChange(cb: (state: SubscriptionAttemptState) => void): () => void {
    this.#listeners.add(cb)
    return () => this.#listeners.delete(cb)
  }

  matches(request: RoomShardInvalidationRequest): boolean {
    return (
      request.roomId === this.#source.roomId &&
      request.inc === this.#source.inc &&
      request.laneKey === this.#source.laneKey &&
      request.subscriberDoId === this.#source.subscriberDoId &&
      request.leaseId === this.#leaseId &&
      request.generationToken === this.#generationToken
    )
  }

  async deliver(frame: Uint8Array, seq: number, timestamp: number): Promise<void> {
    if (this.#state !== 'ready') throw new Error('Cloudflare Room delivery lease is not installed')
    await (this.#receiver(new Uint8Array(frame), { seq, timestamp }) as unknown)
  }

  invalidate(): void {
    this.#close()
  }

  terminate(): void {
    if (this.#unsubscribed) return
    this.#unsubscribed = true
    const needsLateTeardown = !this.#establishmentSettled
    this.#resolveReady()
    this.#terminate()
    void this.#settleTeardown(needsLateTeardown).catch(console.error)
  }

  async unsubscribe(): Promise<void> {
    if (this.#unsubscribed) return
    this.#unsubscribed = true
    const needsLateTeardown = !this.#establishmentSettled
    this.#resolveReady()
    this.#close()
    await this.#settleTeardown(needsLateTeardown)
  }

  async #establish(): Promise<void> {
    try {
      const capture = await this.#source.authority.captureRouteGeneration(
        this.#source.inc,
        this.#attemptId,
        this.#createdAt,
      )
      if (this.#isClosed()) return
      if ('rejected' in capture) throw new Error(capture.reason)
      this.#generationToken = capture.generationToken
      const registered = await this.#source.authority.registerRoute(
        this.#source.roomId,
        this.#source.inc,
        this.#source.laneKey,
        this.#source.subscriberDoId,
        this.#leaseId,
        this.#generationToken,
        this.#attemptId,
        this.#createdAt,
      )
      if (this.#isClosed()) return
      if (!('ok' in registered)) throw new Error(registered.reason)
      this.#transition('ready')
      this.#resolveReady()
      this.#scheduleRenewal()
    } catch (error) {
      if (this.#isClosed()) return
      this.#rejectReady(error)
      this.#close()
    }
  }

  #scheduleRenewal(): void {
    if (this.#state !== 'ready') return
    this.#cancelRenewal?.()
    this.#cancelRenewal = this.#scheduler.schedule(ROUTE_RENEW_EVERY_MS, () => this.#renew())
  }

  async #renew(): Promise<void> {
    this.#cancelRenewal = null
    if (this.#state !== 'ready') return
    try {
      const renewed = await this.#source.authority.renewRoute(
        this.#source.inc,
        this.#source.laneKey,
        this.#source.subscriberDoId,
        this.#leaseId,
        this.#generationToken,
        this.#attemptId,
        this.#createdAt,
      )
      if (this.#state !== 'ready') return
      if (!renewed.ok) {
        this.#close()
        return
      }
      this.#scheduleRenewal()
    } catch {
      this.#close()
    }
  }

  async #teardown(): Promise<void> {
    const outcomes = await Promise.allSettled([
      Promise.resolve().then(() =>
        this.#source.authority.unsubscribeRoute(
          this.#source.inc,
          this.#source.laneKey,
          this.#source.subscriberDoId,
          this.#leaseId,
        ),
      ),
      Promise.resolve().then(() => this.#source.authority.releaseRouteGenerationCapture(this.#attemptId)),
    ])
    const failures = outcomes
      .filter((outcome): outcome is PromiseRejectedResult => outcome.status === 'rejected')
      .map((outcome) => (outcome.reason instanceof Error ? outcome.reason : new Error('unknown teardown failure')))
    if (failures.length === 1) throw new Error(failures[0]!.message)
    if (failures.length > 1) throw new AggregateError(failures, 'Cloudflare Room route teardown failed')
  }

  async #settleTeardown(needsLateTeardown: boolean): Promise<void> {
    const teardown = this.#teardown()
    if (needsLateTeardown && this.#establishment !== null) {
      void this.#establishment.finally(() => this.#teardown()).catch((error) => console.error(error))
    }
    await teardown
  }

  #close(): void {
    if (this.#isClosed()) return
    this.#cancelRenewal?.()
    this.#cancelRenewal = null
    if (!this.#readySettled) this.#rejectReady(new Error('Cloudflare Room subscription closed before acknowledgement'))
    this.#transition('closed')
    this.#onClosed()
  }

  #terminate(): void {
    if (this.#isClosed()) return
    this.#cancelRenewal?.()
    this.#cancelRenewal = null
    this.#transition('terminated')
    this.#onClosed()
  }

  #isClosed(): boolean {
    return this.#state === 'closed' || this.#state === 'terminated'
  }

  #resolveReady(): void {
    if (this.#readySettled) return
    this.#readySettled = true
    this.#settleReady.resolve()
  }

  #rejectReady(error: unknown): void {
    if (this.#readySettled) return
    this.#readySettled = true
    this.#settleReady.reject(error)
  }

  #transition(state: SubscriptionAttemptState): void {
    if (this.#state === state) return
    this.#state = state
    for (const listener of [...this.#listeners]) listener(state)
  }
}
