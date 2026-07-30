import type { BackendReceiver, SubscriptionAttempt, SubscriptionAttemptState } from '../../../../backend/spi.js'
import { ROUTE_RENEW_EVERY_MS } from './routes.js'
import type { CloudflareRoomAuthorityStub, RoomShardInvalidationRequest } from './backend.js'

export type CloudflareRoomSubscriptionSource = {
  roomId: string
  inc: string
  laneKey: string
  subscriberDoId: string
  authority: CloudflareRoomAuthorityStub
}

type CloudflareRoomSubscriptionOptions = {
  onClosed(): void
}

/** Cloudflare's raw driver edge. It acknowledges only after the authority has durably registered the
 * exact route. Room owns retry/replacement policy; shared subscription code owns readiness and local fan-out. */
export class CloudflareRoomSubscriptionAttempt implements SubscriptionAttempt {
  readonly ready: Promise<void>
  readonly #source: CloudflareRoomSubscriptionSource
  readonly #receiver: BackendReceiver
  readonly #onClosed: () => void
  readonly #leaseId = crypto.randomUUID()
  readonly #listeners = new Set<(state: SubscriptionAttemptState) => void>()
  #state: SubscriptionAttemptState = 'establishing'
  #generationToken = ''
  #settleReady!: { resolve: () => void; reject: (error: unknown) => void }
  #readySettled = false
  #cancelRenewal: (() => void) | null = null
  #started = false
  #unsubscribed = false

  constructor(
    source: CloudflareRoomSubscriptionSource,
    receiver: BackendReceiver,
    options: CloudflareRoomSubscriptionOptions,
  ) {
    this.#source = source
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
    this.#resolveReady()
    this.#terminate()
    void this.#teardown().catch(console.error)
  }

  async unsubscribe(): Promise<void> {
    if (this.#unsubscribed) return
    this.#unsubscribed = true
    this.#resolveReady()
    this.#close()
    await this.#teardown()
  }

  async #establish(): Promise<void> {
    try {
      const registered = await this.#source.authority.registerRoute(
        this.#source.roomId,
        this.#source.inc,
        this.#source.laneKey,
        this.#source.subscriberDoId,
        this.#leaseId,
      )
      if (this.#isClosed()) return
      if (!('ok' in registered)) {
        const error = new Error(registered.reason)
        if (registered.terminal === true) {
          this.#rejectReady(error)
          this.#terminate()
          return
        }
        throw error
      }
      this.#generationToken = registered.generationToken
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
    const handle = setTimeout(() => void this.#renew(), ROUTE_RENEW_EVERY_MS)
    this.#cancelRenewal = () => clearTimeout(handle)
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
      )
      if (this.#state !== 'ready') return
      if (!renewed.ok) {
        if (renewed.terminal === true) this.terminate()
        else this.#close()
        return
      }
      this.#scheduleRenewal()
    } catch {
      this.#close()
    }
  }

  async #teardown(): Promise<void> {
    await this.#source.authority.unsubscribeRoute(
      this.#source.inc,
      this.#source.laneKey,
      this.#source.subscriberDoId,
      this.#leaseId,
    )
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
