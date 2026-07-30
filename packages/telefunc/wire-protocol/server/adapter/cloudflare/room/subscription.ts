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
 * exact route. Retry, replacement, readiness generations and local fan-out all live in SubscriptionManager. */
export class CloudflareRoomSubscriptionAttempt implements SubscriptionAttempt {
  readonly ready: Promise<void>
  private readonly _source: CloudflareRoomSubscriptionSource
  private readonly _receiver: BackendReceiver
  private readonly _onClosed: () => void
  private readonly _leaseId = crypto.randomUUID()
  private readonly _listeners = new Set<(state: SubscriptionAttemptState) => void>()
  private _state: SubscriptionAttemptState = 'establishing'
  private _generationToken = ''
  private _settleReady!: { resolve: () => void; reject: (error: unknown) => void }
  private _readySettled = false
  private _cancelRenewal: (() => void) | null = null
  private _establishment: Promise<void> | null = null
  private _establishmentSettled = false
  private _unsubscribed = false

  constructor(
    source: CloudflareRoomSubscriptionSource,
    receiver: BackendReceiver,
    options: CloudflareRoomSubscriptionOptions,
  ) {
    this._source = source
    this._receiver = receiver
    this._onClosed = options.onClosed
    this.ready = new Promise<void>((resolve, reject) => {
      this._settleReady = { resolve, reject }
    })
    void this.ready.catch(() => {})
  }

  start(): void {
    if (this._establishment !== null || this._isClosed()) return
    const establishment = this._establish()
    this._establishment = establishment
    void establishment
      .finally(() => {
        this._establishmentSettled = true
      })
      .catch(() => {})
  }

  state(): SubscriptionAttemptState {
    return this._state
  }

  onStateChange(cb: (state: SubscriptionAttemptState) => void): () => void {
    this._listeners.add(cb)
    return () => this._listeners.delete(cb)
  }

  matches(request: RoomShardInvalidationRequest): boolean {
    return (
      request.roomId === this._source.roomId &&
      request.inc === this._source.inc &&
      request.laneKey === this._source.laneKey &&
      request.subscriberDoId === this._source.subscriberDoId &&
      request.leaseId === this._leaseId &&
      request.generationToken === this._generationToken
    )
  }

  async deliver(frame: Uint8Array, seq: number, timestamp: number): Promise<void> {
    if (this._state !== 'ready') throw new Error('Cloudflare Room delivery lease is not installed')
    await (this._receiver(new Uint8Array(frame), { seq, timestamp }) as unknown)
  }

  invalidate(): void {
    this._close()
  }

  terminate(): void {
    if (this._unsubscribed) return
    this._unsubscribed = true
    const needsLateTeardown = !this._establishmentSettled
    this._resolveReady()
    this._terminate()
    void this._settleTeardown(needsLateTeardown).catch(console.error)
  }

  async unsubscribe(): Promise<void> {
    if (this._unsubscribed) return
    this._unsubscribed = true
    const needsLateTeardown = !this._establishmentSettled
    this._resolveReady()
    this._close()
    await this._settleTeardown(needsLateTeardown)
  }

  private async _establish(): Promise<void> {
    try {
      const registered = await this._source.authority.registerRoute(
        this._source.roomId,
        this._source.inc,
        this._source.laneKey,
        this._source.subscriberDoId,
        this._leaseId,
      )
      if (this._isClosed()) return
      if (!('ok' in registered)) {
        const error = new Error(registered.reason)
        if (registered.terminal === true) {
          this._rejectReady(error)
          this._terminate()
          return
        }
        throw error
      }
      this._generationToken = registered.generationToken
      this._transition('ready')
      this._resolveReady()
      this._scheduleRenewal()
    } catch (error) {
      if (this._isClosed()) return
      this._rejectReady(error)
      this._close()
    }
  }

  private _scheduleRenewal(): void {
    if (this._state !== 'ready') return
    this._cancelRenewal?.()
    const handle = setTimeout(() => void this._renew(), ROUTE_RENEW_EVERY_MS)
    this._cancelRenewal = () => clearTimeout(handle)
  }

  private async _renew(): Promise<void> {
    this._cancelRenewal = null
    if (this._state !== 'ready') return
    try {
      const renewed = await this._source.authority.renewRoute(
        this._source.inc,
        this._source.laneKey,
        this._source.subscriberDoId,
        this._leaseId,
        this._generationToken,
      )
      if (this._state !== 'ready') return
      if (!renewed.ok) {
        if (renewed.terminal === true) this.terminate()
        else this._close()
        return
      }
      this._scheduleRenewal()
    } catch {
      this._close()
    }
  }

  private async _teardown(): Promise<void> {
    await this._source.authority.unsubscribeRoute(
      this._source.inc,
      this._source.laneKey,
      this._source.subscriberDoId,
      this._leaseId,
    )
  }

  private async _settleTeardown(needsLateTeardown: boolean): Promise<void> {
    const teardown = this._teardown()
    if (needsLateTeardown && this._establishment !== null) {
      void this._establishment.finally(() => this._teardown()).catch((error) => console.error(error))
    }
    await teardown
  }

  private _close(): void {
    if (this._isClosed()) return
    this._cancelRenewal?.()
    this._cancelRenewal = null
    if (!this._readySettled) this._rejectReady(new Error('Cloudflare Room subscription closed before acknowledgement'))
    this._transition('closed')
    this._onClosed()
  }

  private _terminate(): void {
    if (this._isClosed()) return
    this._cancelRenewal?.()
    this._cancelRenewal = null
    this._transition('terminated')
    this._onClosed()
  }

  private _isClosed(): boolean {
    return this._state === 'closed' || this._state === 'terminated'
  }

  private _resolveReady(): void {
    if (this._readySettled) return
    this._readySettled = true
    this._settleReady.resolve()
  }

  private _rejectReady(error: unknown): void {
    if (this._readySettled) return
    this._readySettled = true
    this._settleReady.reject(error)
  }

  private _transition(state: SubscriptionAttemptState): void {
    if (this._state === state) return
    this._state = state
    for (const listener of [...this._listeners]) listener(state)
  }
}
