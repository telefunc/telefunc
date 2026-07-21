import type { LaneSubscription, ReadinessState } from '../../../../backend/spi.js'
import { ROUTE_RENEW_EVERY_MS, ROUTE_RENEW_FAILURE_LIMIT } from './routes.js'

// The ratified Cloudflare LaneSubscription lifecycle. It is production code: conformance injects only
// this scheduler, so the same retry/renewal state machine runs with logical time instead of real waits.
export const SUBSCRIPTION_RETRY_ATTEMPTS = 5
export const SUBSCRIPTION_RETRY_BASE_MS = 250
export const SUBSCRIPTION_RETRY_MAX_MS = 4_000

export type SubscriptionScheduler = {
  schedule(delayMs: number, task: () => Promise<void>): () => void
}

export const realSubscriptionScheduler: SubscriptionScheduler = {
  schedule(delayMs, task) {
    const handle = setTimeout(() => void task(), delayMs)
    return () => clearTimeout(handle)
  },
}

export type EstablishResult = { ready: true } | { ready: false; retryable: boolean; reason: string }

export type CloudflareLaneSubscriptionOperations = {
  establish(): Promise<EstablishResult>
  renew(): Promise<boolean>
  unsubscribe(): Promise<void>
  closed?(): void
}

export type CloudflareLaneSubscriptionOptions = {
  scheduler?: SubscriptionScheduler
  jitter?: () => number
}

export class CloudflareLaneSubscription implements LaneSubscription {
  readonly ready: Promise<void>
  readonly #operations: CloudflareLaneSubscriptionOperations
  readonly #scheduler: SubscriptionScheduler
  readonly #jitter: () => number
  readonly #listeners = new Set<(state: ReadinessState) => void>()
  #state: ReadinessState = 'establishing'
  #settle!: { resolve: () => void; reject: (error: unknown) => void }
  #readySettled = false
  #cancelScheduled: (() => void) | null = null
  #renewalFailures = 0
  #closedNotified = false

  constructor(operations: CloudflareLaneSubscriptionOperations, options: CloudflareLaneSubscriptionOptions = {}) {
    this.#operations = operations
    this.#scheduler = options.scheduler ?? realSubscriptionScheduler
    this.#jitter = options.jitter ?? Math.random
    this.ready = new Promise<void>((resolve, reject) => {
      this.#settle = { resolve, reject }
    })
    // A caller may attach after the asynchronous first attempt. Observe internally without changing the
    // promise the caller receives, so a fail-closed establishment never becomes an unhandled rejection.
    void this.ready.catch(() => {})
  }

  start(): void {
    if (this.#state !== 'establishing' || this.#readySettled) return
    void this.#attemptInitial(0)
  }

  state(): ReadinessState {
    return this.#state
  }

  onStateChange(cb: (state: ReadinessState) => void): () => void {
    this.#listeners.add(cb)
    return () => this.#listeners.delete(cb)
  }

  async unsubscribe(): Promise<void> {
    if (this.#state === 'closed') return
    this.#cancelTimer()
    try {
      await this.#operations.unsubscribe()
    } finally {
      // Detach is local-first and terminal even when the transport cannot remove the durable route.
      // The guarded route then becomes unreachable locally and dies by lease expiry.
      this.#close(new Error('subscription unsubscribed'))
    }
  }

  generationDropped(): void {
    this.#cancelTimer()
    this.#close(new Error('subscription generation dropped'))
  }

  backendDisposed(): void {
    this.#cancelTimer()
    this.#close(new Error('subscription backend disposed'))
  }

  async #attemptInitial(attempt: number): Promise<void> {
    if (this.#state === 'closed') return
    const result = await this.#establishSafely()
    if (this.#isClosed()) return
    if (result.ready) {
      if (!this.#readySettled) {
        this.#readySettled = true
        // The first establishing->ready transition belongs only to `ready`, never onStateChange.
        this.#state = 'ready'
        this.#settle.resolve()
      } else {
        // A transient first failure already rejected `ready`; a later bounded recovery is observable on
        // the state surface even though promises cannot be un-rejected.
        this.#transition('ready')
      }
      this.#renewalFailures = 0
      this.#scheduleRenewal()
      return
    }

    if (!this.#readySettled) {
      this.#readySettled = true
      this.#settle.reject(new Error(result.reason))
    }
    if (!result.retryable || attempt + 1 >= SUBSCRIPTION_RETRY_ATTEMPTS) {
      // Initial failure/terminal exhaustion is represented by the ready rejection; do not duplicate it on
      // the later-transition callback surface.
      this.#state = 'closed'
      this.#notifyClosed()
      return
    }
    this.#scheduleRetry(attempt, () => this.#attemptInitial(attempt + 1))
  }

  #scheduleRenewal(): void {
    if (this.#state !== 'ready') return
    this.#cancelTimer()
    this.#cancelScheduled = this.#scheduler.schedule(ROUTE_RENEW_EVERY_MS, () => this.#renew())
  }

  async #renew(): Promise<void> {
    this.#cancelScheduled = null
    if (this.#state !== 'ready') return
    let renewed = false
    try {
      renewed = await this.#operations.renew()
    } catch {
      // Transport failure is a renewal failure and participates in the same K=2 transition.
    }
    if (this.#state !== 'ready') return
    if (renewed) {
      this.#renewalFailures = 0
      this.#scheduleRenewal()
      return
    }
    this.#renewalFailures += 1
    if (this.#renewalFailures < ROUTE_RENEW_FAILURE_LIMIT) {
      this.#scheduleRenewal()
      return
    }
    this.#transition('lost')
    await this.#attemptReestablish(0)
  }

  async #attemptReestablish(attempt: number): Promise<void> {
    if (this.#state !== 'lost') return
    const result = await this.#establishSafely()
    if (this.#state !== 'lost') return
    if (result.ready) {
      this.#renewalFailures = 0
      this.#transition('ready')
      this.#scheduleRenewal()
      return
    }
    if (!result.retryable || attempt + 1 >= SUBSCRIPTION_RETRY_ATTEMPTS) {
      this.#transition('closed')
      this.#notifyClosed()
      return
    }
    this.#scheduleRetry(attempt, () => this.#attemptReestablish(attempt + 1))
  }

  async #establishSafely(): Promise<EstablishResult> {
    try {
      return await this.#operations.establish()
    } catch (error) {
      return { ready: false, retryable: true, reason: (error as Error).message }
    }
  }

  #scheduleRetry(attempt: number, task: () => Promise<void>): void {
    this.#cancelTimer()
    const exponential = SUBSCRIPTION_RETRY_BASE_MS * 2 ** attempt
    const jittered = Math.round(exponential * (0.5 + this.#jitter()))
    const delay = Math.max(0, Math.min(SUBSCRIPTION_RETRY_MAX_MS, jittered))
    this.#cancelScheduled = this.#scheduler.schedule(delay, task)
  }

  #cancelTimer(): void {
    this.#cancelScheduled?.()
    this.#cancelScheduled = null
  }

  #close(error: Error): void {
    if (this.#state === 'closed') return
    if (!this.#readySettled) {
      this.#readySettled = true
      this.#settle.reject(error)
    }
    this.#transition('closed')
    this.#notifyClosed()
  }

  #transition(state: ReadinessState): void {
    if (this.#state === state) return
    this.#state = state
    for (const listener of this.#listeners) listener(state)
  }

  #notifyClosed(): void {
    if (this.#closedNotified) return
    this.#closedNotified = true
    this.#operations.closed?.()
  }

  #isClosed(): boolean {
    return this.#state === 'closed'
  }
}

type LocalAttachment = {
  close(error: Error): void
}

// One production lease lifecycle per local (room, incarnation, lane, representative), with callback
// attachments behind it. The first attachment owns registration; only the last detach tears the route
// down. Each returned subscription still has its own terminal local state.
export class CloudflareLaneSubscriptionMultiplexer {
  readonly #shared: CloudflareLaneSubscription
  readonly #onEmpty: () => void
  readonly #attachments = new Set<LocalAttachment>()
  #emptyNotified = false

  constructor(shared: CloudflareLaneSubscription, onEmpty: () => void) {
    this.#shared = shared
    this.#onEmpty = onEmpty
  }

  attach(detachLocal: () => void): LaneSubscription {
    let closed = false
    let readySettled = false
    let settleReady!: { resolve: () => void; reject: (error: unknown) => void }
    const listeners = new Set<(state: ReadinessState) => void>()
    const ready = new Promise<void>((resolve, reject) => {
      settleReady = { resolve, reject }
    })
    void ready.catch(() => {})

    let attachment!: LocalAttachment
    const offShared = this.#shared.onStateChange((state) => {
      if (closed) return
      if (state === 'closed') {
        attachment.close(new Error('shared subscription lifecycle closed'))
        this.#attachments.delete(attachment)
        if (this.#attachments.size === 0) this.#notifyEmpty()
        return
      }
      for (const listener of listeners) listener(state)
    })
    void this.#shared.ready.then(
      () => {
        if (closed || readySettled) return
        readySettled = true
        settleReady.resolve()
      },
      (error) => {
        if (closed || readySettled) return
        readySettled = true
        settleReady.reject(error)
      },
    )

    attachment = {
      close(error) {
        if (closed) return
        closed = true
        detachLocal()
        offShared()
        if (!readySettled) {
          readySettled = true
          settleReady.reject(error)
        }
        for (const listener of listeners) listener('closed')
        listeners.clear()
      },
    }
    this.#attachments.add(attachment)

    return {
      ready,
      state: () => (closed ? 'closed' : this.#shared.state()),
      onStateChange: (listener) => {
        if (closed) return () => {}
        listeners.add(listener)
        return () => listeners.delete(listener)
      },
      unsubscribe: async () => {
        if (closed) return
        attachment.close(new Error('subscription unsubscribed'))
        this.#attachments.delete(attachment)
        if (this.#attachments.size > 0) return
        this.#notifyEmpty()
        await this.#shared.unsubscribe()
      },
    }
  }

  generationDropped(): void {
    this.#shared.generationDropped()
    this.lifecycleClosed(new Error('subscription generation dropped'))
  }

  backendDisposed(): void {
    this.#shared.backendDisposed()
    this.lifecycleClosed(new Error('subscription backend disposed'))
  }

  lifecycleClosed(error: Error): void {
    for (const attachment of this.#attachments) attachment.close(error)
    this.#attachments.clear()
    this.#notifyEmpty()
  }

  #notifyEmpty(): void {
    if (this.#emptyNotified) return
    this.#emptyNotified = true
    this.#onEmpty()
  }
}
