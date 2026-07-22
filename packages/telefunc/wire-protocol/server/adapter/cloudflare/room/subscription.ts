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
export type SubscriptionRouteResult = boolean | { renewed: boolean; terminal?: boolean; reason?: string }

export type CloudflareLaneSubscriptionOperations = {
  establish(newOperation: boolean): Promise<EstablishResult>
  renew(): Promise<SubscriptionRouteResult>
  validate?(): Promise<SubscriptionRouteResult>
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
  #currentReady!: Promise<void>
  #settleCurrentReady!: { resolve: () => void; reject: (error: unknown) => void }
  #currentReadySettled = false
  #validation: Promise<void> | null = null
  #operationEpoch = 0
  readonly #inFlight = new Set<Promise<void>>()

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
    this.#resetCurrentReady()
  }

  start(): void {
    if (this.#state !== 'establishing' || this.#readySettled) return
    void this.#track(this.#attemptInitial(0))
  }

  state(): ReadinessState {
    return this.#state
  }

  onStateChange(cb: (state: ReadinessState) => void): () => void {
    this.#listeners.add(cb)
    return () => this.#listeners.delete(cb)
  }

  // Readiness for a local attachment created after the shared lifecycle's first establishment. Unlike
  // the SPI's historical one-shot `ready`, this follows the CURRENT route epoch: lost/establishing waits
  // for the next successful establishment and closed rejects.
  currentReady(): Promise<void> {
    if (this.#state === 'ready') return Promise.resolve()
    if (this.#state === 'closed') return Promise.reject(new Error('shared subscription lifecycle closed'))
    return this.#currentReady
  }

  // A route can be authority-evicted (K=3) while the local timer still says ready. A late attachment
  // validates the exact lease before inheriting readiness; a missing live route drives the shared
  // lifecycle through lost -> re-establish, while a generation-token mismatch closes it terminally.
  ensureCurrentReady(): Promise<void> {
    if (this.#state !== 'ready' || this.#operations.validate === undefined) return this.currentReady()
    if (this.#validation !== null) return this.#validation
    this.#validation = this.#track(this.#validateCurrentRoute()).finally(() => {
      this.#validation = null
    })
    return this.#validation
  }

  async unsubscribe(): Promise<void> {
    if (this.#state === 'closed') return
    this.#cancelTimer()
    ++this.#operationEpoch
    // Closing is synchronous and invalidates every late validation/renewal completion before teardown
    // awaits. Wait for already-issued operations to settle, then unsubscribe the final lease epoch.
    this.#close(new Error('subscription unsubscribed'))
    await Promise.allSettled([...this.#inFlight])
    // Local closure above remains terminal when this remote teardown rejects.
    await this.#operations.unsubscribe()
  }

  generationDropped(): void {
    this.#cancelTimer()
    ++this.#operationEpoch
    this.#close(new Error('subscription generation dropped'))
  }

  backendDisposed(): void {
    this.#cancelTimer()
    ++this.#operationEpoch
    this.#close(new Error('subscription backend disposed'))
  }

  async #attemptInitial(attempt: number): Promise<void> {
    if (this.#state === 'closed') return
    const epoch = this.#operationEpoch
    const result = await this.#establishSafely(attempt === 0)
    if (this.#isClosed() || this.#operationEpoch !== epoch) return
    if (result.ready) {
      if (!this.#readySettled) {
        this.#readySettled = true
        // The first establishing->ready transition belongs only to `ready`, never onStateChange.
        this.#state = 'ready'
        this.#resolveCurrentReady()
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
      this.#rejectCurrentReady(new Error(result.reason))
      this.#notifyClosed()
      await this.#cleanupFailedEstablishment()
      return
    }
    this.#scheduleRetry(attempt, () => this.#attemptInitial(attempt + 1))
  }

  #scheduleRenewal(): void {
    if (this.#state !== 'ready') return
    this.#cancelTimer()
    this.#cancelScheduled = this.#scheduler.schedule(ROUTE_RENEW_EVERY_MS, () => this.#track(this.#renew()))
  }

  async #renew(): Promise<void> {
    this.#cancelScheduled = null
    if (this.#state !== 'ready') return
    const epoch = this.#operationEpoch
    let outcome: { renewed: boolean; terminal: boolean; reason?: string } = { renewed: false, terminal: false }
    try {
      outcome = normalizeRouteResult(await this.#operations.renew())
    } catch {
      // Transport failure is a renewal failure and participates in the same K=2 transition.
    }
    if (this.#state !== 'ready' || this.#operationEpoch !== epoch) return
    if (outcome.terminal) {
      this.#cancelTimer()
      this.#close(new Error(outcome.reason ?? 'subscription generation invalidated'))
      return
    }
    if (outcome.renewed) {
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
    const epoch = this.#operationEpoch
    const result = await this.#establishSafely(attempt === 0)
    if (this.#state !== 'lost' || this.#operationEpoch !== epoch) return
    if (result.ready) {
      this.#renewalFailures = 0
      this.#transition('ready')
      this.#scheduleRenewal()
      return
    }
    if (!result.retryable || attempt + 1 >= SUBSCRIPTION_RETRY_ATTEMPTS) {
      this.#transition('closed')
      this.#notifyClosed()
      await this.#cleanupFailedEstablishment()
      return
    }
    this.#scheduleRetry(attempt, () => this.#attemptReestablish(attempt + 1))
  }

  async #establishSafely(newOperation: boolean): Promise<EstablishResult> {
    try {
      return await this.#operations.establish(newOperation)
    } catch (error) {
      return { ready: false, retryable: true, reason: (error as Error).message }
    }
  }

  async #cleanupFailedEstablishment(): Promise<void> {
    try {
      await this.#operations.unsubscribe()
    } catch {
      // The local lifecycle is already terminal. Durable expiry is the backstop if exact teardown fails.
    }
  }

  #scheduleRetry(attempt: number, task: () => Promise<void>): void {
    this.#cancelTimer()
    const exponential = SUBSCRIPTION_RETRY_BASE_MS * 2 ** attempt
    const jittered = Math.round(exponential * (0.5 + this.#jitter()))
    const delay = Math.max(0, Math.min(SUBSCRIPTION_RETRY_MAX_MS, jittered))
    this.#cancelScheduled = this.#scheduler.schedule(delay, () => this.#track(task()))
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
    if (state === 'lost') this.#resetCurrentReady()
    this.#state = state
    if (state === 'ready') this.#resolveCurrentReady()
    if (state === 'closed') this.#rejectCurrentReady(new Error('shared subscription lifecycle closed'))
    for (const listener of [...this.#listeners]) notifyObserver(listener, state)
  }

  #notifyClosed(): void {
    if (this.#closedNotified) return
    this.#closedNotified = true
    try {
      this.#operations.closed?.()
    } catch {
      // State observers and local cleanup hooks cannot corrupt the terminal lifecycle.
    }
  }

  #isClosed(): boolean {
    return this.#state === 'closed'
  }

  async #validateCurrentRoute(): Promise<void> {
    const epoch = this.#operationEpoch
    let outcome: { renewed: boolean; terminal: boolean; reason?: string }
    try {
      outcome = normalizeRouteResult(await this.#operations.validate!())
    } catch {
      outcome = { renewed: false, terminal: false }
    }
    if (this.#state !== 'ready' || this.#operationEpoch !== epoch) return
    if (outcome.terminal) {
      this.#close(new Error(outcome.reason ?? 'subscription generation invalidated'))
      return
    }
    if (!outcome.renewed) {
      this.#transition('lost')
      await this.#attemptReestablish(0)
      await this.currentReady()
    }
  }

  #resetCurrentReady(): void {
    this.#currentReadySettled = false
    this.#currentReady = new Promise<void>((resolve, reject) => {
      this.#settleCurrentReady = { resolve, reject }
    })
    void this.#currentReady.catch(() => {})
  }

  #resolveCurrentReady(): void {
    if (this.#currentReadySettled) return
    this.#currentReadySettled = true
    this.#settleCurrentReady.resolve()
  }

  #rejectCurrentReady(error: Error): void {
    if (this.#currentReadySettled) return
    this.#currentReadySettled = true
    this.#settleCurrentReady.reject(error)
  }

  #track(operation: Promise<void>): Promise<void> {
    this.#inFlight.add(operation)
    void operation.finally(() => this.#inFlight.delete(operation)).catch(() => {})
    return operation
  }
}

function normalizeRouteResult(result: SubscriptionRouteResult): {
  renewed: boolean
  terminal: boolean
  reason?: string
} {
  if (typeof result === 'boolean') return { renewed: result, terminal: false }
  return { renewed: result.renewed, terminal: result.terminal === true, reason: result.reason }
}

function notifyObserver(listener: (state: ReadinessState) => void, state: ReadinessState): void {
  try {
    listener(state)
  } catch {
    // User observation is isolated from state advancement and teardown.
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
    const firstAttachment = this.#attachments.size === 0
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
      if (state === 'ready' && !readySettled) {
        readySettled = true
        settleReady.resolve()
      }
      for (const listener of [...listeners]) notifyObserver(listener, state)
    })
    const attachmentReadiness = firstAttachment ? this.#shared.ready : this.#shared.ensureCurrentReady()
    void attachmentReadiness.then(
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
        try {
          detachLocal()
        } catch {
          // Local observer cleanup cannot block route teardown.
        }
        try {
          offShared()
        } catch {
          // Listener removal is best effort; `closed` still guards any late callback.
        }
        if (!readySettled) {
          readySettled = true
          settleReady.reject(error)
        }
        for (const listener of [...listeners]) notifyObserver(listener, 'closed')
        listeners.clear()
      },
    }
    this.#attachments.add(attachment)
    if (this.#shared.state() === 'closed') {
      attachment.close(new Error('shared subscription lifecycle closed'))
      this.#attachments.delete(attachment)
      if (this.#attachments.size === 0) this.#notifyEmpty()
    }

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
    for (const attachment of [...this.#attachments]) attachment.close(error)
    this.#attachments.clear()
    this.#notifyEmpty()
  }

  #notifyEmpty(): void {
    if (this.#emptyNotified) return
    this.#emptyNotified = true
    try {
      this.#onEmpty()
    } catch {
      // A bookkeeping observer cannot corrupt terminal teardown.
    }
  }
}
