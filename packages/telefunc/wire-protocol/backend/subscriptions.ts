export { SUBSCRIPTION_ESTABLISH_TIMEOUT_MS, SUBSCRIPTION_REPLAN_LIMIT, SubscriptionManager }

import { unrefTimer } from '../../utils/unrefTimer.js'
import type {
  BackendReceiver,
  BackendSubscription,
  SubscriptionAttempt,
  SubscriptionBinding,
  SubscriptionDriver,
  SubscriptionState,
  SubscriptionAttemptState,
} from './spi.js'

/** Raw establishment has no backend-owned time bound. Core gives the initial attempt and five
 * replacements equal shares of the one-minute Room lane terminal policy: 6 × 10 seconds. An
 * acknowledgement beyond one share is classified as hung and consumes a replacement attempt. */
const SUBSCRIPTION_ESTABLISH_TIMEOUT_MS = 10_000
const SUBSCRIPTION_REPLAN_LIMIT = 5

type ReadinessGeneration =
  | { state: 'ready'; promise: Promise<void> }
  | {
      state: 'pending'
      promise: Promise<void>
      resolve: () => void
      reject: (error: Error) => void
    }
  | { state: 'failed'; promise: Promise<void> }

/**
 * The single L2/L3 mechanism: one upstream attempt per source, local fan-out/refcount, stale-attempt
 * rejection, fail-closed readiness, bounded replacement and a per-attempt liveness watchdog.
 */
class SubscriptionManager<Source> {
  private readonly _slots = new Map<string, SubscriptionSlot<Source>>()
  private readonly _driver: SubscriptionDriver<Source>
  private readonly _reportError: (error: unknown) => void
  private readonly _sourceKey: (source: Source) => string

  constructor(
    driver: SubscriptionDriver<Source>,
    reportError: (error: unknown) => void = console.error,
    sourceKey: (source: Source) => string = String,
  ) {
    this._driver = driver
    this._reportError = reportError
    this._sourceKey = sourceKey
  }

  subscribe(source: Source, receiver: BackendReceiver): BackendSubscription {
    const binding = this._driver.bind(source)
    assertBinding(binding)
    const sourceKey = this._sourceKey(source)
    const key = JSON.stringify([sourceKey, binding.partition])
    let slot = this._slots.get(key)
    if (slot === undefined) {
      let created!: SubscriptionSlot<Source>
      created = new SubscriptionSlot(source, binding, this._reportError, sourceKey, () => {
        if (this._slots.get(key) === created) this._slots.delete(key)
      })
      slot = created
      this._slots.set(key, slot)
    }
    return slot.attach(receiver)
  }

  /** Terminally removes sources the backend deliberately destroyed. Unlike an upstream `closed`
   * event, this is not recoverable and must not start a replacement attempt. */
  terminate(predicate: (source: Source) => boolean): void {
    for (const [key, slot] of this._slots) {
      if (!predicate(slot.source)) continue
      this._slots.delete(key)
      void slot.stop()
    }
  }

  dispose(): Promise<void> {
    const cleanups = [...this._slots.values()].map((slot) => slot.stop())
    this._slots.clear()
    return Promise.allSettled(cleanups).then(() => {})
  }
}

class SubscriptionSlot<Source> {
  private readonly _source: Source
  private readonly _binding: SubscriptionBinding
  private readonly _reportError: (error: unknown) => void
  private readonly _sourceKey: string
  private readonly _onEmpty: () => void
  private readonly _receivers = new Map<symbol, BackendReceiver>()
  private readonly _listeners = new Set<(state: SubscriptionState) => void>()
  private _attempt: SubscriptionAttempt | null = null
  private _unobserve: (() => void) | null = null
  private _readiness: ReadinessGeneration = createPendingReadinessGeneration()
  private _state: SubscriptionState = 'establishing'
  private _epoch = 0
  private _replanQueued = false
  private _replanAttempts = 0
  private _establishmentTimer: ReturnType<typeof setTimeout> | null = null
  private _stopped = false

  constructor(
    source: Source,
    binding: SubscriptionBinding,
    reportError: (error: unknown) => void,
    sourceKey: string,
    onEmpty: () => void,
  ) {
    this._source = source
    this._binding = binding
    this._reportError = reportError
    this._sourceKey = sourceKey
    this._onEmpty = onEmpty
  }

  get source(): Source {
    return this._source
  }

  attach(receiver: BackendReceiver): BackendSubscription {
    if (this._stopped) throw new Error('SubscriptionManager: cannot attach to a stopped source')
    const attachment = Symbol()
    this._receivers.set(attachment, receiver)
    if (this._attempt === null && !this._replanQueued) {
      if (this._readiness.state === 'failed') {
        this._readiness = createPendingReadinessGeneration()
        this._replanAttempts = 0
        this._transition('establishing')
      }
      this._start()
    }
    let attached = true
    const listeners = new Set<(state: SubscriptionState) => void>()
    let previousState = this._state
    let awaitingInitialOutcome = previousState === 'establishing'
    const unobserve = this.observe((state) => {
      const suppressInitialReady = awaitingInitialOutcome && previousState === 'establishing' && state === 'ready'
      awaitingInitialOutcome = false
      previousState = state
      if (suppressInitialReady) return
      for (const listener of listeners) listener(state)
    })
    const slot = this
    return {
      get ready() {
        return attached ? slot._readiness.promise : Promise.resolve()
      },
      state: () => (attached ? this._state : 'closed'),
      onStateChange: (listener) => {
        if (!attached) {
          listener('closed')
          return () => {}
        }
        listeners.add(listener)
        return () => listeners.delete(listener)
      },
      unsubscribe: async () => {
        if (!attached) return
        attached = false
        for (const listener of listeners) listener('closed')
        listeners.clear()
        unobserve()
        this._receivers.delete(attachment)
        if (this._receivers.size === 0) {
          this._onEmpty()
          await this.stop()
        }
      },
    }
  }

  observe(listener: (state: SubscriptionState) => void): () => void {
    this._listeners.add(listener)
    return () => this._listeners.delete(listener)
  }

  async stop(): Promise<void> {
    if (this._stopped) return
    this._stopped = true
    this._resolveReady()
    this._transition('closed')
    const attempt = this._attempt
    this._clearCurrent()
    if (attempt !== null) await this._cleanup(attempt)
  }

  private _start(): void {
    if (this._stopped || this._receivers.size === 0 || this._attempt !== null) return
    if (!this._bindingIsValid()) {
      this._ownershipTerminated()
      return
    }
    const epoch = ++this._epoch
    let attempt: SubscriptionAttempt
    try {
      attempt = this._binding.open(
        async (payload, info) => {
          if (epoch !== this._epoch || this._stopped) return
          await Promise.all(
            [...this._receivers.values()].map(async (receiver) => {
              await (receiver(payload, info) as unknown)
            }),
          )
        },
        () => this._receivers.size,
      )
    } catch (error) {
      this._failed(error)
      return
    }
    this._attempt = attempt
    this._armEstablishmentDeadline(attempt)
    try {
      this._unobserve = attempt.onStateChange((state) => this._onStateChange(attempt, state))
      attempt.ready.then(
        () => this._becameReady(attempt),
        (error: unknown) => this._failedCurrent(attempt, error),
      )
      const state = attempt.state()
      if (state === 'ready') this._becameReady(attempt)
      else if (state === 'closed') this._closed(attempt)
      else if (state === 'terminated') this._terminated(attempt)
    } catch (error) {
      this._failedCurrent(attempt, error)
    }
  }

  private _onStateChange(attempt: SubscriptionAttempt, state: SubscriptionAttemptState): void {
    if (this._attempt !== attempt) return
    if (state === 'terminated') {
      this._terminated(attempt)
      return
    }
    if (state === 'ready') {
      this._becameReady(attempt)
      return
    }
    if (state === 'lost' || state === 'establishing') {
      this._markUnavailable(state)
      this._armEstablishmentDeadline(attempt)
      if (state === 'lost') this._reportError(new Error(`Backend subscription lost: ${this._label}`))
      return
    }
    this._closed(attempt)
  }

  private _becameReady(attempt: SubscriptionAttempt): void {
    if (this._attempt !== attempt) return
    let state: SubscriptionAttemptState
    try {
      state = attempt.state()
    } catch (error) {
      this._failedCurrent(attempt, error)
      return
    }
    if (state !== 'ready') return
    this._cancelEstablishmentDeadline()
    this._replanAttempts = 0
    this._resolveReady()
    this._transition('ready')
  }

  private _closed(attempt: SubscriptionAttempt): void {
    if (this._attempt !== attempt) return
    this._markUnavailable('lost')
    this._clearCurrent()
    void this._cleanup(attempt)
    this._scheduleReplan(new Error(`Backend subscription closed: ${this._label}`))
  }

  private _terminated(attempt: SubscriptionAttempt): void {
    if (this._attempt !== attempt) return
    this._ownershipTerminated(attempt)
  }

  private _ownershipTerminated(attempt: SubscriptionAttempt | null = this._attempt): void {
    if (attempt !== null && this._attempt !== attempt) return
    const terminal = new Error(`Backend subscription ownership terminated: ${this._label}`)
    const current = this._attempt
    this._stopped = true
    this._transition('closed')
    this._clearCurrent()
    if (current !== null) void this._cleanup(current)
    this._rejectReady(terminal)
    this._onEmpty()
  }

  private _failedCurrent(attempt: SubscriptionAttempt, error: unknown): void {
    if (this._attempt !== attempt) {
      this._reportError(error)
      return
    }
    this._markUnavailable('lost')
    this._clearCurrent()
    void this._cleanup(attempt)
    this._failed(error)
  }

  private _failed(error: unknown): void {
    const failure = error instanceof Error ? error : new Error(String(error))
    if (this._scheduleReplan(failure) !== 'terminal') this._reportError(failure)
  }

  private _scheduleReplan(cause: Error): 'scheduled' | 'terminal' | 'inactive' {
    if (this._stopped || this._receivers.size === 0 || this._replanQueued) return 'inactive'
    if (!this._bindingIsValid()) {
      this._ownershipTerminated()
      return 'terminal'
    }
    if (this._replanAttempts >= SUBSCRIPTION_REPLAN_LIMIT) {
      const terminal = new Error(
        `Backend subscription failed after ${SUBSCRIPTION_REPLAN_LIMIT} replacement attempts (${this._label}): ${cause.message}`,
      )
      this._transition('closed')
      if (this._rejectReady(terminal)) this._reportError(terminal)
      return 'terminal'
    }
    this._replanAttempts++
    this._replanQueued = true
    queueMicrotask(() => {
      this._replanQueued = false
      if (!this._stopped && this._attempt === null) this._start()
    })
    return 'scheduled'
  }

  private _markUnavailable(state: 'establishing' | 'lost'): void {
    if (this._readiness.state === 'ready') this._readiness = createPendingReadinessGeneration()
    this._transition(state)
  }

  private _resolveReady(): void {
    const generation = this._readiness
    if (generation.state !== 'pending') return
    generation.resolve()
    this._readiness = { state: 'ready', promise: generation.promise }
  }

  private _rejectReady(error: Error): boolean {
    const generation = this._readiness
    if (generation.state !== 'pending') return false
    generation.reject(error)
    this._readiness = { state: 'failed', promise: generation.promise }
    return true
  }

  private _transition(state: SubscriptionState): void {
    if (this._state === state) return
    this._state = state
    for (const listener of [...this._listeners]) listener(state)
  }

  private _clearCurrent(): void {
    this._epoch++
    this._cancelEstablishmentDeadline()
    const unobserve = this._unobserve
    this._unobserve = null
    this._attempt = null
    try {
      unobserve?.()
    } catch (error) {
      this._reportError(error)
    }
  }

  private _armEstablishmentDeadline(attempt: SubscriptionAttempt): void {
    if (this._attempt !== attempt || this._establishmentTimer !== null) return
    this._establishmentTimer = unrefTimer(
      setTimeout(() => this._establishmentExpired(attempt), SUBSCRIPTION_ESTABLISH_TIMEOUT_MS),
    )
  }

  private _cancelEstablishmentDeadline(): void {
    if (this._establishmentTimer === null) return
    clearTimeout(this._establishmentTimer)
    this._establishmentTimer = null
  }

  private _establishmentExpired(attempt: SubscriptionAttempt): void {
    if (this._attempt !== attempt) return
    this._establishmentTimer = null
    let state: SubscriptionAttemptState
    try {
      state = attempt.state()
    } catch {
      state = 'establishing'
    }
    if (state === 'ready') {
      this._becameReady(attempt)
      return
    }
    if (state === 'closed') {
      this._closed(attempt)
      return
    }
    if (state === 'terminated') {
      this._terminated(attempt)
      return
    }
    const failure = new Error(`Backend subscription establishment did not settle within the deadline (${this._label})`)
    this._clearCurrent()
    void this._cleanup(attempt)
    this._failed(failure)
  }

  private async _cleanup(attempt: SubscriptionAttempt): Promise<void> {
    let timer!: ReturnType<typeof setTimeout>
    const cleanup = Promise.resolve()
      .then(() => attempt.unsubscribe())
      .catch((error) => this._reportError(error))
    const deadline = new Promise<void>((resolve) => {
      timer = unrefTimer(
        setTimeout(() => {
          this._reportError(
            new Error(`Backend subscription cleanup did not settle within the deadline (${this._label})`),
          )
          resolve()
        }, SUBSCRIPTION_ESTABLISH_TIMEOUT_MS),
      )
    })
    try {
      await Promise.race([cleanup, deadline])
    } finally {
      clearTimeout(timer)
    }
  }

  private get _label(): string {
    return this._sourceKey
  }

  private _bindingIsValid(): boolean {
    try {
      return this._binding.valid() === true
    } catch (error) {
      this._reportError(error)
      return false
    }
  }
}

function assertBinding(binding: SubscriptionBinding): void {
  if (
    binding === null ||
    typeof binding !== 'object' ||
    typeof binding.partition !== 'string' ||
    typeof binding.valid !== 'function' ||
    typeof binding.open !== 'function'
  ) {
    throw new Error('SubscriptionDriver.bind() must return a string partition plus valid() and open() functions')
  }
}

function createPendingReadinessGeneration(): Extract<ReadinessGeneration, { state: 'pending' }> {
  let resolve!: () => void
  let reject!: (error: Error) => void
  const promise = new Promise<void>((resolvePromise, rejectPromise) => {
    resolve = resolvePromise
    reject = rejectPromise
  })
  void promise.catch(() => {})
  return { state: 'pending', promise, resolve, reject }
}
