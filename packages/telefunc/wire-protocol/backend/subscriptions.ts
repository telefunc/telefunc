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
  readonly #slots = new Map<string, SubscriptionSlot<Source>>()
  readonly #driver: SubscriptionDriver<Source>
  readonly #reportError: (error: unknown) => void
  readonly #sourceKey: (source: Source) => string

  constructor(
    driver: SubscriptionDriver<Source>,
    reportError: (error: unknown) => void = console.error,
    sourceKey: (source: Source) => string = String,
  ) {
    this.#driver = driver
    this.#reportError = reportError
    this.#sourceKey = sourceKey
  }

  subscribe(source: Source, receiver: BackendReceiver): BackendSubscription {
    const binding = this.#driver.bind(source)
    assertBinding(binding)
    const sourceKey = this.#sourceKey(source)
    const key = JSON.stringify([sourceKey, binding.partition])
    let slot = this.#slots.get(key)
    if (slot === undefined) {
      let created!: SubscriptionSlot<Source>
      created = new SubscriptionSlot(source, binding, this.#reportError, sourceKey, () => {
        if (this.#slots.get(key) === created) this.#slots.delete(key)
      })
      slot = created
      this.#slots.set(key, slot)
    }
    return slot.attach(receiver)
  }

  /** Terminally removes sources the backend deliberately destroyed. Unlike an upstream `closed`
   * event, this is not recoverable and must not start a replacement attempt. */
  terminate(predicate: (source: Source) => boolean): void {
    for (const [key, slot] of this.#slots) {
      if (!predicate(slot.source)) continue
      this.#slots.delete(key)
      void slot.stop()
    }
  }

  dispose(): Promise<void> {
    const cleanups = [...this.#slots.values()].map((slot) => slot.stop())
    this.#slots.clear()
    return Promise.allSettled(cleanups).then(() => {})
  }
}

class SubscriptionSlot<Source> {
  readonly #source: Source
  readonly #binding: SubscriptionBinding
  readonly #reportError: (error: unknown) => void
  readonly #sourceKey: string
  readonly #onEmpty: () => void
  readonly #receivers = new Map<symbol, BackendReceiver>()
  readonly #listeners = new Set<(state: SubscriptionState) => void>()
  #attempt: SubscriptionAttempt | null = null
  #unobserve: (() => void) | null = null
  #readiness: ReadinessGeneration = createPendingReadinessGeneration()
  #state: SubscriptionState = 'establishing'
  #epoch = 0
  #replanQueued = false
  #replanAttempts = 0
  #establishmentTimer: ReturnType<typeof setTimeout> | null = null
  #stopped = false

  constructor(
    source: Source,
    binding: SubscriptionBinding,
    reportError: (error: unknown) => void,
    sourceKey: string,
    onEmpty: () => void,
  ) {
    this.#source = source
    this.#binding = binding
    this.#reportError = reportError
    this.#sourceKey = sourceKey
    this.#onEmpty = onEmpty
  }

  get source(): Source {
    return this.#source
  }

  attach(receiver: BackendReceiver): BackendSubscription {
    if (this.#stopped) throw new Error('SubscriptionManager: cannot attach to a stopped source')
    const attachment = Symbol()
    this.#receivers.set(attachment, receiver)
    if (this.#attempt === null && !this.#replanQueued) {
      if (this.#readiness.state === 'failed') {
        this.#readiness = createPendingReadinessGeneration()
        this.#replanAttempts = 0
        this.#transition('establishing')
      }
      this.#start()
    }
    let attached = true
    const listeners = new Set<(state: SubscriptionState) => void>()
    let previousState = this.#state
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
        return attached ? slot.#readiness.promise : Promise.resolve()
      },
      state: () => (attached ? this.#state : 'closed'),
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
        this.#receivers.delete(attachment)
        if (this.#receivers.size === 0) {
          this.#onEmpty()
          await this.stop()
        }
      },
    }
  }

  observe(listener: (state: SubscriptionState) => void): () => void {
    this.#listeners.add(listener)
    return () => this.#listeners.delete(listener)
  }

  async stop(): Promise<void> {
    if (this.#stopped) return
    this.#stopped = true
    this.#resolveReady()
    this.#transition('closed')
    const attempt = this.#attempt
    this.#clearCurrent()
    if (attempt !== null) await this.#cleanup(attempt)
  }

  #start(): void {
    if (this.#stopped || this.#receivers.size === 0 || this.#attempt !== null) return
    if (!this.#bindingIsValid()) {
      this.#ownershipTerminated()
      return
    }
    const epoch = ++this.#epoch
    let attempt: SubscriptionAttempt
    try {
      attempt = this.#binding.open(
        async (payload, info) => {
          if (epoch !== this.#epoch || this.#stopped) return
          await Promise.all(
            [...this.#receivers.values()].map(async (receiver) => {
              await (receiver(payload, info) as unknown)
            }),
          )
        },
        () => this.#receivers.size,
      )
    } catch (error) {
      this.#failed(error)
      return
    }
    this.#attempt = attempt
    this.#armEstablishmentDeadline(attempt)
    try {
      this.#unobserve = attempt.onStateChange((state) => this.#onStateChange(attempt, state))
      attempt.ready.then(
        () => this.#becameReady(attempt),
        (error: unknown) => this.#failedCurrent(attempt, error),
      )
      const state = attempt.state()
      if (state === 'ready') this.#becameReady(attempt)
      else if (state === 'closed') this.#closed(attempt)
      else if (state === 'terminated') this.#terminated(attempt)
    } catch (error) {
      this.#failedCurrent(attempt, error)
    }
  }

  #onStateChange(attempt: SubscriptionAttempt, state: SubscriptionAttemptState): void {
    if (this.#attempt !== attempt) return
    if (state === 'terminated') {
      this.#terminated(attempt)
      return
    }
    if (state === 'ready') {
      this.#becameReady(attempt)
      return
    }
    if (state === 'lost' || state === 'establishing') {
      this.#markUnavailable(state)
      this.#armEstablishmentDeadline(attempt)
      if (state === 'lost') this.#reportError(new Error(`Backend subscription lost: ${this.#label}`))
      return
    }
    this.#closed(attempt)
  }

  #becameReady(attempt: SubscriptionAttempt): void {
    if (this.#attempt !== attempt) return
    let state: SubscriptionAttemptState
    try {
      state = attempt.state()
    } catch (error) {
      this.#failedCurrent(attempt, error)
      return
    }
    if (state !== 'ready') return
    this.#cancelEstablishmentDeadline()
    this.#replanAttempts = 0
    this.#resolveReady()
    this.#transition('ready')
  }

  #closed(attempt: SubscriptionAttempt): void {
    if (this.#attempt !== attempt) return
    this.#markUnavailable('lost')
    this.#clearCurrent()
    void this.#cleanup(attempt)
    this.#scheduleReplan(new Error(`Backend subscription closed: ${this.#label}`))
  }

  #terminated(attempt: SubscriptionAttempt): void {
    if (this.#attempt !== attempt) return
    this.#ownershipTerminated(attempt)
  }

  #ownershipTerminated(attempt: SubscriptionAttempt | null = this.#attempt): void {
    if (attempt !== null && this.#attempt !== attempt) return
    const terminal = new Error(`Backend subscription ownership terminated: ${this.#label}`)
    const current = this.#attempt
    this.#stopped = true
    this.#transition('closed')
    this.#clearCurrent()
    if (current !== null) void this.#cleanup(current)
    this.#rejectReady(terminal)
    this.#onEmpty()
  }

  #failedCurrent(attempt: SubscriptionAttempt, error: unknown): void {
    if (this.#attempt !== attempt) {
      this.#reportError(error)
      return
    }
    this.#markUnavailable('lost')
    this.#clearCurrent()
    void this.#cleanup(attempt)
    this.#failed(error)
  }

  #failed(error: unknown): void {
    const failure = error instanceof Error ? error : new Error(String(error))
    if (this.#scheduleReplan(failure) !== 'terminal') this.#reportError(failure)
  }

  #scheduleReplan(cause: Error): 'scheduled' | 'terminal' | 'inactive' {
    if (this.#stopped || this.#receivers.size === 0 || this.#replanQueued) return 'inactive'
    if (!this.#bindingIsValid()) {
      this.#ownershipTerminated()
      return 'terminal'
    }
    if (this.#replanAttempts >= SUBSCRIPTION_REPLAN_LIMIT) {
      const terminal = new Error(
        `Backend subscription failed after ${SUBSCRIPTION_REPLAN_LIMIT} replacement attempts (${this.#label}): ${cause.message}`,
      )
      this.#transition('closed')
      if (this.#rejectReady(terminal)) this.#reportError(terminal)
      return 'terminal'
    }
    this.#replanAttempts++
    this.#replanQueued = true
    queueMicrotask(() => {
      this.#replanQueued = false
      if (!this.#stopped && this.#attempt === null) this.#start()
    })
    return 'scheduled'
  }

  #markUnavailable(state: 'establishing' | 'lost'): void {
    if (this.#readiness.state === 'ready') this.#readiness = createPendingReadinessGeneration()
    this.#transition(state)
  }

  #resolveReady(): void {
    const generation = this.#readiness
    if (generation.state !== 'pending') return
    generation.resolve()
    this.#readiness = { state: 'ready', promise: generation.promise }
  }

  #rejectReady(error: Error): boolean {
    const generation = this.#readiness
    if (generation.state !== 'pending') return false
    generation.reject(error)
    this.#readiness = { state: 'failed', promise: generation.promise }
    return true
  }

  #transition(state: SubscriptionState): void {
    if (this.#state === state) return
    this.#state = state
    for (const listener of [...this.#listeners]) listener(state)
  }

  #clearCurrent(): void {
    this.#epoch++
    this.#cancelEstablishmentDeadline()
    const unobserve = this.#unobserve
    this.#unobserve = null
    this.#attempt = null
    try {
      unobserve?.()
    } catch (error) {
      this.#reportError(error)
    }
  }

  #armEstablishmentDeadline(attempt: SubscriptionAttempt): void {
    if (this.#attempt !== attempt || this.#establishmentTimer !== null) return
    this.#establishmentTimer = unrefTimer(
      setTimeout(() => this.#establishmentExpired(attempt), SUBSCRIPTION_ESTABLISH_TIMEOUT_MS),
    )
  }

  #cancelEstablishmentDeadline(): void {
    if (this.#establishmentTimer === null) return
    clearTimeout(this.#establishmentTimer)
    this.#establishmentTimer = null
  }

  #establishmentExpired(attempt: SubscriptionAttempt): void {
    if (this.#attempt !== attempt) return
    this.#establishmentTimer = null
    let state: SubscriptionAttemptState
    try {
      state = attempt.state()
    } catch {
      state = 'establishing'
    }
    if (state === 'ready') {
      this.#becameReady(attempt)
      return
    }
    if (state === 'closed') {
      this.#closed(attempt)
      return
    }
    if (state === 'terminated') {
      this.#terminated(attempt)
      return
    }
    const failure = new Error(`Backend subscription establishment did not settle within the deadline (${this.#label})`)
    this.#clearCurrent()
    void this.#cleanup(attempt)
    this.#failed(failure)
  }

  async #cleanup(attempt: SubscriptionAttempt): Promise<void> {
    try {
      await attempt.unsubscribe()
    } catch (error) {
      this.#reportError(error)
    }
  }

  get #label(): string {
    return this.#sourceKey
  }

  #bindingIsValid(): boolean {
    try {
      return this.#binding.valid() === true
    } catch (error) {
      this.#reportError(error)
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
