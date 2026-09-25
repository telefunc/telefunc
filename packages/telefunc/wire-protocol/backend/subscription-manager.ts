export { SubscriptionManager }

import { assert } from '../../utils/assert.js'
import { createDeferred, type Deferred } from '../../utils/createDeferred.js'
import type {
  BackendReceiver,
  BackendSubscription,
  SubscriptionAttempt,
  SubscriptionBinding,
  SubscriptionDriver,
  SubscriptionState,
} from './subscription.js'

type StateListener = (state: SubscriptionState) => void

type SubscriptionSlotConfig = {
  binding: SubscriptionBinding
  reportError: (error: unknown) => void
  sourceKey: string
  cleanup: (attempt: SubscriptionAttempt) => Promise<void>
  onEmpty: () => void
}

class SubscriptionManager<Source> {
  /** Slots by source key, then by driver partition. */
  private readonly _routes = new Map<string, Map<string, SubscriptionSlot>>()
  private readonly _cleanups = new Set<Promise<void>>()

  constructor(
    private readonly _driver: SubscriptionDriver<Source>,
    private readonly _reportError: (error: unknown) => void = console.error,
    private readonly _sourceKey: (source: Source) => string = String,
  ) {}

  subscribe(source: Source, receiver: BackendReceiver): BackendSubscription {
    const binding = this._driver.bind(source)
    const sourceKey = this._sourceKey(source)
    let route = this._routes.get(sourceKey)
    if (route === undefined) this._routes.set(sourceKey, (route = new Map()))
    let slot = route.get(binding.partition)
    if (slot === undefined) {
      const created: SubscriptionSlot = new SubscriptionSlot({
        binding,
        reportError: this._reportError,
        sourceKey,
        cleanup: (attempt) => this._cleanup(attempt),
        onEmpty: () => this._unmap(sourceKey, binding.partition, created),
      })
      route.set(binding.partition, (slot = created))
    }
    return slot.attach(receiver)
  }

  async dispose(): Promise<void> {
    const cleanups = [...this._routes.values()].flatMap((route) => [...route.values()].map((slot) => slot.stop()))
    this._routes.clear()
    await Promise.allSettled([...cleanups, ...this._cleanups])
  }

  private _unmap(sourceKey: string, partition: string, slot: SubscriptionSlot): void {
    const route = this._routes.get(sourceKey)
    if (route?.get(partition) !== slot) return
    route.delete(partition)
    if (route.size === 0) this._routes.delete(sourceKey)
  }

  private _cleanup(attempt: SubscriptionAttempt): Promise<void> {
    const cleanup = Promise.resolve()
      .then(() => attempt.unsubscribe())
      .catch((error) => this._reportError(error))
    this._cleanups.add(cleanup)
    void cleanup.finally(() => this._cleanups.delete(cleanup))
    return cleanup
  }

  /** Whether any subscription is open. */
  hasSubscriptions(): boolean {
    return this._routes.size > 0
  }

  /** Whether a slot on the source's route is still establishing: never ready, stopped or ended. */
  hasEstablishing(source: Source): boolean {
    return this._slotsOf(source).some((slot) => slot.establishing)
  }

  /** Resolves once every slot on the source's route, including ones added meanwhile, is past its establishment. */
  async established(source: Source): Promise<void> {
    for (let waits = this._establishingWaits(source); waits.length > 0; waits = this._establishingWaits(source))
      await Promise.all(waits)
  }

  private _establishingWaits(source: Source): Promise<void>[] {
    return this._slotsOf(source).flatMap((slot) => (slot.establishing ? [slot.established] : []))
  }

  private _slotsOf(source: Source): SubscriptionSlot[] {
    return [...(this._routes.get(this._sourceKey(source))?.values() ?? [])]
  }
}

class SubscriptionSlot {
  private readonly _receivers = new Map<symbol, BackendReceiver>()
  private readonly _listeners = new Set<StateListener>()
  private _attempt: SubscriptionAttempt | null = null
  private _unobserve: (() => void) | null = null
  private _readiness: Deferred<void> = createReadiness()
  /** The first readiness, settled by the first ready, a stop or an end; a later loss opens a new one. */
  readonly established: Promise<void> = this._readiness.promise.then(
    () => {},
    () => {},
  )
  private _wasReady = false
  private _state: SubscriptionState = 'establishing'
  private _stopPromise: Promise<void> | null = null

  constructor(private readonly _config: SubscriptionSlotConfig) {}

  get establishing(): boolean {
    return this._stopPromise === null && !this._wasReady
  }

  attach(receiver: BackendReceiver): BackendSubscription {
    assert(this._stopPromise === null) // the manager unmaps a slot before stopping it
    const attachment = Symbol()
    this._receivers.set(attachment, receiver)
    if (this._attempt === null) this._start()
    let attached = true
    const listeners = new Set<StateListener>()
    const observer: StateListener = (state) => this._notify(listeners, state)
    this._listeners.add(observer)
    const unobserve = () => this._listeners.delete(observer)
    const slot = this
    return {
      get ready() {
        return attached ? slot._readiness.promise : Promise.resolve()
      },
      state: () => (attached ? this._state : 'closed'),
      onStateChange: (listener) => {
        assert(attached)
        listeners.add(listener)
        return () => listeners.delete(listener)
      },
      unsubscribe: async () => {
        if (!attached) return
        attached = false
        if (this._state !== 'closed') this._notify(listeners, 'closed')
        listeners.clear()
        unobserve()
        this._receivers.delete(attachment)
        if (this._receivers.size === 0) {
          this._config.onEmpty()
          await this.stop()
        }
      },
    }
  }

  stop(): Promise<void> {
    if (this._stopPromise !== null) return this._stopPromise
    const attempt = this._attempt
    this._stopPromise = attempt === null ? Promise.resolve() : this._config.cleanup(attempt)
    this._readiness.resolve()
    this._transition('closed')
    this._clearCurrent()
    return this._stopPromise
  }

  private _start(): void {
    let attempt: SubscriptionAttempt
    try {
      attempt = this._config.binding.open(
        (payload, info) => {
          if (this._stopPromise !== null) return
          for (const receiver of [...this._receivers.values()]) {
            try {
              receiver(payload, info)
            } catch (error) {
              this._config.reportError(error)
            }
          }
        },
        () => this._receivers.size,
      )
    } catch (error) {
      this._terminal(error)
      return
    }
    this._attempt = attempt
    this._unobserve = attempt.onStateChange((state, reason) => this._onStateChange(attempt, state, reason))
    // The attempt may have settled inside open(), before it had an observer.
    const state = attempt.state()
    if (state === 'ready' || state === 'closed') this._onStateChange(attempt, state)
  }

  private _onStateChange(attempt: SubscriptionAttempt, state: SubscriptionState, reason?: Error): void {
    assert(this._attempt === attempt) // a slot opens one attempt, and unobserves it before cleanup
    if (state === 'ready') return this._becameReady()
    if (state === 'closed') return this._ended(reason)
    this._markUnavailable(state)
    if (state === 'lost') this._config.reportError(new Error(`Backend subscription lost: ${this._config.sourceKey}`))
  }

  private _becameReady(): void {
    this._wasReady = true
    this._readiness.resolve()
    this._transition('ready')
  }

  /** The driver's reason, if any, is the failure's cause. */
  private _ended(reason: Error | undefined): void {
    this._terminal(new Error(`Backend subscription closed: ${this._config.sourceKey}`, reason && { cause: reason }))
  }

  private _terminal(error: unknown): void {
    const failure = error instanceof Error ? error : new Error(String(error))
    this._stopPromise ??= this._attempt === null ? Promise.resolve() : this._config.cleanup(this._attempt)
    // A resolved readiness cannot carry the failure, so `ready` read from here on is a fresh, rejected one.
    if (this._state === 'ready') this._readiness = createReadiness()
    this._transition('closed')
    this._clearCurrent()
    this._config.onEmpty()
    this._readiness.reject(failure)
  }

  private _markUnavailable(state: 'establishing' | 'lost'): void {
    if (this._state === 'ready') this._readiness = createReadiness()
    this._transition(state)
  }

  private _transition(state: SubscriptionState): void {
    if (this._state === state) return
    this._state = state
    this._notify(this._listeners, state)
  }

  private _clearCurrent(): void {
    this._unobserve?.()
    this._unobserve = null
    this._attempt = null
  }

  /** Consumer listeners are isolated from each other; one that throws is reported. */
  private _notify(listeners: Set<StateListener>, state: SubscriptionState): void {
    for (const listener of [...listeners]) {
      if (listeners.has(listener)) this._report(() => listener(state))
    }
  }

  private _report(notify: () => void): void {
    try {
      notify()
    } catch (error) {
      this._config.reportError(error)
    }
  }
}

function createReadiness(): Deferred<void> {
  const readiness = createDeferred()
  void readiness.promise.catch(() => {})
  return readiness
}
