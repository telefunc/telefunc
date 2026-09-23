export { SubscriptionManager }

import { assert } from '../../utils/assert.js'
import { createDeferred } from '../../utils/createDeferred.js'
import type {
  BackendReceiver,
  BackendSubscription,
  SubscriptionAttempt,
  SubscriptionAttemptState,
  SubscriptionBinding,
  SubscriptionDriver,
  SubscriptionState,
} from './subscription.js'

type ReadinessGeneration = ReturnType<typeof createReadinessGeneration>
type StateListener = (state: SubscriptionState) => void

type SubscriptionSlotConfig<Source> = {
  source: Source
  binding: SubscriptionBinding
  reportError: (error: unknown) => void
  sourceKey: string
  cleanup: (attempt: SubscriptionAttempt) => Promise<void>
  onEmpty: () => void
}

class SubscriptionManager<Source> {
  private readonly _slots = new Map<string, SubscriptionSlot<Source>>()
  private readonly _cleanups = new Set<Promise<void>>()

  constructor(
    private readonly _driver: SubscriptionDriver<Source>,
    private readonly _reportError: (error: unknown) => void = console.error,
    private readonly _sourceKey: (source: Source) => string = String,
  ) {}

  subscribe(source: Source, receiver: BackendReceiver): BackendSubscription {
    const binding = this._driver.bind(source)
    const sourceKey = this._sourceKey(source)
    const key = JSON.stringify([sourceKey, binding.partition])
    let slot = this._slots.get(key)
    if (slot === undefined) {
      slot = new SubscriptionSlot({
        source,
        binding,
        reportError: this._reportError,
        sourceKey,
        cleanup: (attempt) => this._cleanup(attempt),
        onEmpty: () => {
          if (this._slots.get(key) === slot) this._slots.delete(key)
        },
      })
      this._slots.set(key, slot)
    }
    return slot.attach(receiver)
  }

  terminate(predicate: (source: Source) => boolean): void {
    for (const [key, slot] of this._slots) {
      if (!predicate(slot.config.source)) continue
      this._slots.delete(key)
      void slot.stop()
    }
  }

  async dispose(): Promise<void> {
    const cleanups = [...this._slots.values()].map((slot) => slot.stop())
    this._slots.clear()
    await Promise.allSettled([...cleanups, ...this._cleanups])
  }

  private _cleanup(attempt: SubscriptionAttempt): Promise<void> {
    const cleanup = Promise.resolve()
      .then(() => attempt.unsubscribe())
      .catch((error) => this._reportError(error))
    this._cleanups.add(cleanup)
    void cleanup.finally(() => this._cleanups.delete(cleanup))
    return cleanup
  }

  /** Whether a slot on the source's route is neither ready nor ended. */
  hasUnsettled(source: Source): boolean {
    return this._slotsOf(source).some((slot) => slot.unsettled)
  }

  /** Resolves once every slot on the source's route, including ones added meanwhile, is ready, stopped or terminal. */
  async settled(source: Source): Promise<void> {
    for (let waits = this._settledWaits(source); waits.length > 0; waits = this._settledWaits(source))
      await Promise.all(waits)
  }

  private _settledWaits(source: Source): Promise<void>[] {
    return this._slotsOf(source).flatMap((slot) => (slot.unsettled ? [slot.settled()] : []))
  }

  private _slotsOf(source: Source): SubscriptionSlot<Source>[] {
    const sourceKey = this._sourceKey(source)
    return [...this._slots.values()].filter((slot) => slot.config.sourceKey === sourceKey)
  }
}

class SubscriptionSlot<Source> {
  private readonly _receivers = new Map<symbol, BackendReceiver>()
  private readonly _listeners = new Set<StateListener>()
  private _attempt: SubscriptionAttempt | null = null
  private _unobserve: (() => void) | null = null
  private _readiness: ReadinessGeneration = createReadinessGeneration()
  private _state: SubscriptionState = 'establishing'
  private _stopPromise: Promise<void> | null = null

  constructor(readonly config: SubscriptionSlotConfig<Source>) {}

  get unsettled(): boolean {
    return this._stopPromise === null && this._state !== 'ready'
  }

  /** Resolves when the current readiness settles either way. */
  settled(): Promise<void> {
    return this._readiness.promise.then(
      () => {},
      () => {},
    )
  }

  attach(receiver: BackendReceiver): BackendSubscription {
    assert(this._stopPromise === null) // the manager unmaps a slot before stopping it
    const attachment = Symbol()
    this._receivers.set(attachment, receiver)
    if (this._attempt === null) this._start()
    let attached = true
    const listeners = new Set<StateListener>()
    // Consumers attached while establishing await `ready`; only a later lost → ready is an event for them.
    let suppressInitialReady = this._state === 'establishing'
    const observer: StateListener = (state) => {
      if (suppressInitialReady) {
        suppressInitialReady = false
        if (state === 'ready') return
      }
      this._notify(listeners, state)
    }
    this._listeners.add(observer)
    const unobserve = () => this._listeners.delete(observer)
    const slot = this
    return {
      get ready() {
        return attached ? slot._readiness.promise : Promise.resolve()
      },
      state: () => (attached ? this._state : 'closed'),
      onStateChange: (listener) => {
        if (!attached) {
          this._report(() => listener('closed'))
          return () => {}
        }
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
          this.config.onEmpty()
          await this.stop()
        }
      },
    }
  }

  stop(): Promise<void> {
    if (this._stopPromise !== null) return this._stopPromise
    const attempt = this._attempt
    this._stopPromise = attempt === null ? Promise.resolve() : this.config.cleanup(attempt)
    this._readiness.resolve()
    this._transition('closed')
    this._clearCurrent()
    return this._stopPromise
  }

  private _start(): void {
    if (!this.config.binding.valid()) return this._ended('terminated')
    let attempt: SubscriptionAttempt
    try {
      attempt = this.config.binding.open(
        async (payload, info) => {
          if (this._stopPromise !== null) return
          await Promise.all(
            [...this._receivers.values()].map(async (receiver) => {
              try {
                await receiver(payload, info)
              } catch (error) {
                this.config.reportError(error)
              }
            }),
          )
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
    if (state === 'ready' || state === 'closed' || state === 'terminated') this._onStateChange(attempt, state)
  }

  private _onStateChange(attempt: SubscriptionAttempt, state: SubscriptionAttemptState, reason?: Error): void {
    if (this._attempt !== attempt) return
    if (state === 'ready') return this._becameReady()
    if (state === 'closed' || state === 'terminated') return this._ended(state, reason, attempt)
    this._markUnavailable(state)
    if (state === 'lost') this.config.reportError(new Error(`Backend subscription lost: ${this.config.sourceKey}`))
  }

  private _becameReady(): void {
    this._readiness.resolve()
    this._transition('ready')
  }

  /** The driver's reason, if any, is the failure's cause. */
  private _ended(
    state: 'closed' | 'terminated',
    reason?: Error,
    attempt: SubscriptionAttempt | null = this._attempt,
  ): void {
    const what = state === 'closed' ? 'closed' : 'ownership terminated'
    this._terminal(
      new Error(`Backend subscription ${what}: ${this.config.sourceKey}`, reason && { cause: reason }),
      attempt,
    )
  }

  private _terminal(error: unknown, attempt: SubscriptionAttempt | null = this._attempt): void {
    if (attempt !== null && this._attempt !== attempt) return
    const failure = error instanceof Error ? error : new Error(String(error))
    this._stopPromise ??= this._attempt === null ? Promise.resolve() : this.config.cleanup(this._attempt)
    // A resolved readiness cannot carry the failure, so `ready` read from here on is a fresh, rejected one.
    if (this._state === 'ready') this._readiness = createReadinessGeneration()
    this._transition('closed')
    this._clearCurrent()
    this.config.onEmpty()
    this._readiness.reject(failure)
  }

  private _markUnavailable(state: 'establishing' | 'lost'): void {
    if (this._state === 'ready') this._readiness = createReadinessGeneration()
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
      this.config.reportError(error)
    }
  }
}

function createReadinessGeneration() {
  const readiness = createDeferred()
  void readiness.promise.catch(() => {})
  return readiness
}
