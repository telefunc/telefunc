export { SubscriptionManager }

import type {
  BackendReceiver,
  BackendSubscription,
  SubscriptionAttempt,
  SubscriptionAttemptState,
  SubscriptionBinding,
  SubscriptionDriver,
  SubscriptionState,
} from './subscription.js'
import { CHANNEL_BUFFER_LIMIT_BYTES } from '../constants.js'
import { ChannelOverflowError } from '../channel-errors.js'

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

type PendingPublish = {
  payload: Uint8Array
  publish: (payload: Uint8Array) => unknown
  resolve: (value: unknown) => void
  reject: (error: unknown) => void
}

type PendingPublishState = {
  entries: PendingPublish[]
  bytes: number
}

const PENDING_PUBLISH_LIMIT = 1024

class SubscriptionManager<Source> {
  private readonly _slots = new Map<string, SubscriptionSlot<Source>>()
  private readonly _cleanups = new Set<Promise<void>>()
  private readonly _pendingPublishes = new Map<string, PendingPublishState>()

  constructor(
    private readonly _driver: SubscriptionDriver<Source>,
    private readonly _reportError: (error: unknown) => void = console.error,
    private readonly _sourceKey: (source: Source) => string = String,
  ) {}

  subscribe(source: Source, receiver: BackendReceiver): BackendSubscription {
    const binding = this._driver.bind(source)
    assertBinding(binding)
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
          slot!.markRemoved()
          if (this._slots.get(key) === slot) this._slots.delete(key)
        },
      })
      this._slots.set(key, slot)
    }
    return slot.attach(receiver)
  }

  publish<T>(
    source: Source,
    payload: Uint8Array,
    publish: (ownedPayload: Uint8Array) => T | Promise<T>,
  ): T | Promise<T> {
    const sourceKey = this._sourceKey(source)
    let state = this._pendingPublishes.get(sourceKey)
    if (state === undefined && this._readinessWaits(sourceKey).length === 0) return publish(payload)

    const ownedPayload = payload.slice()
    const startFlushing = state === undefined
    state ??= { entries: [], bytes: 0 }
    if (
      state.entries.length >= PENDING_PUBLISH_LIMIT ||
      state.bytes + ownedPayload.byteLength > CHANNEL_BUFFER_LIMIT_BYTES
    ) {
      return Promise.reject(new ChannelOverflowError('Broadcast readiness buffer overflow'))
    }
    if (!this._pendingPublishes.has(sourceKey)) this._pendingPublishes.set(sourceKey, state)
    state.bytes += ownedPayload.byteLength
    const result = new Promise<T>((resolve, reject) => {
      state.entries.push({
        payload: ownedPayload,
        publish,
        resolve: resolve as (value: unknown) => void,
        reject,
      })
    })
    if (startFlushing) void this._flushPublishes(sourceKey, state)
    return result
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

  private async _flushPublishes(sourceKey: string, state: PendingPublishState): Promise<void> {
    try {
      while (state.entries.length > 0) {
        await this._waitUntilReady(sourceKey)
        const entries = state.entries.splice(0)
        state.bytes = 0
        for (const entry of entries) {
          try {
            entry.resolve(entry.publish(entry.payload))
          } catch (error) {
            entry.reject(error)
          }
        }
      }
    } catch (error) {
      for (const entry of state.entries.splice(0)) entry.reject(error)
      state.bytes = 0
    } finally {
      this._pendingPublishes.delete(sourceKey)
    }
  }

  private async _waitUntilReady(sourceKey: string): Promise<void> {
    for (;;) {
      const pending = this._readinessWaits(sourceKey)
      if (pending.length === 0) return
      await Promise.all(pending)
    }
  }

  private _readinessWaits(sourceKey: string): Promise<void>[] {
    return [...this._slots.values()]
      .filter((slot) => slot.config.sourceKey === sourceKey)
      .flatMap((slot) => slot.waitForReadyOrRemoved() ?? [])
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
  private readonly _removed = createReadinessGeneration()

  constructor(readonly config: SubscriptionSlotConfig<Source>) {}

  markRemoved(): void {
    this._removed.resolve()
  }

  waitForReadyOrRemoved(): Promise<void> | null {
    if (this._stopPromise !== null || this._state === 'ready') return null
    return Promise.race([this._readiness.promise, this._removed.promise])
  }

  attach(receiver: BackendReceiver): BackendSubscription {
    if (this._stopPromise !== null) throw new Error('SubscriptionManager: cannot attach to a stopped source')
    const attachment = Symbol()
    this._receivers.set(attachment, receiver)
    if (this._attempt === null) this._start()
    let attached = true
    const listeners = new Set<StateListener>()
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
          this._notify([listener], 'closed')
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
    if (this._stopPromise !== null || this._receivers.size === 0 || this._attempt !== null) return
    if (!this._safely(() => this.config.binding.valid() === true, false)) {
      this._ownershipTerminated()
      return
    }
    let attempt: SubscriptionAttempt
    try {
      attempt = this.config.binding.open(
        async (payload, info) => {
          if (this._stopPromise !== null) return
          await Promise.all(
            [...this._receivers.values()].map(async (receiver) => {
              try {
                await (receiver(payload, info) as unknown)
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
    try {
      const unobserve = attempt.onStateChange((state) => this._onStateChange(attempt, state))
      if (this._attempt === attempt) this._unobserve = unobserve
      else this._safely(unobserve, undefined)
      attempt.ready.then(
        () => this._becameReady(attempt),
        (error: unknown) => this._failCurrent(attempt, error),
      )
      const state = attempt.state()
      if (state === 'ready') this._becameReady(attempt)
      else if (state === 'closed')
        this._failCurrent(attempt, new Error(`Backend subscription closed: ${this.config.sourceKey}`))
      else if (state === 'terminated') this._ownershipTerminated(attempt)
    } catch (error) {
      this._failCurrent(attempt, error)
    }
  }

  private _onStateChange(attempt: SubscriptionAttempt, state: SubscriptionAttemptState): void {
    if (this._attempt !== attempt) return
    if (state === 'terminated') return this._ownershipTerminated(attempt)
    if (state === 'ready') return this._becameReady(attempt)
    if (state === 'closed') {
      return this._failCurrent(attempt, new Error(`Backend subscription closed: ${this.config.sourceKey}`))
    }
    this._markUnavailable(state)
    if (state === 'lost') this.config.reportError(new Error(`Backend subscription lost: ${this.config.sourceKey}`))
  }

  private _becameReady(attempt: SubscriptionAttempt): void {
    if (this._attempt !== attempt) return
    try {
      if (attempt.state() !== 'ready') return
    } catch (error) {
      this._failCurrent(attempt, error)
      return
    }
    this._readiness.resolve()
    this._transition('ready')
  }

  private _ownershipTerminated(attempt: SubscriptionAttempt | null = this._attempt): void {
    this._terminal(new Error(`Backend subscription ownership terminated: ${this.config.sourceKey}`), attempt)
  }

  private _failCurrent(attempt: SubscriptionAttempt, error: unknown): void {
    if (this._attempt !== attempt) return
    this._markUnavailable('lost')
    this._terminal(error, attempt)
  }

  private _terminal(error: unknown, attempt: SubscriptionAttempt | null = this._attempt): void {
    if (attempt !== null && this._attempt !== attempt) return
    const failure = error instanceof Error ? error : new Error(String(error))
    this._stopPromise ??= this._attempt === null ? Promise.resolve() : this.config.cleanup(this._attempt)
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
    const unobserve = this._unobserve
    this._unobserve = null
    this._attempt = null
    if (unobserve !== null) this._safely(unobserve, undefined)
  }

  private _safely<Value>(operation: () => Value, fallback: Value): Value {
    try {
      return operation()
    } catch (error) {
      this.config.reportError(error)
      return fallback
    }
  }

  private _notify(listeners: Iterable<StateListener>, state: SubscriptionState): void {
    for (const listener of [...listeners]) {
      if (listeners instanceof Set && !listeners.has(listener)) continue
      try {
        listener(state)
      } catch (error) {
        this.config.reportError(error)
      }
    }
  }
}

function assertBinding(binding: SubscriptionBinding): void {
  const invalid =
    binding === null ||
    typeof binding !== 'object' ||
    typeof binding.partition !== 'string' ||
    typeof binding.valid !== 'function' ||
    typeof binding.open !== 'function'
  if (invalid)
    throw new Error('SubscriptionDriver.bind() must return a string partition plus valid() and open() functions')
}

function createReadinessGeneration() {
  let resolve!: () => void
  let reject!: (error: Error) => void
  const promise = new Promise<void>((resolvePromise, rejectPromise) => {
    resolve = resolvePromise
    reject = rejectPromise
  })
  void promise.catch(() => {})
  return { promise, resolve, reject }
}
