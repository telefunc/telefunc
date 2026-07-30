export { SubscriptionManager }

import type {
  BackendReceiver,
  BackendSubscription,
  SubscriptionAttempt,
  SubscriptionAttemptState,
  SubscriptionBinding,
  SubscriptionDriver,
  SubscriptionState,
} from './spi.js'
import { CHANNEL_BUFFER_LIMIT_BYTES } from '../constants.js'
import { ChannelOverflowError } from '../channel-errors.js'

type ReadinessGeneration =
  | { state: 'ready'; promise: Promise<void> }
  | {
      state: 'pending'
      promise: Promise<void>
      resolve: () => void
      reject: (error: Error) => void
    }
  | { state: 'failed'; promise: Promise<void> }

type PendingPublish = {
  payload: Uint8Array
  publish: (payload: Uint8Array) => unknown
  resolve: (value: unknown) => void
  reject: (error: unknown) => void
}

type PendingPublishState = {
  entries: PendingPublish[]
  bytes: number
  flushing: boolean
}

const PENDING_PUBLISH_LIMIT = 1024

/**
 * The single L2/L3 mechanism: one upstream attempt per source, local fan-out/refcount, stale-attempt
 * rejection, ownership checks, and readiness signalling. Consumers own retry budgets and deadlines.
 */
class SubscriptionManager<Source> {
  private readonly _slots = new Map<string, SubscriptionSlot<Source>>()
  private readonly _cleanups = new Set<Promise<void>>()
  private readonly _driver: SubscriptionDriver<Source>
  private readonly _reportError: (error: unknown) => void
  private readonly _sourceKey: (source: Source) => string
  private readonly _pendingPublishes = new Map<string, PendingPublishState>()

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
      created = new SubscriptionSlot(
        source,
        binding,
        this._reportError,
        sourceKey,
        (attempt) => this._cleanup(attempt),
        () => {
          created.markRemoved()
          if (this._slots.get(key) === created) this._slots.delete(key)
        },
      )
      slot = created
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
    state ??= { entries: [], bytes: 0, flushing: false }
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
    if (!state.flushing) {
      state.flushing = true
      void this._flushPublishes(sourceKey, state)
    }
    return result
  }

  terminate(predicate: (source: Source) => boolean): void {
    for (const [key, slot] of this._slots) {
      if (!predicate(slot.source)) continue
      this._slots.delete(key)
      void slot.stop()
    }
  }

  async dispose(): Promise<void> {
    const cleanups = [...this._slots.values()].map((slot) => slot.stop())
    this._slots.clear()
    await Promise.allSettled(cleanups)
    await Promise.allSettled([...this._cleanups])
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
      state.flushing = false
      if (state.entries.length === 0) this._pendingPublishes.delete(sourceKey)
      else {
        state.flushing = true
        void this._flushPublishes(sourceKey, state)
      }
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
      .filter((slot) => slot.sourceKey === sourceKey)
      .flatMap((slot) => slot.waitForReadyOrRemoved() ?? [])
  }
}

class SubscriptionSlot<Source> {
  private readonly _source: Source
  private readonly _binding: SubscriptionBinding
  private readonly _reportError: (error: unknown) => void
  private readonly _sourceKey: string
  private readonly _cleanup: (attempt: SubscriptionAttempt) => Promise<void>
  private readonly _onEmpty: () => void
  private readonly _receivers = new Map<symbol, BackendReceiver>()
  private readonly _listeners = new Set<(state: SubscriptionState) => void>()
  private _attempt: SubscriptionAttempt | null = null
  private _unobserve: (() => void) | null = null
  private _readiness: ReadinessGeneration = createPendingReadinessGeneration()
  private _state: SubscriptionState = 'establishing'
  private _epoch = 0
  private _stopped = false
  private _stopPromise: Promise<void> | null = null
  private _removedResolve!: () => void
  private readonly _removed = new Promise<void>((resolve) => {
    this._removedResolve = resolve
  })

  constructor(
    source: Source,
    binding: SubscriptionBinding,
    reportError: (error: unknown) => void,
    sourceKey: string,
    cleanup: (attempt: SubscriptionAttempt) => Promise<void>,
    onEmpty: () => void,
  ) {
    this._source = source
    this._binding = binding
    this._reportError = reportError
    this._sourceKey = sourceKey
    this._cleanup = cleanup
    this._onEmpty = onEmpty
  }

  get source(): Source {
    return this._source
  }

  get sourceKey(): string {
    return this._sourceKey
  }

  markRemoved(): void {
    this._removedResolve()
  }

  waitForReadyOrRemoved(): Promise<void> | null {
    if (this._stopped || this._state === 'ready') return null
    return Promise.race([this._readiness.promise, this._removed])
  }

  attach(receiver: BackendReceiver): BackendSubscription {
    if (this._stopped) throw new Error('SubscriptionManager: cannot attach to a stopped source')
    const attachment = Symbol()
    this._receivers.set(attachment, receiver)
    if (this._attempt === null) this._start()
    let attached = true
    const listeners = new Set<(state: SubscriptionState) => void>()
    let previousState = this._state
    let awaitingInitialOutcome = previousState === 'establishing'
    const unobserve = this.observe((state) => {
      const suppressInitialReady = awaitingInitialOutcome && previousState === 'establishing' && state === 'ready'
      awaitingInitialOutcome = false
      previousState = state
      if (suppressInitialReady) return
      this._notify(listeners, state)
    })
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

  stop(): Promise<void> {
    if (this._stopPromise !== null) return this._stopPromise
    const attempt = this._attempt
    this._stopped = true
    this._stopPromise = attempt === null ? Promise.resolve() : this._cleanup(attempt)
    this._resolveReady()
    this._transition('closed')
    this._clearCurrent()
    return this._stopPromise
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
      this._terminal(error)
      return
    }
    this._attempt = attempt
    try {
      const unobserve = attempt.onStateChange((state) => this._onStateChange(attempt, state))
      if (this._attempt === attempt) this._unobserve = unobserve
      else this._releaseObserver(unobserve)
      attempt.ready.then(
        () => this._becameReady(attempt),
        (error: unknown) => this._failedCurrent(attempt, error),
      )
      const state = attempt.state()
      if (state === 'ready') this._becameReady(attempt)
      else if (state === 'closed') this._closed(attempt)
      else if (state === 'terminated') this._ownershipTerminated(attempt)
    } catch (error) {
      this._failedCurrent(attempt, error)
    }
  }

  private _onStateChange(attempt: SubscriptionAttempt, state: SubscriptionAttemptState): void {
    if (this._attempt !== attempt) return
    if (state === 'terminated') {
      this._ownershipTerminated(attempt)
    } else if (state === 'ready') {
      this._becameReady(attempt)
    } else if (state === 'lost' || state === 'establishing') {
      this._markUnavailable(state)
      if (state === 'lost') this._reportError(new Error(`Backend subscription lost: ${this._sourceKey}`))
    } else {
      this._closed(attempt)
    }
  }

  private _becameReady(attempt: SubscriptionAttempt): void {
    if (this._attempt !== attempt) return
    try {
      if (attempt.state() !== 'ready') return
    } catch (error) {
      this._failedCurrent(attempt, error)
      return
    }
    this._resolveReady()
    this._transition('ready')
  }

  private _closed(attempt: SubscriptionAttempt): void {
    if (this._attempt !== attempt) return
    this._markUnavailable('lost')
    this._terminal(new Error(`Backend subscription closed: ${this._sourceKey}`), attempt)
  }

  private _ownershipTerminated(attempt: SubscriptionAttempt | null = this._attempt): void {
    this._terminal(new Error(`Backend subscription ownership terminated: ${this._sourceKey}`), attempt)
  }

  private _failedCurrent(attempt: SubscriptionAttempt, error: unknown): void {
    if (this._attempt !== attempt) return
    this._markUnavailable('lost')
    this._terminal(error, attempt)
  }

  private _terminal(error: unknown, attempt: SubscriptionAttempt | null = this._attempt): void {
    if (attempt !== null && this._attempt !== attempt) return
    const failure = error instanceof Error ? error : new Error(String(error))
    const current = this._attempt
    this._stopped = true
    this._stopPromise ??= current === null ? Promise.resolve() : this._cleanup(current)
    this._transition('closed')
    this._clearCurrent()
    this._rejectReady(failure)
    this._onEmpty()
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

  private _rejectReady(error: Error): void {
    const generation = this._readiness
    if (generation.state !== 'pending') return
    generation.reject(error)
    this._readiness = { state: 'failed', promise: generation.promise }
  }

  private _transition(state: SubscriptionState): void {
    if (this._state === state) return
    this._state = state
    this._notify(this._listeners, state)
  }

  private _clearCurrent(): void {
    this._epoch++
    const unobserve = this._unobserve
    this._unobserve = null
    this._attempt = null
    if (unobserve !== null) this._releaseObserver(unobserve)
  }

  private _releaseObserver(unobserve: () => void): void {
    try {
      unobserve()
    } catch (error) {
      this._reportError(error)
    }
  }

  private _notify(listeners: Iterable<(state: SubscriptionState) => void>, state: SubscriptionState): void {
    for (const listener of [...listeners]) {
      try {
        listener(state)
      } catch (error) {
        this._reportError(error)
      }
    }
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
