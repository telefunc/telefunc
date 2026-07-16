export { Live, LIVE_BRAND }
export type { ClientLive, LiveEvent }

import { subscribeTagFenced, invalidateTagStatic } from './tags.js'

// A private brand (global-registry symbol, like SERVER_CHANNEL_BRAND) so the wire replacer can
// detect a returned Live across module boundaries. Never exported as a public surface.
const LIVE_BRAND = Symbol.for('telefunc.Live')

/** What travels once a Live crosses the wire: a stale signal, or a pushed value. Phase 1 rides
 *  `invalidate` (the client refetches); `data` carries the future delta push with no primitive
 *  change. The channel/wire layer is added in its own module — this module is the in-memory cell. */
type LiveEvent<T> = { kind: 'invalidate' } | { kind: 'data'; data: T }

/** The consumer end: the same observation taps as the producer, minus the producer verbs (authority
 *  is the one asymmetry). Revived on the client from the wire; server-side it is `Live.client`
 *  re-typed. */
type ClientLive<T> = {
  readonly data: T
  /** Observe pushed values. Returns an idempotent unsubscribe. */
  onData(callback: (data: T) => void): () => void
  /** Observe stale signals. Returns an idempotent unsubscribe. */
  onInvalidate(callback: () => void): () => void
  onClose(callback: (err?: Error) => void): void
  close(): Promise<void>
  readonly isClosed: boolean
}

/** A server-owned source of invalidations for one Live (the engine graph, a tag subscription, …).
 *  Attached before serialization via `_attachSource`; the replacer subscribes it only when the handle
 *  crosses the wire, and releases it on the last owning channel's close. */
type LiveActivationSource = {
  subscribe(onInvalidate: () => void): () => void
}

// Callback-scoped dependency tracking for `Live.derived`: each `Live.derived(fn)` pushes a frame, and
// every `Live.data` getter read during `fn` records its cell on the top frame (the Solid/Vue computed
// idiom — synchronous, nothing request-global). Popped in `finally`, so a throwing `fn` never poisons
// a later derivation.
const trackingStack: Array<Set<Live<unknown>>> = []

function track(cell: Live<unknown>): void {
  trackingStack[trackingStack.length - 1]?.add(cell)
}

/** The producer end of a live value: construct it around a snapshot, drive it (`set`/`update`/
 *  `invalidate`), and return `.client`. Liveness is serialize-time — the wire replacer creates the
 *  channel only if this crosses the wire — so `.client` is a side-effect-free re-type. This module
 *  is the in-memory cell; the channel, the `Live.onInvalidate`/`Live.invalidate` statics, and
 *  `Live.derived` are layered on in their own sub-units. */
class Live<T> {
  readonly [LIVE_BRAND] = true
  private currentData: T
  private closed = false
  private dataTaps: Array<(data: T) => void> = []
  private invalidateTaps: Array<() => void> = []
  private closeCallbacks: Array<(err?: Error) => void> = []
  // One coalesced emission per microtask window. `hasPendingData` is a flag (not a sentinel) so
  // `set(undefined)` is a real pending value; the LAST `set` in the window wins.
  private hasPendingData = false
  private pendingInvalidate = false
  private flushScheduled = false
  // ── serialize-time activation (deferred, cell-local lease-refcounted) ──
  /** Deps read during a `Live.derived` callback, held INERT — subscribed only at serialization. */
  private pendingDeps: Array<Live<unknown>> = []
  /** Server-owned invalidation sources (engine graph, tag subscriptions), subscribed on the first lease. */
  private sources: LiveActivationSource[] = []
  private sourceTeardowns: Array<() => void> = []
  /** Teardowns for activated pending deps (unsubscribe + cascade release). */
  private activationTeardowns: Array<() => void> = []
  /** One lease per owning channel; the source + deps activate on 0→1 and tear down on 1→0. */
  private lease = 0

  constructor(data: T) {
    this.currentData = data
  }

  get data(): T {
    // `Live<T>` is invariant in T (its tap arrays), so widen to the tracking type as `.client` does.
    track(this as unknown as Live<unknown>)
    return this.currentData
  }

  /** Push a new value. Coalesced: many `set`s in one microtask deliver once, with the last value. */
  set(value: T): void {
    if (this.closed) return
    this.currentData = value
    this.hasPendingData = true
    this.scheduleFlush()
  }

  update(fn: (previous: T) => T): void {
    this.set(fn(this.currentData))
  }

  /** Signal the value is stale — the consumer refetches. Coalesced per microtask. */
  invalidate(): void {
    if (this.closed) return
    this.pendingInvalidate = true
    this.scheduleFlush()
  }

  onData(callback: (data: T) => void): () => void {
    return addTap(this.dataTaps, callback)
  }

  onInvalidate(callback: () => void): () => void {
    return addTap(this.invalidateTaps, callback)
  }

  onClose(callback: (err?: Error) => void): void {
    if (this.closed) {
      callback()
      return
    }
    this.closeCallbacks.push(callback)
  }

  close(): Promise<void> {
    if (this.closed) return Promise.resolve()
    this.closed = true
    const callbacks = this.closeCallbacks
    this.closeCallbacks = []
    for (const callback of callbacks) callback()
    return Promise.resolve()
  }

  get isClosed(): boolean {
    return this.closed
  }

  /** The consumer-end view — return this from a telefunction. A side-effect-free re-type (no
   *  channel, no activation): the wire replacer activates at serialization. Mirrors
   *  `ServerChannel.client`. */
  get client(): ClientLive<T> {
    return this as unknown as ClientLive<T>
  }

  static isLive(value: unknown): value is Live<unknown> {
    return typeof value === 'object' && value !== null && LIVE_BRAND in value
  }

  /** Computed sugar: run `fn` once (tracking which deps' `.data` were read), snapshot the value, and
   *  record the deps as INERT pending descriptors — NO subscriptions at call time. The replacer
   *  activates them at serialization (cascade `_activate`); a derived that never serializes subscribes
   *  nothing. Invalidate-only forwarding (a dep's invalidation forwards to the derived; re-derivation
   *  is the client's refetch re-running the telefunction). */
  static derived<R>(fn: () => R): ClientLive<R> {
    const frame = new Set<Live<unknown>>()
    trackingStack.push(frame)
    let value: R
    try {
      value = fn()
    } finally {
      trackingStack.pop()
    }
    const derived = new Live(value)
    derived.pendingDeps = [...frame]
    return derived.client
  }

  /** Subscribe this handle to a server-owned tag (manual liveness). Attaches a FENCED tag source (the
   *  existing TagHub `requestStartSeq`/`scanSince` catch-up — a tag published between the acquiring
   *  read and serialization is still caught) that the replacer activates at serialization and uses to
   *  `invalidate` the handle; the cell-local lease refcounts a handle held by more than one channel. */
  static onInvalidate(key: string, live: Live<unknown>): void {
    live._attachSource({ subscribe: (onInvalidate) => subscribeTagFenced(key, onInvalidate) })
  }

  /** Publish a stale signal for `key`. Inside a request it is queued and published at settle; outside
   *  a request it publishes immediately. Fire-and-forget (`void`); publication is failure-safe. */
  static invalidate(key: string): void {
    invalidateTagStatic(key)
  }

  /** @internal — attach a server-owned invalidation source (the ticket-6 engine seam / tag wiring).
   *  Inert until the first `_activate`. */
  _attachSource(source: LiveActivationSource): void {
    this.sources.push(source)
  }

  /** @internal — serialize-time activation, refcounted by cell-local leases (one per owning channel).
   *  On the first lease it subscribes the source and cascade-activates each pending dep — idempotent,
   *  so a dep also returned elsewhere activates EXACTLY ONCE — wiring each dep's `onInvalidate` to this
   *  cell's `invalidate` (invalidate-only forwarding). */
  _activate(): void {
    this.lease++
    if (this.lease !== 1) return
    for (const source of this.sources) this.sourceTeardowns.push(source.subscribe(() => this.invalidate()))
    for (const dep of this.pendingDeps) {
      dep._activate()
      const off = dep.onInvalidate(() => this.invalidate())
      this.activationTeardowns.push(() => {
        off()
        dep._release()
      })
    }
  }

  /** @internal — release one lease (an owning channel closed). On the LAST release it tears down the
   *  source subscription and cascade-releases each pending dep — exactly once, order-independent. */
  _release(): void {
    if (this.lease === 0) return
    this.lease--
    if (this.lease !== 0) return // a shared cell stays live while any owning channel remains
    for (const teardown of this.sourceTeardowns) teardown()
    this.sourceTeardowns = []
    for (const teardown of this.activationTeardowns) teardown()
    this.activationTeardowns = []
    void this.close()
  }

  /** @internal test-only — activation snapshot: lease count, tap counts, whether the source is live. */
  _activationStateForTesting(): {
    lease: number
    invalidateListeners: number
    dataListeners: number
    sourceActive: boolean
  } {
    return {
      lease: this.lease,
      invalidateListeners: this.invalidateTaps.length,
      dataListeners: this.dataTaps.length,
      sourceActive: this.sourceTeardowns.length > 0,
    }
  }

  private scheduleFlush(): void {
    if (this.flushScheduled) return
    this.flushScheduled = true
    queueMicrotask(() => this.flush())
  }

  private flush(): void {
    this.flushScheduled = false
    if (this.closed) {
      this.hasPendingData = false
      this.pendingInvalidate = false
      return
    }
    if (this.hasPendingData) {
      this.hasPendingData = false
      const data = this.currentData
      for (const tap of [...this.dataTaps]) tap(data)
    }
    if (this.pendingInvalidate) {
      this.pendingInvalidate = false
      for (const tap of [...this.invalidateTaps]) tap()
    }
  }
}

/** Register a tap and return an idempotent unsubscribe that removes exactly one registration. */
function addTap<F>(taps: Array<F>, callback: F): () => void {
  taps.push(callback)
  let removed = false
  return () => {
    if (removed) return
    removed = true
    const index = taps.indexOf(callback)
    if (index >= 0) taps.splice(index, 1)
  }
}
