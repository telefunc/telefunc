export { Live, LiveCell }
export type { LiveEvent, LiveSubscription }

import { subscribeTag, invalidateTagStatic } from './tags.js'

// A PRIVATE brand (global-registry symbol, like SERVER_CHANNEL_BRAND). Detection is internal only —
// never a public surface — so this is neither exported nor exposed via a static; the wire replacer
// reconstructs it locally via the same `Symbol.for('telefunc.Live')`.
const LIVE_BRAND = Symbol.for('telefunc.Live')

/**
 * A live value: read `.data`, and it stays up to date as the server pushes.
 *
 * Return one from a telefunction and the client receives a live handle:
 * ```ts
 * // server
 * async function onGetTodos() { return db.live.select().from(todos) }
 * // client
 * const todos = await onGetTodos()
 * todos.data // Todo[] — updates on its own
 * ```
 */
type Live<T> = {
  readonly data: T
}

/**
 * The `Live` namespace.
 *
 * The annotation is explicit so the emitted `.d.ts` pins the public surface to the three concepts:
 * a `Live<T>` is `{ readonly data: T }`, and `Live.derived` composes one from others. Without it,
 * inference would leak the internal `LiveCell` into the public types.
 */
const Live: {
  /**
   * Derive a live value from other live values. Reading a `Live`'s `.data` inside `compute`
   * registers it as a dependency; the derived value goes stale when any dependency does.
   * ```ts
   * const count = Live.derived(() => todos.data.length)
   * ```
   */
  derived<R>(compute: () => R): Live<R>
} = {
  derived: (compute) => LiveCell.derived(compute),
}

/** What travels once a Live crosses the wire: a stale signal, or a pushed value. Phase 1 rides
 *  `invalidate` (the client refetches); `data` carries the future delta push with no primitive
 *  change. The channel/wire layer is added in its own module — this module is the in-memory cell. */
type LiveEvent<T> = { kind: 'invalidate' } | { kind: 'data'; data: T }

/** @internal The consumer-side subscription behind a `Live<T>` — the seam adapters bind to (invalidate
 *  → refetch, data → cache write). Deliberately NOT on the public `Live<T>`: a user reads `.data`, and
 *  only an adapter needs the taps. Satisfied by both the revived client handle and a server `LiveCell`.
 *
 *  Shared as a TYPE ONLY (via `telefunc/__internal`), never a runtime helper: the adapters that consume
 *  it ship to the BROWSER, and `telefunc/__internal` is a server entry (no browser condition) — so a
 *  runtime import would drag the server context/tagHub graph into a client bundle. A type import erases. */
type LiveSubscription<T> = {
  /** Observe pushed values. Returns an idempotent unsubscribe. */
  onData(callback: (data: T) => void): () => void
  /** Observe stale signals. Returns an idempotent unsubscribe. */
  onInvalidate(callback: () => void): () => void
  close(): Promise<void>
}

/** A server-owned source of invalidations for one Live (the engine graph, a tag subscription, …).
 *  Attached before serialization via `attachSource`; the replacer subscribes it only when the handle
 *  crosses the wire, and releases it on the last owning channel's close. */
type LiveActivationSource = {
  subscribe(onInvalidate: () => void): () => void
}

// Callback-scoped dependency tracking for `Live.derived`: each `Live.derived(fn)` pushes a frame, and
// every `.data` getter read during `fn` records its cell on the top frame (the Solid/Vue computed
// idiom — synchronous, nothing request-global). Popped in `finally`, so a throwing `fn` never poisons
// a later derivation.
const trackingStack: Array<Set<LiveCell<unknown>>> = []

function track(cell: LiveCell<unknown>): void {
  trackingStack[trackingStack.length - 1]?.add(cell)
}

/** @internal The in-memory cell behind every `Live<T>`: the producer end (construct around a snapshot,
 *  drive it via `set`/`update`/`invalidate`) plus the serialize-time activation lifecycle. Unexported
 *  from the package — that IS the boundary that keeps the producer verbs off the public API, so a
 *  telefunction returns the handle directly and the wire replacer serializes it. */
class LiveCell<T> {
  readonly [LIVE_BRAND] = true
  private currentData: T
  private closed = false
  private dataTaps: Array<(data: T) => void> = []
  private invalidateTaps: Array<() => void> = []
  private closeCallbacks: Array<(err?: Error) => void> = []
  /** Bumped on every `set`. Lets a reader tell whether a value it captured is still the current one —
   *  which comparing the values themselves cannot do, since `set` may hand back a mutated object. */
  private dataRevision = 0
  // One coalesced emission per microtask window. `hasPendingData` is a flag (not a sentinel) so
  // `set(undefined)` is a real pending value; the LAST `set` in the window wins.
  private hasPendingData = false
  private pendingInvalidate = false
  private flushScheduled = false
  // ── serialize-time activation (deferred, cell-local lease-refcounted) ──
  /** Deps read during a `Live.derived` callback, held INERT — subscribed only at serialization. */
  private pendingDeps: Array<LiveCell<unknown>> = []
  /** Tag keys this cell should go stale on. Stored INERT — resolving a key to a hub subscription needs
   *  the request's fence, which only serialization has. */
  private tags: string[] = []
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
    // `LiveCell<T>` is invariant in T (its tap arrays), so widen to the tracking type.
    track(this as unknown as LiveCell<unknown>)
    return this.currentData
  }

  /** Push a new value. Coalesced: many `set`s in one microtask deliver once, with the last value. */
  set(value: T): void {
    if (this.closed) return
    this.currentData = value
    this.dataRevision++
    this.hasPendingData = true
    this.scheduleFlush()
  }

  /** Which revision `data` currently holds. Read it beside `data` to capture a snapshot; a later
   *  emission is new to the holder of that snapshot only if the revision has moved past it. */
  get revision(): number {
    return this.dataRevision
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

  /** Computed sugar: run `fn` once (tracking which deps' `.data` were read), snapshot the value, and
   *  record the deps as INERT pending descriptors — NO subscriptions at call time. The replacer
   *  activates them at serialization (cascade `activate`); a derived that never serializes subscribes
   *  nothing. Invalidate-only forwarding (a dep's invalidation forwards to the derived; re-derivation
   *  is the client's refetch re-running the telefunction). */
  static derived<R>(fn: () => R): LiveCell<R> {
    const frame = new Set<LiveCell<unknown>>()
    trackingStack.push(frame)
    let value: R
    try {
      value = fn()
    } finally {
      trackingStack.pop()
    }
    const derived = new LiveCell(value)
    derived.pendingDeps = [...frame]
    return derived
  }

  /** Go stale whenever `key` is invalidated.
   *
   *  This only RECORDS the key. It reads no context, touches no hub, and registers nothing, so it can
   *  be called anywhere in a telefunction — before or after any `await`. That is the point: the fence
   *  that decides what counts as "published since this request read" is stamped once at request entry,
   *  and serialization resolves the key against it. An association that had to capture the fence itself
   *  would have to run before the body's first await, which is an ordering rule nothing enforces and
   *  every caller would eventually get wrong. */
  static onInvalidate(key: string, live: LiveCell<unknown>): void {
    live.tags.push(key)
  }

  /** Publish a stale signal for `key`. Inside a request it is queued and published at settle; outside
   *  a request it publishes immediately. Fire-and-forget (`void`); publication is failure-safe. */
  static invalidate(key: string): void {
    invalidateTagStatic(key)
  }

  /** Attach a server-owned invalidation source (the ticket-6 engine seam / tag wiring). Inert until
   *  the first `activate`. */
  attachSource(source: LiveActivationSource): void {
    this.sources.push(source)
  }

  /** Serialize-time activation, refcounted by cell-local leases (one per owning channel). On the first
   *  lease it resolves this cell's tag keys against the request's fence, subscribes its sources, and
   *  cascade-activates each pending dep — idempotent, so a dep also returned elsewhere activates EXACTLY
   *  ONCE — wiring each dep's `onInvalidate` to this cell's `invalidate` (invalidate-only forwarding).
   *
   *  `requestStartSeq` is threaded down the cascade explicitly rather than read from ambient context:
   *  by serialize time the request context may already be gone, and a dep is activated by whichever
   *  cell owns it, not by the request. */
  activate(requestStartSeq: number): void {
    this.lease++
    if (this.lease !== 1) return
    for (const tag of this.tags) {
      this.sourceTeardowns.push(subscribeTag(tag, requestStartSeq, () => this.invalidate()))
    }
    for (const source of this.sources) this.sourceTeardowns.push(source.subscribe(() => this.invalidate()))
    for (const dep of this.pendingDeps) {
      dep.activate(requestStartSeq)
      const off = dep.onInvalidate(() => this.invalidate())
      this.activationTeardowns.push(() => {
        off()
        dep.release()
      })
    }
  }

  /** Release one lease (an owning channel closed). On the LAST release it tears down the source
   *  subscription and cascade-releases each pending dep — exactly once, order-independent. */
  release(): void {
    if (this.lease === 0) return
    this.lease--
    if (this.lease !== 0) return // a shared cell stays live while any owning channel remains
    for (const teardown of this.sourceTeardowns) teardown()
    this.sourceTeardowns = []
    for (const teardown of this.activationTeardowns) teardown()
    this.activationTeardowns = []
    void this.close()
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
