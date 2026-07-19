export { derived, LiveCell }
export type { Live, LiveEvent, LiveSubscription }

// THE LIVE PRIMITIVE, owned by this package rather than by telefunc core.
//
// It lives here because the feature is ABANDONABLE: telefunc must not carry an uncertain API on behalf of
// an experimental package. Nothing in core knows this type exists — the wire replacer/reviver register
// through telefunc's PUBLIC extension seams (see wireServer.ts / wireClient.ts), so deleting this package
// leaves core exactly as it was.
//
// It is deliberately GENERAL-PURPOSE, not pruned to what the drizzle feature happens to need: it is a
// value that can go stale, with derivation and a serialize-time activation lifecycle. The reactive-db
// engine is one consumer of it, not its definition.
//
// A PRIVATE brand (global-registry symbol). Detection is internal only — never a public surface — so this
// is neither exported nor exposed via a static; the wire replacer reconstructs it locally via the same
// `Symbol.for('telefunc.Live')`. The registry symbol is what lets the two ends agree with no shared
// import, which is precisely what keeps core out of it.
const LIVE_BRAND = Symbol.for('telefunc.Live')

/**
 * A live value: read `.data` for the current snapshot. A `Live` also signals when it goes stale — an
 * adapter (e.g. `@telefunc/tanstack-query`) observes that and refetches, swapping in a fresh handle. The
 * bare handle's own `.data` does not mutate in place; staleness drives a refetch, not an in-place update.
 *
 * Return one from a telefunction and the client receives a live handle:
 * ```ts
 * // server
 * async function onGetTodos() { return db.select().from(todos).live() }
 * // client
 * const todos = await onGetTodos()
 * todos.data // Todo[] — a snapshot; an adapter refetches when it goes stale
 * ```
 */
type Live<T> = {
  readonly data: T
}

/**
 * Derive a live value from other live values. Reading a `Live`'s `.data` inside `compute` registers it as
 * a dependency; the derived value goes stale when any dependency does.
 * ```ts
 * const count = derived(() => todos.data.length)
 * ```
 *
 * The return type is annotated as `Live<R>` rather than inferred, so the emitted `.d.ts` pins the public
 * surface to `{ readonly data: R }` instead of leaking the internal `LiveCell`.
 */
function derived<R>(compute: () => R): Live<R> {
  return LiveCell.derived(compute)
}

/** What travels once a Live crosses the wire: a stale signal telling the client to refetch. The SIGNAL
 *  is the whole message — its arrival is the event, so it carries no payload. (It used to carry a
 *  `{ kind: 'invalidate' }` tag; nothing ever branched on it, in any version — the client listener has
 *  always discarded the argument.) The channel/wire layer lives in its own module — this one is the
 *  in-memory cell. */
type LiveEvent = undefined

/** @internal The consumer-side subscription behind a `Live<T>` — the seam the query adapter binds to
 *  (invalidate → refetch). Deliberately NOT on the public `Live<T>`: a user reads `.data`, and only an
 *  adapter needs the tap. Satisfied by the revived client handle.
 *
 *  Package-internal now: the `./tanstack-query` subpath imports it from here directly, which is what
 *  severed this package's last reach into `telefunc/__internal`. Still a TYPE, never a runtime helper —
 *  the adapter ships to the BROWSER, so a value import from a server module would drag the server graph
 *  into a client bundle. A type import erases. */
type LiveSubscription = {
  /** Observe stale signals. Returns an idempotent unsubscribe. */
  onInvalidate(callback: () => void): () => void
  close(): Promise<void>
}

/** A server-owned source of invalidations for one Live. Attached before serialization via
 *  `attachSource`; the replacer subscribes it only when the handle crosses the wire, and releases it on
 *  the last owning channel's close. */
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
 *  drive it via `invalidate`) plus the serialize-time activation lifecycle. Not exported from the
 *  package's public entry — that IS the boundary that keeps the producer verbs off the public API, so a
 *  telefunction returns the handle directly and the wire replacer serializes it.
 *
 *  This replaces what used to be reached through telefunc's extension host (`host.createLive`): with the
 *  cell owned here, that whole seam had no consumer left in core and was reverted. The engine now
 *  constructs cells directly — one fewer indirection, and one fewer thing core has to carry. */
class LiveCell<T> {
  readonly [LIVE_BRAND] = true
  private currentData: T
  private closed = false
  private invalidateTaps: Array<() => void> = []
  // One coalesced invalidation per microtask window — many `invalidate`s in one window deliver once.
  private pendingInvalidate = false
  private flushScheduled = false
  // ── serialize-time activation (deferred, cell-local lease-refcounted) ──
  /** Deps read during a `Live.derived` callback, held INERT — subscribed only at serialization. */
  private pendingDeps: Array<LiveCell<unknown>> = []
  /** Server-owned invalidation sources, subscribed on the first lease. */
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
    track(this) // reading `.data` inside a `Live.derived` callback registers this cell as a dependency
    return this.currentData
  }

  /** Signal the value is stale — the consumer refetches. Coalesced per microtask. */
  invalidate(): void {
    if (this.closed) return
    this.pendingInvalidate = true
    this.scheduleFlush()
  }

  onInvalidate(callback: () => void): () => void {
    return addTap(this.invalidateTaps, callback)
  }

  /** Stop invalidating. `closed` is read by `invalidate` (a closed cell never fires again); nothing
   *  observes the transition, so there is no close-notification to deliver. */
  close(): Promise<void> {
    this.closed = true
    return Promise.resolve()
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

  /** Attach a server-owned invalidation source. Inert until the first `activate`. */
  attachSource(source: LiveActivationSource): void {
    this.sources.push(source)
  }

  /** Serialize-time activation, refcounted by cell-local leases (one per owning channel). On the first
   *  lease it subscribes this cell's sources and cascade-activates each pending dep — idempotent, so a
   *  dep also returned elsewhere activates EXACTLY ONCE — wiring each dep's `onInvalidate` to this cell's
   *  `invalidate` (invalidate-only forwarding). */
  activate(): void {
    this.lease++
    if (this.lease !== 1) return
    for (const source of this.sources) this.sourceTeardowns.push(source.subscribe(() => this.invalidate()))
    for (const dep of this.pendingDeps) {
      dep.activate()
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
      this.pendingInvalidate = false
      return
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
