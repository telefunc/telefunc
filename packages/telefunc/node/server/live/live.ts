export { Live, LIVE_BRAND }
export type { ClientLive, LiveEvent }

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

  constructor(data: T) {
    this.currentData = data
  }

  get data(): T {
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
