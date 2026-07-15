export { createInvalidationScheduler }
export type { InvalidationScheduler }

import { hashKey, type QueryClient } from '@tanstack/query-core'

/** Per-key coalescing state. `queued` = a microtask flush is pending; `inFlight` = our own
 *  `invalidateQueries` is running; `dirty` = a signal arrived while we were deferring and a
 *  follow-up is owed once the in-flight fetch settles; `disposed` = this state generation was torn
 *  down (a queued microtask or an in-flight settle captured over it must become a no-op, and it
 *  must never touch a replacement state created for the same key). */
type KeyState = {
  queryKey: readonly unknown[]
  queued: boolean
  inFlight: boolean
  dirty: boolean
  disposed: boolean
}

type InvalidationScheduler = {
  /** Request an invalidation of `queryKey`; coalesced per `hashKey`. */
  schedule: (queryKey: readonly unknown[]) => void
  /** Drop all state for one key (called when its subscription is torn down). */
  dispose: (queryKey: readonly unknown[]) => void
  /** Drop everything (called on unmount). */
  disposeAll: () => void
}

/** Coalesces live invalidations per `hashKey(queryKey)`:
 *  - N signals in the same microtask collapse to ONE `invalidateQueries` (never restarts an
 *    in-flight fetch — `cancelRefetch: false`).
 *  - A signal that arrives while the query is fetching sets `dirty`; exactly ONE follow-up runs
 *    after the in-flight fetch settles.
 *  - A rejected invalidation clears the key's state so the next signal still works, and never
 *    surfaces as an unhandled rejection.
 *  Keys are isolated from one another. */
function createInvalidationScheduler(queryClient: QueryClient): InvalidationScheduler {
  const states = new Map<string, KeyState>()
  // A single lazily-created cache subscription used only to notice when an in-flight fetch we are
  // waiting on has settled, so a deferred follow-up can fire.
  let cacheUnsub: (() => void) | null = null

  function fetchStatusOf(queryKey: readonly unknown[]): string | undefined {
    return queryClient.getQueryCache().find({ queryKey, exact: true })?.state.fetchStatus
  }

  function stateFor(queryKey: readonly unknown[]): KeyState {
    const hash = hashKey(queryKey)
    let s = states.get(hash)
    if (!s) {
      s = { queryKey, queued: false, inFlight: false, dirty: false, disposed: false }
      states.set(hash, s)
    }
    return s
  }

  function ensureCacheWatch(): void {
    if (cacheUnsub) return
    cacheUnsub = queryClient.getQueryCache().subscribe((event) => {
      const query = (event as { query?: { queryHash?: string; state?: { fetchStatus?: string } } }).query
      if (!query?.queryHash || query.state?.fetchStatus === 'fetching') return
      const s = states.get(query.queryHash)
      if (s?.dirty && !s.inFlight && !s.queued) scheduleFlush(s)
    })
  }

  function maybeDropCacheWatch(): void {
    if (!cacheUnsub) return
    for (const s of states.values()) if (s.dirty || s.inFlight || s.queued) return
    cacheUnsub()
    cacheUnsub = null
  }

  function schedule(queryKey: readonly unknown[]): void {
    scheduleFlush(stateFor(queryKey))
  }

  function scheduleFlush(s: KeyState): void {
    if (s.disposed || s.queued) return
    s.queued = true
    queueMicrotask(() => {
      s.queued = false
      if (s.disposed) return // torn down before the microtask ran → no invalidation
      if (s.inFlight || fetchStatusOf(s.queryKey) === 'fetching') {
        // Defer: a fetch is already in flight. Wait for it to settle, then follow up exactly once.
        s.dirty = true
        ensureCacheWatch()
        return
      }
      run(s)
    })
  }

  function run(s: KeyState): void {
    s.dirty = false
    s.inFlight = true
    Promise.resolve(
      queryClient.invalidateQueries({ queryKey: s.queryKey, exact: true }, { cancelRefetch: false }),
    ).then(
      () => settle(s),
      () => settle(s),
    )
  }

  function settle(s: KeyState): void {
    s.inFlight = false
    // A disposed (old-generation) settle must NOT reschedule or delete anything — a replacement
    // state may already own this key's map slot.
    if (s.disposed) {
      maybeDropCacheWatch()
      return
    }
    if (s.dirty) {
      scheduleFlush(s)
      return
    }
    // Identity-safe delete: only drop the slot if it still holds THIS state (never a replacement).
    const hash = hashKey(s.queryKey)
    if (!s.queued && states.get(hash) === s) states.delete(hash)
    maybeDropCacheWatch()
  }

  function dispose(queryKey: readonly unknown[]): void {
    const hash = hashKey(queryKey)
    const s = states.get(hash)
    if (s) {
      s.disposed = true // neutralize any queued microtask / in-flight settle captured over it
      states.delete(hash)
    }
    maybeDropCacheWatch()
  }

  function disposeAll(): void {
    for (const s of states.values()) s.disposed = true
    states.clear()
    if (cacheUnsub) {
      cacheUnsub()
      cacheUnsub = null
    }
  }

  return { schedule, dispose, disposeAll }
}
