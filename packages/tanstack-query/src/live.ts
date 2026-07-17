export { live }

import { hashKey, type QueryClient, type QueryFunctionContext, type QueryKey } from '@tanstack/query-core'
import { withContext } from 'telefunc/client'
import type { Live } from 'telefunc'
// TYPE-ONLY (erased at build): `telefunc/__internal` is a server entry with no browser condition, so a
// runtime import would pull the server context/tagHub graph into this browser bundle. The taps already
// exist on the revived handle at runtime — only their type is missing, because the public `Live<T>`
// deliberately doesn't advertise them.
import type { LiveSubscription } from 'telefunc/__internal'

/** The live subscriptions owned by one `QueryClient`, keyed by `hashKey(queryKey)`, plus the single
 *  cache watcher that tears them down.
 *
 *  Module-level (not closed over by `live()`) because `live()` is called INLINE on every render:
 *  `queryFn: live(() => onGetTodos())` mints a new function each time. Per-call state would hand every
 *  render a fresh empty map — losing the previous handle instead of closing it, and adding one more
 *  cache watcher per render. Keyed weakly so a discarded QueryClient takes its registry with it. */
type Registry = {
  subs: Map<string, { close: () => Promise<void> }>
  cacheUnsub: (() => void) | null
}
const registries = new WeakMap<QueryClient, Registry>()

function registryFor(client: QueryClient): Registry {
  let registry = registries.get(client)
  if (!registry) {
    registry = { subs: new Map(), cacheUnsub: null }
    registries.set(client, registry)
  }
  return registry
}

/**
 * Make a live query out of a telefunction call.
 *
 * Wrap the `queryFn` of an ordinary `useQuery` and the query stays up to date on its own: the server
 * pushes new data straight into the cache, and signals staleness by refetching.
 *
 * ```ts
 * const { data: todos } = useQuery({
 *   queryKey: ['todos'],
 *   queryFn: live(() => onGetTodos()), // onGetTodos() returns a Live<Todo[]>
 * })
 * ```
 *
 * `data` is the value itself (`Todo[]`), not the handle — the wrapper unwraps it.
 *
 * This wraps the `queryFn`, not the hook, so it works unchanged with the React, Vue, Solid and Svelte
 * TanStack adapters. There is nothing else to install or configure.
 */
function live<T>(queryFn: () => Live<T> | Promise<Live<T>>): (context: QueryFunctionContext) => Promise<T> {
  return async (context) => {
    // TanStack cancels an in-flight fetch by aborting this signal (see `cancelRefetch` below), and the
    // telefunction should hear about it rather than be abandoned mid-request.
    //
    // The signal goes in when the call is CREATED, not after. Reaching for the returned promise and
    // aborting it afterwards cannot work here: `queryFn` is the user's own closure, so what comes back
    // is usually their wrapper — `live(async () => onGetTodos())` yields the async function's promise,
    // not the telefunction's — and the abort would silently target the wrong object. `withContext`
    // exists for exactly this: it hands the context to whatever call happens inside it.
    if (context.signal.aborted) throw abortError() // already cancelled — don't start the request at all
    const handle = await withContext(queryFn, { signal: context.signal })()
    if (context.signal.aborted) {
      // Cancelled while in flight: the handle still arrived, so close it or its channel leaks.
      void subscriptionOf(handle)
        .close()
        .catch(() => {})
      throw abortError()
    }
    return wire(context.client, context.queryKey, handle)
  }
}

/** The `@internal` consumer seam: a re-type, not a conversion. */
function subscriptionOf<T>(handle: Live<T>): LiveSubscription<T> {
  return handle as unknown as LiveSubscription<T>
}

/** The rejection TanStack expects from a cancelled fetch. */
function abortError(): Error {
  const err = new Error('The live query fetch was aborted.')
  err.name = 'AbortError'
  return err
}

/** Bind one handle to its query: invalidation refetches, a data push writes the cache. Returns the
 *  initial value, which becomes the query's `data`. */
function wire<T>(client: QueryClient, queryKey: QueryKey, handle: Live<T>): T {
  const registry = registryFor(client)
  const hash = hashKey(queryKey)
  const subscription = subscriptionOf(handle)
  // Replace-on-resubscribe: a refetch mints a new handle — close the previous one for this key.
  const previous = registry.subs.get(hash)
  if (previous) void previous.close().catch(() => {})
  ensureCacheWatch(client, registry)
  // Invalidate directly: TanStack owns fetch behavior and invalidation is idempotent, so no
  // per-key coalescing is needed. `cancelRefetch: true` matters — an invalidation landing DURING an
  // in-flight fetch must cancel and restart it. With `false`, the stale fetch would run to completion
  // and clear `isInvalidated` with no follow-up, silently swallowing the invalidation.
  const offInvalidate = subscription.onInvalidate(() => {
    void client.invalidateQueries({ queryKey, exact: true }, { cancelRefetch: true }).catch(() => {})
  })
  const offData = subscription.onData((data) => client.setQueryData(queryKey, data)) // direct cache write
  let closed = false
  registry.subs.set(hash, {
    close: () => {
      if (closed) return Promise.resolve()
      closed = true
      offInvalidate()
      offData()
      return subscription.close()
    },
  })
  return handle.data
}

function ensureCacheWatch(client: QueryClient, registry: Registry): void {
  if (registry.cacheUnsub) return
  // Tear a subscription down when its query LEAVES the cache (GC, removeQueries, clear). A query that
  // merely loses its observers on unmount STAYS wired — otherwise a `staleTime: Infinity` query would
  // go dead on remount, since nothing refetches to re-establish the channel.
  registry.cacheUnsub = client.getQueryCache().subscribe((event) => {
    if (event.type === 'removed') teardown(registry, event.query.queryKey)
  })
}

function teardown(registry: Registry, queryKey: QueryKey): void {
  const hash = hashKey(queryKey)
  const sub = registry.subs.get(hash)
  if (!sub) return
  registry.subs.delete(hash)
  void sub.close().catch(() => {})
  if (registry.subs.size === 0 && registry.cacheUnsub) {
    registry.cacheUnsub()
    registry.cacheUnsub = null
  }
}
