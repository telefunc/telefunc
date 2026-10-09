export { live }

import { hashKey, type QueryClient, type QueryFunctionContext, type QueryKey } from '@tanstack/query-core'
import { withContext } from 'telefunc/client'
import type { Live, LiveSubscription } from '../primitive/live.js'
import { installLiveReviver } from '../primitive/wireClient.js'

// THE CLIENT ADAPTER, shipped as this package's `./tanstack-query` subpath rather than as its own package
// — abandoning the feature should be one deletion, not two.
//
// Importing this module REGISTERS the Live reviver with telefunc's client (a public extension seam). Doing
// it at import time is what guarantees the ordering the protocol needs: a response carrying a Live can only
// arrive from a `queryFn` built here, and building one requires this module to have been imported.
installLiveReviver()

/** The live subscriptions owned by one `QueryClient`, keyed by `hashKey(queryKey)`, plus the single cache
 *  watcher that tears them down.
 *
 *  Module-level (not closed over by `live()`) because `live()` is called INLINE on every render:
 *  `queryFn: live(onGetTodos)` mints a new function each time. Per-call state would hand every render a
 *  fresh empty map — losing the previous handle instead of closing it, and adding one more cache watcher
 *  per render. Keyed weakly so a discarded QueryClient takes its registry with it. */
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
 * Pass the telefunction and its arguments — `live(onGetTodos, todoListId)` — as an ordinary `useQuery`
 * `queryFn`, and the query stays current on its own: when the server signals the result is stale, the
 * wrapper invalidates and refetches. It does not push rows into the cache — staleness drives a refetch.
 *
 * ```ts
 * const { data: todos } = useQuery({
 *   queryKey: ['todos', todoListId],
 *   queryFn: live(onGetTodos, todoListId), // onGetTodos(todoListId) returns a Live<Todo[]>
 * })
 * ```
 *
 * `data` is the value itself (`Todo[]`), not the handle — the wrapper unwraps it.
 *
 * This wraps the `queryFn`, not the hook, so it works unchanged with the React, Vue, Solid and Svelte
 * TanStack adapters. There is nothing else to install or configure.
 */
function live<T, TArgs extends unknown[]>(
  telefunction: (...args: TArgs) => Live<T> | Promise<Live<T>>,
  ...args: TArgs
): (context: QueryFunctionContext) => Promise<T> {
  assertCallable(telefunction)
  return async (context) => {
    // TanStack cancels an in-flight fetch by aborting this signal (see `cancelRefetch` below), and the
    // telefunction should hear about it rather than be abandoned mid-request. The generated stub reads its
    // per-call context synchronously at call time, so the signal has to be in scope when the telefunction
    // actually RUNS — which `withContext` guarantees by calling it for us.
    if (context.signal.aborted) throw abortError() // already cancelled — don't start the request at all
    const handle = await withContext(telefunction, { signal: context.signal })(...args)
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

/** Rejects what could never be called at all, up front — there is nothing to learn by waiting for the
 *  first fetch. Everything else is decided by the call itself. */
function assertCallable(fn: unknown): void {
  if (typeof fn !== 'function') {
    throw new Error(`live() expects a telefunction, e.g. live(onGetTodos, todoListId). It was given ${describe(fn)}.`)
  }
}

function describe(value: unknown): string {
  return value === null ? 'null' : `a ${typeof value}`
}

/** Whether a Live carries the consumer seam. A Live REVIVED from the wire has the invalidation tap and the
 *  channel lifecycle; the public `Live<T>` deliberately does not advertise them, so this is where the
 *  adapter's assumption about what it was handed becomes checkable rather than asserted.
 *
 *  A predicate rather than a cast, and it lives HERE rather than beside `LiveSubscription`: that type is
 *  imported type-only on purpose — this adapter ships to the browser, and a VALUE import from a server
 *  module would drag the server graph into a client bundle. A guard defined locally erases that risk. */
function carriesSubscription<T>(handle: Live<T>): handle is Live<T> & LiveSubscription {
  const candidate = handle as Partial<LiveSubscription>
  return typeof candidate.onInvalidate === 'function' && typeof candidate.close === 'function'
}

/** The consumer seam. Narrowed, not cast: a Live that did not come from a telefunction has no tap to bind,
 *  and saying so here names the mistake instead of failing later inside TanStack with a missing method. */
function subscriptionOf<T>(handle: Live<T>): LiveSubscription {
  if (!carriesSubscription(handle)) {
    throw new Error(
      'live() received a Live that did not come from a telefunction. Only a Live returned across the wire carries the invalidation subscription this adapter binds to.',
    )
  }
  return handle
}

/** The rejection TanStack expects from a cancelled fetch. */
function abortError(): Error {
  const err = new Error('The live query fetch was aborted.')
  err.name = 'AbortError'
  return err
}

/** Bind one handle to its query: an invalidation refetches. Returns the initial value, which becomes the
 *  query's `data`. */
function wire<T>(client: QueryClient, queryKey: QueryKey, handle: Live<T>): T {
  const registry = registryFor(client)
  const hash = hashKey(queryKey)
  const subscription = subscriptionOf(handle)
  // Replace-on-resubscribe: a refetch mints a new handle — close the previous one for this key.
  const previous = registry.subs.get(hash)
  if (previous) void previous.close().catch(() => {})
  ensureCacheWatch(client, registry)
  // Invalidate directly: TanStack owns fetch behavior and invalidation is idempotent, so no per-key
  // coalescing is needed. `cancelRefetch: true` matters — an invalidation landing DURING an in-flight
  // fetch must cancel and restart it. With `false`, the stale fetch would run to completion and clear
  // `isInvalidated` with no follow-up, silently swallowing the invalidation.
  const offInvalidate = subscription.onInvalidate(() => {
    void client.invalidateQueries({ queryKey, exact: true }, { cancelRefetch: true }).catch(() => {})
  })
  let closed = false
  registry.subs.set(hash, {
    close: () => {
      if (closed) return Promise.resolve()
      closed = true
      offInvalidate()
      return subscription.close()
    },
  })
  return handle.data
}

function ensureCacheWatch(client: QueryClient, registry: Registry): void {
  if (registry.cacheUnsub) return
  // Tear a subscription down when its query LEAVES the cache (GC, removeQueries, clear). A query that
  // merely loses its observers on unmount STAYS wired — otherwise a `staleTime: Infinity` query would go
  // dead on remount, since nothing refetches to re-establish the channel.
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
