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
 *  `queryFn: live(onGetTodos)` mints a new function each time. Per-call state would hand every
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
  assertTelefunction(telefunction)
  return async (context) => {
    // TanStack cancels an in-flight fetch by aborting this signal (see `cancelRefetch` below), and the
    // telefunction should hear about it rather than be abandoned mid-request.
    //
    // The signal has to be in scope when the telefunction actually RUNS: the generated stub reads its
    // per-call context (this signal) synchronously at call time. `withContext` sets that context around
    // the call and forwards the args, so `live(onGetTodos, id)` issues `onGetTodos(id)` with the signal
    // attached — no user wrapper closure to thread it through, and nothing to abort after the fact.
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

/**
 * Reject anything that isn't a real telefunction. The value form (`live(onGetTodos, id)`) MUST be the only
 * spelling that works: a wrapper — `live(() => onGetTodos(id))` or, worse, `live(async () => { await x; return
 * onGetTodos(id) })` — silently disconnects cancellation. `withContext` sets the per-call context only for
 * the synchronous window in which the passed function runs; a telefunction the wrapper calls AFTER an await
 * sees no pending context, so the query's abort signal never reaches the request and cancellation quietly
 * does nothing. We catch this up front instead of letting it fail silently at runtime.
 *
 * The check is the generated client stub's `_key` marker (telefunc's client transform stamps it on every
 * telefunction; a plain function has none). This is a RUNTIME guard BY DESIGN: a compile-time brand would
 * require telefunc to brand every telefunction's client stub TYPE — a client-typegen feature that resolves
 * telefunction types from source today — which is out of scope here. So the enforcement is the throw below,
 * not the type.
 */
function assertTelefunction(fn: unknown): void {
  if (typeof fn !== 'function' || typeof (fn as { _key?: unknown })._key !== 'string') {
    throw new Error(
      'live() expects a telefunction — pass it and its arguments directly, e.g. live(onGetTodos, todoListId). ' +
        'It was given a plain function (a wrapper like live(() => onGetTodos(todoListId))?), which detaches the ' +
        "query's cancellation signal from the request: the telefunction call runs outside the signal's context " +
        'window, so cancelling the query would silently do nothing.',
    )
  }
}

/** The `@internal` consumer seam: a re-type, not a conversion. */
function subscriptionOf<T>(handle: Live<T>): LiveSubscription {
  return handle as unknown as LiveSubscription
}

/** The rejection TanStack expects from a cancelled fetch. */
function abortError(): Error {
  const err = new Error('The live query fetch was aborted.')
  err.name = 'AbortError'
  return err
}

/** Bind one handle to its query: an invalidation refetches. Returns the initial value, which becomes
 *  the query's `data`. */
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
