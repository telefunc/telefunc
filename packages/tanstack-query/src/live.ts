export { live }

import { hashKey, type QueryClient, type QueryFunctionContext, type QueryKey } from '@tanstack/query-core'
import { withContextChecked } from 'telefunc/client'
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
 * A synchronous closure works too — `live(() => onGetTodos(todoListId))` — because it reaches the
 * telefunction inside the same per-call context window. What does NOT work is reaching it after an
 * `await` (`live(async () => { await x; return onGetTodos(id) })`): the window has closed by then and
 * the query's cancellation signal would never reach the request. That case throws at fetch time rather
 * than cancelling silently — see the consumption check below.
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
    // telefunction should hear about it rather than be abandoned mid-request.
    //
    // The signal has to be in scope when the telefunction actually RUNS: the generated stub reads its
    // per-call context (this signal) synchronously at call time. `withContextChecked` sets that context
    // around the call, forwards the args, and reports back whether a telefunction actually picked it up
    // — so `live(onGetTodos, id)` and `live(() => onGetTodos(id))` both attach the signal, and anything
    // that reaches the telefunction later (or not at all) is caught here instead of failing silently.
    if (context.signal.aborted) throw abortError() // already cancelled — don't start the request at all
    const { result, consumed } = withContextChecked(telefunction, { signal: context.signal }, ...args)
    if (!consumed) throw contextLostError(result)
    const handle = await result
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

/** Everything callable gets past here; whether it actually reaches a telefunction is decided at fetch
 *  time by the consumption check, which is the question that matters. This only rejects what could never
 *  be called at all — caught up front, since there is nothing to learn by waiting for the first fetch. */
function assertCallable(fn: unknown): void {
  if (typeof fn !== 'function') {
    throw new Error(`live() expects a telefunction, e.g. live(onGetTodos, todoListId). It was given ${describe(fn)}.`)
  }
}

/**
 * The value the function returned is not a telefunction call that took this query's per-call context — so
 * the cancellation signal did not reach the request behind it, and cancelling this query would silently do
 * nothing.
 *
 * Three shapes land here. A function that calls no telefunction at all. A wrapper that reaches its
 * telefunction only AFTER an await (`live(async () => { await x; return onGetTodos(id) })`), by which time
 * the window has closed. And the one a mere consumption test cannot see: a wrapper that calls a
 * telefunction and returns something ELSE — the call it made took the context, but the handle handed back
 * is a different one the signal never reached. Nested checked calls are the same shape, the inner call
 * having taken a context that is not this one.
 *
 * This replaces an earlier `_key` brand check on the passed function. That check asked whether the
 * argument LOOKED like a generated stub, which both rejected the valid synchronous closure and could be
 * satisfied by copying a writable property. Asking instead which context the RETURNED call consumed tests
 * the property the signal's delivery actually depends on, and fails closed on everything it cannot attribute.
 */
function contextLostError(result: unknown): Error {
  // The call is already away and may still resolve to a real handle. We are throwing, so nobody will
  // await it: swallow the rejection (an unhandled one would crash a strict runtime for a mistake we are
  // already reporting) and close it if it turns out to be a Live handle, as the cancellation path does.
  void Promise.resolve(result)
    .then((value) => {
      if (isLiveHandle(value))
        void subscriptionOf(value)
          .close()
          .catch(() => {})
    })
    .catch(() => {})
  return new Error(
    'live() needs a telefunction call, but the function it was given returned something else. ' +
      'Pass the telefunction and its arguments — live(onGetTodos, todoListId) — or RETURN the call from a ' +
      'synchronous wrapper — live(() => onGetTodos(todoListId)). A wrapper that reaches the telefunction only ' +
      'after an await, or that returns a value other than the call it made, hands back something the ' +
      "query's cancellation signal was never attached to — so cancelling the query would silently do nothing.",
  )
}

function isLiveHandle(value: unknown): value is Live<unknown> {
  return typeof (value as { close?: unknown } | null | undefined)?.close === 'function'
}

function describe(value: unknown): string {
  return value === null ? 'null' : `a ${typeof value}`
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
