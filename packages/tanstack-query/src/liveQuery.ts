export { createLiveQuery }
export type { LiveQueryOptions }

import { hashKey, type QueryClient, type QueryKey, type QueryObserverOptions } from '@tanstack/query-core'
import type { Live } from 'telefunc'
// TYPE-ONLY (erased at build): `telefunc/__internal` is a server entry with no browser condition, so a
// runtime import would pull the server context/tagHub graph into this browser bundle. The adapter needs
// the subscription taps, which the public `Live<T>` deliberately doesn't advertise.
import type { LiveSubscription } from 'telefunc/__internal'

/** The full TanStack query-options seam (staleTime, gcTime, refetchOnMount, …) with ONE override: the
 *  user's `queryFn` returns a `Live<T>`, which the adapter unwraps to `T`. Every other option is
 *  forwarded verbatim, so `liveQuery({ queryKey, queryFn, staleTime: Infinity })` type-checks inline.
 *  Framework-agnostic — `QueryObserverOptions` is query-core's base for every framework's `useQuery`. */
type LiveQueryOptions<TData = unknown, TError = Error, TQueryKey extends QueryKey = QueryKey> = Omit<
  QueryObserverOptions<TData, TError, TData, TData, TQueryKey>,
  'queryFn'
> & {
  queryFn: () => Live<TData> | Promise<Live<TData>>
}

/** The RETURNED options: every forwarded TanStack option, with `queryFn` pinned to the PRECISE unwrapped
 *  thunk `() => Promise<TData>` — NOT QueryObserverOptions' `skipToken | QueryFunction<TData>` union — so
 *  callers statically see the honest re-typed data function (§3.F2). Still assignable to `useQuery`. */
type LiveQueryResult<TData, TError, TQueryKey extends QueryKey> = Omit<
  QueryObserverOptions<TData, TError, TData, TData, TQueryKey>,
  'queryFn'
> & { queryFn: () => Promise<TData> }

/** Wire a `QueryClient` for live queries and return the `liveQuery` options adapter.
 *
 *  Framework-agnostic (works with `useQuery` across the React/Solid/Vue TanStack adapters): the input
 *  `queryFn` returns `Promise<Live<T>>`, and the adapter re-types the surfaced `data` to `T` (TanStack
 *  infers `data` from `queryFn`, so this owned seam is the only honest re-typing point).
 *
 *  The `QueryClient` is supplied here rather than read from `QueryFunctionContext.client` (which is not
 *  guaranteed at the declared `@tanstack/query-core >=5.0.0` floor). Beyond the public `.data`, the
 *  adapter binds the taps through telefunc's `@internal` `LiveSubscription` seam — the sanctioned
 *  adapter surface, kept off the public `Live<T>`. */
function createLiveQuery(queryClient: QueryClient) {
  // Per-query live subscription (the Live handle + its teardown), keyed by `hashKey`.
  const subs = new Map<string, { close: () => Promise<void> }>()
  let cacheUnsub: (() => void) | null = null

  function ensureCacheWatch(): void {
    if (cacheUnsub) return
    // Tear a subscription down when its query LEAVES the cache (GC, removeQueries, clear). A query that
    // merely loses its observers on unmount STAYS wired — otherwise a `staleTime: Infinity` query would
    // go dead on remount, since nothing refetches to re-establish the channel.
    cacheUnsub = queryClient.getQueryCache().subscribe((event) => {
      if (event.type === 'removed') teardown(event.query.queryKey)
    })
  }

  function teardown(queryKey: readonly unknown[]): void {
    const hash = hashKey(queryKey)
    const sub = subs.get(hash)
    if (!sub) return
    subs.delete(hash)
    void sub.close().catch(() => {})
    if (subs.size === 0 && cacheUnsub) {
      cacheUnsub()
      cacheUnsub = null
    }
  }

  function wire<T>(queryKey: readonly unknown[], live: Live<T>): T {
    const hash = hashKey(queryKey)
    // The @internal consumer seam: every Live telefunc revives carries these taps; the public `Live<T>`
    // just doesn't advertise them (a user reads `.data`; binding the taps is an adapter's job).
    const subscription = live as unknown as LiveSubscription<T>
    // Replace-on-resubscribe: a refetch mints a new Live — close the previous one for this key.
    const previous = subs.get(hash)
    if (previous) void previous.close().catch(() => {})
    ensureCacheWatch()
    // Direct refetch on invalidation. TanStack owns fetch behavior and client-side invalidation is
    // idempotent, so no per-key coalescing scheduler is needed. `cancelRefetch: true` (TanStack's default
    // cancel + restart) ensures an invalidation that lands DURING an in-flight fetch cancels that stale
    // fetch and refetches — `cancelRefetch: false` would instead let the in-flight fetch complete and clear
    // `isInvalidated` with no follow-up, swallowing the mid-fetch invalidation. A rejected refetch is
    // swallowed so it can't surface as an unhandled rejection.
    const offInvalidate = subscription.onInvalidate(() => {
      void queryClient.invalidateQueries({ queryKey, exact: true }, { cancelRefetch: true }).catch(() => {})
    })
    const offData = subscription.onData((data) => queryClient.setQueryData(queryKey, data)) // direct cache write
    let closed = false
    subs.set(hash, {
      close: () => {
        if (closed) return Promise.resolve()
        closed = true
        offInvalidate()
        offData()
        return subscription.close()
      },
    })
    return live.data
  }

  // Forward every TanStack option (the §3.F rest seam) and re-type only `queryFn`: TanStack infers
  // `data` from `queryFn`, so unwrapping `Live<T>` → `T` here is the single honest re-typing point.
  return function liveQuery<TData, TError = Error, TQueryKey extends QueryKey = QueryKey>(
    options: LiveQueryOptions<TData, TError, TQueryKey>,
  ): LiveQueryResult<TData, TError, TQueryKey> {
    const { queryFn, queryKey, ...rest } = options
    const key = (queryKey ?? []) as TQueryKey
    return {
      ...rest,
      queryKey,
      queryFn: async () => wire(key, await queryFn()),
    } as LiveQueryResult<TData, TError, TQueryKey>
  }
}
