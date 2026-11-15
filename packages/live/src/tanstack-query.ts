export { live }

import type { Query, QueryClient, QueryFunctionContext, QueryKey } from '@tanstack/query-core'
import { withContext } from 'telefunc/client'
import { assertUsage } from './assert.js'
import type { Live } from './live.js'
import { processWide } from './processWide.js'
import { Refetcher } from './refetcher.js'
import { closeSubscription, subscriptionOf, type Subscription } from './wire/shared.js'

/** The live queries of one `QueryClient`, by `queryHash` (the cache's own key: a query's `queryHash`, or what its
 *  `queryKeyHashFn` makes), and how to stop watching its cache. Kept per client, not per `live()` call: `queryFn: live(onGetTodos)` makes a new
 *  function on each render. */
type Registry = { queries: Map<string, Watched>; stopWatchingCache: (() => void) | undefined }
/** A query's `Live` (holding it keeps its channel open) and when to refetch it. */
type Watched = { value: Live<unknown> | undefined; refetcher: Refetcher }
const registries = processWide('tanstackRegistries', () => new WeakMap<QueryClient, Registry>())

/**
 * Makes a TanStack Query `queryFn` of a telefunction returning a `Live`: the query refetches when the value goes stale
 * on the server. Its `data` is the value itself.
 *
 * ```ts
 * const { data: todos } = useQuery({ queryKey: ['todos', userId], queryFn: live(onGetTodos, userId) })
 * ```
 *
 * It wraps the `queryFn` and imports only `@tanstack/query-core`, no framework adapter. Its end-to-end tests use React.
 */
function live<T, TArgs extends unknown[]>(
  telefunction: (...args: TArgs) => Promise<Live<T>>,
  ...args: TArgs
): (context: QueryFunctionContext) => Promise<T> {
  assertUsage(
    typeof telefunction === 'function',
    'live() takes a telefunction and its arguments, e.g. live(onGetTodos, userId).',
  )
  return async (context) => {
    if (context.signal.aborted) throw abortError()
    // Lets an aborted fetch cancel the telefunction's request.
    const value = await withContext(telefunction, { signal: context.signal })(...args)
    const subscription = subscriptionOf(value)
    if (!subscription) {
      // The telefunction ran here, on the server (SSR): no channel reaches the browser with the HTML.
      refetchAfterHydration(context.client, context.queryKey)
      return value.data
    }
    if (context.signal.aborted) {
      void subscription.close()
      throw abortError()
    }
    const query = queryOf(context.client, context.queryKey)
    // Left the cache while it fetched: no removal will close it.
    if (!query) void subscription.close()
    else bind(context.client, query, value, subscription)
    return value.data
  }
}

/** Refetches the query when `value` goes stale. A refetch's new value replaces it. */
function bind(client: QueryClient, query: Query, value: Live<unknown>, subscription: Subscription): void {
  const registry = registryOf(client)
  let watched = registry.queries.get(query.queryHash)
  if (!watched) {
    const refetch = async () => {
      // A running fetch (its persister saving, its retries) may hold data read before the value went stale: wait it out.
      await query.promise?.catch(() => {})
      await client.invalidateQueries({ predicate: (cached) => cached === query }, { cancelRefetch: false })
    }
    watched = { value: undefined, refetcher: new Refetcher(refetch) }
    registry.queries.set(query.queryHash, watched)
  }
  closeSubscription(watched.value)
  watched.value = value
  const { refetcher } = watched
  subscription.onStale(() => refetcher.stale())
}

function registryOf(client: QueryClient): Registry {
  let registry = registries.get(client)
  if (!registry) {
    registry = { queries: new Map(), stopWatchingCache: undefined }
    registries.set(client, registry)
  }
  // A query leaving the cache (garbage-collected, removed) stops being watched; one merely unmounted stays live.
  registry.stopWatchingCache ??= client.getQueryCache().subscribe((event) => {
    if (event.type === 'removed') unbind(registry, event.query.queryHash)
  })
  return registry
}

function unbind(registry: Registry, queryHash: string): void {
  const watched = registry.queries.get(queryHash)
  watched?.refetcher.stop()
  closeSubscription(watched?.value)
  registry.queries.delete(queryHash)
  if (registry.queries.size > 0) return
  registry.stopWatchingCache?.()
  registry.stopWatchingCache = undefined
}

/** Marks a query fetched on the server stale once its value is in: the browser then fetches it again on mount, whatever
 *  its `staleTime`, and that fetch opens the channel. TanStack's dehydrated state carries the mark. */
function refetchAfterHydration(client: QueryClient, queryKey: QueryKey): void {
  const query = queryOf(client, queryKey)
  if (!query) return
  const stop = client.getQueryCache().subscribe((event) => {
    if (event.query !== query || event.type !== 'updated') return
    if (event.action.type === 'success' || event.action.type === 'error') stop()
    if (event.action.type !== 'success') return
    // Streamed hydration may have sent the value already: the browser takes a later state of it only when its
    // `dataUpdatedAt` is newer.
    query.setState({ ...query.state, isInvalidated: true, dataUpdatedAt: query.state.dataUpdatedAt + 1 })
  })
}

/** The query a fetch with `queryKey` is for: TanStack passes a query's own key to its `queryFn`, whatever hashes it
 *  (`queryHash`, `queryKeyHashFn`). `undefined` once it left the cache. */
function queryOf(client: QueryClient, queryKey: QueryKey): Query | undefined {
  return client
    .getQueryCache()
    .getAll()
    .find((query) => query.queryKey === queryKey)
}

/** The rejection TanStack expects from a cancelled fetch. */
function abortError(): Error {
  const err = new Error('The live query fetch was aborted.')
  err.name = 'AbortError'
  return err
}
