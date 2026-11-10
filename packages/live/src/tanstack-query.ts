export { live }

import { hashKey, type QueryClient, type QueryFunctionContext, type QueryKey } from '@tanstack/query-core'
import { withContext } from 'telefunc/client'
import { assertUsage } from './assert.js'
import type { Live } from './live.js'
import { closeSubscription, subscriptionOf, type Subscription } from './wire/shared.js'

/** The `Live` values of one `QueryClient`'s live queries, by `hashKey(queryKey)`, and how to stop watching its cache.
 *  Kept per client, not per `live()` call: `queryFn: live(onGetTodos)` makes a new function on each render. */
type Registry = { values: Map<string, Live<unknown>>; stopWatchingCache: (() => void) | undefined }
const registries = new WeakMap<QueryClient, Registry>()

/**
 * Makes a TanStack Query `queryFn` of a telefunction returning a `Live`: the query refetches when the value goes stale
 * on the server. Its `data` is the value itself.
 *
 * ```ts
 * const { data: todos } = useQuery({ queryKey: ['todos', userId], queryFn: live(onGetTodos, userId) })
 * ```
 *
 * It wraps the `queryFn`, so it works with every TanStack framework adapter (React, Vue, Solid, Svelte).
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
    bind(context.client, context.queryKey, value, subscription)
    return value.data
  }
}

/** Refetches the query when `value` goes stale. A refetch's new value replaces it. */
function bind(client: QueryClient, queryKey: QueryKey, value: Live<unknown>, subscription: Subscription): void {
  const registry = registryOf(client)
  const hash = hashKey(queryKey)
  closeSubscription(registry.values.get(hash))
  registry.values.set(hash, value)
  // `cancelRefetch`: a value going stale while it is being fetched again restarts the fetch.
  subscription.onStale(() => void client.invalidateQueries({ queryKey, exact: true }, { cancelRefetch: true }))
}

function registryOf(client: QueryClient): Registry {
  let registry = registries.get(client)
  if (!registry) {
    registry = { values: new Map(), stopWatchingCache: undefined }
    registries.set(client, registry)
  }
  // A query leaving the cache (garbage-collected, removed) stops being watched; one merely unmounted stays live.
  registry.stopWatchingCache ??= client.getQueryCache().subscribe((event) => {
    if (event.type === 'removed') unbind(registry, event.query.queryKey)
  })
  return registry
}

function unbind(registry: Registry, queryKey: QueryKey): void {
  const hash = hashKey(queryKey)
  closeSubscription(registry.values.get(hash))
  registry.values.delete(hash)
  if (registry.values.size > 0) return
  registry.stopWatchingCache?.()
  registry.stopWatchingCache = undefined
}

/** Marks a query fetched on the server stale once its value is in: the browser then fetches it again on mount, whatever
 *  its `staleTime`, and that fetch opens the channel. TanStack's dehydrated state carries the mark. */
function refetchAfterHydration(client: QueryClient, queryKey: QueryKey): void {
  const cache = client.getQueryCache()
  const query = cache.find({ queryKey, exact: true })
  if (!query) return
  const stop = cache.subscribe((event) => {
    if (event.query !== query || event.type !== 'updated') return
    if (event.action.type === 'success' || event.action.type === 'error') stop()
    if (event.action.type !== 'success') return
    // Streamed hydration may have sent the value already: the browser takes a later state of it only when its
    // `dataUpdatedAt` is newer.
    query.setState({ ...query.state, isInvalidated: true, dataUpdatedAt: query.state.dataUpdatedAt + 1 })
  })
}

/** The rejection TanStack expects from a cancelled fetch. */
function abortError(): Error {
  const err = new Error('The live query fetch was aborted.')
  err.name = 'AbortError'
  return err
}
