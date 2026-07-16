export { withTelefunc }
export { createLiveQuery } from './liveQuery.js'
export type { LiveQueryOptions } from './liveQuery.js'

import { hashKey, type MutationOptions, type QueryClient, type QueryPersister } from '@tanstack/query-core'
import { __TQ__CHANNEL_KEY, __TQ__DATA_KEY, isWrapper } from './shared.js'
import { assignDeep } from './utils/assignDeep.js'
import { createInvalidationScheduler } from './client/invalidationScheduler.js'

const WITH_TELEFUNC_MARK = Symbol.for('telefunc.tanstack-query.wired')

function isPromise(value: unknown): value is Promise<unknown> {
  return typeof value === 'object' && value !== null && 'then' in value && typeof value.then === 'function'
}

function isDev(): boolean {
  return typeof process !== 'undefined' && process.env?.NODE_ENV !== 'production'
}

/** Wire Telefunc live invalidation into an existing `QueryClient`. Liveness is server-owned: the
 *  client sends no live bytes; it only unwraps the server's live-query wrapper and refetches when
 *  the server's channel signals.
 *
 *  ```ts
 *  import { QueryClient } from '@tanstack/react-query'
 *  import { withTelefunc } from '@telefunc/tanstack-query'
 *  const queryClient = withTelefunc(new QueryClient())
 *  ```
 */
function withTelefunc<T extends QueryClient>(queryClient: T): T {
  if (typeof window === 'undefined') return queryClient // server: no-op
  // Idempotent: a second call must not stack persisters, listeners, method wrappers, or closes.
  const marked = queryClient as T & { [WITH_TELEFUNC_MARK]?: true }
  if (marked[WITH_TELEFUNC_MARK]) return queryClient
  marked[WITH_TELEFUNC_MARK] = true

  const scheduler = createInvalidationScheduler(queryClient)
  const subs = new Map<string, { close: () => Promise<unknown> }>()

  // Observe (and swallow) an async close rejection so it never becomes an unhandled rejection.
  function observe(closing: unknown): void {
    if (isPromise(closing)) closing.catch(() => {})
  }

  function handleResult(queryKey: readonly unknown[], result: unknown): unknown {
    if (!isWrapper(result)) {
      if (isDev()) warnIfNestedWrapper(result)
      return result
    }
    const data = result[__TQ__DATA_KEY]
    const channel = result[__TQ__CHANNEL_KEY]
    const hashed = hashKey(queryKey)

    // Replace-on-resubscribe: close the previous channel for this key before storing the new one.
    const prev = subs.get(hashed)
    if (prev) observe(prev.close())

    const unlisten = channel.listen(() => scheduler.schedule(queryKey))
    let closed = false
    subs.set(hashed, {
      close: () => {
        if (closed) return Promise.resolve()
        closed = true
        unlisten?.()
        scheduler.dispose(queryKey)
        return Promise.resolve(channel.close())
      },
    })
    return data
  }

  function teardown(queryKey: readonly unknown[]): void {
    const hashed = hashKey(queryKey)
    const sub = subs.get(hashed)
    if (!sub) return
    subs.delete(hashed)
    observe(sub.close())
  }

  // Tear down every current subscription. Does NOT drop the cache subscription, so the client stays
  // usable after `clear()`.
  function closeAllSubs(): void {
    for (const sub of subs.values()) observe(sub.close())
    subs.clear()
  }

  // --- Persister: unwrap the server wrapper; send no live bytes ---
  const defaultOptions = queryClient.getDefaultOptions()
  const userPersister = defaultOptions.queries?.persister
  const persister: QueryPersister = (queryFn, context, query) => {
    const queryKey = context.queryKey
    const call = () => queryFn(context)
    const execute = userPersister ? () => userPersister(call, context, query) : call
    const result = execute()
    return isPromise(result)
      ? result.then((resolved) => handleResult(queryKey, resolved))
      : handleResult(queryKey, result)
  }
  queryClient.setDefaultOptions(assignDeep(defaultOptions, { queries: { persister } }))

  const unsubCache = queryClient.getQueryCache().subscribe((event) => {
    if (event.type === 'removed') teardown(event.query.queryKey)
  })

  // --- Mutations: `meta.invalidates` is client-local only; the user's `onSuccess` is preserved ---
  const defaultMutationOptionsOriginal = queryClient.defaultMutationOptions.bind(queryClient)
  queryClient.defaultMutationOptions = <O extends MutationOptions<any, any, any, any>>(options?: O): O => {
    const invalidates = options?.meta?.invalidates
    if (!Array.isArray(invalidates) || invalidates.length === 0) return defaultMutationOptionsOriginal(options)

    const userOnSuccess = options?.onSuccess
    return defaultMutationOptionsOriginal({
      ...options,
      onSuccess: (...args: unknown[]) => {
        for (const key of invalidates as readonly (readonly unknown[])[]) {
          queryClient.invalidateQueries({ queryKey: key })
        }
        return (userOnSuccess as ((...a: unknown[]) => unknown) | undefined)?.(...args)
      },
    } as O)
  }

  const unmountOriginal = queryClient.unmount.bind(queryClient)
  queryClient.unmount = () => {
    closeAllSubs()
    unsubCache()
    scheduler.disposeAll()
    unmountOriginal()
  }

  const clearOriginal = queryClient.clear.bind(queryClient)
  queryClient.clear = () => {
    closeAllSubs()
    clearOriginal()
  }

  return queryClient
}

/** Dev-only rule-2 diagnostic: warn if a live-query wrapper is buried inside a transformed result.
 *  Depth-bounded and cycle-safe — never recurses arbitrarily through the result graph. */
function warnIfNestedWrapper(result: unknown): void {
  const MAX_DEPTH = 4
  const seen = new Set<object>()
  const found = (function scan(value: unknown, depth: number): boolean {
    if (depth > MAX_DEPTH || typeof value !== 'object' || value === null) return false
    if (seen.has(value)) return false
    seen.add(value)
    if (isWrapper(value)) return true
    for (const key of Object.keys(value)) {
      if (scan((value as Record<string, unknown>)[key], depth + 1)) return true
    }
    return false
  })(result, 0)
  if (found) {
    console.warn(
      '[telefunc:tanstack-query] A live-query wrapper was found nested inside a transformed result. ' +
        'Do not transform the result of a live telefunction on the client — return it unchanged so ' +
        'liveness can be wired (rule 2).',
    )
  }
}
