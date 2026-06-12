export { withTelefunc }

import { hashKey, type MutationOptions, type QueryClient, type QueryPersister } from '@tanstack/query-core'
import { withContext } from 'telefunc/client'
import {
  EXTENSION_NAME,
  __TQ__DATA_KEY,
  __TQ__CHANNEL_KEY,
  isGlobalKey,
  type TanstackQueryExtensionData,
  type TanstackQueryResult,
} from './shared.js'
import { assignDeep } from './utils/assignDeep.js'
import { partition } from './utils/partition.js'

function isTanstackQueryResult(v: unknown): v is TanstackQueryResult {
  return v !== null && typeof v === 'object' && __TQ__DATA_KEY in v && __TQ__CHANNEL_KEY in v
}

function isPromise(val: unknown): val is Promise<unknown> {
  return typeof val === 'object' && val !== null && 'then' in val && typeof val.then === 'function'
}

/** Wire Telefunc invalidation into an existing `QueryClient`.
 *
 *  ```ts
 *  import { QueryClient } from '@tanstack/react-query'
 *  import { withTelefunc } from '@telefunc/tanstack-query'
 *  const queryClient = withTelefunc(new QueryClient())
 *  ```
 */
function withTelefunc<T extends QueryClient>(queryClient: T): T {
  const isServer = typeof window === 'undefined'
  if (isServer) return queryClient

  const subs = new Map<string, { close: () => Promise<unknown> }>()

  function handleResult(queryKey: readonly unknown[], result: unknown): unknown {
    if (!isTanstackQueryResult(result)) return result

    const { [__TQ__DATA_KEY]: data, [__TQ__CHANNEL_KEY]: channel } = result
    const hashed = hashKey(queryKey)

    const prev = subs.get(hashed)
    if (prev) prev.close()

    channel.listen(() => {
      queryClient.invalidateQueries({ queryKey })
    })
    subs.set(hashed, { close: () => channel.close() })

    return data
  }

  function teardown(query: { queryKey: readonly unknown[] }) {
    const hashed = hashKey(query.queryKey)
    const sub = subs.get(hashed)
    if (sub) {
      sub.close()
      subs.delete(hashed)
    }
  }

  function cleanup() {
    unsubCache?.()
    unsubCache = null
    for (const sub of subs.values()) {
      sub.close()
    }
    subs.clear()
  }

  const defaultOptions = queryClient.getDefaultOptions()
  const userPersister = defaultOptions.queries?.persister
  const persister: QueryPersister = (queryFn, context, query) => {
    const queryKey = context.queryKey
    const wrappedQueryFn = isGlobalKey(queryKey)
      ? withContext(() => queryFn(context), {
          extensions: { [EXTENSION_NAME]: { queryKey } satisfies TanstackQueryExtensionData },
        })
      : () => queryFn(context)
    const execute = userPersister ? () => userPersister(wrappedQueryFn, context, query) : wrappedQueryFn
    const result = execute()
    if (isPromise(result)) {
      return result.then((resolved) => handleResult(queryKey, resolved))
    }
    return handleResult(queryKey, result)
  }
  queryClient.setDefaultOptions(assignDeep(defaultOptions, { queries: { persister } }))

  let unsubCache: (() => void) | null = queryClient.getQueryCache().subscribe((event) => {
    if (event.type === 'removed') teardown(event.query)
  })

  const defaultMutationOptionsOriginal = queryClient.defaultMutationOptions.bind(queryClient)
  queryClient.defaultMutationOptions = <O extends MutationOptions<any, any, any, any>>(options?: O): O => {
    const invalidates = options?.meta?.invalidates
    if (!Array.isArray(invalidates)) return defaultMutationOptionsOriginal(options)

    const [globalKeys, localKeys] = partition(invalidates, isGlobalKey)

    const userOnSuccess = options?.onSuccess
    return defaultMutationOptionsOriginal({
      ...options,
      mutationFn:
        globalKeys.length > 0 && options?.mutationFn
          ? withContext(options.mutationFn, {
              extensions: { [EXTENSION_NAME]: { invalidates: globalKeys } satisfies TanstackQueryExtensionData },
            })
          : options?.mutationFn,
      onSuccess:
        localKeys.length > 0
          ? (...args) => {
              for (const key of localKeys) queryClient.invalidateQueries({ queryKey: key })
              return userOnSuccess?.(...args)
            }
          : userOnSuccess,
    } as O)
  }

  const unmountOriginal = queryClient.unmount.bind(queryClient)
  queryClient.unmount = () => {
    cleanup()
    unmountOriginal()
  }

  const clearOriginal = queryClient.clear.bind(queryClient)
  queryClient.clear = () => {
    cleanup()
    clearOriginal()
  }

  return queryClient
}
