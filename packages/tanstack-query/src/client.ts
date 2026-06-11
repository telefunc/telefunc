export { QueryClient }

import {
  QueryClient as BaseQueryClient,
  hashKey,
  type MutationOptions,
  type QueryClientConfig,
  type QueryPersister,
} from '@tanstack/query-core'
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

class QueryClient extends BaseQueryClient {
  #subs = new Map<string, { close: () => Promise<unknown> }>()
  #unsubCache: (() => void) | null = null

  constructor(config?: QueryClientConfig) {
    const isServer = typeof window === 'undefined'

    const userPersister = config?.defaultOptions?.queries?.persister
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
        return result.then((resolved) => this.#handleResult(queryKey, resolved))
      }
      return this.#handleResult(queryKey, result)
    }

    super(isServer ? (config ?? {}) : assignDeep(config ?? {}, { defaultOptions: { queries: { persister } } }))
    if (isServer) return

    this.#unsubCache = this.getQueryCache().subscribe((event) => {
      if (event.type === 'removed') this.#teardown(event.query)
    })
  }

  #handleResult(queryKey: readonly unknown[], result: unknown): unknown {
    if (!isTanstackQueryResult(result)) return result

    const { [__TQ__DATA_KEY]: data, [__TQ__CHANNEL_KEY]: channel } = result
    const hashed = hashKey(queryKey)

    const prev = this.#subs.get(hashed)
    if (prev) prev.close()

    channel.listen(() => {
      this.invalidateQueries({ queryKey })
    })
    this.#subs.set(hashed, { close: () => channel.close() })

    return data
  }

  #teardown(query: { queryKey: readonly unknown[] }) {
    const hashed = hashKey(query.queryKey)
    const sub = this.#subs.get(hashed)
    if (sub) {
      sub.close()
      this.#subs.delete(hashed)
    }
  }

  #cleanup() {
    this.#unsubCache?.()
    this.#unsubCache = null
    for (const sub of this.#subs.values()) {
      sub.close()
    }
    this.#subs.clear()
  }

  override unmount() {
    this.#cleanup()
    super.unmount()
  }

  override defaultMutationOptions<T extends MutationOptions<any, any, any, any>>(options?: T): T {
    const invalidates = options?.meta?.invalidates
    if (!Array.isArray(invalidates)) return super.defaultMutationOptions(options)

    const [globalKeys, localKeys] = partition(invalidates, isGlobalKey)

    const userOnSuccess = options?.onSuccess
    return super.defaultMutationOptions({
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
              for (const key of localKeys) this.invalidateQueries({ queryKey: key })
              return userOnSuccess?.(...args)
            }
          : userOnSuccess,
    } as T)
  }

  override clear() {
    this.#cleanup()
    super.clear()
  }
}
