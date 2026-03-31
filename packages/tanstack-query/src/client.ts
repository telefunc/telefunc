export { LiveQueryClient }

import { QueryClient, type QueryClientConfig, type QueryPersister } from '@tanstack/query-core'
import { withContext } from 'telefunc/client'
import { EXTENSION_NAME, type LiveQueryResult } from './shared.js'
import { assignDeep } from './utils/assignDeep.js'

function isLiveQueryResult(v: unknown): v is LiveQueryResult {
  return v !== null && typeof v === 'object' && 'data' in v && 'channel' in v
}

function isPromise(val: unknown): val is Promise<unknown> {
  return typeof val === 'object' && val !== null && 'then' in val && typeof (val as any).then === 'function'
}

function hashQueryKey(queryKey: readonly unknown[]): string {
  return JSON.stringify(queryKey)
}

class LiveQueryClient extends QueryClient {
  #subs = new Map<string, { close: () => Promise<unknown> }>()
  #unsubCache: (() => void) | null = null

  constructor(config?: QueryClientConfig) {
    const isServer = typeof window === 'undefined'

    const userPersister = config?.defaultOptions?.queries?.persister
    const persister: QueryPersister = (queryFn, context, query) => {
      const queryKey = context.queryKey
      const wrappedQueryFn = withContext((() => queryFn(context)) as (...args: unknown[]) => unknown, {
        extensions: { [EXTENSION_NAME]: { queryKey: [...queryKey] } },
      })
      const execute = userPersister
        ? () => userPersister(wrappedQueryFn as typeof queryFn, context, query)
        : wrappedQueryFn
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
    if (!isLiveQueryResult(result)) return result

    const { data, channel } = result
    const hashed = hashQueryKey(queryKey)

    const prev = this.#subs.get(hashed)
    if (prev) prev.close()

    channel.listen(() => {
      this.invalidateQueries({ queryKey })
    })
    this.#subs.set(hashed, { close: () => channel.close() })

    return data
  }

  #teardown(query: { queryKey: readonly unknown[] }) {
    const hashed = hashQueryKey(query.queryKey)
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

  override clear() {
    this.#cleanup()
    super.clear()
  }
}
