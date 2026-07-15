import { describe, expect, it, vi } from 'vitest'
import { hashKey, type QueryClient } from '@tanstack/query-core'
import { createInvalidationScheduler } from './invalidationScheduler.js'

const tick = () => new Promise<void>((resolve) => setTimeout(resolve, 0))

type Deferred = { promise: Promise<void>; resolve: () => void; reject: (e: unknown) => void }
function makeDeferred(): Deferred {
  let resolve!: () => void
  let reject!: (e: unknown) => void
  const promise = new Promise<void>((res, rej) => {
    resolve = () => res()
    reject = rej
  })
  return { promise, resolve, reject }
}

function makeFakeQueryClient() {
  const listeners: Array<(e: unknown) => void> = []
  const fetchStatus = new Map<string, string>()
  const deferreds: Deferred[] = []
  const invalidateQueries = vi.fn((_filters: unknown, _opts: unknown) => {
    const d = makeDeferred()
    deferreds.push(d)
    return d.promise
  })
  const queryCache = {
    find: ({ queryKey }: { queryKey: readonly unknown[]; exact?: boolean }) => {
      const h = hashKey(queryKey)
      const fs = fetchStatus.get(h)
      return fs ? { queryHash: h, state: { fetchStatus: fs } } : undefined
    },
    subscribe: (cb: (e: unknown) => void) => {
      listeners.push(cb)
      return () => {
        const i = listeners.indexOf(cb)
        if (i >= 0) listeners.splice(i, 1)
      }
    },
  }
  const client = { getQueryCache: () => queryCache, invalidateQueries } as unknown as QueryClient
  return {
    client,
    invalidateQueries,
    deferreds,
    setFetching: (queryKey: readonly unknown[]) => fetchStatus.set(hashKey(queryKey), 'fetching'),
    emitSettled: (queryKey: readonly unknown[]) => {
      const h = hashKey(queryKey)
      fetchStatus.set(h, 'idle')
      listeners
        .slice()
        .forEach((cb) => cb({ type: 'updated', query: { queryHash: h, state: { fetchStatus: 'idle' } } }))
    },
    subscriberCount: () => listeners.length,
  }
}

describe('InvalidationScheduler (§3.C)', () => {
  it('T2.C1 N signals for one key in the same microtask coalesce to ONE invalidateQueries', async () => {
    const q = makeFakeQueryClient()
    const s = createInvalidationScheduler(q.client)
    s.schedule(['todos'])
    s.schedule(['todos'])
    s.schedule(['todos'])
    expect(q.invalidateQueries).not.toHaveBeenCalled()
    await tick()
    expect(q.invalidateQueries).toHaveBeenCalledTimes(1)
  })

  it('T2.C2 burst during an in-flight fetch → exactly one follow-up after it settles', async () => {
    const q = makeFakeQueryClient()
    const s = createInvalidationScheduler(q.client)
    q.setFetching(['todos']) // the query is already fetching
    s.schedule(['todos'])
    s.schedule(['todos'])
    s.schedule(['todos'])
    await tick()
    expect(q.invalidateQueries).not.toHaveBeenCalled() // deferred while fetching
    q.emitSettled(['todos']) // the fetch completes
    await tick()
    expect(q.invalidateQueries).toHaveBeenCalledTimes(1) // exactly one follow-up
  })

  it('T2.C2b a rejected invalidation clears state so the next signal still works (no unhandled rejection)', async () => {
    const q = makeFakeQueryClient()
    const s = createInvalidationScheduler(q.client)
    s.schedule(['todos'])
    await tick()
    expect(q.invalidateQueries).toHaveBeenCalledTimes(1)
    q.deferreds[0]!.reject(new Error('boom')) // the invalidation rejects
    await tick()
    // recovery: a later signal still schedules and fires
    s.schedule(['todos'])
    await tick()
    expect(q.invalidateQueries).toHaveBeenCalledTimes(2)
  })

  it('T2.C3 invalidateQueries is called with exact:true and cancelRefetch:false', async () => {
    const q = makeFakeQueryClient()
    const s = createInvalidationScheduler(q.client)
    s.schedule(['todos'])
    await tick()
    expect(q.invalidateQueries).toHaveBeenCalledWith({ queryKey: ['todos'], exact: true }, { cancelRefetch: false })
  })

  it('T2.C4 signals for different keys schedule independently, keyed correctly', async () => {
    const q = makeFakeQueryClient()
    const s = createInvalidationScheduler(q.client)
    s.schedule(['a'])
    s.schedule(['b'])
    await tick()
    expect(q.invalidateQueries).toHaveBeenCalledTimes(2)
    expect(q.invalidateQueries).toHaveBeenCalledWith({ queryKey: ['a'], exact: true }, { cancelRefetch: false })
    expect(q.invalidateQueries).toHaveBeenCalledWith({ queryKey: ['b'], exact: true }, { cancelRefetch: false })
  })

  it('disposeAll drops the cache subscription (bounded)', async () => {
    const q = makeFakeQueryClient()
    const s = createInvalidationScheduler(q.client)
    q.setFetching(['todos'])
    s.schedule(['todos'])
    await tick() // defers → opens the cache watch
    expect(q.subscriberCount()).toBe(1)
    s.disposeAll()
    expect(q.subscriberCount()).toBe(0)
  })

  it('T2.C-stale-1 schedule then dispose before the microtask → ZERO invalidateQueries', async () => {
    const q = makeFakeQueryClient()
    const s = createInvalidationScheduler(q.client)
    s.schedule(['todos'])
    s.dispose(['todos']) // torn down before the queued microtask runs
    await tick()
    expect(q.invalidateQueries).not.toHaveBeenCalled() // the stale queued work is cancelled
  })

  it('T2.C-stale-2 an old in-flight settle cannot mutate/delete a replacement; the replacement yields exactly one follow-up', async () => {
    const q = makeFakeQueryClient()
    const s = createInvalidationScheduler(q.client)

    // generation 1 goes in-flight (call #1)
    s.schedule(['todos'])
    await tick()
    expect(q.invalidateQueries).toHaveBeenCalledTimes(1)

    // replace: dispose gen1, a fresh signal creates gen2 which goes in-flight (call #2)
    s.dispose(['todos'])
    s.schedule(['todos'])
    await tick()
    expect(q.invalidateQueries).toHaveBeenCalledTimes(2)

    // gen1's stale completion must NOT delete/restart gen2
    q.deferreds[0]!.resolve()
    await tick()

    // a signal arrives while gen2 is still in-flight → it defers, never restarts
    s.schedule(['todos'])
    await tick()
    expect(q.invalidateQueries).toHaveBeenCalledTimes(2)

    // gen2 settles → exactly ONE follow-up (call #3)
    q.deferreds[1]!.resolve()
    await tick()
    expect(q.invalidateQueries).toHaveBeenCalledTimes(3)
  })
})
