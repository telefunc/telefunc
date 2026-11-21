// A real QueryClient runs the adapter; the telefunction and its channel are stand-ins that return a `Live` with a
// subscription, as the wire's client half does.

import { QueryClient, QueryObserver } from '@tanstack/query-core'
import { afterEach, describe, expect, it, vi } from 'vitest'
import type { Live } from './live.js'
import { live } from './tanstack-query.js'
import { SUBSCRIPTION } from './wire/shared.js'

/** A telefunction whose n-th call returns `n`, and its values' subscriptions. */
function telefunction() {
  const values: { goStale: () => void; closed: number }[] = []
  const call = async (): Promise<Live<number>> => {
    const value = { goStale: () => {}, closed: 0 }
    values.push(value)
    const subscription = {
      onStale(onStale: () => void) {
        value.goStale = onStale
        return () => {}
      },
      async close() {
        value.closed++
      },
    }
    return { data: values.length, [SUBSCRIPTION]: subscription } as Live<number>
  }
  return { call, values }
}

let client: QueryClient
afterEach(() => client.clear())

describe('live()', () => {
  it('refetches a value that goes stale while its fetch is still being persisted', async () => {
    client = new QueryClient()
    const { call, values } = telefunction()
    const { promise: saved, resolve: save } = Promise.withResolvers<void>()
    const observer = new QueryObserver(client, {
      queryKey: ['todos'],
      queryFn: live(call),
      staleTime: Number.POSITIVE_INFINITY,
      persister: async (queryFn, context) => {
        const data = await queryFn(context)
        await saved
        return data
      },
    })
    const stop = observer.subscribe(() => {})
    await vi.waitFor(() => expect(values).toHaveLength(1))
    values[0]!.goStale()
    save()
    await vi.waitFor(() => expect(client.getQueryData(['todos'])).toBe(2))
    expect(values).toHaveLength(2)
    stop()
  })

  it('closes the previous value of a query that another copy of the package fetched', async () => {
    client = new QueryClient()
    const { live: liveOfCopy } = await import('./tanstack-query.js?copy')
    const { call, values } = telefunction()
    await client.fetchQuery({ queryKey: ['todos'], queryFn: live(call) })
    await client.fetchQuery({ queryKey: ['todos'], queryFn: liveOfCopy(call) })
    expect(values.map((value) => value.closed)).toEqual([1, 0])
    client.clear()
    expect(values.map((value) => value.closed)).toEqual([1, 1])
  })

  it('refetches a query with its own queryHash when its value goes stale, and closes it as it leaves the cache', async () => {
    client = new QueryClient()
    const { call, values } = telefunction()
    const observer = new QueryObserver(client, {
      queryKey: ['todos'],
      queryHash: 'todos-v1',
      queryFn: live(call),
      staleTime: Number.POSITIVE_INFINITY,
    })
    const stop = observer.subscribe(() => {})
    await vi.waitFor(() => expect(values).toHaveLength(1))
    expect(values[0]!.closed).toBe(0)
    values[0]!.goStale()
    await vi.waitFor(() => expect(observer.getCurrentResult().data).toBe(2))
    expect(values.map((value) => value.closed)).toEqual([1, 0])
    stop()
    client.clear()
    expect(values.map((value) => value.closed)).toEqual([1, 1])
  })

  it("rejects a query with staleTime: 'static', set on its observer or on its client's defaults", async () => {
    const { call, values } = telefunction()
    client = new QueryClient()
    const observer = new QueryObserver(client, {
      queryKey: ['todos'],
      queryFn: live(call),
      staleTime: 'static',
      retry: false,
    })
    const stop = observer.subscribe(() => {})
    await vi.waitFor(() => expect(observer.getCurrentResult().error?.message).toContain('[Wrong Usage]'))
    stop()
    client.clear()
    client = new QueryClient({ defaultOptions: { queries: { staleTime: 'static' } } })
    await expect(client.fetchQuery({ queryKey: ['todos'], queryFn: live(call) })).rejects.toThrow('[Wrong Usage]')
    expect(values).toHaveLength(0)
  })

  it('keys a query by the hash its client makes with its queryKeyHashFn', async () => {
    const queryKeyHashFn = (key: unknown) => JSON.stringify(key, (_, v) => (typeof v === 'bigint' ? String(v) : v))
    client = new QueryClient({ defaultOptions: { queries: { queryKeyHashFn } } })
    let closed = 0
    const subscription = { onStale: () => () => {}, close: async () => void closed++ }
    const call = async () => ({ data: 42, [SUBSCRIPTION]: subscription }) as Live<number>
    expect(await client.fetchQuery({ queryKey: ['todo', 1n], queryFn: live(call) })).toBe(42)
    client.clear()
    expect(closed).toBe(1)
  })
})
