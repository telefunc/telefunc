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
})
