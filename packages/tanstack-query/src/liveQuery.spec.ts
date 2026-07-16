import { describe, expect, it, vi } from 'vitest'
import { QueryClient } from '@tanstack/query-core'
import type { ClientLive } from 'telefunc'
import { createLiveQuery } from './liveQuery.js'

const tick = () => new Promise<void>((resolve) => queueMicrotask(() => resolve()))

function makeFakeClientLive<T>(initial: T) {
  const invalidateCbs: Array<() => void> = []
  const dataCbs: Array<(data: T) => void> = []
  let closed = false
  const close = vi.fn(() => {
    closed = true
    return Promise.resolve()
  })
  const live: ClientLive<T> = {
    data: initial,
    onData: (cb) => {
      dataCbs.push(cb)
      return () => {}
    },
    onInvalidate: (cb) => {
      invalidateCbs.push(cb)
      return () => {}
    },
    onClose: () => {},
    close,
    get isClosed() {
      return closed
    },
  }
  return {
    live,
    close,
    fireInvalidate: () => invalidateCbs.forEach((cb) => cb()),
    fireData: (data: T) => dataCbs.forEach((cb) => cb(data)),
  }
}

describe('liveQuery — TanStack adapter over ClientLive (§3.F)', () => {
  it('T12.F2 the surfaced data re-types to T (queryFn returns Promise<T>, not Promise<ClientLive<T>>)', () => {
    const liveQuery = createLiveQuery(new QueryClient())
    const options = liveQuery({
      queryKey: ['n'],
      queryFn: async (): Promise<ClientLive<number>> => makeFakeClientLive(1).live,
    })
    // Compile-time proof (§3.F2): `queryFn` is PRECISELY `() => Promise<number>` (not QueryObserverOptions'
    // `skipToken | QueryFunction` union), so TanStack infers `data: number`, not `ClientLive<number>`.
    const check: () => Promise<number> = options.queryFn
    expect(typeof check).toBe('function')
  })

  it('T12.F2 forwards the full TanStack options seam inline (§3.F rest) — staleTime/gcTime accepted', () => {
    const liveQuery = createLiveQuery(new QueryClient())
    // Compile-time proof: extra options are accepted inline (no excess-property error) and forwarded —
    // this is exactly what a `staleTime: Infinity` live query needs (rely on invalidation, not polling).
    const options = liveQuery({
      queryKey: ['n'],
      queryFn: async (): Promise<ClientLive<number>> => makeFakeClientLive(1).live,
      staleTime: Infinity,
      gcTime: 5000,
      refetchOnMount: false,
    })
    expect(options.staleTime).toBe(Infinity)
    expect(options.gcTime).toBe(5000)
    expect(options.refetchOnMount).toBe(false)
  })

  it('T12.F3 unwraps .data into the cache; onInvalidate → one coalesced invalidateQueries', async () => {
    const queryClient = new QueryClient()
    const liveQuery = createLiveQuery(queryClient)
    const fake = makeFakeClientLive('v1')
    const data = await queryClient.fetchQuery(liveQuery({ queryKey: ['todos'], queryFn: async () => fake.live }))
    expect(data).toBe('v1') // .data unwrapped into the cache
    expect(queryClient.getQueryData(['todos'])).toBe('v1')

    const invalidateSpy = vi.spyOn(queryClient, 'invalidateQueries')
    fake.fireInvalidate()
    fake.fireInvalidate() // coalesced
    await tick()
    expect(invalidateSpy).toHaveBeenCalledTimes(1)
    expect(invalidateSpy).toHaveBeenCalledWith({ queryKey: ['todos'], exact: true }, { cancelRefetch: false })
  })

  it('T12.F3 onData writes the cache directly and does NOT refetch', async () => {
    const queryClient = new QueryClient()
    const liveQuery = createLiveQuery(queryClient)
    const fake = makeFakeClientLive('v1')
    await queryClient.fetchQuery(liveQuery({ queryKey: ['todos'], queryFn: async () => fake.live }))

    const setDataSpy = vi.spyOn(queryClient, 'setQueryData')
    const invalidateSpy = vi.spyOn(queryClient, 'invalidateQueries')
    fake.fireData('v2')
    expect(setDataSpy).toHaveBeenCalledWith(['todos'], 'v2')
    expect(queryClient.getQueryData(['todos'])).toBe('v2')
    await tick()
    expect(invalidateSpy).not.toHaveBeenCalled() // a data push writes the cache, never refetches
  })

  it('T12.F4 teardown: removing the query from the cache closes the ClientLive', async () => {
    const queryClient = new QueryClient()
    const liveQuery = createLiveQuery(queryClient)
    const fake = makeFakeClientLive('v1')
    await queryClient.fetchQuery(liveQuery({ queryKey: ['todos'], queryFn: async () => fake.live }))
    expect(fake.close).not.toHaveBeenCalled()
    queryClient.removeQueries({ queryKey: ['todos'] })
    expect(fake.close).toHaveBeenCalledTimes(1)
  })

  it('T12.F3 replace-on-resubscribe: a refetch mints a new ClientLive and closes the previous one', async () => {
    const queryClient = new QueryClient()
    const liveQuery = createLiveQuery(queryClient)
    const first = makeFakeClientLive('a')
    const second = makeFakeClientLive('b')
    let call = 0
    const options = liveQuery({
      queryKey: ['x'],
      queryFn: async () => (call++ === 0 ? first.live : second.live),
    })
    await queryClient.fetchQuery(options)
    await queryClient.fetchQuery({ ...options, staleTime: 0 }) // force a second fetch
    expect(first.close).toHaveBeenCalledTimes(1) // the previous ClientLive is closed
    expect(second.close).not.toHaveBeenCalled()
  })
})
