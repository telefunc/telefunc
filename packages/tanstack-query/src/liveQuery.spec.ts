import { describe, expect, it, vi } from 'vitest'
import { QueryClient, QueryObserver } from '@tanstack/query-core'
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

  it('T12.F3 unwraps .data into the cache; onInvalidate calls invalidateQueries directly (idempotent, no coalescing)', async () => {
    const queryClient = new QueryClient()
    const liveQuery = createLiveQuery(queryClient)
    const fake = makeFakeClientLive('v1')
    const data = await queryClient.fetchQuery(liveQuery({ queryKey: ['todos'], queryFn: async () => fake.live }))
    expect(data).toBe('v1') // .data unwrapped into the cache
    expect(queryClient.getQueryData(['todos'])).toBe('v1')

    const invalidateSpy = vi.spyOn(queryClient, 'invalidateQueries')
    fake.fireInvalidate() // direct: each signal invalidates immediately (TanStack owns fetch; invalidation is idempotent)
    // cancelRefetch:true → an invalidation landing DURING an in-flight fetch cancels it and refetches
    // (never swallowed); see the mid-fetch test below.
    expect(invalidateSpy).toHaveBeenCalledWith({ queryKey: ['todos'], exact: true }, { cancelRefetch: true })
    fake.fireInvalidate() // no per-key coalescing scheduler — a second signal is a second (harmless) invalidate
    expect(invalidateSpy).toHaveBeenCalledTimes(2)
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

  it('T12.F5 an invalidation arriving DURING an in-flight fetch is not swallowed (cancelRefetch:true cancels + refetches)', async () => {
    const flush = async () => {
      for (let i = 0; i < 6; i++) await new Promise<void>((resolve) => setTimeout(resolve, 0))
    }
    const queryClient = new QueryClient()
    const liveQuery = createLiveQuery(queryClient)

    let fetchCount = 0
    const releases: Array<(live: ClientLive<string>) => void> = []
    // Every fetch is GATED on a manual release, so the test can fire an invalidation while a fetch is
    // still in-flight and observe whether it produces a follow-up fetch.
    const options = liveQuery({
      queryKey: ['todos'],
      queryFn: () =>
        new Promise<ClientLive<string>>((resolve) => {
          fetchCount++
          releases.push(resolve)
        }),
    })

    // An ACTIVE observer so invalidateQueries actually drives refetches (imperative fetchQuery would not).
    const observer = new QueryObserver(queryClient, options)
    const unsub = observer.subscribe(() => {})
    await flush()
    expect(fetchCount).toBe(1) // initial fetch in-flight

    // Settle the initial fetch → the query holds v1 and onInvalidate is wired on the first ClientLive.
    const first = makeFakeClientLive('v1')
    releases[0]!(first.live)
    await flush()
    expect(queryClient.getQueryData(['todos'])).toBe('v1')

    // Invalidate #1 → a refetch starts (fetch #2), left in-flight (gated).
    first.fireInvalidate()
    await flush()
    expect(fetchCount).toBe(2)

    // Invalidate #2 DURING the in-flight refetch. cancelRefetch:true cancels fetch #2 and starts fetch #3
    // — the mid-flight invalidation is NOT swallowed. (Under cancelRefetch:false this stays at 2: the
    // stale in-flight fetch completes and clears isInvalidated with no follow-up — finding #2.)
    first.fireInvalidate()
    await flush()
    expect(fetchCount).toBe(3)

    // Cleanup: release any still-gated fetches + unsubscribe.
    const leftover = makeFakeClientLive('vN')
    for (const release of releases.slice(1)) release(leftover.live)
    await flush()
    unsub()
  })
})
