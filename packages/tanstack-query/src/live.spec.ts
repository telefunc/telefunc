import { describe, expect, it, vi } from 'vitest'
import { QueryClient, QueryObserver } from '@tanstack/query-core'
import type { Live } from 'telefunc'
import type { LiveSubscription } from 'telefunc/__internal'
import { live } from './live.js'

const tick = () => new Promise<void>((resolve) => queueMicrotask(() => resolve()))

/** Stand in for a revived client Live: publicly a `Live<T>` (`.data`), carrying the `@internal`
 *  subscription taps the adapter binds — the same two-faced shape the wire reviver produces. */
function makeFakeLive<T>(initial: T) {
  const invalidateCbs: Array<() => void> = []
  const dataCbs: Array<(data: T) => void> = []
  const close = vi.fn(() => Promise.resolve())
  const handle: Live<T> & LiveSubscription<T> = {
    data: initial,
    onData: (cb) => {
      dataCbs.push(cb)
      return () => {}
    },
    onInvalidate: (cb) => {
      invalidateCbs.push(cb)
      return () => {}
    },
    close,
  }
  return {
    handle,
    close,
    fireInvalidate: () => invalidateCbs.forEach((cb) => cb()),
    fireData: (data: T) => dataCbs.forEach((cb) => cb(data)),
  }
}

describe('live() — the TanStack queryFn wrapper', () => {
  it('surfaces the value, not the handle: data re-types Live<T> → T', async () => {
    const queryClient = new QueryClient()
    const fake = makeFakeLive('v1')
    // Compile-time proof: `live()` returns a QueryFunction whose data is `string`, so TanStack infers
    // `data: string` rather than `Live<string>` — this wrapper is the one honest re-typing point.
    const data = await queryClient.fetchQuery({ queryKey: ['todos'], queryFn: live(() => fake.handle) })
    expect(data).toBe('v1')
    expect(queryClient.getQueryData(['todos'])).toBe('v1')
  })

  it('invalidation refetches the query it came from — keyed off the context, not a passed-in key', async () => {
    const queryClient = new QueryClient()
    const fake = makeFakeLive('v1')
    await queryClient.fetchQuery({ queryKey: ['todos'], queryFn: live(async () => fake.handle) })

    const invalidateSpy = vi.spyOn(queryClient, 'invalidateQueries')
    fake.fireInvalidate()
    // cancelRefetch:true → an invalidation landing DURING an in-flight fetch cancels it and refetches
    // rather than being swallowed; proven by the mid-fetch test below.
    expect(invalidateSpy).toHaveBeenCalledWith({ queryKey: ['todos'], exact: true }, { cancelRefetch: true })
    fake.fireInvalidate() // invalidation is idempotent — a second signal is a second harmless invalidate
    expect(invalidateSpy).toHaveBeenCalledTimes(2)
  })

  it('a pushed value writes the cache directly and does NOT refetch', async () => {
    const queryClient = new QueryClient()
    const fake = makeFakeLive('v1')
    await queryClient.fetchQuery({ queryKey: ['todos'], queryFn: live(async () => fake.handle) })

    const invalidateSpy = vi.spyOn(queryClient, 'invalidateQueries')
    fake.fireData('v2')
    expect(queryClient.getQueryData(['todos'])).toBe('v2')
    await tick()
    expect(invalidateSpy).not.toHaveBeenCalled()
  })

  it('removing the query from the cache closes the live handle', async () => {
    const queryClient = new QueryClient()
    const fake = makeFakeLive('v1')
    await queryClient.fetchQuery({ queryKey: ['todos'], queryFn: live(async () => fake.handle) })
    expect(fake.close).not.toHaveBeenCalled()
    queryClient.removeQueries({ queryKey: ['todos'] })
    expect(fake.close).toHaveBeenCalledTimes(1)
  })

  it('a refetch mints a new handle and closes the previous one', async () => {
    const queryClient = new QueryClient()
    const first = makeFakeLive('a')
    const second = makeFakeLive('b')
    let call = 0
    const queryFn = live(async () => (call++ === 0 ? first.handle : second.handle))
    await queryClient.fetchQuery({ queryKey: ['x'], queryFn })
    await queryClient.fetchQuery({ queryKey: ['x'], queryFn, staleTime: 0 }) // force a second fetch
    expect(first.close).toHaveBeenCalledTimes(1)
    expect(second.close).not.toHaveBeenCalled()
  })

  it('called inline on every render, it still replaces rather than duplicates its subscription', async () => {
    // `queryFn: live(...)` is re-evaluated each render, so `live()` must hold no per-call state:
    // a fresh map per render would lose the previous handle instead of closing it.
    const queryClient = new QueryClient()
    const first = makeFakeLive('a')
    const second = makeFakeLive('b')
    await queryClient.fetchQuery({ queryKey: ['x'], queryFn: live(async () => first.handle) })
    await queryClient.fetchQuery({ queryKey: ['x'], queryFn: live(async () => second.handle), staleTime: 0 })
    expect(first.close).toHaveBeenCalledTimes(1) // a DIFFERENT live() closure still found and closed it
    queryClient.removeQueries({ queryKey: ['x'] })
    expect(second.close).toHaveBeenCalledTimes(1) // and one cache watcher still tears the survivor down
  })

  it('an invalidation arriving DURING an in-flight fetch is not swallowed', async () => {
    const flush = async () => {
      for (let i = 0; i < 6; i++) await new Promise<void>((resolve) => setTimeout(resolve, 0))
    }
    const queryClient = new QueryClient()

    let fetchCount = 0
    const releases: Array<(handle: Live<string>) => void> = []
    // Every fetch is GATED on a manual release, so the test can fire an invalidation while a fetch is
    // still in-flight and observe whether it produces a follow-up fetch.
    const options = {
      queryKey: ['todos'],
      queryFn: live(
        () =>
          new Promise<Live<string>>((resolve) => {
            fetchCount++
            releases.push(resolve)
          }),
      ),
    }

    // An ACTIVE observer so invalidateQueries actually drives refetches (imperative fetchQuery would not).
    const observer = new QueryObserver(queryClient, options)
    const unsub = observer.subscribe(() => {})
    await flush()
    expect(fetchCount).toBe(1) // initial fetch in-flight

    const first = makeFakeLive('v1')
    releases[0]!(first.handle)
    await flush()
    expect(queryClient.getQueryData(['todos'])).toBe('v1')

    // Invalidate #1 → a refetch starts (fetch #2), left in-flight (gated).
    first.fireInvalidate()
    await flush()
    expect(fetchCount).toBe(2)

    // Invalidate #2 DURING the in-flight refetch. cancelRefetch:true cancels fetch #2 and starts fetch
    // #3 — the mid-flight invalidation is NOT swallowed. (Under cancelRefetch:false this stays at 2:
    // the stale fetch completes and clears isInvalidated with no follow-up.)
    first.fireInvalidate()
    await flush()
    expect(fetchCount).toBe(3)

    const leftover = makeFakeLive('vN')
    for (const release of releases.slice(1)) release(leftover.handle)
    await flush()
    unsub()
  })

  it('a fetch cancelled in flight closes the handle it still receives (no orphaned channel)', async () => {
    const flush = async () => {
      for (let i = 0; i < 6; i++) await new Promise<void>((resolve) => setTimeout(resolve, 0))
    }
    const queryClient = new QueryClient()
    let release: ((handle: Live<string>) => void) | undefined
    const observer = new QueryObserver(queryClient, {
      queryKey: ['todos'],
      queryFn: live(() => new Promise<Live<string>>((resolve) => (release = resolve))),
    })
    const unsub = observer.subscribe(() => {})
    await flush()

    void queryClient.cancelQueries({ queryKey: ['todos'] }) // aborts the signal while the fetch is gated
    await flush()
    const late = makeFakeLive('late')
    release!(late.handle) // the handle arrives anyway — it must not be left open
    await flush()
    expect(late.close).toHaveBeenCalledTimes(1)
    unsub()
  })
})
