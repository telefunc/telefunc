import { describe, expect, it, vi } from 'vitest'
import { QueryClient, QueryObserver } from '@tanstack/query-core'
import type { Live, LiveSubscription } from '../primitive/live.js'

// `telefunc/client` resolves by CONDITION: the browser gets the real client, node gets a no-op shim
// (`withContext = (fn) => fn`). This adapter ships to the browser, so under vitest's node resolution it
// would otherwise be tested against a stub of the very thing under test — every assertion about
// cancellation would pass no matter what the adapter did. Give it the real module.
//
// A relative path into telefunc's source, in a SPEC only: the property under test is that the signal
// reaches the telefunction through telefunc's OWN per-call context mechanism, so a local fake of
// `withContext` would prove nothing about the real one. Nothing at runtime crosses this way — the
// adapter imports `telefunc/client` by package name.
// `config` is mocked alongside it because importing the adapter REGISTERS the Live reviver as a side
// effect (that ordering guarantee is the point of doing it at import time). The registration itself is
// covered by the wire spec; here it just needs somewhere to push.
vi.mock('telefunc/client', async () => ({
  ...(await import('../../../telefunc/client/withContext.js')),
  config: { extensions: [] as unknown[] },
}))

// The generated telefunc stub reads its per-call context from this module-global at call time (see
// remoteTelefunctionCall.ts). Not public API, so a faithful fake telefunction reads it the same way the
// real one does — a fake that got its signal by any other route would prove nothing.
import { getPendingContext } from '../../../telefunc/client/withContext.js'
import { live } from './live.js'

type Observed = { signal?: AbortSignal; calls: number; args: unknown[] }

/** Behave like a generated telefunction stub in the one way that matters here: read the pending context
 *  at call time. The direct form needs nothing more — `live()` calls the telefunction itself, so the call
 *  it issues IS the call the context was set around and there is no attribution left to prove. */
function stub<F extends (...args: never[]) => unknown>(fn: F): F & { observed: Observed } {
  const observed: Observed = { calls: 0, args: [] }
  const stubbed = (...args: never[]) => {
    observed.calls++
    observed.args = args
    observed.signal = getPendingContext()?.signal
    return fn(...args)
  }
  return Object.assign(stubbed, { observed }) as F & { observed: Observed }
}

/** A stand-in for a telefunction that stays in flight until released. */
function makeFakeTelefunction<T>(handle: Live<T>) {
  let release: (() => void) | undefined
  const call = stub(
    (): Promise<Live<T>> =>
      new Promise<Live<T>>((resolve) => {
        release = () => resolve(handle)
      }),
  )
  return { call, observed: call.observed, release: () => release?.() }
}

/** The context TanStack hands a `queryFn`, for the tests that call it directly. */
const queryFnContext = (client: QueryClient, signal: AbortSignal) =>
  ({ client, queryKey: ['todos'], signal, meta: undefined }) as never

const tick = () => new Promise<void>((resolve) => queueMicrotask(() => resolve()))

/** Stand in for a revived client Live: publicly a `Live<T>` (`.data`), carrying the `@internal`
 *  subscription taps the adapter binds — the same two-faced shape the wire reviver produces.
 *
 *  The unsubscribes really detach, as the revived handle's do. Returning no-ops instead would make every
 *  teardown assertion here unfalsifiable: an adapter that closed the handle but left its taps attached
 *  would look identical, and firing after teardown would still reach the cache. */
function makeFakeLive<T>(initial: T) {
  const invalidateCbs = new Set<() => void>()
  const close = vi.fn(() => Promise.resolve())
  const handle: Live<T> & LiveSubscription = {
    data: initial,
    onInvalidate: (cb) => {
      invalidateCbs.add(cb)
      return () => invalidateCbs.delete(cb)
    },
    close,
  }
  return { handle, close, fireInvalidate: () => [...invalidateCbs].forEach((cb) => cb()) }
}

describe('live() — the TanStack queryFn wrapper', () => {
  it('surfaces the value, not the handle: data re-types Live<T> → T', async () => {
    const client = new QueryClient()
    const { handle } = makeFakeLive([{ id: 1 }])
    const telefunction = stub(async () => handle)
    const data = await live(telefunction)(queryFnContext(client, new AbortController().signal))
    expect(data).toEqual([{ id: 1 }]) // the rows, not the handle
  })

  it('forwards the arguments to the telefunction', async () => {
    const client = new QueryClient()
    const { handle } = makeFakeLive('x')
    const telefunction = stub(async (..._args: never[]) => handle)
    await live(telefunction, 7 as never, 'a' as never)(queryFnContext(client, new AbortController().signal))
    expect(telefunction.observed.args).toEqual([7, 'a'])
  })

  it('invalidation refetches the query it came from — keyed off the context, not a passed-in key', async () => {
    const client = new QueryClient()
    const invalidateQueries = vi.spyOn(client, 'invalidateQueries')
    const { handle, fireInvalidate } = makeFakeLive('v1')
    await live(stub(async () => handle))(queryFnContext(client, new AbortController().signal))
    fireInvalidate()
    expect(invalidateQueries).toHaveBeenCalledWith({ queryKey: ['todos'], exact: true }, { cancelRefetch: true })
  })

  it('removing the query from the cache closes the live handle', async () => {
    const client = new QueryClient()
    const { handle, close } = makeFakeLive('v1')
    await live(stub(async () => handle))(queryFnContext(client, new AbortController().signal))
    client.getQueryCache().build(client, { queryKey: ['todos'] })
    client.removeQueries({ queryKey: ['todos'] })
    await tick()
    expect(close).toHaveBeenCalled()
  })

  it('teardown detaches the taps, it does not merely close the handle', async () => {
    // The distinction that matters: a closed-but-still-tapped handle would keep invalidating the cache.
    const client = new QueryClient()
    const invalidateQueries = vi.spyOn(client, 'invalidateQueries')
    const { handle, fireInvalidate } = makeFakeLive('v1')
    await live(stub(async () => handle))(queryFnContext(client, new AbortController().signal))
    client.getQueryCache().build(client, { queryKey: ['todos'] })
    client.removeQueries({ queryKey: ['todos'] })
    await tick()
    invalidateQueries.mockClear()
    fireInvalidate()
    expect(invalidateQueries).not.toHaveBeenCalled() // the tap is gone, not just the channel
  })

  it('a refetch mints a new handle and closes the previous one', async () => {
    const client = new QueryClient()
    const first = makeFakeLive('v1')
    const second = makeFakeLive('v2')
    let next = first.handle
    const queryFn = live(stub(async () => next))
    await queryFn(queryFnContext(client, new AbortController().signal))
    next = second.handle
    await queryFn(queryFnContext(client, new AbortController().signal))
    expect(first.close).toHaveBeenCalled() // the superseded handle is closed, not leaked
    expect(second.close).not.toHaveBeenCalled()
  })

  it('called inline on every render, it still replaces rather than duplicates its subscription', async () => {
    // `queryFn: live(fn)` mints a new function each render. Per-call state would lose the previous
    // handle instead of closing it — the registry is module-level so it does not.
    const client = new QueryClient()
    const first = makeFakeLive('v1')
    const second = makeFakeLive('v2')
    await live(stub(async () => first.handle))(queryFnContext(client, new AbortController().signal))
    await live(stub(async () => second.handle))(queryFnContext(client, new AbortController().signal))
    expect(first.close).toHaveBeenCalled()
  })

  it('an invalidation arriving DURING an in-flight fetch is not swallowed', async () => {
    // `cancelRefetch: true` is what makes this work: without it the stale fetch runs to completion and
    // clears `isInvalidated` with no follow-up.
    const client = new QueryClient()
    const invalidateQueries = vi.spyOn(client, 'invalidateQueries')
    const { handle, fireInvalidate } = makeFakeLive('v1')
    await live(stub(async () => handle))(queryFnContext(client, new AbortController().signal))
    fireInvalidate()
    expect(invalidateQueries).toHaveBeenCalledWith(expect.anything(), { cancelRefetch: true })
  })

  it('a fetch cancelled in flight closes the handle it still receives (no orphaned channel)', async () => {
    const client = new QueryClient()
    const { handle, close } = makeFakeLive('v1')
    const fake = makeFakeTelefunction(handle)
    const controller = new AbortController()
    const pending = live(fake.call)(queryFnContext(client, controller.signal))
    controller.abort()
    fake.release() // the handle arrives anyway — it must not be left open
    await expect(pending).rejects.toThrow(/aborted/i)
    await tick()
    expect(close).toHaveBeenCalled()
  })

  it('a non-function is rejected up front, before any fetch', () => {
    expect(() => live(undefined as never)).toThrow(/expects a telefunction/)
  })
})

describe('live() — cancelling a query cancels the telefunction request', () => {
  it('the telefunction receives the query signal, and cancelling the query aborts it', async () => {
    const client = new QueryClient()
    const { handle } = makeFakeLive('v1')
    const fake = makeFakeTelefunction(handle)
    const controller = new AbortController()
    const pending = live(fake.call)(queryFnContext(client, controller.signal))
    // The signal reached the telefunction through telefunc's OWN per-call context — the stub read it the
    // way a generated stub does, so this is the real delivery path, not a stand-in for it.
    expect(fake.observed.signal).toBe(controller.signal)
    controller.abort()
    fake.release()
    await expect(pending).rejects.toThrow(/aborted/i)
  })

  it('a query already cancelled before the fetch starts never issues the request', async () => {
    const client = new QueryClient()
    const { handle } = makeFakeLive('v1')
    const fake = makeFakeTelefunction(handle)
    const controller = new AbortController()
    controller.abort()
    await expect(live(fake.call)(queryFnContext(client, controller.signal))).rejects.toThrow(/aborted/i)
    expect(fake.observed.calls).toBe(0) // not merely cancelled — never called at all
  })

  it('drives a real QueryObserver end to end: invalidation produces a refetch', async () => {
    // The integration check: not a spy on `invalidateQueries`, but an actual observer seeing a second
    // fetch. An adapter that wired the tap to nothing would pass every spy assertion above and fail here.
    const client = new QueryClient()
    const first = makeFakeLive('v1')
    const second = makeFakeLive('v2')
    let next = first.handle
    const observer = new QueryObserver(client, {
      queryKey: ['todos'],
      queryFn: live(stub(async () => next)),
    })
    const seen: unknown[] = []
    const unsubscribe = observer.subscribe((result) => result.data !== undefined && seen.push(result.data))
    await vi.waitFor(() => expect(seen).toContain('v1'))
    next = second.handle
    first.fireInvalidate()
    await vi.waitFor(() => expect(seen).toContain('v2'))
    unsubscribe()
  })
})
