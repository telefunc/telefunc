import { describe, expect, it, vi } from 'vitest'
import { QueryClient, QueryObserver } from '@tanstack/query-core'
import type { Live } from 'telefunc'
import type { LiveSubscription } from 'telefunc/__internal'
// `telefunc/client` resolves by CONDITION: the browser gets the real client, node gets a no-op shim
// (`withContext = (fn) => fn`, `abort = () => {}`). This adapter ships to the browser, so under vitest's
// node resolution it would otherwise be tested against a stub of the very thing under test — every
// assertion about cancellation would pass no matter what the adapter did. Give it the real module.
vi.mock('telefunc/client', async () => await import('../../telefunc/client/withContext.js'))

// The generated telefunc stub reads its per-call context from this module-global at call time (see
// remoteTelefunctionCall.ts). It is not public API, so a faithful fake telefunction reads it the same
// way the real one does — a fake that got its signal by any other route would prove nothing about
// whether a real telefunction receives it.
import { getPendingContext, withContextChecked } from '../../telefunc/client/withContext.js'
import { markContextConsumer } from '../../telefunc/client/callAttribution.js'
import { live } from './live.js'

type Observed = { signal?: AbortSignal; calls: number }

/** Make a fake behave like a generated telefunction stub in the two ways that matter.
 *  `remoteTelefunctionCall` reads `getPendingContext()` as its very first statement, and STAMPS the call it
 *  returns with the context that read consumed — and it is the stamp, not the read, that `live()` checks.
 *  A fake that only did the read would be indistinguishable from a wrapper that consumes the context and
 *  returns something else, which is precisely the case these tests exist to separate.
 *  `observed` records what the stub saw, so cancellation can be checked at the call itself. */
function stub<F extends (...args: never[]) => unknown>(fn: F): F & { observed: Observed } {
  const observed: Observed = { calls: 0 }
  const stubbed = (...args: never[]) => {
    observed.calls++
    const context = getPendingContext()
    observed.signal = context?.signal
    const call = fn(...args)
    if (context && typeof call === 'object' && call !== null) markContextConsumer(call, context)
    return call
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
 *  The unsubscribes really detach, as the revived handle's do. Returning no-ops instead would make
 *  every teardown assertion here unfalsifiable: an adapter that closed the handle but left its taps
 *  attached would look identical, and firing after teardown would still reach the cache. */
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
  return {
    handle,
    close,
    fireInvalidate: () => [...invalidateCbs].forEach((cb) => cb()),
  }
}

describe('live() — the TanStack queryFn wrapper', () => {
  it('surfaces the value, not the handle: data re-types Live<T> → T', async () => {
    const queryClient = new QueryClient()
    const fake = makeFakeLive('v1')
    // Compile-time proof: `live()` returns a QueryFunction whose data is `string`, so TanStack infers
    // `data: string` rather than `Live<string>` — this wrapper is the one honest re-typing point.
    const data = await queryClient.fetchQuery({ queryKey: ['todos'], queryFn: live(stub(() => fake.handle)) })
    expect(data).toBe('v1')
    expect(queryClient.getQueryData(['todos'])).toBe('v1')
  })

  it('the value form live(fn, ...args) forwards the arguments to the telefunction', async () => {
    const queryClient = new QueryClient()
    const handle = makeFakeLive('v1').handle
    const received: unknown[] = []
    // `live(onGetTodos, id)` issues `onGetTodos(id)` — the args ride through to the telefunction call,
    // no closure needed to bake them in.
    const telefn = stub((a: number, b: string): Promise<Live<string>> => {
      received.push(a, b)
      return Promise.resolve(handle)
    })
    const data = await queryClient.fetchQuery({ queryKey: ['todos'], queryFn: live(telefn, 7, 'abc') })
    expect(data).toBe('v1')
    expect(received).toEqual([7, 'abc'])
  })

  it('invalidation refetches the query it came from — keyed off the context, not a passed-in key', async () => {
    const queryClient = new QueryClient()
    const fake = makeFakeLive('v1')
    await queryClient.fetchQuery({ queryKey: ['todos'], queryFn: live(stub(async () => fake.handle)) })

    const invalidateSpy = vi.spyOn(queryClient, 'invalidateQueries')
    fake.fireInvalidate()
    // cancelRefetch:true → an invalidation landing DURING an in-flight fetch cancels it and refetches
    // rather than being swallowed; proven by the mid-fetch test below.
    expect(invalidateSpy).toHaveBeenCalledWith({ queryKey: ['todos'], exact: true }, { cancelRefetch: true })
    fake.fireInvalidate() // invalidation is idempotent — a second signal is a second harmless invalidate
    expect(invalidateSpy).toHaveBeenCalledTimes(2)
  })

  it('removing the query from the cache closes the live handle', async () => {
    const queryClient = new QueryClient()
    const fake = makeFakeLive('v1')
    await queryClient.fetchQuery({ queryKey: ['todos'], queryFn: live(stub(async () => fake.handle)) })
    expect(fake.close).not.toHaveBeenCalled()
    queryClient.removeQueries({ queryKey: ['todos'] })
    expect(fake.close).toHaveBeenCalledTimes(1)
  })

  it('teardown detaches the taps, it does not merely close the handle', async () => {
    const queryClient = new QueryClient()
    const fake = makeFakeLive('v1')
    await queryClient.fetchQuery({ queryKey: ['todos'], queryFn: live(stub(async () => fake.handle)) })
    queryClient.removeQueries({ queryKey: ['todos'] })

    // A handle can be closed and still have this adapter's callback hanging off it — closing is the
    // handle's business, detaching is ours. An invalidate still attached here would refetch a query the
    // cache no longer holds, and keep this closure alive with it.
    const invalidateSpy = vi.spyOn(queryClient, 'invalidateQueries')
    fake.fireInvalidate()
    expect(invalidateSpy).not.toHaveBeenCalled()
    expect(queryClient.getQueryData(['todos'])).toBeUndefined()
  })

  it('a refetch mints a new handle and closes the previous one', async () => {
    const queryClient = new QueryClient()
    const first = makeFakeLive('a')
    const second = makeFakeLive('b')
    let call = 0
    const queryFn = live(stub(async () => (call++ === 0 ? first.handle : second.handle)))
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
    await queryClient.fetchQuery({ queryKey: ['x'], queryFn: live(stub(async () => first.handle)) })
    await queryClient.fetchQuery({ queryKey: ['x'], queryFn: live(stub(async () => second.handle)), staleTime: 0 })
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
        stub(
          () =>
            new Promise<Live<string>>((resolve) => {
              fetchCount++
              releases.push(resolve)
            }),
        ),
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
      queryFn: live(stub(() => new Promise<Live<string>>((resolve) => (release = resolve)))),
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

describe('live() — cancelling a query cancels the telefunction request', () => {
  const flush = async () => {
    for (let i = 0; i < 6; i++) await new Promise<void>((resolve) => setTimeout(resolve, 0))
  }

  it('the telefunction receives the query signal, and cancelling the query aborts it', async () => {
    const queryClient = new QueryClient()
    const fake = makeFakeTelefunction(makeFakeLive('v1').handle)
    // The value form: the telefunction is passed directly, so `withContext` sets the per-call context
    // exactly around its own invocation and the signal reaches it synchronously — no wrapper to detach it.
    const observer = new QueryObserver(queryClient, {
      queryKey: ['todos'],
      queryFn: live(fake.call),
    })
    const unsub = observer.subscribe(() => {})
    await flush()

    expect(fake.observed.calls).toBe(1)
    expect(fake.observed.signal).toBeInstanceOf(AbortSignal) // the signal reached the call itself
    expect(fake.observed.signal!.aborted).toBe(false)

    void queryClient.cancelQueries({ queryKey: ['todos'] })
    await flush()
    // The request is actually cancelled — not merely abandoned while it runs to completion.
    expect(fake.observed.signal!.aborted).toBe(true)
    fake.release()
    await flush()
    unsub()
  })

  it('a query already cancelled before the fetch starts never issues the request', async () => {
    const queryClient = new QueryClient()
    const fake = makeFakeTelefunction(makeFakeLive('v1').handle)
    const queryFn = live(fake.call)
    const controller = new AbortController()
    controller.abort() // cancelled before TanStack ever calls us

    // Attaching an abort listener would never have fired here — an already-aborted signal does not
    // replay. The only way not to issue a doomed request is to check before starting.
    await expect(queryFn(queryFnContext(queryClient, controller.signal))).rejects.toThrow(/aborted/)
    expect(fake.observed.calls).toBe(0)
  })

  it('a SYNCHRONOUS wrapper works, and the signal still reaches the telefunction', async () => {
    const queryClient = new QueryClient()
    const fake = makeFakeTelefunction(makeFakeLive('v1').handle)
    // `live(() => onGetTodos(id))` reaches the telefunction inside the per-call context window, so the
    // stub picks the signal up exactly as it does for the direct form. This spelling used to be rejected
    // on the premise that "any wrapper detaches cancellation" — only DELAYED wrappers do.
    const observer = new QueryObserver(queryClient, {
      queryKey: ['todos'],
      queryFn: live(() => fake.call()),
    })
    const unsub = observer.subscribe(() => {})
    await flush()

    expect(fake.observed.calls).toBe(1)
    expect(fake.observed.signal).toBeInstanceOf(AbortSignal) // the signal crossed the closure
    void queryClient.cancelQueries({ queryKey: ['todos'] })
    await flush()
    expect(fake.observed.signal!.aborted).toBe(true) // and cancelling really does abort the request
    fake.release()
    await flush()
    unsub()
  })

  it('a DELAYED wrapper throws at fetch time — and its call really did lose the signal', async () => {
    const queryClient = new QueryClient()
    const fake = makeFakeTelefunction(makeFakeLive('v1').handle)
    const queryFn = live(async () => {
      await tick()
      return fake.call()
    })
    const controller = new AbortController()

    await expect(queryFn(queryFnContext(queryClient, controller.signal))).rejects.toThrow(/telefunction call/)
    // The distinctness gate for the throw above: the wrapper is not rejected on suspicion, it is rejected
    // because the context was demonstrably not taken. Its telefunction DOES run — one call, after the
    // window closed — and it sees no signal at all. That is the silent cancellation loss, made loud.
    await flush()
    expect(fake.observed.calls).toBe(1)
    expect(fake.observed.signal).toBeUndefined()
  })

  it('a function that reaches no telefunction throws the same way, and its handle is not left open', async () => {
    const queryClient = new QueryClient()
    const fake = makeFakeLive('v1')
    // Not passed through `stub`, so it never reads the pending context — indistinguishable, from the
    // signal's point of view, from the delayed wrapper: nothing consumed the context.
    const queryFn = live(() => Promise.resolve(fake.handle))

    await expect(queryFn(queryFnContext(queryClient, new AbortController().signal))).rejects.toThrow(
      /telefunction call/,
    )
    // The rejected call is already away; whatever it resolves to must not be left holding a channel.
    await flush()
    expect(fake.close).toHaveBeenCalledTimes(1)
  })

  it('a non-function is rejected up front, before any fetch', () => {
    expect(() => live(undefined as never)).toThrow(/expects a telefunction/)
    expect(() => live(42 as never)).toThrow(/a number/)
  })

  // A consumption bit says SOMETHING took the context. These are the schedules where something did, and the
  // handle that came back still never saw the signal — so only attributing the context to the RETURNED call
  // separates them. Each is paired with the passing shape it differs from by one step, so the tests cannot
  // both pass by the check being uniformly strict.
  describe('the context must be taken by the call that is RETURNED', () => {
    it('calls a telefunction, drops it, and returns another: consumed, but the wrong handle', async () => {
      const queryClient = new QueryClient()
      const dropped = makeFakeTelefunction(makeFakeLive('a').handle)
      const returned = makeFakeLive('b')
      // A consumes the context and is thrown away; B is handed back untouched by it. Under a bare
      // consumption test this passes and the query silently loses cancellation.
      const queryFn = live(() => {
        void dropped.call()
        return Promise.resolve(returned.handle)
      })

      await expect(queryFn(queryFnContext(queryClient, new AbortController().signal))).rejects.toThrow(
        /telefunction call/,
      )
      expect(dropped.observed.calls).toBe(1) // the consuming call really did happen…
      expect(dropped.observed.signal).toBeInstanceOf(AbortSignal) // …and really did take the context
      await flush()
      expect(returned.close).toHaveBeenCalledTimes(1) // the handle that came back is not left open
    })

    it('CONTROL: returning that same call instead of dropping it passes', async () => {
      const queryClient = new QueryClient()
      const fake = makeFakeTelefunction(makeFakeLive('a').handle)
      // One step apart from the case above: the call it made is the value it returns.
      const observer = new QueryObserver(queryClient, { queryKey: ['todos'], queryFn: live(() => fake.call()) })
      const unsub = observer.subscribe(() => {})
      await flush()
      expect(fake.observed.calls).toBe(1)
      fake.release()
      await flush()
      expect(queryClient.getQueryData(['todos'])).toBe('a')
      unsub()
    })

    it('a NESTED checked call takes its own context, so the outer one is not satisfied by it', async () => {
      const queryClient = new QueryClient()
      const fake = makeFakeTelefunction(makeFakeLive('v1').handle)
      const innerContext = { signal: new AbortController().signal }
      // The inner window consumes and stamps with ITS context, then closes. The call handed outward is real
      // and was made in a window — just not this query's — so its signal is the inner one, not the query's.
      const queryFn = live(() => withContextChecked(fake.call, innerContext).result)

      await expect(queryFn(queryFnContext(queryClient, new AbortController().signal))).rejects.toThrow(
        /telefunction call/,
      )
      expect(fake.observed.calls).toBe(1)
      expect(fake.observed.signal).toBe(innerContext.signal) // it got the INNER signal — the query's never arrived
      fake.release()
    })

    it('CONTROL: the same call without the nested window passes', async () => {
      const queryClient = new QueryClient()
      const fake = makeFakeTelefunction(makeFakeLive('v1').handle)
      const observer = new QueryObserver(queryClient, { queryKey: ['todos'], queryFn: live(() => fake.call()) })
      const unsub = observer.subscribe(() => {})
      await flush()
      expect(fake.observed.calls).toBe(1)
      expect(fake.observed.signal).toBeInstanceOf(AbortSignal)
      fake.release()
      await flush()
      unsub()
    })
  })
})
