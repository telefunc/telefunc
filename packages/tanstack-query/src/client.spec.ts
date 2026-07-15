import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { withTelefunc } from './client.js'
import { __TQ__CHANNEL_KEY, __TQ__DATA_KEY } from './shared.js'

const tick = () => new Promise<void>((resolve) => setTimeout(resolve, 0))

beforeEach(() => vi.stubGlobal('window', {}))
afterEach(() => vi.unstubAllGlobals())

function makeFakeQueryClient() {
  let defaultOptions: { queries?: { persister?: unknown } } = {}
  const cacheListeners: Array<(e: unknown) => void> = []
  const queryCache = {
    subscribe: vi.fn((cb: (e: unknown) => void) => {
      cacheListeners.push(cb)
      return () => {
        const i = cacheListeners.indexOf(cb)
        if (i >= 0) cacheListeners.splice(i, 1)
      }
    }),
    find: vi.fn(() => undefined),
  }
  const client = {
    getDefaultOptions: () => defaultOptions,
    setDefaultOptions: vi.fn((o: typeof defaultOptions) => {
      defaultOptions = o
    }),
    getQueryCache: () => queryCache,
    invalidateQueries: vi.fn(() => Promise.resolve()),
    defaultMutationOptions: vi.fn((o: unknown) => o),
    unmount: vi.fn(),
    clear: vi.fn(),
  }
  return {
    client: client as never,
    raw: client,
    queryCache,
    getPersister: () => defaultOptions.queries?.persister as (fn: unknown, ctx: unknown, q: unknown) => unknown,
    emitRemoved: (queryKey: readonly unknown[]) =>
      cacheListeners.slice().forEach((cb) => cb({ type: 'removed', query: { queryKey } })),
    cacheListenerCount: () => cacheListeners.length,
  }
}

function makeClientChannel(opts: { closeRejects?: boolean } = {}) {
  const listeners: Array<() => void> = []
  const unlisten = vi.fn()
  const channel = {
    listen: vi.fn((cb: () => void) => {
      listeners.push(cb)
      return unlisten
    }),
    close: vi.fn(() => (opts.closeRejects ? Promise.reject(new Error('close failed')) : Promise.resolve())),
  }
  return { channel, fire: () => listeners.slice().forEach((cb) => cb()), unlisten }
}

function wrapper(data: unknown, channel: unknown) {
  return { [__TQ__DATA_KEY]: data, [__TQ__CHANNEL_KEY]: channel }
}

const ctx = (queryKey: readonly unknown[]) => ({ queryKey })

describe('withTelefunc client (§3.B)', () => {
  it('T2.B1 persister unwraps the wrapper and wires the channel', () => {
    const q = makeFakeQueryClient()
    withTelefunc(q.client)
    const ch = makeClientChannel()
    const persister = q.getPersister()
    const out = persister(() => wrapper('DATA', ch.channel), ctx(['k']), {})
    expect(out).toBe('DATA')
    expect(ch.channel.listen).toHaveBeenCalledTimes(1)
  })

  it('T2.B2 channel signal → scheduler → invalidateQueries({queryKey, exact:true}, {cancelRefetch:false})', async () => {
    const q = makeFakeQueryClient()
    withTelefunc(q.client)
    const ch = makeClientChannel()
    q.getPersister()(() => wrapper('DATA', ch.channel), ctx(['k']), {})
    ch.fire()
    await tick()
    expect(q.raw.invalidateQueries).toHaveBeenCalledWith({ queryKey: ['k'], exact: true }, { cancelRefetch: false })
  })

  it('T2.B3 replace-on-resubscribe closes the previous channel once; rejection observed', async () => {
    const q = makeFakeQueryClient()
    withTelefunc(q.client)
    const ch1 = makeClientChannel({ closeRejects: true })
    const ch2 = makeClientChannel()
    const persister = q.getPersister()
    persister(() => wrapper('d1', ch1.channel), ctx(['k']), {})
    persister(() => wrapper('d2', ch1.channel === ch2.channel ? ch1.channel : ch2.channel), ctx(['k']), {})
    expect(ch1.channel.close).toHaveBeenCalledTimes(1) // previous closed on resubscribe
    expect(ch1.unlisten).toHaveBeenCalledTimes(1)
    await tick() // the rejected close settles without an unhandled rejection
    // close is exactly-once: tearing down the (now current) ch2 closes it once
    q.emitRemoved(['k'])
    expect(ch2.channel.close).toHaveBeenCalledTimes(1)
  })

  it('T2.B4 teardown matrix: removed, unmount, clear (+ clear stays resubscribable)', async () => {
    // removed
    {
      const q = makeFakeQueryClient()
      withTelefunc(q.client)
      const ch = makeClientChannel()
      q.getPersister()(() => wrapper('d', ch.channel), ctx(['k']), {})
      q.emitRemoved(['k'])
      expect(ch.channel.close).toHaveBeenCalledTimes(1)
    }
    // unmount
    {
      const q = makeFakeQueryClient()
      withTelefunc(q.client)
      const ch = makeClientChannel()
      q.getPersister()(() => wrapper('d', ch.channel), ctx(['k']), {})
      q.raw.unmount()
      expect(ch.channel.close).toHaveBeenCalledTimes(1)
    }
    // clear + resubscribe + remove
    {
      const q = makeFakeQueryClient()
      withTelefunc(q.client)
      const persister = q.getPersister()
      const ch1 = makeClientChannel()
      persister(() => wrapper('d1', ch1.channel), ctx(['k']), {})
      q.raw.clear()
      expect(ch1.channel.close).toHaveBeenCalledTimes(1)
      // the cache subscription survived clear() → a new live query still tears down on removed
      const ch2 = makeClientChannel()
      persister(() => wrapper('d2', ch2.channel), ctx(['k2']), {})
      q.emitRemoved(['k2'])
      expect(ch2.channel.close).toHaveBeenCalledTimes(1)
    }
  })

  it('T2.B5 dev warns on a nested wrapper; not on plain / cyclic / deep-ordinary results', () => {
    const q = makeFakeQueryClient()
    withTelefunc(q.client)
    const persister = q.getPersister()
    const warn = vi.spyOn(console, 'warn').mockImplementation(() => {})

    // nested wrapper (rule-2 violation)
    const ch = makeClientChannel()
    persister(() => ({ transformed: { inner: wrapper('d', ch.channel) } }), ctx(['n']), {})
    expect(warn).toHaveBeenCalledTimes(1)
    warn.mockClear()

    // plain result → no warn
    persister(() => ({ a: 1, b: [2, 3] }), ctx(['p']), {})
    expect(warn).not.toHaveBeenCalled()

    // cyclic plain object → no warn, terminates
    const cyc: Record<string, unknown> = { a: 1 }
    cyc.self = cyc
    expect(() => persister(() => cyc, ctx(['c']), {})).not.toThrow()
    expect(warn).not.toHaveBeenCalled()

    // ordinary object nested deeper than the bound → no warn
    persister(() => ({ l1: { l2: { l3: { l4: { l5: { l6: 'deep' } } } } } }), ctx(['d']), {})
    expect(warn).not.toHaveBeenCalled()
    warn.mockRestore()
  })

  it('T2.B6 mutation meta.invalidates is client-local and preserves the user onSuccess exactly once', () => {
    const q = makeFakeQueryClient()
    withTelefunc(q.client)
    const userOnSuccess = vi.fn()
    const wrapped = q.raw.defaultMutationOptions({
      meta: { invalidates: [['a'], ['b']] },
      onSuccess: userOnSuccess,
    }) as { onSuccess: (...a: unknown[]) => unknown }
    wrapped.onSuccess('data', 'vars', 'onMutateCtx')
    expect(q.raw.invalidateQueries).toHaveBeenCalledWith({ queryKey: ['a'] })
    expect(q.raw.invalidateQueries).toHaveBeenCalledWith({ queryKey: ['b'] })
    expect(userOnSuccess).toHaveBeenCalledTimes(1)
    expect(userOnSuccess).toHaveBeenCalledWith('data', 'vars', 'onMutateCtx')
  })

  it('T2.B7 withTelefunc attaches no request wrapper — persister passthrough + unwrapped mutationFn', () => {
    // The only client seam that can stage per-call request extension bytes is telefunc's per-call
    // context wrapper. withTelefunc never wraps the query call or the mutationFn in it, so nothing
    // live is ever serialized. Here we pin the structural guarantee (no wrapping); the actual
    // outgoing request body is inspected on the real wire in the browser Ce2e (live-query.e2e.cjs).
    const q = makeFakeQueryClient()
    withTelefunc(q.client)

    // Query: the persister invokes the query function with exactly the context — no wrapper.
    const queryFn = vi.fn(() => 'plain')
    const context = ctx(['k'])
    const out = q.getPersister()(queryFn, context, {})
    expect(out).toBe('plain')
    expect(queryFn).toHaveBeenCalledWith(context)

    // Mutation: withTelefunc wraps only onSuccess; the user's mutationFn passes through untouched, so
    // no per-call extension context is ever staged for a mutation either.
    const userMutationFn = vi.fn()
    const resolved = q.raw.defaultMutationOptions({
      meta: { invalidates: [['k']] },
      mutationFn: userMutationFn,
    }) as { mutationFn: unknown }
    expect(resolved.mutationFn).toBe(userMutationFn)

    // A plain (non-live) result wires no channel and schedules no invalidation.
    expect(q.raw.invalidateQueries).not.toHaveBeenCalled()
  })

  it('T2.B8 withTelefunc is idempotent (no stacked persisters/listeners/wrappers)', () => {
    const q = makeFakeQueryClient()
    withTelefunc(q.client)
    withTelefunc(q.client)
    expect(q.raw.setDefaultOptions).toHaveBeenCalledTimes(1)
    expect(q.queryCache.subscribe).toHaveBeenCalledTimes(1)
    // one mutation wrapper: meta.invalidates fires exactly one local invalidate per key
    const wrapped = q.raw.defaultMutationOptions({ meta: { invalidates: [['a']] } }) as {
      onSuccess: (...a: unknown[]) => unknown
    }
    wrapped.onSuccess()
    expect(q.raw.invalidateQueries).toHaveBeenCalledTimes(1)
  })

  it('T2.B1-neg an own-key business object with a non-channel value is returned unchanged (no throw)', () => {
    const q = makeFakeQueryClient()
    withTelefunc(q.client)
    // both reserved keys present, but `__tq__channel` is business data, not a channel
    const business = { __tq__data: 'business', __tq__channel: { business: true } }
    let out: unknown
    expect(() => {
      out = q.getPersister()(() => business, ctx(['biz']), {})
    }).not.toThrow()
    expect(out).toEqual(business)
  })

  it('T2.B4-stale a channel signal cancelled by teardown before its microtask does not invalidate', async () => {
    const q = makeFakeQueryClient()
    withTelefunc(q.client)
    const ch = makeClientChannel()
    q.getPersister()(() => wrapper('d', ch.channel), ctx(['k']), {})
    ch.fire() // scheduler.schedule → microtask queued
    q.emitRemoved(['k']) // cache removed → teardown → scheduler.dispose, before the microtask runs
    await tick()
    expect(q.raw.invalidateQueries).not.toHaveBeenCalled()
  })
})
