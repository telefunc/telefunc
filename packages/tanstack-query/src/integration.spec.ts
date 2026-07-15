import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { QueryClient } from '@tanstack/query-core'
import { Channel, LiveQuotaExceededError, reserveLiveQuery } from 'telefunc'
import { withTelefunc } from './client.js'
import { buildLiveResult } from './liveResult.js'
import { __TQ__CHANNEL_KEY, __TQ__DATA_KEY, isWrapper } from './shared.js'

// Node-level integration coverage (contract option b): the server hook, the client `withTelefunc`,
// the invalidation scheduler, the reservation limiter, and the telefunc `Channel` are all the REAL
// objects here — only the wire hop between a server `send` and the client's deserialized channel is
// left to the browser e2e (that hop cannot exist without the HTTP/SSE transport).

const tick = () => new Promise<void>((resolve) => setTimeout(resolve, 0))
const realReserve = (key: string) => reserveLiveQuery(key)

describe('integration: real server hook + real Channel + real limiter', () => {
  it('buildLiveResult wraps with a real Channel and a real reservation, and a source fire drives a real send', () => {
    let onInvalidate: (() => void) | undefined
    const source = {
      identity: 'todos:global',
      subscribe: (cb: () => void) => {
        onInvalidate = cb
        return () => {}
      },
    }
    const channel = new Channel<never, 'invalidate'>()
    try {
      const result = buildLiveResult('DATA', {
        takeLiveSources: () => [source],
        reserveLiveQuery: realReserve, // REAL per-connection limiter
        createChannel: () => channel, // REAL telefunc Channel
      })

      expect(isWrapper(result)).toBe(true)
      const wrapped = result as Record<string, unknown>
      expect(wrapped[__TQ__DATA_KEY]).toBe('DATA')
      const ch = wrapped[__TQ__CHANNEL_KEY] as { listen: unknown; close: unknown }
      expect(typeof ch.listen).toBe('function')
      expect(typeof ch.close).toBe('function')

      // Firing the source drives a real coalesced `send('invalidate')` into the real channel's
      // pre-peer buffer (the channel is unattached) — it must not throw.
      expect(() => onInvalidate?.()).not.toThrow()
    } finally {
      channel.abort() // release the real reservation so the global ledger stays clean
    }
  })

  it('a real channel permanent close (abort) runs the source teardown wired by the hook', () => {
    const teardown = vi.fn()
    const source = { identity: 's', subscribe: () => teardown }
    const channel = new Channel<never, 'invalidate'>()
    buildLiveResult('DATA', {
      takeLiveSources: () => [source],
      reserveLiveQuery: realReserve,
      createChannel: () => channel,
    })
    // `abort()` drives a real permanent close synchronously (no peer/ack wait); the hook's
    // `onClose(teardown)` (and the reservation's onClose release) fire on the real channel.
    channel.abort()
    expect(teardown).toHaveBeenCalledTimes(1)
  })

  it('T2.A5 real fixed-key limiter: 100 held, the 101st throws LiveQuotaExceededError and disposes the taken source', () => {
    // Fill the in-flight ledger to the cap on the REAL fixed key by running 100 real hooks, then
    // prove the real (N+1)th transition. Every reservation is released in `finally` (abort → onClose
    // → release) so the shared global ledger returns to 0.
    const channels: Channel<never, 'invalidate'>[] = []
    try {
      for (let i = 0; i < 100; i++) {
        const channel = new Channel<never, 'invalidate'>()
        channels.push(channel)
        buildLiveResult('d', {
          takeLiveSources: () => [{ identity: `held-${i}`, subscribe: () => () => {} }],
          reserveLiveQuery: realReserve,
          createChannel: () => channel,
        })
      }

      const dispose = vi.fn()
      expect(() =>
        buildLiveResult('d', {
          takeLiveSources: () => [{ identity: 'overflow', subscribe: () => () => {}, dispose }],
          reserveLiveQuery: realReserve,
          createChannel: () => new Channel<never, 'invalidate'>(),
        }),
      ).toThrow(LiveQuotaExceededError)
      expect(dispose).toHaveBeenCalledTimes(1) // the taken source is disposed on rejection
    } finally {
      for (const channel of channels) channel.abort()
    }
  })
})

describe('integration: real QueryClient + real withTelefunc', () => {
  beforeEach(() => vi.stubGlobal('window', {}))
  afterEach(() => vi.unstubAllGlobals())

  it('the real persister unwraps a wrapper into the real cache, and a channel signal invalidates the real query', async () => {
    const listeners: Array<() => void> = []
    const clientChannel = {
      listen: (cb: () => void) => {
        listeners.push(cb)
        return () => {}
      },
      close: () => Promise.resolve(),
    }
    const wrapper = { [__TQ__DATA_KEY]: 'v1', [__TQ__CHANNEL_KEY]: clientChannel }

    const queryClient = withTelefunc(new QueryClient())
    // A real fetch flows through the installed persister, which unwraps the wrapper.
    const data = await queryClient.fetchQuery({ queryKey: ['todos'], queryFn: async () => wrapper })
    expect(data).toBe('v1')
    expect(queryClient.getQueryData(['todos'])).toBe('v1')

    // The server's invalidate signal (delivered here as the wire would) drives a REAL
    // invalidateQueries on the REAL QueryClient.
    for (const cb of listeners) cb()
    await tick()
    const query = queryClient.getQueryCache().find({ queryKey: ['todos'], exact: true })
    expect(query?.state.isInvalidated).toBe(true)

    queryClient.unmount()
  })

  it('an ordinary business result with both reserved keys but no channel is returned unchanged (no throw)', async () => {
    // isWrapper must reject a non-channel value → the persister returns the business object as-is.
    const business = { __tq__data: 'business', __tq__channel: { business: true } }
    const queryClient = withTelefunc(new QueryClient())
    const data = await queryClient.fetchQuery({ queryKey: ['biz'], queryFn: async () => business })
    expect(data).toEqual(business)
    queryClient.unmount()
  })

  it('a real mutation meta.invalidates invalidates the real local cache and preserves user onSuccess', () => {
    const queryClient = withTelefunc(new QueryClient())
    const invalidateSpy = vi.spyOn(queryClient, 'invalidateQueries')
    const userOnSuccess = vi.fn()

    const resolved = queryClient.defaultMutationOptions({
      meta: { invalidates: [['todos']] },
      onSuccess: userOnSuccess,
    }) as { onSuccess?: (...a: unknown[]) => unknown }

    resolved.onSuccess?.('data', 'vars', 'onMutateCtx')
    expect(invalidateSpy).toHaveBeenCalledWith({ queryKey: ['todos'] })
    expect(userOnSuccess).toHaveBeenCalledTimes(1)
    expect(userOnSuccess).toHaveBeenCalledWith('data', 'vars', 'onMutateCtx')

    queryClient.unmount()
  })
})
