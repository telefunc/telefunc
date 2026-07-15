import { describe, expect, it, vi } from 'vitest'
import { LiveQuotaExceededError } from 'telefunc'
import { LIVE_QUERY_INFLIGHT_KEY, buildLiveResult, makeLiveResultHook } from './liveResult.js'
import './server.js' // registers the extension (exercises the public side-effect entry)
import { __TQ__CHANNEL_KEY, __TQ__DATA_KEY, isWrapper } from './shared.js'

// ── Fakes (repo idiom: hand-rolled, per-test) ──────────────────────────────

function makeFakeChannel() {
  const openCbs: Array<() => void> = []
  const closeCbs: Array<() => void> = []
  return {
    send: vi.fn(),
    onOpen: (cb: () => void) => openCbs.push(cb),
    onClose: (cb: () => void) => closeCbs.push(cb),
    close: vi.fn(),
    // The wrapper carries `channel.client` — a real ClientChannel exposes callable listen/close,
    // which `isWrapper` validates. The fake mirrors that shape.
    client: { listen: () => () => {}, close: () => Promise.resolve() } as never,
    _fireOpen: () => openCbs.forEach((cb) => cb()),
    _fireClose: () => closeCbs.forEach((cb) => cb()),
  }
}

function makeFakeReservation() {
  let released = false
  const releaseSpy = vi.fn()
  const release = () => {
    if (released) return
    released = true
    releaseSpy()
  }
  const transferToChannel = vi.fn((ch: unknown) => (ch as { onClose: (cb: () => void) => void }).onClose(release))
  return { reservation: { release, transferToChannel }, releaseSpy, transferToChannel }
}

function makeFakeSource(
  identity: string,
  opts: { syncFire?: number; subscribeThrows?: boolean; teardownThrows?: boolean; disposeThrows?: boolean } = {},
) {
  const teardown = vi.fn(() => {
    if (opts.teardownThrows) throw new Error(`teardown threw: ${identity}`)
  })
  const dispose = vi.fn(() => {
    if (opts.disposeThrows) throw new Error(`dispose threw: ${identity}`)
  })
  const subscribe = vi.fn((onInvalidate: () => void) => {
    if (opts.subscribeThrows) throw new Error(`subscribe threw: ${identity}`)
    for (let i = 0; i < (opts.syncFire ?? 0); i++) onInvalidate()
    return teardown
  })
  return { source: { identity, subscribe, dispose }, teardown, dispose, subscribe }
}

function makeDeps(cfg: {
  sources: Array<{ identity: string; subscribe: (cb: () => void) => () => void; dispose?: () => void }>
  reservation?: { release: () => void; transferToChannel: (ch: unknown) => void }
  channel?: ReturnType<typeof makeFakeChannel>
  reserveThrows?: unknown
  order?: string[]
}) {
  const channel = cfg.channel ?? makeFakeChannel()
  return {
    deps: {
      takeLiveSources: vi.fn(() => cfg.sources as never),
      reserveLiveQuery: vi.fn((key: string) => {
        cfg.order?.push('reserve')
        expect(key).toBe(LIVE_QUERY_INFLIGHT_KEY)
        if (cfg.reserveThrows) throw cfg.reserveThrows
        return (cfg.reservation ?? makeFakeReservation().reservation) as never
      }),
      createChannel: vi.fn(() => {
        cfg.order?.push('createChannel')
        return channel as never
      }),
    },
    channel,
  }
}

const flushMicrotasks = () => new Promise<void>((resolve) => queueMicrotask(() => resolve()))

// ── T2.A ───────────────────────────────────────────────────────────────────

describe('server result hook (§3.A)', () => {
  it('T2.A1 wrapper only when sources exist', () => {
    const { deps } = makeDeps({ sources: [] })
    const result = buildLiveResult({ hello: 'world' }, deps)
    expect(result).toEqual({ hello: 'world' })
    expect(isWrapper(result)).toBe(false)
    expect(deps.createChannel).not.toHaveBeenCalled()
  })

  it('T2.A2 one channel for N sources; all subscribed; wrapper carries channel.client', () => {
    const a = makeFakeSource('a')
    const b = makeFakeSource('b')
    const c = makeFakeSource('c')
    const { deps, channel } = makeDeps({ sources: [a.source, b.source, c.source] })
    const result = buildLiveResult('DATA', deps)
    expect(deps.createChannel).toHaveBeenCalledTimes(1)
    expect(a.subscribe).toHaveBeenCalledTimes(1)
    expect(b.subscribe).toHaveBeenCalledTimes(1)
    expect(c.subscribe).toHaveBeenCalledTimes(1)
    expect(isWrapper(result)).toBe(true)
    expect((result as Record<string, unknown>)[__TQ__DATA_KEY]).toBe('DATA')
    expect((result as Record<string, unknown>)[__TQ__CHANNEL_KEY]).toBe(channel.client)
  })

  it('T2.A3a any source fires → one coalesced send per microtask window', async () => {
    const a = makeFakeSource('a')
    const b = makeFakeSource('b')
    const { deps, channel } = makeDeps({ sources: [a.source, b.source] })
    buildLiveResult('DATA', deps)
    // Grab the onInvalidate each source received and fire both in the same tick.
    const onInvA = a.subscribe.mock.calls[0]![0]
    const onInvB = b.subscribe.mock.calls[0]![0]
    onInvA()
    onInvB()
    onInvA()
    expect(channel.send).not.toHaveBeenCalled() // deferred to the microtask
    await flushMicrotasks()
    expect(channel.send).toHaveBeenCalledTimes(1)
    expect(channel.send).toHaveBeenCalledWith('invalidate')
  })

  it('T2.A3b synchronous replay during subscribe() is buffered + coalesced, survives hook return', async () => {
    const a = makeFakeSource('a', { syncFire: 3 }) // fires 3x synchronously inside subscribe()
    const { deps, channel } = makeDeps({ sources: [a.source] })
    buildLiveResult('DATA', deps)
    expect(channel.send).not.toHaveBeenCalled() // no synchronous send during the hook
    await flushMicrotasks()
    expect(channel.send).toHaveBeenCalledTimes(1) // 3 sync fires collapse to one send
  })

  it('T2.A4 reserve-before-acquire (fixed key), no channel if reserve throws', () => {
    const order: string[] = []
    const a = makeFakeSource('a')
    const { deps } = makeDeps({ sources: [a.source], order })
    buildLiveResult('DATA', deps)
    expect(order).toEqual(['reserve', 'createChannel'])

    const order2: string[] = []
    const b = makeFakeSource('b')
    const { deps: deps2 } = makeDeps({ sources: [b.source], reserveThrows: new Error('cap'), order: order2 })
    expect(() => buildLiveResult('DATA', deps2)).toThrow('cap')
    expect(order2).toEqual(['reserve']) // createChannel never reached
    expect(deps2.createChannel).not.toHaveBeenCalled()
  })

  it('T2.A5 typed rejection at cap disposes taken sources attempt-all, never masking the typed error', () => {
    const a = makeFakeSource('a', { disposeThrows: true }) // a throwing disposer must not mask/skip
    const b = makeFakeSource('b')
    const err = new LiveQuotaExceededError('x', 100)
    const { deps } = makeDeps({ sources: [a.source, b.source], reserveThrows: err })
    expect(() => buildLiveResult('DATA', deps)).toThrow(LiveQuotaExceededError) // typed error preserved
    expect(a.dispose).toHaveBeenCalledTimes(1)
    expect(b.dispose).toHaveBeenCalledTimes(1) // attempt-all: a's throw did not skip b
    expect(deps.createChannel).not.toHaveBeenCalled()
  })

  it('T2.A6 release ends the in-flight window (attach OR permanent close, once)', () => {
    // (a) attach frees the slot while the channel stays open
    {
      const r = makeFakeReservation()
      const { deps, channel } = makeDeps({ sources: [makeFakeSource('a').source], reservation: r.reservation })
      buildLiveResult('DATA', deps)
      expect(r.transferToChannel).toHaveBeenCalledTimes(1)
      expect(r.releaseSpy).not.toHaveBeenCalled()
      channel._fireOpen()
      expect(r.releaseSpy).toHaveBeenCalledTimes(1)
      expect(channel.close).not.toHaveBeenCalled()
    }
    // (b) never-attached: permanent close releases once
    {
      const r = makeFakeReservation()
      const { deps, channel } = makeDeps({ sources: [makeFakeSource('a').source], reservation: r.reservation })
      buildLiveResult('DATA', deps)
      channel._fireClose()
      expect(r.releaseSpy).toHaveBeenCalledTimes(1)
    }
    // (c) attach then close → release exactly once total
    {
      const r = makeFakeReservation()
      const { deps, channel } = makeDeps({ sources: [makeFakeSource('a').source], reservation: r.reservation })
      buildLiveResult('DATA', deps)
      channel._fireOpen()
      channel._fireClose()
      expect(r.releaseSpy).toHaveBeenCalledTimes(1)
    }
    // (d) transient reconnect (neither open nor close) → no release
    {
      const r = makeFakeReservation()
      const { deps } = makeDeps({ sources: [makeFakeSource('a').source], reservation: r.reservation })
      buildLiveResult('DATA', deps)
      expect(r.releaseSpy).not.toHaveBeenCalled()
    }
  })

  it('T2.A7 kth-subscribe throw → unwind (teardown 1..k-1, dispose k..N, release), no wrapper', () => {
    const r = makeFakeReservation()
    const s0 = makeFakeSource('s0') // subscribes ok
    const s1 = makeFakeSource('s1') // subscribes ok
    const s2 = makeFakeSource('s2', { subscribeThrows: true }) // the kth (index 2) throws
    const s3 = makeFakeSource('s3') // never attempted
    const { deps } = makeDeps({ sources: [s0.source, s1.source, s2.source, s3.source], reservation: r.reservation })
    expect(() => buildLiveResult('DATA', deps)).toThrow('subscribe threw: s2')
    // 1..k-1 torn down
    expect(s0.teardown).toHaveBeenCalledTimes(1)
    expect(s1.teardown).toHaveBeenCalledTimes(1)
    // kth (threw) + rest disposed, never torn down
    expect(s2.dispose).toHaveBeenCalledTimes(1)
    expect(s2.teardown).not.toHaveBeenCalled()
    expect(s3.dispose).toHaveBeenCalledTimes(1)
    expect(s3.subscribe).not.toHaveBeenCalled()
    // reservation released, no double terminal action
    expect(r.releaseSpy).toHaveBeenCalledTimes(1)
    expect(s0.dispose).not.toHaveBeenCalled() // successfully-subscribed sources are NOT disposed here
  })

  it('T2.A7b a throwing teardown during unwind does not skip the rest', () => {
    const r = makeFakeReservation()
    const s0 = makeFakeSource('s0', { teardownThrows: true }) // throws while unwinding
    const s1 = makeFakeSource('s1') // must still be torn down
    const s2 = makeFakeSource('s2', { subscribeThrows: true })
    const { deps } = makeDeps({ sources: [s0.source, s1.source, s2.source], reservation: r.reservation })
    expect(() => buildLiveResult('DATA', deps)).toThrow('subscribe threw: s2')
    expect(s0.teardown).toHaveBeenCalledTimes(1)
    expect(s1.teardown).toHaveBeenCalledTimes(1) // not skipped by s0's throw
    expect(s2.dispose).toHaveBeenCalledTimes(1)
    expect(r.releaseSpy).toHaveBeenCalledTimes(1)
  })

  it('T2.A8 permanent close tears down every subscription, attempt-all', () => {
    const s0 = makeFakeSource('s0', { teardownThrows: true })
    const s1 = makeFakeSource('s1')
    const s2 = makeFakeSource('s2')
    const { deps, channel } = makeDeps({ sources: [s0.source, s1.source, s2.source] })
    buildLiveResult('DATA', deps)
    channel._fireClose()
    expect(s0.teardown).toHaveBeenCalledTimes(1)
    expect(s1.teardown).toHaveBeenCalledTimes(1) // s0's throw did not skip the rest
    expect(s2.teardown).toHaveBeenCalledTimes(1)
  })

  it('T2.A10 drizzle-agnostic: any LiveSource shape is handled identically', () => {
    // A "manual" (tag-like) source with no dispose and a "drizzle-like" source with dispose are
    // both just LiveSources past the seam.
    const manual = { identity: 'tag', subscribe: vi.fn(() => vi.fn()) }
    const drizzleLike = makeFakeSource('drizzle')
    const { deps } = makeDeps({ sources: [manual, drizzleLike.source] })
    const result = buildLiveResult('DATA', deps)
    expect(isWrapper(result)).toBe(true)
    expect(manual.subscribe).toHaveBeenCalledTimes(1)
    expect(drizzleLike.subscribe).toHaveBeenCalledTimes(1)
  })
})

// ── T2.E1 forged-legacy inertness ────────────────────────────────────────────

describe('forged legacy payload is inert (§3.E)', () => {
  it('T2.E1 the hook reads only ctx.result — forged ctx.data under the extension name never drives liveness', () => {
    // The registered hook receives request-carried extension data as `ctx.data`, exactly as the OLD
    // client-driven design carried `{queryKey, invalidates}` under the extension name. It must be
    // ignored entirely — liveness comes only from server-owned sources.
    const forged = { queryKey: ['x'], invalidates: [['y']] }

    // No server-owned source → result returned unchanged, no channel, despite forged ctx.data.
    const { deps: noSources } = makeDeps({ sources: [] })
    const hookNoSources = makeLiveResultHook(noSources)
    const passthrough = hookNoSources({ result: { real: 1 }, data: forged })
    expect(isWrapper(passthrough)).toBe(false)
    expect(passthrough).toEqual({ real: 1 })
    expect(noSources.createChannel).not.toHaveBeenCalled()

    // With a server-owned source present, the SAME forged ctx.data is irrelevant — the wrapper comes
    // from the source seam, and the forged queryKey/invalidates drive nothing.
    const { deps: withSource } = makeDeps({ sources: [makeFakeSource('s').source] })
    const hookWithSource = makeLiveResultHook(withSource)
    const wrapped = hookWithSource({ result: 'DATA', data: forged })
    expect(isWrapper(wrapped)).toBe(true)
    expect((wrapped as Record<string, unknown>)[__TQ__DATA_KEY]).toBe('DATA')
  })
})
