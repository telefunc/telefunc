import { describe, expect, it, vi } from 'vitest'
import { Live } from './live.js'
import type { ClientLive } from './live.js'

// Deterministic microtask flush: the cell schedules its coalesced emission with `queueMicrotask`,
// so this queued-after callback resolves only once that flush has run (FIFO).
const tick = () => new Promise<void>((resolve) => queueMicrotask(() => resolve()))

describe('Live core cell (§3.A)', () => {
  it('T12.A1 producer verbs + data + side-effect-free .client', async () => {
    const live = new Live<number>(1)
    expect(live.data).toBe(1)
    live.set(2)
    await tick()
    expect(live.data).toBe(2)
    live.update((n) => n + 10)
    await tick()
    expect(live.data).toBe(12)
    live.invalidate() // invalidate never touches data
    await tick()
    expect(live.data).toBe(12)

    // `.client` is a side-effect-free re-type: same underlying value + the shared consumer surface.
    const client: ClientLive<number> = live.client
    expect(client.data).toBe(12)
    expect(client.isClosed).toBe(false)
    expect(typeof client.onData).toBe('function')
    expect(typeof client.onInvalidate).toBe('function')
  })

  it('T12.A2 symmetric taps fire; unsubscribe is idempotent; a late tap does not replay', async () => {
    const live = new Live('a')
    const onData = vi.fn()
    const onInvalidate = vi.fn()
    const offData = live.onData(onData)
    live.onInvalidate(onInvalidate)
    live.set('b')
    live.invalidate()
    await tick()
    expect(onData).toHaveBeenCalledTimes(1)
    expect(onData).toHaveBeenCalledWith('b')
    expect(onInvalidate).toHaveBeenCalledTimes(1)

    // Unsubscribe removes exactly one registration and is idempotent (a second call is a no-op).
    offData()
    offData()
    live.set('c')
    await tick()
    expect(onData).toHaveBeenCalledTimes(1) // no further calls after off

    // A late tap (subscribed after the emission already flushed) does not replay the past value.
    const lateTap = vi.fn()
    live.onData(lateTap)
    await tick()
    expect(lateTap).not.toHaveBeenCalled()
  })

  it('T12.A3 per-cell coalescing, incl. set(undefined)', async () => {
    // Three rapid sets → one onData carrying the third value.
    const live = new Live<string | undefined>('x')
    const onData = vi.fn()
    live.onData(onData)
    live.set('a')
    live.set('b')
    live.set('c')
    await tick()
    expect(onData).toHaveBeenCalledTimes(1)
    expect(onData).toHaveBeenCalledWith('c')

    // set(undefined) is a real pending data event, not swallowed by a `!== undefined` check.
    onData.mockClear()
    live.set(undefined)
    await tick()
    expect(onData).toHaveBeenCalledTimes(1)
    expect(onData).toHaveBeenCalledWith(undefined)

    // Three rapid invalidates → one onInvalidate.
    const inv = new Live(0)
    const onInvalidate = vi.fn()
    inv.onInvalidate(onInvalidate)
    inv.invalidate()
    inv.invalidate()
    inv.invalidate()
    await tick()
    expect(onInvalidate).toHaveBeenCalledTimes(1)

    // A data + an invalidate in one window → both fire once (data is not dropped by the invalidate).
    const both = new Live('p')
    const bothData = vi.fn()
    const bothInv = vi.fn()
    both.onData(bothData)
    both.onInvalidate(bothInv)
    both.set('q')
    both.invalidate()
    await tick()
    expect(bothData).toHaveBeenCalledTimes(1)
    expect(bothData).toHaveBeenCalledWith('q')
    expect(bothInv).toHaveBeenCalledTimes(1)
  })

  it('T12.A4 set/update/invalidate after close are inert (no throw, no emission, data unchanged)', async () => {
    const live = new Live('a')
    const onData = vi.fn()
    const onInvalidate = vi.fn()
    live.onData(onData)
    live.onInvalidate(onInvalidate)
    await live.close()
    expect(live.isClosed).toBe(true)
    expect(() => live.set('b')).not.toThrow()
    expect(() => live.update((s) => s + '!')).not.toThrow()
    expect(() => live.invalidate()).not.toThrow()
    await tick()
    expect(onData).not.toHaveBeenCalled()
    expect(onInvalidate).not.toHaveBeenCalled()
    expect(live.data).toBe('a')
  })

  it('T12.A1/A4 close fires onClose once (idempotent); a late onClose fires immediately; brand detect', async () => {
    const live = new Live('a')
    const onClose = vi.fn()
    live.onClose(onClose)
    expect(Live.isLive(live)).toBe(true)
    expect(Live.isLive({})).toBe(false)
    await live.close()
    expect(onClose).toHaveBeenCalledTimes(1)
    await live.close() // idempotent — no second fire
    expect(onClose).toHaveBeenCalledTimes(1)
    // An onClose registered after close fires immediately.
    const late = vi.fn()
    live.onClose(late)
    expect(late).toHaveBeenCalledTimes(1)
  })
})
