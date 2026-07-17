import { describe, expect, it, vi } from 'vitest'
import { LiveCell } from './live.js'

// Deterministic microtask flush: the cell schedules its coalesced invalidation with `queueMicrotask`,
// so this queued-after callback resolves only once that flush has run (FIFO).
const tick = () => new Promise<void>((resolve) => queueMicrotask(() => resolve()))

describe('Live core cell', () => {
  it('holds its snapshot and IS the handle a telefunction returns; invalidate never touches data', async () => {
    const live = new LiveCell<number>(12)
    expect(live.data).toBe(12)
    live.invalidate() // the primitive is invalidation-only — the value is a fixed snapshot
    await tick()
    expect(live.data).toBe(12) // a telefunction returns the cell itself; `.data` is all a consumer reads
    expect(live.isClosed).toBe(false)
  })

  it('onInvalidate fires once per coalesced window; unsubscribe is idempotent; a late tap does not replay', async () => {
    const live = new LiveCell('a')
    const onInvalidate = vi.fn()
    const off = live.onInvalidate(onInvalidate)
    live.invalidate()
    await tick()
    expect(onInvalidate).toHaveBeenCalledTimes(1)

    // Unsubscribe removes exactly one registration and is idempotent (a second call is a no-op).
    off()
    off()
    live.invalidate()
    await tick()
    expect(onInvalidate).toHaveBeenCalledTimes(1) // no further calls after off

    // A late tap (subscribed after the emission already flushed) does not replay the past signal.
    const lateTap = vi.fn()
    live.onInvalidate(lateTap)
    await tick()
    expect(lateTap).not.toHaveBeenCalled()
  })

  it('per-cell coalescing: many invalidates in one window deliver one onInvalidate', async () => {
    const live = new LiveCell(0)
    const onInvalidate = vi.fn()
    live.onInvalidate(onInvalidate)
    live.invalidate()
    live.invalidate()
    live.invalidate()
    await tick()
    expect(onInvalidate).toHaveBeenCalledTimes(1)
  })

  it('invalidate after close is inert (no throw, no emission)', async () => {
    const live = new LiveCell('a')
    const onInvalidate = vi.fn()
    live.onInvalidate(onInvalidate)
    await live.close()
    expect(live.isClosed).toBe(true)
    expect(() => live.invalidate()).not.toThrow()
    await tick()
    expect(onInvalidate).not.toHaveBeenCalled()
    expect(live.data).toBe('a')
  })

  it('close fires onClose once (idempotent); a late onClose fires immediately', async () => {
    const live = new LiveCell('a')
    const onClose = vi.fn()
    live.onClose(onClose)
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
