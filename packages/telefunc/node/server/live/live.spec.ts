import { describe, expect, it, vi } from 'vitest'
import { Live } from './live.js'
import type { ClientLive } from './live.js'

describe('Live core cell (§3.A)', () => {
  it('T12.A1 snapshot data + invalidate (never touches data) + side-effect-free .client', () => {
    const live = new Live<number>(1)
    expect(live.data).toBe(1)
    live.invalidate() // invalidation-only — the snapshot data is fixed
    expect(live.data).toBe(1)

    // `.client` is a side-effect-free re-type: same underlying value + the shared consumer surface.
    const client: ClientLive<number> = live.client
    expect(client.data).toBe(1)
    expect(client.isClosed).toBe(false)
    expect(typeof client.onInvalidate).toBe('function')
  })

  it('T12.A2 invalidation taps fire DIRECTLY; unsubscribe is idempotent', () => {
    const live = new Live('a')
    const onInvalidate = vi.fn()
    const off = live.onInvalidate(onInvalidate)
    live.invalidate()
    expect(onInvalidate).toHaveBeenCalledTimes(1) // fired directly — no microtask coalescing
    live.invalidate()
    expect(onInvalidate).toHaveBeenCalledTimes(2) // each signal fires (client-side invalidation is idempotent)

    // Unsubscribe removes exactly one registration and is idempotent (a second call is a no-op).
    off()
    off()
    live.invalidate()
    expect(onInvalidate).toHaveBeenCalledTimes(2) // no further calls after off
  })

  it('T12.A4 invalidate after close is inert (no throw, no emission, data unchanged)', async () => {
    const live = new Live('a')
    const onInvalidate = vi.fn()
    live.onInvalidate(onInvalidate)
    await live.close()
    expect(live.isClosed).toBe(true)
    expect(() => live.invalidate()).not.toThrow()
    expect(onInvalidate).not.toHaveBeenCalled()
    expect(live.data).toBe('a')
  })

  it('T12.A1/A4 close fires onClose once (idempotent); a late onClose fires immediately', async () => {
    const live = new Live('a')
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
