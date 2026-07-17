import { describe, expect, it, vi } from 'vitest'
import { onPostSerialize, drainPostSerializeDisposers } from './postSerialize.js'
import type { Context } from './context.js'

const ctx = () => ({}) as Context

describe('post-serialize disposer-drain', () => {
  it('runs every registered disposer once, in registration order', () => {
    const c = ctx()
    const calls: number[] = []
    onPostSerialize(c, () => calls.push(1))
    onPostSerialize(c, () => calls.push(2))
    drainPostSerializeDisposers(c)
    expect(calls).toEqual([1, 2])
  })

  it('is idempotent — a second drain is a no-op', () => {
    const c = ctx()
    const d = vi.fn()
    onPostSerialize(c, d)
    drainPostSerializeDisposers(c)
    drainPostSerializeDisposers(c)
    expect(d).toHaveBeenCalledTimes(1)
  })

  it('isolates a throwing disposer — the others still run, nothing propagates', () => {
    const c = ctx()
    const after = vi.fn()
    const errSpy = vi.spyOn(console, 'error').mockImplementation(() => {})
    onPostSerialize(c, () => {
      throw new Error('boom')
    })
    onPostSerialize(c, after)
    expect(() => drainPostSerializeDisposers(c)).not.toThrow()
    expect(after).toHaveBeenCalledTimes(1)
    errSpy.mockRestore()
  })

  it('is scoped to the specific context object (not ambient)', () => {
    const a = ctx()
    const b = ctx()
    const d = vi.fn()
    onPostSerialize(a, d)
    drainPostSerializeDisposers(b) // a different context — nothing to drain
    expect(d).not.toHaveBeenCalled()
    drainPostSerializeDisposers(a)
    expect(d).toHaveBeenCalledTimes(1)
  })

  it('drain with no registrations is a safe no-op', () => {
    expect(() => drainPostSerializeDisposers(ctx())).not.toThrow()
  })
})
