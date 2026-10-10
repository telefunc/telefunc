import { describe, expect, it } from 'vitest'
import { createDeferred } from '../../../../utils/createDeferred.js'
import { OrderedStubs } from './ordered-stubs.js'

type Stub = { id: number; calls: string[] }

function stubs() {
  const opened: Stub[] = []
  const open = () => {
    const stub = { id: opened.length, calls: [] }
    opened.push(stub)
    return stub
  }
  return { opened, open }
}

describe('OrderedStubs', () => {
  it('shares one stub per target while calls are in flight, and opens a fresh one once they settled', async () => {
    const calls = new OrderedStubs<Stub>()
    const { opened, open } = stubs()
    const first = createDeferred()
    const a = calls.call('t', open, (stub) => (stub.calls.push('a'), first.promise))
    const b = calls.call('t', open, (stub) => (stub.calls.push('b'), Promise.resolve()))
    calls.call('other', open, (stub) => (stub.calls.push('x'), Promise.resolve()))
    expect(opened.map((stub) => stub.calls)).toEqual([['a', 'b'], ['x']])
    first.resolve()
    await Promise.all([a, b])
    await calls.call('t', open, (stub) => (stub.calls.push('c'), Promise.resolve()))
    expect(opened.map((stub) => stub.calls)).toEqual([['a', 'b'], ['x'], ['c']])
  })

  it('replaces a stub that rejected, holding the replacement until the failed stub’s calls settle', async () => {
    const calls = new OrderedStubs<Stub>()
    const { opened, open } = stubs()
    const pending = createDeferred()
    const order: string[] = []
    const failed = calls.call('t', open, () => Promise.reject(new Error('broken')))
    const inFlight = calls.call('t', open, () => pending.promise.then(() => void order.push('in-flight')))
    await expect(failed).rejects.toThrow('broken')
    const next = calls.call('t', open, () => (order.push('next'), Promise.resolve()))
    const after = calls.call('t', open, () => (order.push('after'), Promise.resolve()))
    await Promise.resolve()
    expect(opened).toHaveLength(2)
    expect(order).toEqual([])
    pending.resolve()
    await Promise.all([inFlight, next, after])
    expect(order).toEqual(['in-flight', 'next', 'after'])
  })

  it('reports a synchronous throw as a rejection', async () => {
    const calls = new OrderedStubs<Stub>()
    const { opened, open } = stubs()
    await expect(
      calls.call('t', open, () => {
        throw new Error('sync')
      }),
    ).rejects.toThrow('sync')
    await calls.call('t', open, () => Promise.resolve())
    expect(opened).toHaveLength(2)
  })
})
