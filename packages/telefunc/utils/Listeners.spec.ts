import { describe, expect, it } from 'vitest'
import { Listeners } from './Listeners.js'

describe('Listeners', () => {
  it('skips a listener that an earlier one removed during the same dispatch', () => {
    const listeners = new Listeners<() => void>()
    const called: string[] = []
    listeners.add(() => removeSecond())
    const removeSecond = listeners.add(() => called.push('second'))

    dispatch(listeners)

    expect(called).toEqual([])
  })

  it('skips a listener whose signal an earlier one aborted during the same dispatch', () => {
    const listeners = new Listeners<() => void>()
    const called: string[] = []
    const controller = new AbortController()
    listeners.add(() => controller.abort())
    listeners.add(() => called.push('second'), { signal: controller.signal })

    dispatch(listeners)

    expect(called).toEqual([])
  })

  it('calls a callback added twice once, where an earlier listener removed one of its two registrations', () => {
    const listeners = new Listeners<() => void>()
    let calls = 0
    const callback = () => calls++
    listeners.add(() => removeOne())
    const removeOne = listeners.add(callback)
    listeners.add(callback)

    dispatch(listeners)

    expect(calls).toBe(1)
  })

  it('skips the listeners that a clear() during the dispatch removed', () => {
    const listeners = new Listeners<() => void>()
    const called: string[] = []
    listeners.add(() => listeners.clear())
    listeners.add(() => called.push('second'))

    dispatch(listeners)

    expect(called).toEqual([])
  })

  it('calls a listener that removes itself once, and the one after it', () => {
    const listeners = new Listeners<() => void>()
    const called: string[] = []
    const removeFirst = listeners.add(() => {
      called.push('first')
      removeFirst()
    })
    listeners.add(() => called.push('second'))

    dispatch(listeners)
    dispatch(listeners)

    expect(called).toEqual(['first', 'second', 'second'])
  })

  it('calls a listener added during a dispatch from the next dispatch on', () => {
    const listeners = new Listeners<() => void>()
    const called: string[] = []
    const removeAdder = listeners.add(() => {
      removeAdder()
      listeners.add(() => called.push('added'))
    })

    dispatch(listeners)
    expect(called).toEqual([])
    dispatch(listeners)

    expect(called).toEqual(['added'])
  })
})

/** Calls each listener as every user of `Listeners` does. */
function dispatch(listeners: Listeners<() => void>): void {
  for (const { listener } of listeners.list()) listener?.()
}
