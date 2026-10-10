import { expect, test } from 'vitest'
import { Listeners } from './Listeners.js'

test('a dispatch reaches the listeners it started with, in order, whatever they add or remove meanwhile', () => {
  const listeners = new Listeners<() => void>()
  const seen: string[] = []
  let removeB = () => {}
  listeners.add(() => {
    seen.push('a')
    removeB()
    listeners.add(() => void seen.push('c'))
  })
  removeB = listeners.add(() => void seen.push('b'))
  for (const listener of listeners.list()) listener()
  expect(seen).toEqual(['a', 'b'])
  expect(listeners.size).toBe(2)
})

test('a callback added twice is removed one registration at a time, each removal once', () => {
  const listeners = new Listeners<string>()
  listeners.add('x')
  listeners.add('y')
  const removeSecondX = listeners.add('x')
  removeSecondX()
  removeSecondX()
  expect(listeners.list()).toEqual(['x', 'y'])
  expect(listeners.size).toBe(2)
})
