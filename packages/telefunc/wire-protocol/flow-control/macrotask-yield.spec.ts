import { afterEach, expect, test, vi } from 'vitest'
import { macrotaskYield } from './macrotask-yield.js'

afterEach(() => {
  vi.unstubAllGlobals()
})

test('a missing MessageChannel is a usage error naming the Workers compatibility date, and leaves no later yield hanging', async () => {
  vi.stubGlobal('MessageChannel', undefined)
  expect(() => macrotaskYield.yield()).toThrow('compatibility_date to 2025-08-15')
  vi.unstubAllGlobals()
  await macrotaskYield.yield()
})
