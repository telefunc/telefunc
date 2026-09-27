import { afterEach, expect, test, vi } from 'vitest'
import { macrotaskYield } from './macrotask-yield.js'

afterEach(() => {
  vi.unstubAllGlobals()
})

test('yields a macrotask where MessageChannel is missing, as in workerd before compatibility date 2025-08-15', async () => {
  vi.stubGlobal('MessageChannel', undefined)
  await macrotaskYield.yield()
  await macrotaskYield.yield()
})
