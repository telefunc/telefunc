import { afterEach, expect, test, vi } from 'vitest'
import { FlowControl } from './flow-control.js'

afterEach(() => {
  vi.unstubAllGlobals()
})

test('a channel created where MessageChannel is missing is a usage error naming the Workers compatibility date', () => {
  vi.stubGlobal('MessageChannel', undefined)
  expect(() => new FlowControl({ byteWindowUpdate() {}, msgWindowUpdate() {}, bdpPing() {} }, () => 0)).toThrow(
    'compatibility_date to 2025-08-15',
  )
})
