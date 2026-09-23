import { expect, test } from 'vitest'
import { getRawContext, isAsyncMode, restoreContext } from './context.js'

test("a nested sync-mode restore keeps the enclosing scope's other keys", () => {
  expect(isAsyncMode()).toBe(false)
  const ADAPTER = Symbol('adapter')
  const REQUEST = Symbol('request')
  const seen = restoreContext({ [ADAPTER]: 'outer', [REQUEST]: 'outer' }, () =>
    restoreContext({ [REQUEST]: 'inner' }, () => getRawContext()),
  )
  expect(seen?.[ADAPTER]).toBe('outer')
  expect(seen?.[REQUEST]).toBe('inner')
})
