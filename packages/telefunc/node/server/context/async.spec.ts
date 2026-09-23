import { expect, test } from 'vitest'
import './async.js'
import { getRawContext, restoreContext } from './context.js'

test("a restored context keeps the enclosing scope's other keys", () => {
  const ADAPTER = Symbol('adapter')
  const REQUEST = Symbol('request')
  const seen = restoreContext({ [ADAPTER]: 'outer', [REQUEST]: 'outer' }, () =>
    restoreContext({ [REQUEST]: 'inner' }, () => getRawContext()),
  )
  expect(seen?.[ADAPTER]).toBe('outer')
  expect(seen?.[REQUEST]).toBe('inner')
})
