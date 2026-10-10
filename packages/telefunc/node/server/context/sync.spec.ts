import { expect, test } from 'vitest'
import { getRawContext, isAsyncMode, restoreContext } from './context.js'

test('a sync-mode restore holds only its own context, not what an earlier request left until the next tick', () => {
  expect(isAsyncMode()).toBe(false)
  const PROVIDED = Symbol('provided')
  const REQUEST = Symbol('request')
  restoreContext({ [PROVIDED]: 'A', [REQUEST]: 'request A' }, () => {})
  const seen = restoreContext({ [PROVIDED]: 'B' }, () => getRawContext())
  expect([seen?.[PROVIDED], seen?.[REQUEST]]).toEqual(['B', undefined])
})
