import { expect, test } from 'vitest'
import { withContext } from './withContext.js'

test.each([2 ** 31, Infinity, -1, 0.5])(
  "withContext() refuses a channel.idleTimeout of %s, whose timer can't wait it",
  (idleTimeout) => {
    expect(() => withContext(async () => {}, { channel: { idleTimeout } })).toThrow('at most 2147483647')
  },
)

test('withContext() takes a channel.idleTimeout up to the longest a timer waits', () => {
  expect(() => withContext(async () => {}, { channel: { idleTimeout: 2 ** 31 - 1 } })).not.toThrow()
  expect(() => withContext(async () => {}, { channel: { idleTimeout: 0 } })).not.toThrow()
})
