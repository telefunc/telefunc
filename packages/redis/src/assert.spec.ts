import { expect, test } from 'vitest'
import { assert } from './assert.js'

test('a bug names the package without a version', () => {
  expect(() => assert(false, 'invariant')).toThrow(/^\[@telefunc\/redis\]\[Bug\] .* invariant$/)
})
