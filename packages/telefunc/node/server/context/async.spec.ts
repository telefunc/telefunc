import { expect, test, vi } from 'vitest'
import { AsyncLocalStorage } from 'node:async_hooks'
import './async.js'
import { getRawContext, provideContext, restoreContext } from './context.js'
import { PROVIDED_CONTEXT } from './getContext.js'

// First: the warning is once per process, and a later test provides a context.
test('restores a serve() context without warning when provideTelefuncContext() was never used', () => {
  const warn = vi.spyOn(console, 'warn').mockImplementation(() => {})
  const seen = restoreContext({ [PROVIDED_CONTEXT]: { user: 'u1' } }, () => getRawContext()?.[PROVIDED_CONTEXT])
  expect(seen).toEqual({ user: 'u1' })
  expect(warn).not.toHaveBeenCalled()
  warn.mockRestore()
})

test("a restored context keeps the enclosing scope's other keys", () => {
  const ADAPTER = Symbol('adapter')
  const REQUEST = Symbol('request')
  const seen = restoreContext({ [ADAPTER]: 'outer', [REQUEST]: 'outer' }, () =>
    restoreContext({ [REQUEST]: 'inner' }, () => getRawContext()),
  )
  expect(seen?.[ADAPTER]).toBe('outer')
  expect(seen?.[REQUEST]).toBe('inner')
})

test('provides context the sync way where AsyncLocalStorage has no enterWith, as on workerd', () => {
  const { enterWith } = AsyncLocalStorage.prototype
  Object.defineProperty(AsyncLocalStorage.prototype, 'enterWith', { value: undefined, configurable: true })
  try {
    provideContext({ user: 'workerd' })
    expect(getRawContext()?.[PROVIDED_CONTEXT]).toEqual({ user: 'workerd' })
  } finally {
    Object.defineProperty(AsyncLocalStorage.prototype, 'enterWith', { value: enterWith, configurable: true })
  }
})
