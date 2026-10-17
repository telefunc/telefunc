import { expect, test, vi } from 'vitest'
import './async.js'
import { getRawContext, restoreContext } from './context.js'
import { PROVIDED_CONTEXT } from './getContext.js'

test('restores a serve() context without warning when provideTelefuncContext() was never used', () => {
  const warn = vi.spyOn(console, 'warn').mockImplementation(() => {})
  const seen = restoreContext({ [PROVIDED_CONTEXT]: { user: 'u1' } }, () => getRawContext()?.[PROVIDED_CONTEXT])
  expect(seen).toEqual({ user: 'u1' })
  expect(warn).not.toHaveBeenCalled()
  warn.mockRestore()
})
