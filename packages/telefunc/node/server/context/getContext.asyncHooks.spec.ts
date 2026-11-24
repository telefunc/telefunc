import '../async_hooks.js'
import { expect, test } from 'vitest'

import { getContext, provideTelefuncContext } from './getContext.js'

/** The user `getContext()` gives the code calling it, or `'throws'`. */
function readUser(): string {
  try {
    return getContext<{ user: string }>().user
  } catch {
    return 'throws'
  }
}

async function onReadAroundAwait() {
  const before = readUser()
  await Promise.resolve()
  return { before, after: readUser() }
}

test('provideTelefuncContext(context, fn) gives each of two callers started together its own context before and after an `await`', async () => {
  const returns = await Promise.all([
    provideTelefuncContext({ user: 'A' }, onReadAroundAwait),
    provideTelefuncContext({ user: 'B' }, onReadAroundAwait),
  ])
  expect(returns).toEqual([
    { before: 'A', after: 'A' },
    { before: 'B', after: 'B' },
  ])
})

test('the context provideTelefuncContext(context, fn) provides ends with `fn`', async () => {
  expect(await provideTelefuncContext({ user: 'A' }, onReadAroundAwait)).toEqual({ before: 'A', after: 'A' })
  expect(readUser()).toBe('throws')
})

test('provideTelefuncContext(context, undefined) throws', () => {
  // @ts-expect-error
  expect(() => provideTelefuncContext({ user: 'A' }, undefined)).toThrow()
  expect(readUser()).toBe('throws')
})

test('provideTelefuncContext(context) provides the context to the code that runs after it', async () => {
  provideTelefuncContext<{ user: string }>({ user: 'A' })
  expect(await onReadAroundAwait()).toEqual({ before: 'A', after: 'A' })
})
