import { expect, test, vi } from 'vitest'

import { getContext, provideTelefuncContext } from './getContext.js'

// Stands in for telefunc@0.3.1 loaded first: the `restoreContext` it registers on `globalThis` keeps the context until a timer fires.
vi.hoisted(() => {
  const syncState: { context: object | null } = { context: null }
  Object.assign(globalThis, {
    _telefunc: {
      'context/sync.ts': syncState,
      'context.ts': {
        getContext: () => syncState.context,
        restoreContext<T>(context: object, fn: () => T): T {
          syncState.context = context
          setTimeout(() => {
            syncState.context = null
          }, 0)
          return fn()
        },
        neverRestored: true,
        isAsyncMode: false,
      },
    },
  })
})

/** The user `getContext()` gives the code calling it, or `'throws'`. */
function readUser(): string {
  try {
    return getContext<{ user: string }>().user
  } catch {
    return 'throws'
  }
}

test('code resumed after the `fn` of another caller returned does not read the context provided to that `fn`', async () => {
  const userAfterAwait = provideTelefuncContext({ user: 'A' }, async () => {
    expect(readUser()).toBe('A')
    await Promise.resolve()
    return readUser()
  })
  provideTelefuncContext({ user: 'B' }, () => {})
  expect(await userAfterAwait).toBe('throws')
})
