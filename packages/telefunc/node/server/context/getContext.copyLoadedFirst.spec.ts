import { expect, test, vi } from 'vitest'

import { getContext, provideTelefuncContext } from './getContext.js'

// What telefunc@0.3.1 registers on `globalThis` when it is loaded first: its functions keep a context until a timer fires.
vi.hoisted(() => {
  const syncState: { context: object | null } = { context: null }
  function keepUntilTimer(context: object) {
    syncState.context = context
    setTimeout(() => {
      syncState.context = null
    }, 0)
  }
  Object.assign(globalThis, {
    _telefunc: {
      'context/sync.ts': syncState,
      'context.ts': {
        getContext: () => syncState.context,
        restoreContext<T>(context: object, fn: () => T): T {
          keepUntilTimer(context)
          return fn()
        },
        provideTelefuncContext(context: object) {
          keepUntilTimer({ [Symbol.for('telefunc.providedContext')]: context })
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
    await Promise.resolve()
    return readUser()
  })
  provideTelefuncContext({ user: 'B' }, () => {})
  expect(await userAfterAwait).toBe('throws')
})

test('provideTelefuncContext(context) without `fn` throws', () => {
  expect(() => provideTelefuncContext({ user: 'A' })).toThrow()
})
