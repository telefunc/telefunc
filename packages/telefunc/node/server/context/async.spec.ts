import { AsyncLocalStorage } from 'node:async_hooks'
import { beforeAll, expect, test } from 'vitest'
import { installAsyncContext } from './async.js'
import { getRawContext, restoreContext, withoutContext, type Context } from './context.js'
import { PROVIDED_CONTEXT, WITHHELD_CONTEXT } from './getContext.js'

/** An adapter's own key, as Cloudflare's session scope sets one around the requests it restores. */
const SESSION = Symbol('session')
/** A key the nested scope sets itself. */
const REQUEST = Symbol('request')

beforeAll(() => installAsyncContext(AsyncLocalStorage))

/** Each way the outer scope carries a context of its own: one it provides, and one it withholds. */
const OUTER_SCOPES: Record<string, (fn: () => Context | null) => Context | null> = {
  provides: (fn) => restoreContext({ [SESSION]: 'session', [PROVIDED_CONTEXT]: { user: 'alice' } }, fn),
  withholds: (fn) => restoreContext({ [SESSION]: 'session' }, () => withoutContext(fn)),
}

for (const [outer, inOuterScope] of Object.entries(OUTER_SCOPES)) {
  test(`a scope restored inside one that ${outer} a context keeps the outer adapter's keys and none of that context`, () => {
    const nested = inOuterScope(() => restoreContext({ [REQUEST]: 'request' }, getRawContext))
    expect(nested?.[SESSION]).toBe('session')
    expect(nested?.[REQUEST]).toBe('request')
    expect(nested?.[PROVIDED_CONTEXT]).toBeUndefined()
    expect(nested?.[WITHHELD_CONTEXT]).toBeUndefined()
  })

  test(`a scope restored inside one that ${outer} a context reads the context it provides itself`, () => {
    const nested = inOuterScope(() => restoreContext({ [PROVIDED_CONTEXT]: { user: 'bob' } }, getRawContext))
    expect(nested?.[PROVIDED_CONTEXT]).toEqual({ user: 'bob' })
    expect(nested?.[WITHHELD_CONTEXT]).toBeUndefined()
  })
}
