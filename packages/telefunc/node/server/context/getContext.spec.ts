// getContext() once AsyncLocalStorage is installed, inside an adapter's scope (telefunc/cloudflare's running object).

import { AsyncLocalStorage } from 'node:async_hooks'
import { expect, test } from 'vitest'
import { installAsyncContext } from './async.js'
import { restoreContext } from './context.js'
import { PROVIDED_CONTEXT, getContext } from './getContext.js'

installAsyncContext(AsyncLocalStorage)
const RUNNING_OBJECT = Symbol.for('telefunc.cloudflare.object')

test("getContext() throws in an adapter's scope that holds none of the app's context", () => {
  restoreContext({ [RUNNING_OBJECT]: 'telefunc-shard-weur-0' }, () => {
    expect(() => getContext()).toThrow('Cannot access context object')
  })
})

test("a telefunction's context, inside an adapter's scope, is read after an await, beside the adapter's key", async () => {
  await restoreContext({ [RUNNING_OBJECT]: 'telefunc-shard-weur-0' }, () =>
    restoreContext({ [PROVIDED_CONTEXT]: { user: 'ada' } }, async () => {
      await new Promise((resolve) => setTimeout(resolve, 1))
      expect(getContext<{ user: string }>().user).toBe('ada')
    }),
  )
})
