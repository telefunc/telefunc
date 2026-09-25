export { provideTelefuncContext_async }

import { AsyncLocalStorage } from 'node:async_hooks'
import { assert, assertWarning, assertUsage } from '../../../utils/assert.js'
import { getGlobalObject } from '../../../utils/getGlobalObject.js'
import { isObject } from '../../../utils/isObject.js'
import { installAsyncMode } from './context.js'
import { getSyncContext, provideTelefuncContext_sync } from './sync.js'
import { PROVIDED_CONTEXT } from './getContext.js'
import type { Context } from './context.js'
import type { Telefunc } from './TelefuncNamespace.js'

const globalObject = getGlobalObject<{ asyncStore?: AsyncLocalStorage<Context>; provided?: true }>(
  'getContext/async.ts',
  {},
)

installAsyncMode({
  provideTelefuncContext_async,
  restoreContext_async,
  // Where the runtime lacks enterWith (workerd), provided context is held the sync way; a scope's store still wins.
  getContextStore: () => globalObject.asyncStore?.getStore() ?? getSyncContext(),
})

function provideTelefuncContext_async(context: Telefunc.Context): void {
  assertUsage(isObject(context), '[provideTelefuncContext(context)] Argument `context` should be an object')
  globalObject.provided = true
  globalObject.asyncStore = globalObject.asyncStore ?? new AsyncLocalStorage()
  if (typeof globalObject.asyncStore.enterWith !== 'function') return provideTelefuncContext_sync(context)
  globalObject.asyncStore.enterWith({ [PROVIDED_CONTEXT]: context })
}

function restoreContext_async<T>(rawContext: Context, fn: () => T): T {
  assert(isObject(rawContext))
  // Async mode also serves adapters (Cloudflare's session scope), so only an app that provides its own context is told.
  assertWarning(
    !(globalObject.provided && rawContext[PROVIDED_CONTEXT]),
    'When using `provideTelefuncContext()` (i.e. Async Hooks), then providing the `context` object to the server middleware `serve()` has no effect.',
    { onlyOnce: true },
  )
  globalObject.asyncStore = globalObject.asyncStore ?? new AsyncLocalStorage()
  return globalObject.asyncStore.run(rawContext, fn)
}
