export { provideTelefuncContext_async }

import { AsyncLocalStorage } from 'node:async_hooks'
import { assert, assertWarning, assertUsage } from '../../../utils/assert.js'
import { getGlobalObject } from '../../../utils/getGlobalObject.js'
import { isObject } from '../../../utils/isObject.js'
import { installAsyncMode } from './context.js'
import { PROVIDED_CONTEXT } from './getContext.js'
import type { Context } from './context.js'

const globalObject = getGlobalObject<{ asyncStore?: AsyncLocalStorage<Context> }>('getContext/async.ts', {})

installAsyncMode({
  getContext_async,
  provideTelefuncContext_async,
  restoreContext_async,
})

function getContext_async() {
  return globalObject.asyncStore?.getStore() ?? null
}

function provideTelefuncContext_async(rawContext: Context, fn?: () => unknown): unknown {
  globalObject.asyncStore = globalObject.asyncStore ?? new AsyncLocalStorage()
  if (fn) return globalObject.asyncStore.run(rawContext, fn)
  assertUsage(
    typeof globalObject.asyncStore.enterWith === 'function',
    '[provideTelefuncContext()] This runtime does not support AsyncLocalStorage.enterWith(). Pass context directly to serve() instead.',
  )
  globalObject.asyncStore.enterWith(rawContext)
}

function restoreContext_async<T>(rawContext: Context, fn: () => T): T {
  assert(isObject(rawContext))
  assertWarning(
    !rawContext[PROVIDED_CONTEXT],
    'When using `provideTelefuncContext()` (i.e. Async Hooks), then providing the `context` object to the server middleware `serve()` has no effect.',
    { onlyOnce: true },
  )
  globalObject.asyncStore = globalObject.asyncStore ?? new AsyncLocalStorage()
  return globalObject.asyncStore.run(rawContext, fn)
}
