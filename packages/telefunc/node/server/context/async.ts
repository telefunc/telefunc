export { provideTelefuncContext_async }

import { AsyncLocalStorage } from 'node:async_hooks'
import { assert, assertWarning, assertUsage } from '../../../utils/assert.js'
import { getGlobalObject } from '../../../utils/getGlobalObject.js'
import { isObject } from '../../../utils/isObject.js'
import { installAsyncMode } from './context.js'
import { PROVIDED_CONTEXT } from './getContext.js'
import type { Context } from './context.js'
import type { Telefunc } from './TelefuncNamespace.js'

const globalObject = getGlobalObject<{ asyncStore?: AsyncLocalStorage<Context> }>('getContext/async.ts', {})

installAsyncMode({
  provideTelefuncContext_async,
  restoreContext_async,
  getContextStore: () => globalObject.asyncStore?.getStore() ?? null,
})

function provideTelefuncContext_async(context: Telefunc.Context): void {
  assertUsage(isObject(context), '[provideTelefuncContext(context)] Argument `context` should be an object')
  globalObject.asyncStore = globalObject.asyncStore ?? new AsyncLocalStorage()
  assertUsage(
    typeof globalObject.asyncStore.enterWith === 'function',
    '[provideTelefuncContext()] This runtime does not support AsyncLocalStorage.enterWith(). Pass context directly to telefunc() instead.',
  )
  globalObject.asyncStore.enterWith({ [PROVIDED_CONTEXT]: context })
}

function restoreContext_async<T>(rawContext: Context, fn: () => T): T {
  assert(isObject(rawContext))
  assertWarning(
    !rawContext[PROVIDED_CONTEXT],
    'When using `provideTelefuncContext()` (i.e. Async Hooks), then providing the `context` object to the server middleware `telefunc()` has no effect.',
    { onlyOnce: true },
  )
  globalObject.asyncStore = globalObject.asyncStore ?? new AsyncLocalStorage()
  return globalObject.asyncStore.run(rawContext, fn)
}
