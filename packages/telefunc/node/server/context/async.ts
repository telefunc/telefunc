export { installAsyncContext }

import type { AsyncLocalStorage } from 'node:async_hooks'
import { assert, assertWarning, assertUsage } from '../../../utils/assert.js'
import { getGlobalObject } from '../../../utils/getGlobalObject.js'
import { isObject } from '../../../utils/isObject.js'
import { installAsyncMode } from './context.js'
import { PROVIDED_CONTEXT } from './getContext.js'
import { getSyncContext, provideTelefuncContext_sync } from './sync.js'
import type { Context } from './context.js'
import type { Telefunc } from './TelefuncNamespace.js'

const globalObject = getGlobalObject<{ asyncStore?: AsyncLocalStorage<Context>; provided?: true }>(
  'getContext/async.ts',
  {},
)

/** Takes `AsyncLocalStorage` from its caller, so that a runtime that may lack `node:async_hooks` loads it itself. */
function installAsyncContext(AsyncLocalStorageClass: typeof AsyncLocalStorage): void {
  globalObject.asyncStore = globalObject.asyncStore ?? new AsyncLocalStorageClass()
  installAsyncMode({
    provideTelefuncContext_async,
    restoreContext_async,
    getContextStore: () => globalObject.asyncStore?.getStore() ?? getSyncContext(),
  })
}

function provideTelefuncContext_async(context: Telefunc.Context): void {
  assertUsage(isObject(context), '[provideTelefuncContext(context)] Argument `context` should be an object')
  const { asyncStore } = globalObject
  assert(asyncStore)
  globalObject.provided = true
  if (enterWith(asyncStore, { [PROVIDED_CONTEXT]: context })) return
  // Where it can't (workerd), the context joins the scope it is provided in, or outside any lasts until the next timer.
  const scope = asyncStore.getStore()
  if (scope) scope[PROVIDED_CONTEXT] = context
  else provideTelefuncContext_sync(context)
}

/** False where the runtime has no `enterWith()`, or one that throws, as workerd's does. */
function enterWith(asyncStore: AsyncLocalStorage<Context>, store: Context): boolean {
  try {
    asyncStore.enterWith(store)
    return true
  } catch {
    return false
  }
}

function restoreContext_async<T>(rawContext: Context, fn: () => T): T {
  assert(isObject(rawContext))
  // Async mode also serves adapters (the Durable Object Cloudflare's code runs in), so only an app that provides its own
  // context is told.
  assertWarning(
    !(globalObject.provided && rawContext[PROVIDED_CONTEXT]),
    'When using `provideTelefuncContext()` (i.e. Async Hooks), then providing the `context` object to the server middleware `serve()` has no effect.',
    { onlyOnce: true },
  )
  assert(globalObject.asyncStore)
  // A nested scope, such as a telefunction's call in a Durable Object's scope, overrides only the keys it sets.
  return globalObject.asyncStore.run({ ...globalObject.asyncStore.getStore(), ...rawContext }, fn)
}
