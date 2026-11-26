export { installAsyncContext }

import type { AsyncLocalStorage } from 'node:async_hooks'
import { assert, assertUsage } from '../../../utils/assert.js'
import { getGlobalObject } from '../../../utils/getGlobalObject.js'
import { isObject } from '../../../utils/isObject.js'
import { installAsyncMode } from './context.js'
import { PROVIDED_CONTEXT, WITHHELD_CONTEXT } from './getContext.js'
import { REQUEST_CONTEXT } from './requestContext.js'
import type { Context } from './context.js'
import type { Telefunc } from './TelefuncNamespace.js'

const globalObject = getGlobalObject<{ asyncStore?: AsyncLocalStorage<Context> }>('getContext/async.ts', {})

/** Takes `AsyncLocalStorage` from its caller, so that a runtime that may lack `node:async_hooks` loads it itself. */
function installAsyncContext(AsyncLocalStorageClass: typeof AsyncLocalStorage): void {
  globalObject.asyncStore = globalObject.asyncStore ?? new AsyncLocalStorageClass()
  installAsyncMode({
    getContext_async,
    provideTelefuncContext_async,
    restoreContext_async,
    withoutContext_async,
  })
}

function getContext_async() {
  return globalObject.asyncStore?.getStore() ?? null
}

function provideTelefuncContext_async(context: Telefunc.Context): void {
  assertUsage(isObject(context), '[provideTelefuncContext(context)] Argument `context` should be an object')
  assert(globalObject.asyncStore)
  assertUsage(
    typeof globalObject.asyncStore.enterWith === 'function',
    '[provideTelefuncContext()] This runtime does not support AsyncLocalStorage.enterWith(). Pass context directly to serve() instead.',
  )
  globalObject.asyncStore.enterWith({ [PROVIDED_CONTEXT]: context })
}

function restoreContext_async<T>(rawContext: Context, fn: () => T): T {
  assert(isObject(rawContext))
  assert(globalObject.asyncStore)
  // A nested scope, such as a request in Cloudflare's session scope, overrides only the keys it sets. The context it
  // restores is its own: one the outer scope provided or withheld never reaches it.
  const {
    [PROVIDED_CONTEXT]: _provided,
    [WITHHELD_CONTEXT]: _withheld,
    ...outerScope
  } = globalObject.asyncStore.getStore() ?? {}
  return globalObject.asyncStore.run({ ...outerScope, ...rawContext }, fn)
}

function withoutContext_async<Args extends unknown[], R>(fn: (...args: Args) => R, ...args: Args): R {
  assert(globalObject.asyncStore)
  const store = globalObject.asyncStore.getStore()
  // Outside any store there is no context to withhold, and inside a withheld one it is withheld already.
  if (store === undefined || store[WITHHELD_CONTEXT]) return fn(...args)
  // An adapter's scope stays: on Cloudflare, the session a publish or subscribe from `fn` goes through.
  const { [PROVIDED_CONTEXT]: _provided, [REQUEST_CONTEXT]: _request, ...adapterScope } = store
  return globalObject.asyncStore.run({ ...adapterScope, [WITHHELD_CONTEXT]: true }, fn, ...args)
}
