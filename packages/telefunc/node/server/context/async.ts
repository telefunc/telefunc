import { AsyncLocalStorage } from 'node:async_hooks'
import { assert } from '../../../utils/assert.js'
import { getGlobalObject } from '../../../utils/getGlobalObject.js'
import { isObject } from '../../../utils/isObject.js'
import { installAsyncMode } from './context.js'
import type { Context } from './context.js'

const globalObject = getGlobalObject<{ asyncStore?: AsyncLocalStorage<Context> }>('getContext/async.ts', {})

installAsyncMode({
  getContext_async,
  restoreContext_async,
})

function getContext_async() {
  return globalObject.asyncStore?.getStore() ?? null
}

function restoreContext_async<T>(rawContext: Context, fn: () => T): T {
  assert(isObject(rawContext))
  globalObject.asyncStore = globalObject.asyncStore ?? new AsyncLocalStorage()
  return globalObject.asyncStore.run(rawContext, fn)
}
