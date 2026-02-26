export { getContext_sync }
export { provideTelefuncContext_sync }
export { restoreContext_sync }

import { assert, assertUsage } from '../../../utils/assert.js'
import { getGlobalObject } from '../../../utils/getGlobalObject.js'
import { isObject } from '../../../utils/isObject.js'
import type { Telefunc } from './TelefuncNamespace.js'

const globalObject = getGlobalObject<{
  context: null | Telefunc.Context
  hasRestoreAccess: boolean
  neverProvided: boolean
  neverRestored: boolean
}>('getContext/sync.ts', {
  context: null,
  hasRestoreAccess: false,
  neverProvided: true,
  neverRestored: true,
})

function getContext_sync(): Telefunc.Context {
  if (globalObject.context === null) {
    // Using `neverRestored` to detect SSR doesn't always work.
    //  - Reliable alternative: use an Async Hook in dev to detect SSR.
    if (globalObject.neverRestored) {
      assertUsage(
        false,
        'Using Telefunc to fetch the initial data of your page is discouraged, see https://telefunc.com/initial-data',
      )
    }
    assertUsage(false, '[getContext()] Cannot access context object, see https://telefunc.com/getContext#access')
  }
  assert(isObject(globalObject.context))
  return globalObject.context
}

function restoreContext_sync(context: null | Telefunc.Context) {
  globalObject.neverRestored = false
  provide(context)
}

function provideTelefuncContext_sync(context: Telefunc.Context) {
  assertUsage(isObject(context), '[provideTelefuncContext(context)] Argument `context` should be an object')
  provide(context)
}

function provide(context: null | Telefunc.Context) {
  assert(context === null || isObject(context))
  if (context) {
    globalObject.neverProvided = false
  }
  // Always initialize context — use {} if no user context provided so getContext() works
  // inside a telefunc execution for built-in methods like onConnectionAbort().
  globalObject.context = context ?? ({} as Telefunc.Context)
  globalObject.hasRestoreAccess = true
  // We don't use process.nextTick() to avoid dependency on Node.js
  setTimeout(() => {
    globalObject.context = null
    globalObject.hasRestoreAccess = false
  }, 0)
}
