export { getContext_sync }
export { provideTelefuncContext_sync }
export { restoreContext_sync }
export { withoutContext_sync }

import { getGlobalObject } from '../../../utils/getGlobalObject.js'
import { assertUsage } from '../../../utils/assert.js'
import { isObject } from '../../../utils/isObject.js'
import { PROVIDED_CONTEXT } from './getContext.js'
import type { Context } from './context.js'
import type { Telefunc } from './TelefuncNamespace.js'

const syncState = getGlobalObject<{ context: Context | null }>('context/sync.ts', {
  context: null,
})

function getContext_sync(): Context | null {
  return syncState.context
}

function withoutContext_sync<Args extends unknown[], R>(fn: (...args: Args) => R, ...args: Args): R {
  const { context } = syncState
  syncState.context = null
  try {
    return fn(...args)
  } finally {
    syncState.context = context
  }
}

function provideTelefuncContext_sync(context: Telefunc.Context) {
  assertUsage(isObject(context), '[provideTelefuncContext(context)] Argument `context` should be an object')
  syncState.context = { [PROVIDED_CONTEXT]: context }
  setTimeout(() => {
    syncState.context = null
  }, 0)
}

function restoreContext_sync<T>(context: Context, fn: () => T): T {
  syncState.context = context
  // We don't use process.nextTick() to avoid dependency on Node.js
  setTimeout(() => {
    syncState.context = null
  }, 0)
  return fn()
}
