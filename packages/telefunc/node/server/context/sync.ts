export { restoreContext_sync }
export { provideTelefuncContext_sync }

import { getGlobalObject } from '../../../utils/getGlobalObject.js'
import { assertUsage } from '../../../utils/assert.js'
import type { Context } from './context.js'

const syncState = getGlobalObject<{ context: Context | null }>('context/sync.ts', {
  context: null,
})

function restoreContext_sync<T>(context: Context, fn: () => T): T {
  const previous = syncState.context
  syncState.context = context
  try {
    return fn()
  } finally {
    syncState.context = previous
  }
}

function provideTelefuncContext_sync() {
  assertUsage(
    false,
    "[provideTelefuncContext()] The context object cannot be provided this way without Async Hooks: add `import 'telefunc/async_hooks'`, or pass `context` to serve(), see https://telefunc.com/provideTelefuncContext",
  )
}

/** @internal — Read sync context. Used by context.ts as the default getter. */
export function getSyncContext(): Context | null {
  return syncState.context
}
