export { getContext_sync }
export { provideTelefuncContext_sync }
export { restoreContext_sync }

import { getGlobalObject } from '../../../utils/getGlobalObject.js'
import { assertUsage } from '../../../utils/assert.js'
import type { Context } from './context.js'

const syncState = getGlobalObject<{ context: Context | null }>('context/sync.ts', {
  context: null,
})

function getContext_sync(): Context | null {
  return syncState.context
}

function provideTelefuncContext_sync(context: Context, fn?: () => unknown): unknown {
  assertUsage(
    fn,
    "[provideTelefuncContext()] The context object cannot be provided this way without Async Hooks: add `import 'telefunc/async_hooks'`, or pass `context` to serve(), see https://telefunc.com/provideTelefuncContext",
  )
  return restoreContext_sync(context, fn)
}

function restoreContext_sync<T>(context: Context, fn: () => T): T {
  const previous = syncState.context
  syncState.context = context
  try {
    return fn()
  } finally {
    syncState.context = previous
  }
}
