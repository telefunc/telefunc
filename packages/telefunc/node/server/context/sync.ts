export { getContext_sync }
export { restoreContext_sync }

import { getGlobalObject } from '../../../utils/getGlobalObject.js'
import type { Context } from './context.js'

const syncState = getGlobalObject<{ context: Context | null }>('context/sync.ts', {
  context: null,
})

function getContext_sync(): Context | null {
  return syncState.context
}

function restoreContext_sync<T>(context: Context, fn: () => T): T {
  // Calls can be nested: a callback given to `getContext().onClose()` runs inside the telefunction when the request is already aborted
  const previous = syncState.context
  syncState.context = context
  try {
    return fn()
  } finally {
    // Only while fn() runs synchronously, because any form of queuing leaks the context to other requests
    // https://github.com/telefunc/telefunc/pull/520#issuecomment-7134162671
    syncState.context = previous
  }
}
