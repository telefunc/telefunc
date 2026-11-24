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
  const previous = syncState.context
  syncState.context = context
  try {
    return fn()
  } finally {
    syncState.context = previous
  }
}
