export { getContext_sync }
export { restoreContext_sync }
export { withoutContext_sync }

import { getGlobalObject } from '../../../utils/getGlobalObject.js'
import type { Context } from './context.js'

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

// We provide `context` only while `fn()` runs synchronously
// - The code that runs afterwards may belong to another HTTP request => using a queue (e.g. `process.nextTick()`) means potentially leaking `context` to another HTTP request and is a security issue, see https://github.com/telefunc/telefunc/pull/520#issuecomment-7134162671
function restoreContext_sync<T>(context: Context, fn: () => T): T {
  const previous = syncState.context
  syncState.context = context
  try {
    return fn()
  } finally {
    // We restore the previous value because calls can be nested: a `getContext().onClose()` callback runs inside the telefunction when the request is already aborted
    syncState.context = previous
  }
}
