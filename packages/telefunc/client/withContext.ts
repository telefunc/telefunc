export { withContext }
export type { ClientCallContext }

import { getGlobalObject } from '../utils/getGlobalObject.js'

/** Per-call context options for the HTTP transport layer. */
type ClientCallContext = {
  /** AbortSignal to cancel the telefunc call. */
  signal?: AbortSignal
  /** Additional HTTP headers for this call. */
  headers?: Record<string, string>
}

const globalObject = getGlobalObject<{ pendingContext: ClientCallContext | null }>('withContext.ts', {
  pendingContext: null,
})

/** Read the pending context set by withContext().
 *
 *  Must be called synchronously inside remoteTelefunctionCall() —
 *  before any await — to capture the context while it's still set.
 *  The context is reset by withContext()'s finally block after the call returns.
 */
export function getPendingContext(): ClientCallContext | null {
  return globalObject.pendingContext
}

/** Wrap a telefunc function with per-call context (signal, headers).
 *
 *  ```ts
 *  import { withContext } from 'telefunc/client'
 *  const call = withContext(onLoadTodoItem, { signal, headers: { Priority: 'u=0' } })
 *  const res = await call(id)
 *  ```
 */
function withContext<F extends (...args: any[]) => any>(telefunc: F, context: ClientCallContext): F {
  return ((...args: any[]) => {
    globalObject.pendingContext = context
    try {
      // telefunc(...args) synchronously enters remoteTelefunctionCall,
      // which reads globalObject.pendingContext before any await.
      return telefunc(...args)
    } finally {
      globalObject.pendingContext = null
    }
  }) as F
}
