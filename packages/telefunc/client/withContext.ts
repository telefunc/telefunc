export { withContext }
export type { ClientCallContext }

/** Per-call context options for the HTTP transport layer. */
type ClientCallContext = {
  /** AbortSignal to cancel the telefunc call. */
  signal?: AbortSignal
  /** Additional HTTP headers for this call. */
  headers?: Record<string, string>
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
    ;(telefunc as any)._context = context
    try {
      return telefunc(...args)
    } finally {
      ;(telefunc as any)._context = undefined
    }
  }) as F
}
