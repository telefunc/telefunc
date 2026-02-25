export { restoreRequestContext }
export { getRequestContext }
export { installAsyncRequestContext }
export type { RequestContext }

import { getGlobalObject } from '../../utils/getGlobalObject.js'

/** Internal per-request state. Extensible for future per-request internals. */
type RequestContext = {
  /** The request's AbortSignal — fires when the client disconnects. */
  abortSignal: AbortSignal
  /** Set to true when the response has been fully sent. Prevents abort callbacks from firing after completion. */
  completed: boolean
}

type GetRequestContext = () => RequestContext | null
type RestoreRequestContext = (ctx: RequestContext | null) => void

const globalObject = getGlobalObject<{
  getRequestContext: GetRequestContext
  restoreRequestContext: RestoreRequestContext
}>('requestContext.ts', {
  getRequestContext: getRequestContext_sync,
  restoreRequestContext: restoreRequestContext_sync,
})

// ── Sync mode (default) ─────────────────────────────────────────────

const syncState = getGlobalObject<{ requestContext: RequestContext | null }>('requestContext/sync.ts', {
  requestContext: null,
})

function getRequestContext_sync(): RequestContext | null {
  return syncState.requestContext
}

function restoreRequestContext_sync(ctx: RequestContext | null): void {
  syncState.requestContext = ctx
  // Same lifecycle as user context: cleared on next macrotask
  setTimeout(() => {
    syncState.requestContext = null
  }, 0)
}

// ── Async mode (AsyncLocalStorage) ──────────────────────────────────

function installAsyncRequestContext({
  getRequestContext_async,
  restoreRequestContext_async,
}: {
  getRequestContext_async: GetRequestContext
  restoreRequestContext_async: RestoreRequestContext
}): void {
  globalObject.getRequestContext = getRequestContext_async
  globalObject.restoreRequestContext = restoreRequestContext_async
}

// ── Internal accessors ──────────────────────────────────────────────

function restoreRequestContext(ctx: RequestContext | null): void {
  globalObject.restoreRequestContext(ctx)
}

function getRequestContext(): RequestContext | null {
  return globalObject.getRequestContext()
}
