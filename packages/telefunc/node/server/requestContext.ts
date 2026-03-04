export { createRequestContext }
export { restoreRequestContext }
export { getRequestContext }
export { installAsyncRequestContext }
export type { RequestContext }

import { getGlobalObject } from '../../utils/getGlobalObject.js'

/** Internal per-request state. */
type RequestContext = {
  /** The request's AbortSignal — fires when the client disconnects. */
  abortSignal: AbortSignal
  /** Register a callback that fires when the request lifecycle ends for any reason
   *  (response sent, stream complete, or client disconnect). Fires exactly once. */
  onConnectionClose: (cb: () => void) => void
  /** Register a callback that fires only when the client disconnects unexpectedly
   *  (before the response/stream completes). Does NOT fire on normal completion. */
  onConnectionAbort: (cb: () => void) => void
  /** Mark the response as complete (stream finished or non-streaming body ready).
   *  Fires onConnectionClose callbacks. Does NOT fire onConnectionAbort. */
  markComplete: () => void
  /** Mark as aborted (client disconnected unexpectedly).
   *  Fires onConnectionAbort callbacks, then onConnectionClose callbacks. */
  markAborted: () => void
}

/** Create a RequestContext and wire the abort signal to markAborted(). */
function createRequestContext(abortSignal: AbortSignal): RequestContext {
  const closeCallbacks: Array<() => void> = []
  const abortCallbacks: Array<() => void> = []
  let closed = false
  let aborted = false

  const fireClose = () => {
    if (closed) return
    closed = true
    for (const cb of closeCallbacks) {
      try {
        cb()
      } catch {
        // User callback errors are silently swallowed
      }
    }
    closeCallbacks.length = 0
    abortCallbacks.length = 0
  }

  const fireAbort = () => {
    if (closed) return
    aborted = true
    for (const cb of abortCallbacks) {
      try {
        cb()
      } catch {
        // User callback errors are silently swallowed
      }
    }
    abortCallbacks.length = 0
    fireClose()
  }

  const ctx: RequestContext = {
    abortSignal,
    onConnectionClose(cb) {
      if (closed) {
        cb()
        return
      }
      closeCallbacks.push(cb)
    },
    onConnectionAbort(cb) {
      // Already closed via abort → fire immediately
      if (closed && aborted) {
        cb()
        return
      }
      // Already closed normally → abort never happened
      if (closed) return
      abortCallbacks.push(cb)
    },
    markComplete: fireClose,
    markAborted: fireAbort,
  }

  // Wire abort signal → markAborted (centralized here, not scattered across callers)
  if (abortSignal.aborted) {
    fireAbort()
  } else {
    abortSignal.addEventListener('abort', () => fireAbort(), { once: true })
  }

  return ctx
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
