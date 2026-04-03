export { createRequestContext }
export { restoreRequestContext }
export { getRequestContext }
export { installAsyncRequestContext }
export type { RequestContext, ResponseAbortSource }

import { getGlobalObject } from '../../utils/getGlobalObject.js'
import { Abort } from './Abort.js'
import type { AbortError } from './Abort.js'

type ResponseAbortSource = {
  /** Abort the whole telefunc response with Abort semantics. Fires exactly once. */
  abort: (abortValue?: unknown) => void
  /** Register a callback fired when the response aborts. Fires exactly once. */
  onAbort: (cb: (abortError: AbortError) => void) => void
  /** Promise rejected with the Abort error once the response aborts. */
  errorPromise: Promise<never>
}

/** Internal per-request state. */
type RequestContext = {
  /** The current Web Request for this telefunc execution. */
  request: Request
  /** The POST request's AbortSignal. */
  abortSignal: AbortSignal
  /** AbortSignal that fires when all channels, streams, and pumps are done (same timing as onClose). */
  signal: AbortSignal
  /** Response-wide abort source used by returned values and response transports. */
  responseAbort: ResponseAbortSource
  /** Fires when the telefunction throws (or has already thrown) at the top level. */
  onTopLevelError: (cb: () => void) => void
  /** @internal Called by the framework after executeTelefunction catches a top-level throw. */
  markTopLevelError: () => void
  /** Register a callback that fires when the request lifecycle ends for any reason
   *  (response sent, stream complete, or client disconnect). Fires exactly once. */
  onClose: (cb: () => void) => void
  /** Mark the response as complete.
   *  Fires onClose callbacks. */
  markComplete: () => void
}

/** Create a RequestContext and wire the abort signal to markComplete(). */
function createRequestContext(request: Request): RequestContext {
  const closeCallbacks: Array<() => void> = []
  let closed = false

  const responseAbort = createResponseAbortSource(() => fireClose())

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
  }

  const errorCallbacks: Array<() => void> = []
  let errored = false
  const fireError = () => {
    if (errored) return
    errored = true
    for (const cb of errorCallbacks) {
      try {
        cb()
      } catch {}
    }
    errorCallbacks.length = 0
  }

  const closeController = new AbortController()
  closeCallbacks.push(() => closeController.abort())

  const ctx: RequestContext = {
    request,
    abortSignal: request.signal,
    signal: closeController.signal,
    responseAbort,
    onTopLevelError(cb) {
      if (errored) {
        cb()
        return
      }
      errorCallbacks.push(cb)
    },
    markTopLevelError: fireError,
    onClose(cb) {
      if (closed) {
        cb()
        return
      }
      closeCallbacks.push(cb)
    },
    markComplete: fireClose,
  }

  // Wire abort signal → markComplete
  if (request.signal.aborted) {
    fireClose()
  } else {
    request.signal.addEventListener('abort', () => fireClose(), { once: true })
  }

  return ctx
}

function createResponseAbortSource(onAbort: () => void): ResponseAbortSource {
  const abortCallbacks: Array<(abortError: AbortError) => void> = []
  let aborted = false
  let abortError: AbortError | null = null
  let rejectAbortPromise: ((err: unknown) => void) | null = null
  const errorPromise = new Promise<never>((_resolve, reject) => {
    rejectAbortPromise = reject
  })
  errorPromise.catch(() => {})

  return {
    abort(abortValue) {
      if (aborted) return
      aborted = true
      abortError = Abort(abortValue)
      rejectAbortPromise?.(abortError)
      for (const cb of abortCallbacks) {
        try {
          cb(abortError)
        } catch {
          // Internal abort callbacks are silently swallowed
        }
      }
      abortCallbacks.length = 0
      onAbort()
    },
    onAbort(cb) {
      if (abortError) {
        cb(abortError)
        return
      }
      abortCallbacks.push(cb)
    },
    errorPromise,
  }
}

type GetRequestContext = () => RequestContext | null
type RestoreRequestContext = <T>(ctx: RequestContext | null, fn: () => T) => T

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

function restoreRequestContext_sync<T>(ctx: RequestContext | null, fn: () => T): T {
  syncState.requestContext = ctx
  // Same lifecycle as user context: cleared on next macrotask
  setTimeout(() => {
    syncState.requestContext = null
  }, 0)
  return fn()
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

function restoreRequestContext<T>(ctx: RequestContext | null, fn: () => T): T {
  return globalObject.restoreRequestContext(ctx, fn)
}

function getRequestContext(): RequestContext | null {
  return globalObject.getRequestContext()
}
