export { createRequestContext }
export { REQUEST_CONTEXT }
export type { RequestContext, ResponseAbortSource }

import { Abort } from '../Abort.js'
import type { AbortError } from '../Abort.js'

const REQUEST_CONTEXT: unique symbol = Symbol.for('telefunc.requestContext')

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
  /** Mark the response as complete. Fires onClose callbacks.
   *  Gated by trackPending: if pending items exist, defers until all complete. */
  markComplete: () => void
  /** Register a pending item (channel, stream pump, inline stream).
   *  Returns a completion callback. markComplete is deferred until all pending items complete. */
  trackPending: () => () => void
}

/** Create a RequestContext and wire the abort signal to markComplete. */
function createRequestContext(request: Request): RequestContext {
  const closeCallbacks: Array<() => void> = []
  let closed = false

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

  // Reference counting: fireClose fires when all holds are released.
  // Each trackPending() adds a hold and returns a one-shot release function.
  let pendingCount = 0
  const trackPending = () => {
    pendingCount++
    let released = false
    return () => {
      if (released) return
      released = true
      if (--pendingCount === 0) fireClose()
    }
  }
  // markComplete is the initial hold — released when serialization finishes
  // or when the client disconnects, whichever comes first.
  const markComplete = trackPending()

  const responseAbort = createResponseAbortSource(markComplete)

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
    markComplete,
    trackPending,
  }

  // request.signal fires on abnormal client disconnect.
  // During telefunc execution (pendingCount=1): releasePending fires onClose immediately.
  // After serialization (pending channels/streams): defers until they all complete.
  if (request.signal.aborted) {
    markComplete()
  } else {
    request.signal.addEventListener('abort', markComplete, { once: true })
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
