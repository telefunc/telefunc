export { createRequestContext }
export { REQUEST_CONTEXT }
export type { RequestContext, ResponseAbortSource }

import { Abort } from '../Abort.js'
import type { AbortError } from '../Abort.js'
import { handleCallbackBug } from '../runTelefunc/validateTelefunctionError.js'
import { isPromise } from '../../../utils/isPromise.js'

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
  /** Fires once the call answers with a status other than 200; at once if it already has. */
  onCallFailed: (cb: () => void) => void
  /** @internal Called by `runTelefunc` once the call answered with a status other than 200. */
  markCallFailed: () => void
  /** Register a callback that fires when the request lifecycle ends for any reason
   *  (response sent, stream complete, or client disconnect). Fires exactly once. */
  onClose: (cb: () => void) => void
  /** The call ended, whichever way: fires onClose callbacks once no item of trackPending is pending. */
  markComplete: () => void
  /** Take a hold (a channel, a stream pump, an inline body) and return its one-shot release.
   *  onClose callbacks fire once no hold is left. */
  trackPending: () => () => void
}

/** Create a RequestContext and wire the abort signal to markComplete. */
function createRequestContext(request: Request): RequestContext {
  const closeCallbacks: Array<() => void> = []
  let closed = false

  const fireClose = () => {
    if (closed) return
    closed = true
    for (const cb of closeCallbacks) invokeCloseCallback(cb)
    closeCallbacks.length = 0
  }

  const failedCallbacks: Array<() => void> = []
  let failed = false
  const fireFailed = () => {
    if (failed) return
    failed = true
    for (const cb of failedCallbacks) {
      try {
        cb()
      } catch {}
    }
    failedCallbacks.length = 0
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
  // markComplete is the initial hold — released when the call ends, whichever way,
  // or when the client disconnects, whichever comes first.
  const markComplete = trackPending()

  const responseAbort = createResponseAbortSource(markComplete)

  const ctx: RequestContext = {
    request,
    abortSignal: request.signal,
    signal: closeController.signal,
    responseAbort,
    onCallFailed(cb) {
      if (failed) {
        cb()
        return
      }
      failedCallbacks.push(cb)
    },
    markCallFailed: fireFailed,
    onClose(cb) {
      if (closed) {
        invokeCloseCallback(cb)
        return
      }
      closeCallbacks.push(cb)
    },
    markComplete,
    trackPending,
  }

  // request.signal fires when the page disconnects: it releases the call's hold, so onClose waits only for what
  // the response still carries.
  if (request.signal.aborted) {
    markComplete()
  } else {
    request.signal.addEventListener('abort', markComplete, { once: true })
  }

  return ctx
}

function createResponseAbortSource(releaseCallHold: () => void): ResponseAbortSource {
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
      releaseCallHold()
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

function invokeCloseCallback(cb: () => void): void {
  try {
    const result = cb()
    if (isPromise(result)) result.catch(handleCallbackBug)
  } catch (err) {
    handleCallbackBug(err)
  }
}
