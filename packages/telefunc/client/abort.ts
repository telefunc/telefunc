export { abort, withAbort, setAbortController }

const ABORT_CONTROLLER = Symbol.for('telefuncAbort')

type WithAbortController = { [ABORT_CONTROLLER]?: AbortController }

function setAbortController(promise: Promise<unknown>, controller: AbortController): void {
  const p = promise as WithAbortController
  p[ABORT_CONTROLLER] = controller
}

function getAbortController(promise: Promise<unknown>): AbortController | undefined {
  const p = promise as WithAbortController
  return p[ABORT_CONTROLLER]
}

/** Immediately abort a pending telefunc call.
 *
 *  ```ts
 *  import { abort } from 'telefunc/client'
 *  const promise = onSlowTelefunc()
 *  abort(promise)
 *  ```
 */
function abort(promise: Promise<unknown>): void {
  const controller = getAbortController(promise)
  if (controller) controller.abort()
}

/** Wire an AbortSignal to a telefunc call. Returns the same promise for chaining.
 *
 *  ```ts
 *  import { withAbort } from 'telefunc/client'
 *  const controller = new AbortController()
 *  const value = await withAbort(onSlowTelefunc(), controller.signal)
 *  // later: controller.abort()
 *  ```
 */
function withAbort<T>(promise: Promise<T>, signal: AbortSignal): Promise<T> {
  const controller = getAbortController(promise)
  if (controller) {
    if (signal.aborted) {
      controller.abort()
    } else {
      signal.addEventListener('abort', () => controller.abort(), { once: true })
    }
  }
  return promise
}
