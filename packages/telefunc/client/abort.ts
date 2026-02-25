export { abort, withAbort, setAbortController }

import { isAsyncGenerator } from '../utils/isAsyncGenerator.js'

const ABORT_CONTROLLER = Symbol.for('telefuncAbort')

type TelefuncCall = Promise<unknown> | AsyncGenerator<unknown>

type WithAbortController = { [ABORT_CONTROLLER]?: AbortController }

function setAbortController(call: TelefuncCall, controller: AbortController): void {
  const p = call as WithAbortController
  p[ABORT_CONTROLLER] = controller
}

function getAbortController(call: TelefuncCall): AbortController | undefined {
  const p = call as WithAbortController
  return p[ABORT_CONTROLLER]
}

/** Immediately abort a pending telefunc call.
 *
 *  ```ts
 *  import { abort } from 'telefunc/client'
 *  const call = onSlowTelefunc()
 *  abort(call)
 *  ```
 */
function abort(call: TelefuncCall): void {
  // If an async generator is active, close it first so the stream reader
  // is cancelled cleanly before the fetch is aborted.
  if (isAsyncGenerator(call)) {
    call.return(undefined)
  }
  const controller = getAbortController(call)
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
function withAbort<T extends TelefuncCall>(call: T, signal: AbortSignal): T {
  const controller = getAbortController(call)
  if (controller) {
    if (signal.aborted) {
      controller.abort()
    } else {
      signal.addEventListener('abort', () => controller.abort(), { once: true })
    }
  }
  return call
}
