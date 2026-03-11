export { abort, setAbortController }

import { assertUsage } from '../utils/assert.js'

const ABORT_CONTROLLER = Symbol.for('telefuncAbort')

type TelefuncCall = object | Function

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
 *  Aborts the underlying fetch. Rejects with an `Abort` error;
 *  for streaming calls mid-stream, the next read rejects instead.
 *
 *  Works with promises, async generators, or multiplexed return objects
 *  (objects containing generators/streams/promises).
 *
 *  ```ts
 *  import { abort } from 'telefunc/client'
 *  const call = onSlowTelefunc()
 *  abort(call)
 *
 *  // Also works on awaited multiplexed returns:
 *  const res = await onDashboard()
 *  abort(res)
 *  ```
 */
function abort(call: TelefuncCall): void {
  const controller = getAbortController(call)
  assertUsage(controller, '`abort()`: the argument is not a pending telefunc call')
  controller.abort()
}
