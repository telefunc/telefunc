export { abort, setAbortController }

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
 *  Aborts the underlying fetch. Rejects with a cancel error (`isCancel: true`);
 *  for streaming calls mid-stream, the next read rejects instead.
 *
 *  ```ts
 *  import { abort } from 'telefunc/client'
 *  const call = onSlowTelefunc()
 *  abort(call)
 *  ```
 */
function abort(call: TelefuncCall): void {
  const controller = getAbortController(call)
  if (controller) controller.abort()
}
