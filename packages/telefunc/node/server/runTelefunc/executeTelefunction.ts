export { executeTelefunction }

import { isAbort } from '../Abort.js'
import { restoreContext } from '../context/context.js'
import type { Context } from '../context/context.js'
import { createRequestContext } from '../context/requestContext.js'
import type { Telefunction } from '../types.js'
import type { TelefuncServerExtension } from '../extensions.js'
import { getProjectError } from '../../../utils/assert.js'
import { isPromise } from '../../../utils/isPromise.js'
import { isAsyncGenerator } from '../../../utils/isAsyncGenerator.js'
import { validateTelefunctionError } from './validateTelefunctionError.js'
import { settleLiveState } from './settleLiveState.js'
import { stampRequestStartFence } from '../live/tags.js'
import type { ConfigResolved } from '../serverConfig.js'

// Lifecycle: START (core request-start tag fence) → BODY → RESULT (every onTelefunctionResult, success
// only, as a transform chain) → SETTLED (core settleLiveState, on every path). The FIRST throw is the
// single primary error surfaced as the top-level error; later throws are logged. Core settleLiveState
// runs even on body-error/abort, so a request's queued tag invalidations still publish. A body Abort
// classifies as 'abort' (never converted to an error by a later throw).
async function executeTelefunction(runContext: {
  telefunction: Telefunction
  telefunctionName: string
  telefuncFilePath: string
  telefunctionArgs: unknown[]
  context: Context
  requestContext: ReturnType<typeof createRequestContext>
  request: Request
  requestExtensions: Record<string, Record<string, unknown>>
  serverConfig: ConfigResolved
}) {
  const { telefunction, telefunctionArgs, requestExtensions } = runContext
  const { extensions } = runContext.serverConfig

  /** Restore unified context before executing `fn`, so hooks and the body can read `getContext()`. */
  const withContext = <T>(fn: () => T): T => restoreContext(runContext.context, fn)
  const dataFor = (ext: TelefuncServerExtension): Record<string, unknown> | undefined => requestExtensions[ext.name]

  // Held in an object so a reassignment inside a hook's catch is seen at every later read (a plain
  // `let` narrowed by control-flow analysis would ignore the closure assignments).
  const phase = {
    outcome: 'success' as 'success' | 'error' | 'abort',
    primaryError: undefined as unknown,
    hasPrimaryError: false,
  }
  let telefunctionReturn: unknown

  // Record a thrown error. On the still-successful path it becomes the single primary error and flips
  // the outcome; once an error or abort is already settled, it is logged, never replacing them. Used
  // by every phase — nothing escapes before SETTLED runs.
  const raisePrimary = (err: unknown): void => {
    if (phase.outcome === 'success' && !phase.hasPrimaryError) {
      phase.hasPrimaryError = true
      phase.primaryError = err
      phase.outcome = 'error'
    } else {
      logSecondary(err)
    }
  }
  const raiseHookError = raisePrimary

  const raiseBodyError = (err: unknown): void => {
    if (isAbort(err)) {
      phase.outcome = 'abort'
      telefunctionReturn = err.abortValue
      runContext.requestContext.responseAbort.abort(err.abortValue)
      return
    }
    // `validateTelefunctionError` throws a usage error for a primitive throw / `throw Abort` — surface
    // that usage error as the primary instead of letting it escape (which would skip SETTLED).
    let surfaced = err
    try {
      validateTelefunctionError(err, runContext)
    } catch (validationError) {
      surfaced = validationError
    }
    raisePrimary(surfaced)
  }

  // ── START ── the core request-start tag fence (before the body), so a tag published between the
  // acquiring read and a later liveTag()/live handle still replays.
  try {
    await withContext(() => stampRequestStartFence())
  } catch (err) {
    raiseHookError(err)
  }

  // ── BODY ── skipped if START already produced a primary error.
  if (phase.outcome === 'success') {
    let resultSync: unknown
    try {
      resultSync = withContext(() => telefunction.apply(null, telefunctionArgs))
    } catch (err) {
      raiseBodyError(err)
    }
    if (phase.outcome === 'success') {
      if (isPromise(resultSync)) {
        try {
          telefunctionReturn = await resultSync
        } catch (err) {
          raiseBodyError(err)
        }
      } else if (isAsyncGenerator(resultSync)) {
        telefunctionReturn = resultSync
      } else {
        // A non-async return is a usage error — recorded (not thrown) so SETTLED still runs.
        raiseBodyError(returnTypeError(runContext.telefunctionName, runContext.telefuncFilePath))
      }
    }
  }

  // ── RESULT ── success only; a transform chain — each hook receives the last successful result.
  // All result hooks are attempted; a throw makes the outcome an error (result discarded) but does
  // not skip the remaining hooks, which keep transforming the last-good value.
  if (phase.outcome === 'success') {
    for (const ext of extensions) {
      const hook = ext.hooks?.onTelefunctionResult
      if (!hook) continue
      try {
        telefunctionReturn = await withContext(() => hook({ result: telefunctionReturn, data: dataFor(ext) }))
      } catch (err) {
        raiseHookError(err)
      }
    }
  }

  // ── SETTLED ── core settleLiveState runs on EVERY path (success/error/abort), so a request's queued
  // tag invalidations publish even when the body threw or aborted.
  try {
    await withContext(() => settleLiveState())
  } catch (err) {
    raiseHookError(err)
  }

  return {
    telefunctionReturn,
    telefunctionAborted: phase.outcome === 'abort',
    telefunctionHasErrored: phase.outcome === 'error',
    telefunctionTopLevelError: phase.outcome === 'error' ? phase.primaryError : undefined,
  }
}

/** A lifecycle hook that threw after the primary error was already set — surfaced nowhere, so it is
 *  logged here rather than lost. */
function logSecondary(err: unknown): void {
  console.error('[telefunc] a lifecycle hook threw after the primary error (logged, not surfaced):', err)
}

function returnTypeError(telefunctionName: string, telefuncFilePath: string): Error {
  return getProjectError(
    `The telefunction ${telefunctionName}() (${telefuncFilePath}) did not return a promise or async generator. A telefunction should always be defined as \`async function\` or \`async function*\`.`,
  )
}
