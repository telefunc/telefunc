export { executeTelefunction }

import { isAbort } from '../Abort.js'
import { restoreContext, Telefunc } from '../getContext.js'
import { createRequestContext, restoreRequestContext } from '../requestContext.js'
import type { Telefunction } from '../types.js'
import { assertUsage } from '../../../utils/assert.js'
import { isPromise } from '../../../utils/isPromise.js'
import { isAsyncGenerator } from '../../../utils/isAsyncGenerator.js'
import { validateTelefunctionError } from './validateTelefunctionError.js'
import type { ConfigResolved } from '../serverConfig.js'

async function executeTelefunction(runContext: {
  telefunction: Telefunction
  telefunctionName: string
  telefuncFilePath: string
  telefunctionArgs: unknown[]
  providedContext: Telefunc.Context | null
  requestContext: ReturnType<typeof createRequestContext>
  request: Request
  requestExtensions: Record<string, Record<string, unknown>>
  serverConfig: ConfigResolved
}) {
  const { telefunction, telefunctionArgs, requestExtensions } = runContext
  const { extensions } = runContext.serverConfig

  /** Restore request + user context before executing `fn`. */
  const withContext = <T>(fn: () => T): T =>
    restoreContext(runContext.providedContext, () => restoreRequestContext(runContext.requestContext, fn))

  let telefunctionReturn: unknown
  let telefunctionTopLevelError: unknown
  let telefunctionHasErrored = false
  let telefunctionAborted = false
  const onTopLevelError = (err: unknown) => {
    validateTelefunctionError(err, runContext)
    if (isAbort(err)) {
      telefunctionAborted = true
      telefunctionReturn = err.abortValue
      runContext.requestContext.responseAbort.abort(err.abortValue)
    } else {
      telefunctionHasErrored = true
      telefunctionTopLevelError = err
    }
  }

  let resultSync: unknown
  try {
    resultSync = withContext(() => telefunction.apply(null, telefunctionArgs))
  } catch (err: unknown) {
    onTopLevelError(err)
  }

  if (!telefunctionHasErrored && !telefunctionAborted) {
    assertUsage(
      isPromise(resultSync) || isAsyncGenerator(resultSync),
      `The telefunction ${runContext.telefunctionName}() (${runContext.telefuncFilePath}) did not return a promise or async generator. A telefunction should always be defined as \`async function\` or \`async function*\`.`,
    )
    if (isPromise(resultSync)) {
      try {
        telefunctionReturn = await resultSync
      } catch (err: unknown) {
        onTopLevelError(err)
      }
    } else {
      telefunctionReturn = resultSync
    }
  }

  if (!telefunctionHasErrored && !telefunctionAborted) {
    for (const ext of extensions) {
      if (ext.hooks?.onTransformResult && requestExtensions[ext.name]) {
        const data = requestExtensions[ext.name]!
        telefunctionReturn = await withContext(() =>
          ext.hooks!.onTransformResult!({ result: telefunctionReturn, data }),
        )
      }
    }
  }

  return { telefunctionReturn, telefunctionAborted, telefunctionHasErrored, telefunctionTopLevelError }
}
