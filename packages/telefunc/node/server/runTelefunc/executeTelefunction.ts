export { executeTelefunction }

import { isAbort, Abort } from '../Abort.js'
import { restoreContext, Telefunc } from '../getContext.js'
import type { Telefunction } from '../types.js'
import { assertUsage } from '../../../utils/assert.js'
import { isPromise } from '../../../utils/isPromise.js'
import { isAsyncGenerator } from '../../../utils/isAsyncGenerator.js'

async function executeTelefunction(runContext: {
  telefunction: Telefunction
  telefunctionName: string
  telefuncFilePath: string
  telefunctionArgs: unknown[]
  providedContext: Telefunc.Context | null
}) {
  const { telefunction, telefunctionArgs } = runContext

  restoreContext(runContext.providedContext)

  let telefunctionReturn: unknown
  let telefunctionError: unknown
  let telefunctionHasErrored = false
  let telefunctionAborted = false
  const onError = (err: unknown) => {
    assertUsage(
      typeof err === 'object' && err !== null,
      `The telefunction ${runContext.telefunctionName}() (${runContext.telefuncFilePath}) threw a non-object error: \`${err}\`. Make sure the telefunction does \`throw new Error(${err})\` instead.`,
    )
    assertUsage(
      err !== Abort,
      `Missing parentheses \`()\` in \`throw Abort\` (it should be \`throw Abort()\`) at telefunction ${runContext.telefunctionName}() (${runContext.telefuncFilePath}).`,
    )
    if (isAbort(err)) {
      telefunctionAborted = true
      telefunctionReturn = err.abortValue
    } else {
      telefunctionHasErrored = true
      telefunctionError = err
    }
  }

  let resultSync: unknown
  try {
    resultSync = telefunction.apply(null, telefunctionArgs)
  } catch (err: unknown) {
    onError(err)
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
        onError(err)
      }
    } else {
      telefunctionReturn = resultSync
    }
  }

  return { telefunctionReturn, telefunctionAborted, telefunctionHasErrored, telefunctionError }
}
