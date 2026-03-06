export { throwCancelError, throwAbortError, makeAbortError, throwBugError, makeBugError }

import { objectAssign } from '../../utils/objectAssign.js'
import { callOnAbortListeners } from './onAbort.js'
import { STATUS_BODY_INTERNAL_SERVER_ERROR } from '../../shared/constants.js'

function throwCancelError(): never {
  const cancelError = new Error('Telefunc call cancelled')
  objectAssign(cancelError, { isCancel: true as const })
  throw cancelError
}

type AbortError = Error & { isAbort: true; abortValue: unknown }

function makeAbortError(abortValue: unknown, message = 'Aborted'): AbortError {
  const err = new Error(message)
  objectAssign(err, { isAbort: true as const, abortValue })
  return err as AbortError
}

function throwAbortError(telefunctionName: string, telefuncFilePath: string, abortValue: unknown): never {
  const err = makeAbortError(abortValue, `Aborted telefunction call ${telefunctionName}() (${telefuncFilePath}).`)
  callOnAbortListeners(err)
  throw err
}

function makeBugError(errMsg = `${STATUS_BODY_INTERNAL_SERVER_ERROR} — see server logs`): Error {
  return new Error(errMsg)
}

function throwBugError(errMsg?: string): never {
  throw makeBugError(errMsg)
}
