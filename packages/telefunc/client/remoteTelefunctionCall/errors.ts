export { throwCancelError, throwAbortError, throwBugError }

import { objectAssign } from '../../utils/objectAssign.js'
import { callOnAbortListeners } from './onAbort.js'
import { STATUS_BODY_INTERNAL_SERVER_ERROR } from '../../shared/constants.js'

function throwCancelError(): never {
  const cancelError = new Error('Telefunc call cancelled')
  objectAssign(cancelError, { isCancel: true as const })
  throw cancelError
}

function throwAbortError(telefunctionName: string, telefuncFilePath: string, abortValue: unknown): never {
  const telefunctionCallError = new Error(`Aborted telefunction call ${telefunctionName}() (${telefuncFilePath}).`)
  objectAssign(telefunctionCallError, { isAbort: true as const, abortValue })
  callOnAbortListeners(telefunctionCallError)
  throw telefunctionCallError
}

function throwBugError(errMsg = `${STATUS_BODY_INTERNAL_SERVER_ERROR} — see server logs`): never {
  throw new Error(errMsg)
}
