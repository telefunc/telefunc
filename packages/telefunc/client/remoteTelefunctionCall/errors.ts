export { throwAbortError, throwIfAborted, makeAbortError, throwBugError, makeBugError }

import { createAbortError } from '../../shared/Abort.js'
import { callOnAbortListeners } from './onAbort.js'
import { STATUS_BODY_INTERNAL_SERVER_ERROR } from '../../shared/constants.js'

function makeAbortError(
  abortValue: unknown,
  messageOrContext?: string | { telefunctionName: string; telefuncFilePath: string },
) {
  return createAbortError(abortValue, getAbortMessage(messageOrContext))
}

function throwAbortError(telefunctionName: string, telefuncFilePath: string, abortValue: unknown): never {
  const err = makeAbortError(abortValue, { telefunctionName, telefuncFilePath })
  callOnAbortListeners(err)
  throw err
}

/** Where a call resumes after an `await`: an abort in between ends it there, before it starts anything more. */
function throwIfAborted(call: {
  abortController: AbortController
  telefunctionName: string
  telefuncFilePath: string
}) {
  if (call.abortController.signal.aborted) throwAbortError(call.telefunctionName, call.telefuncFilePath, undefined)
}

function getAbortMessage(messageOrContext?: string | { telefunctionName: string; telefuncFilePath: string }) {
  if (!messageOrContext) return undefined
  if (typeof messageOrContext === 'string') return messageOrContext
  return `Aborted telefunction call ${messageOrContext.telefunctionName}() (${messageOrContext.telefuncFilePath}).`
}

function makeBugError(errMsg = `${STATUS_BODY_INTERNAL_SERVER_ERROR} — see server logs`): Error {
  return new Error(errMsg)
}

function throwBugError(errMsg?: string): never {
  throw makeBugError(errMsg)
}
