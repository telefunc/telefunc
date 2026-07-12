export { throwAbortError, makeAbortError, throwBugError, makeBugError }

import { createAbortError, abortValueMessage } from '../../shared/Abort.js'
import { callOnAbortListeners } from './onAbort.js'
import { STATUS_BODY_INTERNAL_SERVER_ERROR } from '../../shared/constants.js'

function makeAbortError(
  abortValue: unknown,
  messageOrContext?: string | { telefunctionName: string; telefuncFilePath: string },
) {
  // The abort value's own message wins: `throw Abort('You are banned')` should read as
  // `You are banned`, not the generic call description. The context is the fallback for
  // value-less (client-initiated) and structured (`{ code }`, no `.message`) aborts.
  return createAbortError(abortValue, abortValueMessage(abortValue) ?? contextMessage(messageOrContext))
}

function throwAbortError(telefunctionName: string, telefuncFilePath: string, abortValue: unknown): never {
  const err = makeAbortError(abortValue, { telefunctionName, telefuncFilePath })
  callOnAbortListeners(err)
  throw err
}

function contextMessage(messageOrContext?: string | { telefunctionName: string; telefuncFilePath: string }) {
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
