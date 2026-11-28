export { throwAbortError, throwIfAborted, abortable, makeAbortError, throwBugError, makeBugError }

import { createDeferred } from '../../utils/createDeferred.js'
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

const ABORTED = Symbol('ABORTED')

/** Waits for what `start()` returns unless the call is aborted: an abort rejects at once, and an aborted call starts
 *  nothing. */
async function abortable<T>(
  call: { abortController: AbortController; telefunctionName: string; telefuncFilePath: string },
  start: () => Promise<T>,
): Promise<Awaited<T>> {
  throwIfAborted(call)
  const { signal } = call.abortController
  const aborted = createDeferred<typeof ABORTED>()
  const onAbort = () => aborted.resolve(ABORTED)
  signal.addEventListener('abort', onAbort, { once: true })
  let result: Awaited<T> | typeof ABORTED
  try {
    result = await Promise.race([start(), aborted.promise])
  } catch (err) {
    // A rejection that the abort caused ends the call as aborted.
    if (!signal.aborted) throw err
    result = ABORTED
  } finally {
    signal.removeEventListener('abort', onAbort)
  }
  if (result === ABORTED) throwAbortError(call.telefunctionName, call.telefuncFilePath, undefined)
  return result
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
