export { stopLogged }

import { isPromise } from '../../utils/isPromise.js'

/** Calls `stop` and waits on nothing it returns: what it throws, or what the promise it may return rejects with, is
 *  logged, and nothing of `stop` is kept meanwhile, as a broker may never answer. */
function stopLogged(stop: () => unknown): void {
  let stopping: unknown
  try {
    stopping = stop()
  } catch (error) {
    return logStopFailure(error)
  }
  if (isPromise(stopping)) void stopping.then(undefined, logStopFailure)
}

function logStopFailure(error: unknown): void {
  console.error(error)
}
