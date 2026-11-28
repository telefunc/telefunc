export { onBug }
export { callBugListeners }

import { getGlobalObject } from '../../../utils/getGlobalObject.js'
import { isPromise } from '../../../utils/isPromise.js'
import { logError } from './handleError.js'

const globalObject = getGlobalObject<{ bugListeners: ((err: unknown) => void)[] }>('onBug.ts', {
  bugListeners: [],
})

function onBug(listener: (err: unknown) => void) {
  globalObject.bugListeners.push(listener)
}

/** Calls the listeners registered when it is called. What one throws, or what a promise it returns rejects with, is
 *  logged, whatever it is: the other listeners still get the bug, and whoever reported it goes on. */
function callBugListeners(err: unknown) {
  for (const listener of [...globalObject.bugListeners]) {
    try {
      const listening = listener(err)
      if (isPromise(listening)) void listening.then(undefined, logListenerError)
    } catch (listenerErr) {
      logListenerError(listenerErr)
    }
  }
}

/** A monitor's failure may itself throw while being formatted. Reporting still reaches the other monitors. */
function logListenerError(error: unknown): void {
  try {
    logError(error)
  } catch {
    console.error('A Telefunc onBug listener failed, and its error could not be logged')
  }
}
