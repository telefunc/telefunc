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

/** What a listener throws, or what a promise it returns rejects with, is logged, whatever it is: the other listeners
 *  still get the bug, and whoever reported it goes on. */
function callBugListeners(err: unknown) {
  for (const listener of globalObject.bugListeners) {
    let listening: unknown
    try {
      listening = listener(err)
    } catch (listenerErr) {
      logError(listenerErr)
      continue
    }
    if (isPromise(listening)) void listening.then(undefined, logError)
  }
}
