export { onBug }
export { callBugListeners }

import { getGlobalObject } from '../../../utils/getGlobalObject.js'

const globalObject = getGlobalObject<{ bugListeners: ((err: unknown) => void)[] }>('onBug.ts', {
  bugListeners: [],
})

function onBug(listener: (err: unknown) => void) {
  globalObject.bugListeners.push(listener)
}

function callBugListeners(err: unknown) {
  globalObject.bugListeners.forEach((listener) => {
    listener(err)
  })
}
