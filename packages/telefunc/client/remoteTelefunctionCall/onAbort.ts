export { onAbort }
export { callOnAbortListeners }
export { onTelefunctionRemoteCallError }

import type { AbortError } from '../../shared/Abort.js'
import { assertWarning } from '../../utils/assert.js'

type Listener = (err: AbortError) => void

/** Outdated: use onAbort() instead */
function onTelefunctionRemoteCallError(listener: (err: Error) => void) {
  assertWarning(false, 'onTelefunctionRemoteCallError() deprecated in favor of onAbort()', {
    onlyOnce: true,
    showStackTrace: true,
  })
  onAbort(listener)
}

function onAbort(listener: Listener) {
  window.__telefunc_errorListeners = window.__telefunc_errorListeners || []
  window.__telefunc_errorListeners.push(listener)
}

function callOnAbortListeners(err: AbortError) {
  ;(window.__telefunc_errorListeners || []).forEach((listener) => {
    listener(err)
  })
}

declare global {
  interface Window {
    __telefunc_errorListeners: Listener[]
  }
}
