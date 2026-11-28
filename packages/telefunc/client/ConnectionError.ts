export { ConnectionError }

import { NetworkError } from '../shared/NetworkError.js'
import { markErrorClass } from '../shared/markErrorClass.js'

/**
 * Thrown when a regular telefunction call can't reach the server or load client code needed for the call.
 * @deprecated Catch `NetworkError` instead — `ConnectionError` is a `NetworkError` with `isChannel: false`.
 */
class ConnectionError extends NetworkError {
  /**
   * @deprecated Use `instanceof NetworkError` (or `instanceof ConnectionError`) instead.
   */
  readonly isConnectionError = true as const

  constructor(message = 'No Server Connection', options?: { cause?: unknown }) {
    super(message, false, options)
    this.name = 'ConnectionError'
  }
}

markErrorClass(ConnectionError, 'ConnectionError')
