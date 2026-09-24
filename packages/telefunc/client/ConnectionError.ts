export { ConnectionError }

import { NetworkError } from '../shared/NetworkError.js'

/**
 * Thrown when a regular telefunction call can't reach the server.
 * @deprecated Catch `NetworkError` instead — `ConnectionError` is a `NetworkError` with `isChannel: false`.
 */
class ConnectionError extends NetworkError {
  /**
   * @deprecated Use `instanceof NetworkError` (or `instanceof ConnectionError`) instead.
   */
  readonly isConnectionError = true as const

  constructor(message = 'No Server Connection') {
    super(message, false)
    this.name = 'ConnectionError'
  }
}
