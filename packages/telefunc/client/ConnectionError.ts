export { ConnectionError }

class ConnectionError extends Error {
  isConnectionError = true as const
  constructor(message = 'No Server Connection') {
    super(message)
    Object.setPrototypeOf(this, new.target.prototype)
    this.name = 'ConnectionError'
    Error.captureStackTrace?.(this, ConnectionError)
  }
}
