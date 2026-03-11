export { ConnectionError }

class ConnectionError extends Error {
  constructor(message = 'No Server Connection') {
    super(message)
    Object.setPrototypeOf(this, new.target.prototype)
    this.name = 'ConnectionError'
    Error.captureStackTrace?.(this, ConnectionError)
  }
}
