export { ShieldValidationError, isShieldValidationError }

import { markErrorClass } from './markErrorClass.js'

class ShieldValidationError extends Error {
  constructor(message: string) {
    super(message)
    Object.setPrototypeOf(this, new.target.prototype)
    this.name = 'ShieldValidationError'
    Error.captureStackTrace?.(this, ShieldValidationError)
  }
}

markErrorClass(ShieldValidationError, 'ShieldValidationError')

function isShieldValidationError(thing: unknown): thing is ShieldValidationError {
  return thing instanceof ShieldValidationError
}
