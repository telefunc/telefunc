export { ShieldValidationError, isShieldValidationError }

const shieldValidationErrorBrand = Symbol.for('telefunc.ShieldValidationError')

class ShieldValidationError extends Error {
  readonly [shieldValidationErrorBrand] = true as const

  constructor(message: string) {
    super(message)
    Object.setPrototypeOf(this, new.target.prototype)
    this.name = 'ShieldValidationError'
    Error.captureStackTrace?.(this, ShieldValidationError)
  }
}

function isShieldValidationError(thing: unknown): thing is ShieldValidationError {
  return (
    thing instanceof ShieldValidationError ||
    (typeof thing === 'object' && thing !== null && shieldValidationErrorBrand in thing)
  )
}
