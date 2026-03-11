export { Abort, AbortError, isAbort, createAbortError }

import { assertUsage } from '../utils/assert.js'

const abortBrand = Symbol.for('telefunc.Abort')
const DEFAULT_ABORT_MESSAGE = 'Aborted'

class AbortError extends Error {
  readonly abortValue: unknown
  readonly [abortBrand] = true as const

  constructor(abortValue?: unknown, message?: string) {
    super(message ?? getAbortMessage(abortValue))
    Object.setPrototypeOf(this, new.target.prototype)
    this.name = 'Abort'
    this.abortValue = abortValue
    Error.captureStackTrace?.(this, Abort)
  }
}

type AbortFactory = {
  (abortValue?: unknown): AbortError
  [Symbol.hasInstance](thing: unknown): boolean
}

const Abort = function Abort(abortValue?: unknown): AbortError {
  assertUsage(!new.target, 'Do not use the `new` operator: use `throw Abort()` instead of `throw new Abort()`.')
  assertUsage(
    arguments.length <= 1,
    'Abort() accepts only a single argument: use `throw Abort([arg1, arg2])` instead of `throw Abort(arg1, arg2).`',
  )
  return createAbortError(abortValue)
} as AbortFactory

Object.defineProperty(Abort, Symbol.hasInstance, {
  value: (thing: unknown) => isAbort(thing),
})

function createAbortError(abortValue?: unknown, message?: string): AbortError {
  return new AbortError(abortValue, message)
}

function isAbort(thing: unknown): thing is AbortError {
  return thing instanceof AbortError || (typeof thing === 'object' && thing !== null && abortBrand in thing)
}

function getAbortMessage(abortValue: unknown): string {
  if (typeof abortValue === 'string') return abortValue
  if (abortValue instanceof Error) return abortValue.message
  if (typeof abortValue === 'object' && abortValue !== null && 'message' in abortValue) {
    const { message } = abortValue as { message?: unknown }
    if (typeof message === 'string') return message
  }
  return DEFAULT_ABORT_MESSAGE
}
