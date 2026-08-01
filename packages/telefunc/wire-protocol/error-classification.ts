export { classifyTelefuncError }
export type { TelefuncErrorClassification }

import { isAbort, type AbortError } from '../shared/Abort.js'
import { isShieldValidationError, type ShieldValidationError } from '../shared/ShieldValidationError.js'

type TelefuncErrorClassification =
  | { kind: 'abort'; error: AbortError }
  | { kind: 'shield'; error: ShieldValidationError }
  | { kind: 'expected'; error: Error }
  | { kind: 'bug'; error: unknown }

/** Shared error precedence. Callers supply their operational predicate; Abort and shield failures are universal. */
function classifyTelefuncError(
  error: unknown,
  isExpected: (error: unknown) => error is Error,
): TelefuncErrorClassification {
  if (isAbort(error)) return { kind: 'abort', error }
  if (isExpected(error)) return { kind: 'expected', error }
  if (isShieldValidationError(error)) return { kind: 'shield', error }
  return { kind: 'bug', error }
}
