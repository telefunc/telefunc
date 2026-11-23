export { BackendError }
export type { BackendErrorCode, BackendCallOutcome }

const backendErrorMark = Symbol.for('telefunc.BackendError')

/** What went wrong with the store, the same for every backend:
 *  - `'unreachable'`: the connection to it failed or closed; it works again once the store is reachable.
 *  - `'unavailable'`: it can't serve this now, and a later call may succeed (loading its data, busy, out of memory, a
 *    replica, replicas behind, a failover); the message says what it needs if the state lasts.
 *  - `'denied'`: it refuses until an operator changes it (its permissions, credentials or commands).
 *  - `'failed'`: it failed otherwise; the message says what to change. */
type BackendErrorCode = 'unreachable' | 'unavailable' | 'denied' | 'failed'

/** `'not-run'`: the call changed nothing, so making it again is safe. `'may-have-run'`: it may have taken effect, now or
 *  later; read what it writes before making it again. */
type BackendCallOutcome = 'not-run' | 'may-have-run'

/** A Broadcast or Room call the backend's store failed. `instanceof BackendError` holds for one made by another copy of
 *  Telefunc too. */
class BackendError extends Error {
  readonly code: BackendErrorCode
  readonly outcome: BackendCallOutcome

  constructor({ code, outcome, message }: { code: BackendErrorCode; outcome: BackendCallOutcome; message: string }) {
    super(message)
    this.name = 'BackendError'
    this.code = code
    this.outcome = outcome
  }

  static [Symbol.hasInstance](thing: unknown): thing is BackendError {
    return typeof thing === 'object' && thing !== null && backendErrorMark in thing
  }
}
Object.defineProperty(BackendError.prototype, backendErrorMark, { value: true })
