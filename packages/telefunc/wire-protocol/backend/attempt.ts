export { DriverAttempt }

import { createDeferred } from '../../utils/createDeferred.js'
import type { SubscriptionAttempt, SubscriptionAttemptState } from './subscription.js'

/** A driver attempt's state, listeners and readiness. `ready` resolves on the first `ready` and rejects if the
 *  attempt ends before it; an ended attempt never transitions again. */
abstract class DriverAttempt implements SubscriptionAttempt {
  readonly #readiness = createDeferred()
  readonly #listeners = new Set<(state: SubscriptionAttemptState) => void>()
  #state: SubscriptionAttemptState = 'establishing'

  constructor() {
    void this.#readiness.promise.catch(() => {})
  }

  get ready(): Promise<void> {
    return this.#readiness.promise
  }

  get ended(): boolean {
    return this.#state === 'closed' || this.#state === 'terminated'
  }

  state(): SubscriptionAttemptState {
    return this.#state
  }

  onStateChange(listener: (state: SubscriptionAttemptState) => void): () => void {
    this.#listeners.add(listener)
    return () => this.#listeners.delete(listener)
  }

  abstract unsubscribe(): Promise<void>

  protected transition(state: SubscriptionAttemptState, error?: unknown): void {
    if (this.#state === state || this.ended) return
    this.#state = state
    if (state === 'ready') this.#readiness.resolve()
    else if (this.ended) this.#readiness.reject(error ?? new Error(`The subscription ${state} before it was ready`))
    for (const listener of [...this.#listeners]) listener(state)
  }
}
