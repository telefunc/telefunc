export { DriverAttempt }

import type { SubscriptionAttempt, SubscriptionState } from './subscription.js'

type StateListener = (state: SubscriptionState, reason?: Error) => void

/** A driver attempt's state and listeners; an ended attempt never transitions again. */
abstract class DriverAttempt implements SubscriptionAttempt {
  readonly #listeners = new Set<StateListener>()
  #state: SubscriptionState = 'establishing'

  get ended(): boolean {
    return this.#state === 'closed'
  }

  state(): SubscriptionState {
    return this.#state
  }

  onStateChange(listener: StateListener): () => void {
    this.#listeners.add(listener)
    return () => this.#listeners.delete(listener)
  }

  abstract unsubscribe(): Promise<void>

  /** `reason` explains an end, when the driver has one. */
  protected transition(state: SubscriptionState, reason?: unknown): void {
    if (this.#state === state || this.ended) return
    this.#state = state
    const error = reason === undefined || reason instanceof Error ? reason : new Error(String(reason))
    for (const listener of [...this.#listeners]) listener(state, error)
  }
}
