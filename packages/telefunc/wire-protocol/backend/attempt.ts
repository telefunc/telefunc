export { DriverAttempt, SourceDroppedError }

import type { ReportedState, SubscriptionAttempt, SubscriptionState } from './subscription.js'

type StateListener = (state: ReportedState, reason?: Error) => void

/** A driver attempt's state and listeners; an ended attempt never transitions again. */
abstract class DriverAttempt implements SubscriptionAttempt {
  private readonly _listeners = new Set<StateListener>()
  private _state: SubscriptionState = 'establishing'

  protected get ended(): boolean {
    return this._state === 'closed'
  }

  state(): SubscriptionState {
    return this._state
  }

  onStateChange(listener: StateListener): () => void {
    this._listeners.add(listener)
    return () => this._listeners.delete(listener)
  }

  abstract unsubscribe(): Promise<void>

  /** `reason` explains an end, when the driver has one. */
  protected transition(state: ReportedState, reason?: unknown): void {
    if (this._state === state || this.ended) return
    this._state = state
    const error = reason === undefined || reason instanceof Error ? reason : new Error(String(reason))
    for (const listener of this._listeners) listener(state, error)
  }
}

/** The reason a driver ends an attempt whose source the backend dropped, as a Room generation no head names any more:
 *  an end its consumer expects, so the subscription closes without a bug report. */
class SourceDroppedError extends Error {}
