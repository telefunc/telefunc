export { LaneSubscription }

import { assertIsNotBrowser } from '../../../utils/assertIsNotBrowser.js'
import type { BackendSubscription } from '../../backend/subscription.js'
assertIsNotBrowser()

/** A lane subscription the room wants: holder-facing readiness survives failed attempts, and Room's recovery policy replaces them. */
class LaneSubscription {
  private _subscription: BackendSubscription | null = null
  private _subscribe: (() => BackendSubscription) | null = null
  private _unobserve: (() => void) | null = null
  private _readyPromise = Promise.resolve()
  private _resolveReady: (() => void) | null = null

  constructor(
    private readonly _onTerminal: (slot: LaneSubscription, error?: unknown) => void,
    private readonly _onRecovered: () => void,
  ) {}

  get active(): boolean {
    return this._subscription !== null && this._subscription.state() !== 'closed'
  }

  get established(): boolean {
    return this._subscription?.state() === 'ready'
  }

  get wanted(): boolean {
    return this._subscribe !== null
  }

  get ready(): Promise<void> {
    return this._readyPromise
  }

  /** The current attempt's readiness, for Room's bounded recovery policy. */
  get attemptReady(): Promise<void> {
    return this._subscription?.ready ?? Promise.resolve()
  }

  /** Starts a wanted slot; a closed subscription is replaced by the recovery policy, not by re-planning. */
  sync(want: boolean, subscribe: () => BackendSubscription): void {
    if (!want) return this.stop()
    this._subscribe = subscribe
    if (this._subscription !== null) return
    this.retry()
  }

  retry(): void {
    if (this._subscribe === null) return
    this._ensurePendingReady()
    const previous = this._subscription
    this._unobserve?.()
    const subscription = this._subscribe()
    this._subscription = subscription
    let wasReady = subscription.state() === 'ready'
    if (wasReady) this._settleReady()
    let lostAfterReady = false
    void subscription.ready.then(
      () => {
        if (this._subscription === subscription && subscription.state() === 'ready') {
          wasReady = true
          this._settleReady()
        }
      },
      () => {}, // an end is handled once, on `closed`
    )
    // Every reassignment of `_subscription` unobserves first, so this listener only hears the current one.
    this._unobserve = subscription.onStateChange((state) => {
      if (state === 'lost') {
        if (wasReady) lostAfterReady = true
        this._ensurePendingReady()
      } else if (state === 'ready') {
        if (lostAfterReady) this._onRecovered()
        wasReady = true
        lostAfterReady = false
        this._settleReady()
      } else if (state === 'closed') {
        this._ended(subscription)
      }
    })
    // It may have ended as it opened, before it had an observer.
    if (subscription.state() === 'closed') this._ended(subscription)
    if (previous) void previous.unsubscribe()
  }

  /** A terminal end rejects `ready` with the driver's reason right after announcing `closed`; a stop resolves it. */
  private _ended(subscription: BackendSubscription): void {
    this._ensurePendingReady()
    const end = (error?: unknown) => {
      if (this._subscription === subscription) this._onTerminal(this, error)
    }
    void subscription.ready.then(() => end(), end)
  }

  /** Exhausted policy keeps demand and holder readiness pending, but drops the dead attempt until the next planning pass. */
  dropAttempt(): void {
    this._dropSubscription()
  }

  stop(): void {
    this._subscribe = null
    this._dropSubscription()
    this._settleReady()
    this._readyPromise = Promise.resolve()
  }

  private _dropSubscription(): void {
    const subscription = this._subscription
    this._subscription = null
    this._unobserve?.()
    this._unobserve = null
    if (subscription) void subscription.unsubscribe()
  }

  private _ensurePendingReady(): void {
    if (this._resolveReady !== null) return
    this._readyPromise = new Promise<void>((resolve) => {
      this._resolveReady = resolve
    })
  }

  private _settleReady(): void {
    this._resolveReady?.()
    this._resolveReady = null
  }
}
