export { LaneSubscription }
export type { LaneSubscriptionOptions, LaneSubscriptionOwner }

import { assertIsNotBrowser } from '../utils/assertIsNotBrowser.js'
import { createDeferred, type Deferred } from '../utils/createDeferred.js'
import type { BackendSubscription } from 'telefunc/__internal'
import type { RoomSubscriptionSource } from '../backend/contract.js'
assertIsNotBrowser()

/** The plan that holds a lane subscription, told as its lane ends, recovers, and goes pending or ready. */
interface LaneSubscriptionOwner {
  /** The lane ended for good: the owner's recovery policy replaces it. */
  onLaneTerminal(slot: LaneSubscription): void
  /** The lane is ready again after a loss. */
  onLaneRecovered(): void
  onLanePending(slot: LaneSubscription, pending: boolean): void
}

type LaneSubscriptionOptions = { owner: LaneSubscriptionOwner; lane: RoomSubscriptionSource['lane'] }

/** A lane subscription the room wants: holder-facing readiness survives failed attempts, and Room's recovery policy
 *  replaces them. */
class LaneSubscription {
  private _subscription: BackendSubscription | null = null
  private _subscribe: (() => BackendSubscription) | null = null
  private _unobserve: (() => void) | null = null
  private _readyPromise = Promise.resolve()
  private _pendingReady: Deferred<void> | null = null
  private _delivering = false

  /** The lane it subscribes, which tells its owner what its readiness replans. */
  readonly lane: RoomSubscriptionSource['lane']
  private readonly owner: LaneSubscriptionOwner

  constructor({ owner, lane }: LaneSubscriptionOptions) {
    this.owner = owner
    this.lane = lane
  }

  get established(): boolean {
    return this._subscription?.state() === 'ready'
  }

  get wanted(): boolean {
    return this._subscribe !== null
  }

  /** Whether an attempt was ready since the slot started; a loss or a dropped attempt keeps it. */
  get delivering(): boolean {
    return this._delivering
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
    this.resume()
  }

  /** Opens an attempt of a wanted slot that has none, once its recovery gave up. */
  resume(): void {
    if (this._subscription === null) this.retry()
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
        if (lostAfterReady) this.owner.onLaneRecovered()
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

  /** A terminal end rejects `ready` right after announcing `closed`, and the backend reports it; a stop resolves it. */
  private _ended(subscription: BackendSubscription): void {
    this._ensurePendingReady()
    const end = () => {
      if (this._subscription === subscription) this.owner.onLaneTerminal(this)
    }
    void subscription.ready.then(end, end)
  }

  stop(): void {
    this._subscribe = null
    this._delivering = false
    this.dropAttempt()
    this._settleReady()
    this._readyPromise = Promise.resolve()
  }

  /** Exhausted policy keeps demand and holder readiness pending, but drops the dead attempt until the slot resumes. */
  dropAttempt(): void {
    const subscription = this._subscription
    this._subscription = null
    this._unobserve?.()
    this._unobserve = null
    if (subscription) void subscription.unsubscribe()
  }

  private _ensurePendingReady(): void {
    if (this._pendingReady !== null) return
    this._pendingReady = createDeferred()
    this._readyPromise = this._pendingReady.promise
    this.owner.onLanePending(this, true)
  }

  private _settleReady(): void {
    if (this._subscribe !== null) this._delivering = true
    if (this._pendingReady === null) return
    this._pendingReady.resolve()
    this._pendingReady = null
    this.owner.onLanePending(this, false)
  }
}
