export { LaneSubscription }
export type { LaneAddress, LaneSubscriptionOptions, LaneSubscriptionOwner }

import { assertIsNotBrowser } from '../../utils/assertIsNotBrowser.js'
import { createDeferred, type Deferred } from 'telefunc/__internal'
import type { BackendListener, BackendSubscription, OrderingInfo } from 'telefunc/__internal'
import type { RoomSubscriptionSource } from '../../backend/contract.js'
assertIsNotBrowser()

/** The plan that holds a lane subscription: it subscribes the lane, takes what the lane delivers, and is told as the
 *  lane ends, recovers, and goes pending or ready. */
interface LaneSubscriptionOwner {
  /** One attempt at `address`, delivering to `listener`. */
  subscribeLane(address: LaneAddress, listener: BackendListener<Uint8Array>): BackendSubscription
  onLaneDelivery(lane: LaneSubscription, payload: Uint8Array, info: OrderingInfo): void
  /** The lane ended for good: its incarnation was dropped, or it failed. */
  onLaneEnded(lane: LaneSubscription): void
  /** The lane is ready again after a loss. */
  onLaneRecovered(): void
  onLanePending(lane: LaneSubscription, pending: boolean): void
}

type LaneAddress = RoomSubscriptionSource['lane']

type LaneSubscriptionOptions = { owner: LaneSubscriptionOwner; address: LaneAddress }

/** A lane subscription the room wants. Its driver recovers a loss; an end is final, and a failure rejects its readiness
 *  with the backend's cause. */
class LaneSubscription {
  private subscription: BackendSubscription | null = null
  private isWanted = false
  private unobserve: (() => void) | null = null
  private readyPromise = Promise.resolve()
  private pendingReady: Deferred<void> | null = null
  private _delivering = false

  /** The lane it subscribes, which tells its owner what its readiness replans. */
  readonly address: LaneAddress
  private readonly owner: LaneSubscriptionOwner
  /** Each attempt's listener, built once per lane. */
  private readonly listener: BackendListener<Uint8Array>

  constructor({ owner, address }: LaneSubscriptionOptions) {
    this.owner = owner
    this.address = address
    this.listener = (payload, info) => owner.onLaneDelivery(this, payload, info)
  }

  get established(): boolean {
    return this.subscription?.state() === 'ready'
  }

  get wanted(): boolean {
    return this.isWanted
  }

  /** Whether its subscription was ready since the lane started; a loss keeps it. */
  get delivering(): boolean {
    return this._delivering
  }

  get ready(): Promise<void> {
    return this.readyPromise
  }

  /** The lane is wanted: it subscribes unless it has a subscription, which its driver keeps or which ended for good. */
  start(): void {
    this.isWanted = true
    if (this.subscription === null) this.subscribe()
  }

  private subscribe(): void {
    this.ensurePendingReady()
    const subscription = this.owner.subscribeLane(this.address, this.listener)
    this.subscription = subscription
    let wasReady = subscription.state() === 'ready'
    if (wasReady) this.settleReady()
    let lostAfterReady = false
    const settleOnReady = async () => {
      try {
        await subscription.ready
      } catch {
        return // an end is handled once, on `closed`
      }
      if (this.subscription === subscription && subscription.state() === 'ready') {
        wasReady = true
        this.settleReady()
      }
    }
    void settleOnReady()
    this.unobserve = subscription.onStateChange((state) => {
      if (state === 'lost') {
        if (wasReady) lostAfterReady = true
        this.ensurePendingReady()
      } else if (state === 'ready') {
        if (lostAfterReady) this.owner.onLaneRecovered()
        wasReady = true
        lostAfterReady = false
        this.settleReady()
      } else if (state === 'closed') {
        this.ended(subscription)
      }
    })
    // It may have ended as it opened, before it had an observer.
    if (subscription.state() === 'closed') this.ended(subscription)
  }

  /** Core rejects `ready` right after announcing `closed`: with the driver's reason as the cause where the lane failed
   *  (Redis refused it), which its readiness rejects with; without one where the backend dropped its incarnation. */
  private ended(subscription: BackendSubscription): void {
    const pending = this.ensurePendingReady()
    void subscription.ready.catch((failure: Error) => {
      if (this.subscription !== subscription) return
      if (failure.cause !== undefined) pending.reject(failure)
      this.owner.onLaneEnded(this)
    })
  }

  stop(): void {
    this.isWanted = false
    this._delivering = false
    this.unsubscribe()
    this.settleReady()
    this.readyPromise = Promise.resolve()
  }

  private unsubscribe(): void {
    const subscription = this.subscription
    this.subscription = null
    this.unobserve?.()
    this.unobserve = null
    if (subscription) void subscription.unsubscribe()
  }

  private ensurePendingReady(): Deferred<void> {
    if (this.pendingReady !== null) return this.pendingReady
    const pending = createDeferred()
    // A failure is reported once by core; a consumer that waits on `ready` gets it too.
    void pending.promise.catch(() => {})
    this.pendingReady = pending
    this.readyPromise = pending.promise
    this.owner.onLanePending(this, true)
    return pending
  }

  private settleReady(): void {
    if (this.isWanted) this._delivering = true
    if (this.pendingReady === null) return
    this.pendingReady.resolve()
    this.pendingReady = null
    this.owner.onLanePending(this, false)
  }
}
