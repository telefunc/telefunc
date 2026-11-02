export { LaneSubscription }
export type { LaneAddress, LaneSubscriptionOptions, LaneSubscriptionOwner }

import { assertIsNotBrowser } from '../../utils/assertIsNotBrowser.js'
import { createDeferred, type Deferred } from '../../utils/createDeferred.js'
import type { BackendSubscription, OrderingInfo } from 'telefunc/__internal'
import type { LaneConsumer } from '../../backend/RoomPlane.js'
import type { RoomSubscriptionSource } from '../../backend/contract.js'
assertIsNotBrowser()

/** The plan that holds a lane subscription: it subscribes the lane, takes what the lane delivers, and is told as the
 *  lane ends, recovers, and goes pending or ready. */
interface LaneSubscriptionOwner {
  /** One attempt at `address`, delivering to `receiver`. */
  subscribeLane(address: LaneAddress, receiver: LaneConsumer): BackendSubscription
  onLaneDelivery(lane: LaneSubscription, payload: Uint8Array, info: OrderingInfo): void
  /** The lane ended for good: the owner's recovery policy replaces it. */
  onLaneTerminal(lane: LaneSubscription): void
  /** The lane is ready again after a loss. */
  onLaneRecovered(): void
  onLanePending(lane: LaneSubscription, pending: boolean): void
}

type LaneAddress = RoomSubscriptionSource['lane']

type LaneSubscriptionOptions = { owner: LaneSubscriptionOwner; address: LaneAddress }

/** A lane subscription the room wants: holder-facing readiness survives failed attempts, and Room's recovery policy
 *  replaces them. */
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
  /** Each attempt's receiver, built once per lane. */
  private readonly receive: LaneConsumer

  constructor({ owner, address }: LaneSubscriptionOptions) {
    this.owner = owner
    this.address = address
    this.receive = (payload, info) => owner.onLaneDelivery(this, payload, info)
  }

  get established(): boolean {
    return this.subscription?.state() === 'ready'
  }

  get wanted(): boolean {
    return this.isWanted
  }

  /** Whether an attempt was ready since the lane started; a loss or a dropped attempt keeps it. */
  get delivering(): boolean {
    return this._delivering
  }

  get ready(): Promise<void> {
    return this.readyPromise
  }

  /** The current attempt's readiness, for Room's bounded recovery policy. */
  get attemptReady(): Promise<void> {
    return this.subscription?.ready ?? Promise.resolve()
  }

  /** The lane is wanted: it subscribes unless an attempt is open. A closed attempt is replaced by the recovery policy,
   *  not by starting again. */
  start(): void {
    this.isWanted = true
    this.resume()
  }

  /** Opens an attempt of a wanted lane that has none, once its recovery gave up. */
  resume(): void {
    if (this.subscription === null) this.retry()
  }

  retry(): void {
    if (!this.isWanted) return
    this.ensurePendingReady()
    const previous = this.subscription
    this.unobserve?.()
    const subscription = this.owner.subscribeLane(this.address, this.receive)
    this.subscription = subscription
    let wasReady = subscription.state() === 'ready'
    if (wasReady) this.settleReady()
    let lostAfterReady = false
    void subscription.ready.then(
      () => {
        if (this.subscription === subscription && subscription.state() === 'ready') {
          wasReady = true
          this.settleReady()
        }
      },
      () => {}, // an end is handled once, on `closed`
    )
    // Every reassignment of `subscription` unobserves first, so this listener only hears the current one.
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
    if (previous) void previous.unsubscribe()
  }

  /** A terminal end rejects `ready` right after announcing `closed`, and the backend reports it unless it dropped the
   *  lane's incarnation; a stop resolves it. */
  private ended(subscription: BackendSubscription): void {
    this.ensurePendingReady()
    const end = () => {
      if (this.subscription === subscription) this.owner.onLaneTerminal(this)
    }
    void subscription.ready.then(end, end)
  }

  stop(): void {
    this.isWanted = false
    this._delivering = false
    this.dropAttempt()
    this.settleReady()
    this.readyPromise = Promise.resolve()
  }

  /** Exhausted policy keeps demand and holder readiness pending, but drops the dead attempt until the lane resumes. */
  dropAttempt(): void {
    const subscription = this.subscription
    this.subscription = null
    this.unobserve?.()
    this.unobserve = null
    if (subscription) void subscription.unsubscribe()
  }

  private ensurePendingReady(): void {
    if (this.pendingReady !== null) return
    this.pendingReady = createDeferred()
    this.readyPromise = this.pendingReady.promise
    this.owner.onLanePending(this, true)
  }

  private settleReady(): void {
    if (this.isWanted) this._delivering = true
    if (this.pendingReady === null) return
    this.pendingReady.resolve()
    this.pendingReady = null
    this.owner.onLanePending(this, false)
  }
}
