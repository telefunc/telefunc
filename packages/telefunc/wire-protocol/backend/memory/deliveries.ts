export { MemoryDeliveries, MemorySubscriptionAttempt, removeSubscription, sumReceiverCounts, copyBytes }
export type { Subscriptions }

import { Fifo } from '../../../utils/Fifo.js'
import type { BackendPayload, BackendReceiver } from '../subscription.js'
import { DriverAttempt } from '../attempt.js'
import { nextTurn } from '../../flowControl/macrotaskYield.js'

/** Subscriptions by key; a change replaces a key's array, so an array read at a publish or commit is its targets. */
type Subscriptions = Map<string, readonly MemorySubscriptionAttempt[]>

/** Deliveries one event-loop turn runs; the rest wait for the next, so a listener answering itself
 *  can't starve the loop. */
const DELIVERIES_PER_TURN = 1024

/** The queue's head while a publish delivers at once: it keeps what that delivery causes queued behind it. */
const DELIVERING_NOW = (): void => {}

function copyBytes(bytes: Uint8Array): Uint8Array {
  return new Uint8Array(bytes)
}

function sumReceiverCounts(targets: readonly MemorySubscriptionAttempt[]): number {
  return targets.reduce((total, target) => total + target.receiverCount(), 0)
}

function removeSubscription(subs: Subscriptions, key: string, sub: MemorySubscriptionAttempt): void {
  const current = subs.get(key)
  if (current === undefined || !current.includes(sub)) return
  const rest = current.filter((other) => other !== sub)
  if (rest.length === 0) subs.delete(key)
  else subs.set(key, rest)
}

class MemorySubscriptionAttempt extends DriverAttempt {
  private readonly _receiver: BackendReceiver<BackendPayload>
  private readonly _localReceiverCount: () => number
  private readonly _detach: () => void

  constructor({
    receiver,
    localReceiverCount,
    detach,
  }: {
    receiver: BackendReceiver<BackendPayload>
    localReceiverCount: () => number
    detach: () => void
  }) {
    super()
    this._receiver = receiver
    this._localReceiverCount = localReceiverCount
    this._detach = detach
  }

  establish(): void {
    this.transition('ready')
  }

  async unsubscribe(): Promise<void> {
    this._detach()
    this.transition('closed')
  }

  deliver(payload: BackendPayload, info: { seq: number; timestamp: number }): void {
    this._receiver(payload, info)
  }

  receiverCount(): number {
    return this._localReceiverCount()
  }
}

/** Deliveries in seq order: the running one stays queued, so a publish made inside it is delivered after. */
class MemoryDeliveries {
  private readonly _queue = new Fifo<() => void>()
  private _deliveredThisTurn = 0

  get length(): number {
    return this._queue.length
  }

  /** A turn's worth of deliveries is queued. */
  get isFull(): boolean {
    return this._queue.length >= DELIVERIES_PER_TURN
  }

  deliversNow(): boolean {
    return this._queue.length === 0 && this._deliveredThisTurn < DELIVERIES_PER_TURN
  }

  /** Runs a delivery while nothing is queued, holding the queue's head as `_drain()` does. */
  deliverNow(delivery: () => void): void {
    if (this._deliveredThisTurn++ === 0) nextTurn(() => this._nextTurn())
    this._queue.push(DELIVERING_NOW)
    delivery()
    this._queue.shift()
    this._drain()
  }

  /** Runs `delivery` after those queued before it: now, unless one is running or this turn's deliveries ran out. */
  deliver(delivery: () => void): void {
    const now = this.deliversNow()
    this._queue.push(delivery)
    if (now) this._drain()
  }

  private _drain(): void {
    for (let delivery = this._queue.peek(); delivery !== undefined; delivery = this._queue.peek()) {
      if (this._deliveredThisTurn === DELIVERIES_PER_TURN) return
      if (this._deliveredThisTurn++ === 0) nextTurn(() => this._nextTurn())
      delivery()
      this._queue.shift()
    }
  }

  private _nextTurn(): void {
    this._deliveredThisTurn = 0
    this._drain()
  }
}
