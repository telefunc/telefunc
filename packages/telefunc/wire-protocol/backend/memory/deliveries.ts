export { MemoryPubSub }
export type { Subscriptions }

import { Fifo } from '../../../utils/Fifo.js'
import type { BackendPayload, BackendReceiver } from '../subscription.js'
import type { OrderingInfo } from '../../wire/orderingFrame.js'
import { DriverAttempt } from '../attempt.js'
import { nextTurn } from '../../flowControl/macrotaskYield.js'

/** Subscriptions by key; a change replaces a key's array, so an array read at a publish or commit is its targets. */
type Subscriptions = Map<string, readonly MemorySubscriptionAttempt[]>

/** A publish's position and the local consumers it reached. */
type Receipt = OrderingInfo & { receivers: number }

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

/** An in-process driver's pub/sub: subscriptions by key, delivered in order, a turn's worth at a time. */
class MemoryPubSub {
  private readonly _deliveries = new MemoryDeliveries()
  /** Deliveries a microtask is yet to queue. */
  private _queuing = 0

  /** Subscribes `receiver` to `key` of `subs`; `onDetach` runs once it is removed. */
  subscribe({
    subs,
    key,
    receiver,
    localReceiverCount,
    onDetach,
  }: {
    subs: Subscriptions
    key: string
    receiver: BackendReceiver<BackendPayload>
    localReceiverCount: () => number
    onDetach?: () => void
  }): MemorySubscriptionAttempt {
    const sub: MemorySubscriptionAttempt = new MemorySubscriptionAttempt({
      receiver,
      localReceiverCount,
      detach: () => {
        removeSubscription(subs, key, sub)
        onDetach?.()
      },
    })
    subs.set(key, [...(subs.get(key) ?? []), sub])
    // Ready once what was published before it is delivered, as a network subscriber hears its confirmation after the
    // messages sent before it.
    if (this._queuing === 0 && this._deliveries.length === 0) sub.establish()
    else queueMicrotask(() => this._deliveries.deliver(() => sub.establish()))
    return sub
  }

  /** How many local consumers `targets` reach. */
  receivers(...targets: (readonly MemorySubscriptionAttempt[])[]): number {
    return targets.reduce((total, of) => total + sumReceiverCounts(of), 0)
  }

  /** Delivers `payload` to `targets` at once while nothing is queued; else queues it. */
  publish(
    targets: readonly MemorySubscriptionAttempt[],
    payload: BackendPayload,
    mark: OrderingInfo,
  ): Receipt | Promise<Receipt> {
    // Counted before delivery, which may unsubscribe or subscribe.
    const receipt = { seq: mark.seq, timestamp: mark.timestamp, receivers: sumReceiverCounts(targets) }
    if (this._deliveries.deliversNow()) {
      this._deliveries.deliverNow(() => {
        for (const target of targets) target.deliver(payload, mark)
      })
      return receipt
    }
    const held = typeof payload === 'string' ? payload : copyBytes(payload)
    // A queued publish resolves as it is delivered, so a publisher awaiting each can't outrun its subscribers.
    return new Promise((resolve) => {
      this._deliveries.deliver(() => {
        for (const target of targets) target.deliver(held, mark)
        resolve(receipt)
      })
    })
  }

  /** Queues `delivery` from a microtask, so a subscription made meanwhile is ready after it. Past a turn's deliveries,
   *  returns the next turn, which the caller waits for as for a network write, so it can't outrun them. */
  deliverSoon(delivery: () => void): Promise<void> | null {
    this._queuing++
    queueMicrotask(() => {
      this._queuing--
      this._deliveries.deliver(delivery)
    })
    return this._deliveries.isFull ? new Promise<void>(nextTurn) : null
  }
}
