export { MemoryPubSub }
export type { Subscribers }

import { Fifo } from '../../../utils/Fifo.js'
import type { BackendPayload, Subscriber } from '../driver.js'
import type { OrderingInfo } from '../../wire/orderingFrame.js'
import { nextTurn } from '../../flowControl/macrotaskYield.js'

/** Subscribers by key; a change replaces a key's array, so an array read at a publish or commit is its targets. */
type Subscribers = Map<string, readonly Subscriber[]>

/** A publish's ordering and the local consumers it reached. */
type Receipt = OrderingInfo & { receivers: number }

/** Deliveries one event-loop turn runs; the rest wait for the next, so a listener answering itself
 *  can't starve the loop. */
const DELIVERIES_PER_TURN = 1024

/** The queue's head while a publish delivers at once: it keeps what that delivery causes queued behind it. */
const DELIVERING_NOW = (): void => {}

function copyBytes(bytes: Uint8Array): Uint8Array {
  return new Uint8Array(bytes)
}

function countConsumers(targets: readonly Subscriber[]): number {
  return targets.reduce((total, target) => total + target.consumers, 0)
}

/** Deliveries in seq order: the running one stays queued, so a publish made inside it is delivered after. */
class MemoryDeliveries {
  private readonly queue = new Fifo<() => void>()
  private deliveredThisTurn = 0

  get length(): number {
    return this.queue.length
  }

  /** A turn's worth of deliveries is queued. */
  get isFull(): boolean {
    return this.queue.length >= DELIVERIES_PER_TURN
  }

  deliversNow(): boolean {
    return this.queue.length === 0 && this.deliveredThisTurn < DELIVERIES_PER_TURN
  }

  /** Runs a delivery while nothing is queued, holding the queue's head as `_drain()` does. */
  deliverNow(delivery: () => void): void {
    if (this.deliveredThisTurn++ === 0) nextTurn(() => this.nextTurn())
    this.queue.push(DELIVERING_NOW)
    delivery()
    this.queue.shift()
    this.drain()
  }

  /** Runs `delivery` after those queued before it: now, unless one is running or this turn's deliveries ran out. */
  deliver(delivery: () => void): void {
    const now = this.deliversNow()
    this.queue.push(delivery)
    if (now) this.drain()
  }

  private drain(): void {
    for (let delivery = this.queue.peek(); delivery !== undefined; delivery = this.queue.peek()) {
      if (this.deliveredThisTurn === DELIVERIES_PER_TURN) return
      if (this.deliveredThisTurn++ === 0) nextTurn(() => this.nextTurn())
      delivery()
      this.queue.shift()
    }
  }

  private nextTurn(): void {
    this.deliveredThisTurn = 0
    this.drain()
  }
}

/** An in-process driver's pub/sub: subscribers by key, delivered in order, a turn's worth at a time. */
class MemoryPubSub {
  private readonly deliveries = new MemoryDeliveries()
  /** Deliveries a microtask is yet to queue. */
  private queuing = 0

  subscribe(subscribers: Subscribers, key: string, subscriber: Subscriber): void {
    subscribers.set(key, [...(subscribers.get(key) ?? []), subscriber])
    // Ready once what was published before it is delivered, as a network subscriber hears its confirmation after the
    // messages sent before it, and before what is published after it.
    if (this.queuing > 0) queueMicrotask(() => this.deliveries.deliver(() => subscriber.ready()))
    else if (this.deliveries.length > 0) this.deliveries.deliver(() => subscriber.ready())
    else subscriber.ready()
  }

  unsubscribe(subscribers: Subscribers, key: string, subscriber: Subscriber): void {
    const current = subscribers.get(key)
    if (current === undefined || !current.includes(subscriber)) return
    const rest = current.filter((other) => other !== subscriber)
    if (rest.length === 0) subscribers.delete(key)
    else subscribers.set(key, rest)
  }

  /** How many local consumers `targets` reach. */
  consumers(...targets: (readonly Subscriber[])[]): number {
    return targets.reduce((total, of) => total + countConsumers(of), 0)
  }

  /** Delivers `payload` to `targets` at once while nothing is queued; else queues it. */
  publish(targets: readonly Subscriber[], payload: BackendPayload, mark: OrderingInfo): Receipt | Promise<Receipt> {
    // Counted before delivery, which may unsubscribe or subscribe.
    const receipt = { seq: mark.seq, timestamp: mark.timestamp, receivers: countConsumers(targets) }
    if (this.deliveries.deliversNow()) {
      this.deliveries.deliverNow(() => {
        for (const target of targets) target.message(payload, mark)
      })
      return receipt
    }
    const held = typeof payload === 'string' ? payload : copyBytes(payload)
    // A queued publish resolves as it is delivered, so a publisher awaiting each can't outrun its subscribers.
    return new Promise((resolve) => {
      this.deliveries.deliver(() => {
        for (const target of targets) target.message(held, mark)
        resolve(receipt)
      })
    })
  }

  /** Queues `delivery` from a microtask, so a subscription made meanwhile is ready after it. Past a turn's deliveries,
   *  returns the next turn, which the caller waits for as for a network write, so it can't outrun them. */
  deliverSoon(delivery: () => void): Promise<void> | null {
    this.queuing++
    queueMicrotask(() => {
      this.queuing--
      this.deliveries.deliver(delivery)
    })
    return this.deliveries.isFull ? new Promise<void>(nextTurn) : null
  }
}
