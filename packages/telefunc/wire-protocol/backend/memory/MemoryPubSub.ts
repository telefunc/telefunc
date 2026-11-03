export { MemoryPubSub }
export type { MemoryAttemptOwner, Subscribers }

import { Fifo } from '../../../utils/Fifo.js'
import { DriverAttempt, type BackendPayload, type BackendReceiver } from '../driver.js'
import type { OrderingInfo } from '../../wire/orderingFrame.js'
import { nextTurn } from '../../flowControl/macrotaskYield.js'

/** Receivers by key; a change replaces a key's array, so an array read at a publish or commit is its targets. */
type Subscribers = Map<string, readonly BackendReceiver[]>

/** Told each time an attempt on `key` lets go. */
type MemoryAttemptOwner = { released(key: string): void }

type MemorySubscription = {
  subscribers: Subscribers
  key: string
  receiver: BackendReceiver
  owner?: MemoryAttemptOwner
}

/** Deliveries one event-loop turn runs; the rest wait for the next, so a listener answering itself
 *  can't starve the loop. */
const DELIVERIES_PER_TURN = 1024

/** The queue's head while a publish delivers at once: it keeps what that delivery causes queued behind it. */
const DELIVERING_NOW = (): void => {}

function copyBytes(bytes: Uint8Array): Uint8Array {
  return new Uint8Array(bytes)
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

  /** Runs a delivery while nothing is queued, holding the queue's head as `drain()` does. */
  deliverNow(delivery: () => void): void {
    if (this.deliveredThisTurn++ === 0) nextTurn(() => this.startTurn())
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
      if (this.deliveredThisTurn++ === 0) nextTurn(() => this.startTurn())
      delivery()
      this.queue.shift()
    }
  }

  private startTurn(): void {
    this.deliveredThisTurn = 0
    this.drain()
  }
}

/** An in-process driver's pub/sub: subscribers by key, delivered in order, a turn's worth at a time. */
class MemoryPubSub {
  private readonly deliveries = new MemoryDeliveries()
  /** Deliveries a microtask is yet to queue. */
  private queuing = 0

  subscribe(subscription: MemorySubscription): DriverAttempt {
    const { subscribers, key, receiver } = subscription
    subscribers.set(key, [...(subscribers.get(key) ?? []), receiver])
    const attempt = new MemoryAttempt({ pubSub: this, subscription })
    // Ready once what was published before it is delivered, as a network subscriber hears its confirmation after the
    // messages sent before it, and before what is published after it.
    if (this.queuing > 0) queueMicrotask(() => this.deliveries.deliver(() => attempt.delivered()))
    else if (this.deliveries.length > 0) this.deliveries.deliver(() => attempt.delivered())
    else attempt.delivered()
    return attempt
  }

  unsubscribe({ subscribers, key, receiver, owner }: MemorySubscription): void {
    const current = subscribers.get(key)
    if (current === undefined || !current.includes(receiver)) return
    const rest = current.filter((other) => other !== receiver)
    if (rest.length === 0) subscribers.delete(key)
    else subscribers.set(key, rest)
    owner?.released(key)
  }

  /** Delivers `payload` to `targets` at once while nothing is queued; else queues it. */
  publish(
    targets: readonly BackendReceiver[],
    payload: BackendPayload,
    mark: OrderingInfo,
  ): OrderingInfo | Promise<OrderingInfo> {
    const receipt = { seq: mark.seq, timestamp: mark.timestamp }
    if (this.deliveries.deliversNow()) {
      this.deliveries.deliverNow(() => {
        for (const target of targets) target.deliver(payload, mark)
      })
      return receipt
    }
    const held = typeof payload === 'string' ? payload : copyBytes(payload)
    // A queued publish resolves as it is delivered, so a publisher awaiting each can't outrun its subscribers.
    return new Promise((resolve) => {
      this.deliveries.deliver(() => {
        for (const target of targets) target.deliver(held, mark)
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

type MemoryAttemptOptions = { pubSub: MemoryPubSub; subscription: MemorySubscription }

/** An in-process subscription: its receiver is in its key's targets until it unsubscribes. */
class MemoryAttempt extends DriverAttempt {
  private readonly pubSub: MemoryPubSub
  private readonly subscription: MemorySubscription

  constructor({ pubSub, subscription }: MemoryAttemptOptions) {
    super()
    this.pubSub = pubSub
    this.subscription = subscription
  }

  /** What was published before it is delivered. */
  delivered(): void {
    this.transition('ready')
  }

  async unsubscribe(): Promise<void> {
    this.transition('closed')
    this.pubSub.unsubscribe(this.subscription)
  }
}
