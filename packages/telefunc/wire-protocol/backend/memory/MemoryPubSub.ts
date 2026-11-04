export { MemoryPubSub }
export type { MemoryAttemptOwner, Subscribers }

import { Fifo } from '../../../utils/Fifo.js'
import { utf8ByteLength } from '../../../utils/utf8ByteLength.js'
import { getChannelLimits } from '../../../node/server/serverConfig.js'
import { ChannelOverflowError } from '../../channel/channelErrors.js'
import { HELD_SEND_OVERHEAD_BYTES } from '../../channel/server/ServerChannelBuffer.js'
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

/** Deliveries one event-loop turn runs; the rest wait for the next, so a listener answering itself, directly or after
 *  an `await`, can't starve the loop. */
const DELIVERIES_PER_TURN = 1024

/** The queue's head while a publish delivers at once: it keeps what that delivery causes queued behind it. */
const DELIVERING_NOW = (): void => {}

function copyBytes(bytes: Uint8Array): Uint8Array {
  return new Uint8Array(bytes)
}

/** Deliveries in seq order: the running one stays queued, so a publish made inside it is delivered after. */
class MemoryDeliveries {
  private readonly queue = new Fifo<() => void>()
  /** Deliveries since the last reset, a synchronous run of publishes past half of `DELIVERIES_PER_TURN` counting once. */
  private deliveredSinceReset = 0
  /** A publish ran in this synchronous run: code publishing in a loop holds the event loop itself, so one made while
   *  this is set has no turn to wait for, and the run counts once. */
  private inSyncRun = false

  get length(): number {
    return this.queue.length
  }

  /** A turn's worth of deliveries is queued. */
  get isFull(): boolean {
    return this.queue.length >= DELIVERIES_PER_TURN
  }

  /** Runs a publish's delivery, holding the queue's head as `drain()` does, and returns its result; null if it has to
   *  wait: one is queued or running, or a new synchronous run finds this turn's deliveries spent. */
  publishNow<Result>(delivery: () => Result): Result | null {
    const startsRun = !this.inSyncRun
    // Below half a turn's deliveries, each counts: nothing waits soon, so a publish needs no microtask.
    if (startsRun && this.deliveredSinceReset >= DELIVERIES_PER_TURN / 2) {
      this.inSyncRun = true
      queueMicrotask(this.endSyncRun)
    }
    if (this.queue.length > 0 || (startsRun && this.deliveredSinceReset >= DELIVERIES_PER_TURN)) return null
    if (startsRun) this.countDelivery()
    this.queue.push(DELIVERING_NOW)
    const result = delivery()
    this.queue.shift()
    this.drain()
    return result
  }

  /** Runs `delivery` after those queued before it: now, unless one is running or this turn's deliveries ran out. */
  deliver(delivery: () => void): void {
    const now = this.queue.length === 0 && this.deliveredSinceReset < DELIVERIES_PER_TURN
    this.queue.push(delivery)
    if (now) this.drain()
  }

  private drain(): void {
    for (let delivery = this.queue.peek(); delivery !== undefined; delivery = this.queue.peek()) {
      if (this.deliveredSinceReset === DELIVERIES_PER_TURN) return
      this.countDelivery()
      delivery()
      this.queue.shift()
    }
  }

  private countDelivery(): void {
    // Only once half a turn's deliveries ran: a macrotask every turn slows a server that publishes once per turn.
    if (++this.deliveredSinceReset === DELIVERIES_PER_TURN / 2) nextTurn(() => this.startTurn())
  }

  private readonly endSyncRun = (): void => {
    this.inSyncRun = false
  }

  private startTurn(): void {
    this.deliveredSinceReset = 0
    this.drain()
  }
}

/** An in-process driver's pub/sub: subscribers by key, delivered in order, a turn's worth at a time. */
class MemoryPubSub {
  private readonly deliveries = new MemoryDeliveries()
  /** Deliveries a microtask is yet to queue. */
  private queuing = 0
  /** What publishes hold waiting, of each kind. */
  private readonly waitingBytes = { text: 0, binary: 0 }

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

  /** Delivers `payload` to `targets` at once while nothing is queued; else queues it, or rejects past the buffer limit
   *  of its kind. `takeOrdering` runs only for a publish that goes on. */
  publish(
    targets: readonly BackendReceiver[],
    payload: BackendPayload,
    takeOrdering: () => OrderingInfo,
  ): OrderingInfo | Promise<OrderingInfo> {
    const receiptOf = ({ seq, timestamp }: OrderingInfo): OrderingInfo => ({ seq, timestamp })
    const delivered = this.deliveries.publishNow(() => {
      const mark = takeOrdering()
      for (const target of targets) target.deliver(payload, mark)
      return receiptOf(mark)
    })
    if (delivered !== null) return delivered
    const isText = typeof payload === 'string'
    const kind = isText ? 'text' : 'binary'
    const size = (isText ? utf8ByteLength(payload) : payload.byteLength) + HELD_SEND_OVERHEAD_BYTES
    const setting = isText ? 'bufferLimit' : 'bufferLimitBinary'
    // What is bounded is how much waits: one message alone goes through, whatever its size.
    if (this.waitingBytes[kind] > 0 && this.waitingBytes[kind] + size > getChannelLimits()[setting]) {
      return Promise.reject(
        new ChannelOverflowError(
          `Publish rejected: publishes waiting to be delivered exceeded config.channel.${setting}; await each publish or raise the setting`,
        ),
      )
    }
    this.waitingBytes[kind] += size
    const mark = takeOrdering()
    const held = isText ? payload : copyBytes(payload)
    // A queued publish resolves as it is delivered, so a publisher awaiting each can't outrun its subscribers.
    return new Promise((resolve) => {
      this.deliveries.deliver(() => {
        this.waitingBytes[kind] -= size
        for (const target of targets) target.deliver(held, mark)
        resolve(receiptOf(mark))
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
