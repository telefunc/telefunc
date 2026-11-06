export { MemoryPubSub }
export type { MemoryAttemptOwner, Subscribers }

import { Fifo } from '../../../utils/Fifo.js'
import { isCloudflareWorkers } from '../../../utils/isCloudflareWorkers.js'
import { utf8ByteLength } from '../../../utils/utf8ByteLength.js'
import { CHANNEL_HOLD_MAX_BYTES } from '../../constants.js'
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

/** A Worker's timers die with the request that set them, so what waits for a turn would wait for good. There deliveries
 *  go on, and the runtime's own limits are what end a listener that never stops answering. */
const TURNS_OUTLIVE_REQUESTS = !isCloudflareWorkers()

/** Deliveries in seq order: a publish made inside one is delivered after it. */
class MemoryDeliveries {
  private readonly queue = new Fifo<() => void>()
  private delivering = false
  private holding = false

  /** A listener is running. */
  get isDelivering(): boolean {
    return this.delivering
  }

  /** What comes now waits for the next `drain()`. */
  get isHolding(): boolean {
    return this.holding
  }

  /** Nothing runs or waits. */
  get isIdle(): boolean {
    return !this.delivering && !this.holding && this.queue.length === 0
  }

  hold(): void {
    this.holding = true
  }

  /** Whether a publish delivers now, until `leavePublish()`: it waits if one is queued or running. */
  enterPublish(): boolean {
    if (!this.isIdle) return false
    this.delivering = true
    return true
  }

  leavePublish(): void {
    this.delivering = false
    this.drain()
  }

  /** Runs `delivery` after those queued before it: now, unless one is running or queued. */
  deliver(delivery: () => void): void {
    const now = this.isIdle
    this.queue.push(delivery)
    if (now) this.drain()
  }

  /** Delivers what waits now. What its listeners publish waits for the next turn, so a listener answering itself can't
   *  starve the event loop. */
  readonly drain = (): void => {
    this.holding = false
    for (
      let waiting = TURNS_OUTLIVE_REQUESTS ? this.queue.length : Infinity;
      waiting > 0 && this.queue.length > 0;
      waiting--
    ) {
      const delivery = this.queue.shift()!
      this.delivering = true
      // What throws here is a bug, reported: it stops neither the queue behind it nor the publishes waiting on it.
      try {
        delivery()
      } catch (error) {
        console.error(error)
      }
      this.delivering = false
    }
    if (this.queue.length > 0) nextTurn(this.drain)
  }
}

type MemoryPubSubOptions = {
  /** How many publishes or commits go before those from the next microtask on wait for the next turn. */
  operationsBetweenTurns: number
}

/** An in-process driver's pub/sub: subscribers by key, delivered in order, a turn's worth at a time. */
class MemoryPubSub {
  private readonly deliveries = new MemoryDeliveries()
  private readonly operationsBetweenTurns: number
  private operationsSinceTurn = 0
  /** Deliveries a microtask is yet to queue. */
  private queuing = 0
  /** What waits to be delivered: at most `CHANNEL_HOLD_MAX_BYTES` and one message, since a publish outside a listener is
   *  never refused. */
  private waitingBytes = 0

  constructor({ operationsBetweenTurns }: MemoryPubSubOptions) {
    this.operationsBetweenTurns = TURNS_OUTLIVE_REQUESTS ? operationsBetweenTurns : Infinity
  }

  subscribe(subscription: MemorySubscription): DriverAttempt {
    const { subscribers, key, receiver } = subscription
    subscribers.set(key, [...(subscribers.get(key) ?? []), receiver])
    const attempt = new MemoryAttempt({ pubSub: this, subscription })
    // Ready once what was published before it is delivered, as a network subscriber hears its confirmation after the
    // messages sent before it, and before what is published after it.
    if (this.queuing > 0) queueMicrotask(() => this.deliveries.deliver(() => attempt.delivered()))
    else if (!this.deliveries.isIdle) this.deliveries.deliver(() => attempt.delivered())
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

  /** Delivers `payload` to `targets` at once while nothing is queued; else queues it, or, past
   *  `CHANNEL_HOLD_MAX_BYTES`, rejects it from a listener and delivers what waits from elsewhere. `takeOrdering` runs only
   *  for a publish that goes on. */
  publish(
    targets: readonly BackendReceiver[],
    payload: BackendPayload,
    key: string,
    takeOrdering: (key: string) => OrderingInfo,
  ): OrderingInfo | Promise<OrderingInfo> {
    if (this.deliveries.enterPublish()) {
      try {
        this.countOperation()
        const mark = takeOrdering(key)
        this.deliverTo(targets, payload, mark)
        return mark
      } finally {
        this.deliveries.leavePublish()
      }
    }
    const isText = typeof payload === 'string'
    const size = (isText ? utf8ByteLength(payload) : payload.byteLength) + HELD_SEND_OVERHEAD_BYTES
    // One message alone goes through whatever its size.
    if (this.waitingBytes > 0 && this.waitingBytes + size > CHANNEL_HOLD_MAX_BYTES) {
      if (this.deliveries.isDelivering) {
        return Promise.reject(
          new ChannelOverflowError(
            `Publish rejected: publishes waiting to be delivered exceeded ${CHANNEL_HOLD_MAX_BYTES} bytes; publish less from the listener`,
          ),
        )
      }
      // Outside a listener nothing rejects: what waits goes now.
      this.deliveries.drain()
    }
    this.waitingBytes += size
    const mark = takeOrdering(key)
    const held = isText ? payload : new Uint8Array(payload)
    // A queued publish resolves as it is delivered, so a publisher awaiting each can't outrun its subscribers.
    return new Promise((resolve) => {
      this.deliveries.deliver(() => {
        this.waitingBytes -= size
        this.deliverTo(targets, held, mark)
        resolve(mark)
      })
    })
  }

  /** Hands a message to each of `targets`: one that throws is reported, and the others still get it. */
  deliverTo(targets: readonly BackendReceiver[], payload: BackendPayload, ordering: OrderingInfo): void {
    for (const target of targets) {
      try {
        target.deliver(payload, ordering)
      } catch (error) {
        console.error(error)
      }
    }
  }

  /** Queues `delivery` from a microtask, so a subscription made meanwhile is ready after it. Returns the turn the caller
   *  waits for while what comes now waits. */
  deliverSoon(delivery: () => void): Promise<void> | null {
    this.queuing++
    queueMicrotask(() => {
      this.queuing--
      this.deliveries.deliver(delivery)
    })
    this.countOperation()
    return this.deliveries.isHolding ? new Promise<void>(nextTurn) : null
  }

  /** Awaited, a publish or commit resolves without a turn, and a listener can answer after an `await` that nobody awaits:
   *  chains of microtasks that would starve the event loop, which only something left waiting ends. A synchronous loop is
   *  no such chain and can't be interrupted anyway, so it goes on. */
  private countOperation(): void {
    if (++this.operationsSinceTurn !== this.operationsBetweenTurns) return
    queueMicrotask(this.holdOperations)
    // Now: a producer's next turn, which it schedules once its loop ends, finds the hold over.
    nextTurn(this.deliveries.drain)
  }

  private readonly holdOperations = (): void => {
    this.operationsSinceTurn = 0
    this.deliveries.hold()
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
