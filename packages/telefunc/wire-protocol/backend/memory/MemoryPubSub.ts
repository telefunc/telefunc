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

/** Queued deliveries from which a Room commit waits for the next turn, as for a network write, so it can't outrun them. */
const QUEUED_DELIVERIES_MAX = 1024

/** Deliveries in seq order: a publish made inside one is delivered after it. */
class MemoryDeliveries {
  private readonly queue = new Fifo<() => void>()
  private delivering = false

  /** A listener is running. */
  get isDelivering(): boolean {
    return this.delivering
  }

  /** Nothing runs or waits. */
  get isIdle(): boolean {
    return !this.delivering && this.queue.length === 0
  }

  get isFull(): boolean {
    return this.queue.length >= QUEUED_DELIVERIES_MAX
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
  private readonly drain = (): void => {
    for (let waiting = this.queue.length; waiting > 0; waiting--) {
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

/** An in-process driver's pub/sub: subscribers by key, delivered in order, a turn's worth at a time. */
class MemoryPubSub {
  private readonly deliveries = new MemoryDeliveries()
  /** Deliveries a microtask is yet to queue. */
  private queuing = 0
  /** What listeners' publishes hold waiting, of each kind. */
  private readonly waitingBytes = { text: 0, binary: 0 }

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

  /** Delivers `payload` to `targets` at once while nothing is queued; else queues it, or, from a listener, rejects past
   *  the buffer limit of its kind. `takeOrdering` runs only for a publish that goes on. */
  publish(
    targets: readonly BackendReceiver[],
    payload: BackendPayload,
    key: string,
    takeOrdering: (key: string) => OrderingInfo,
  ): OrderingInfo | Promise<OrderingInfo> {
    if (this.deliveries.enterPublish()) {
      const mark = takeOrdering(key)
      this.deliverTo(targets, payload, mark)
      this.deliveries.leavePublish()
      return mark
    }
    const isText = typeof payload === 'string'
    const kind = isText ? 'text' : 'binary'
    // What is bounded is what listeners pile up, one message alone going through whatever its size. A publish from
    // outside a listener waits its turn.
    const fromListener = this.deliveries.isDelivering
    const size = fromListener ? (isText ? utf8ByteLength(payload) : payload.byteLength) + HELD_SEND_OVERHEAD_BYTES : 0
    const setting = isText ? 'bufferLimit' : 'bufferLimitBinary'
    if (fromListener && this.waitingBytes[kind] > 0 && this.waitingBytes[kind] + size > getChannelLimits()[setting]) {
      return Promise.reject(
        new ChannelOverflowError(
          `Publish rejected: publishes made from a listener, waiting to be delivered, exceeded config.channel.${setting}; raise the setting or publish less from the listener`,
        ),
      )
    }
    this.waitingBytes[kind] += size
    const mark = takeOrdering(key)
    const held = isText ? payload : new Uint8Array(payload)
    // A queued publish resolves as it is delivered, so a publisher awaiting each can't outrun its subscribers.
    return new Promise((resolve) => {
      this.deliveries.deliver(() => {
        this.waitingBytes[kind] -= size
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

  /** Queues `delivery` from a microtask, so a subscription made meanwhile is ready after it. With the queue full, returns
   *  the next turn, which the caller waits for. */
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
