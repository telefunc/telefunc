export { MemoryCommitQueue }
export type { MemoryAttempt }

import {
  addSubscriber,
  type BackendReceiver,
  DriverAttempt,
  Fifo,
  isCloudflareWorkers,
  nextTurn,
  removeSubscriber,
  type Subscribers,
} from 'telefunc/__internal'
import { assert } from '../utils/assert.js'

/** A Worker's timers die with the request that set them, so what waits for a turn would wait for good. There deliveries
 *  go on, and the runtime's own limits are what end a listener that never stops answering. */
const TURNS_OUTLIVE_REQUESTS = !isCloudflareWorkers()

/** Deliveries in seq order: a commit made inside one is delivered after it. */
class MemoryDeliveries {
  private readonly queue = new Fifo<() => void>()
  private delivering = false
  private holding = false

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

  /** Runs `delivery` after those queued before it: now, unless one is running or queued. */
  deliver(delivery: () => void): void {
    const now = this.isIdle
    this.queue.push(delivery)
    if (now) this.drain()
  }

  /** Delivers what waits now. What its listeners commit waits for the next turn, so a listener answering itself can't
   *  starve the event loop. */
  readonly drain = (): void => {
    this.holding = false
    for (
      let waiting = TURNS_OUTLIVE_REQUESTS ? this.queue.length : Infinity;
      waiting > 0 && this.queue.length > 0;
      waiting--
    ) {
      const delivery = this.queue.shift()
      assert(delivery !== undefined)
      this.delivering = true
      // What throws here is a bug, reported: it stops neither the queue behind it nor the commits waiting on it.
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

type MemoryCommitQueueOptions = {
  /** How many commits go before those from the next microtask on wait for the next turn. */
  commitsBetweenTurns: number
}

/** The in-memory driver's commits, delivered in order, a turn's worth at a time. */
class MemoryCommitQueue {
  private readonly deliveries = new MemoryDeliveries()
  private readonly commitsBetweenTurns: number
  private commitsSinceTurn = 0
  /** Deliveries a microtask is yet to queue. */
  private queuing = 0

  constructor({ commitsBetweenTurns }: MemoryCommitQueueOptions) {
    this.commitsBetweenTurns = TURNS_OUTLIVE_REQUESTS ? commitsBetweenTurns : Infinity
  }

  /** Adds `receiver` to `key`'s subscribers: ready once what was committed before it is delivered, as a network
   *  subscriber hears its confirmation after the messages sent before it, and before what is committed after it. The
   *  attempt is in `of` until it ends. */
  subscribe(subscribers: Subscribers, key: string, receiver: BackendReceiver, of: Set<MemoryAttempt>): DriverAttempt {
    addSubscriber(subscribers, key, receiver)
    const attempt = new MemoryAttempt(() => {
      removeSubscriber(subscribers, key, receiver)
      of.delete(attempt)
    })
    of.add(attempt)
    if (this.queuing > 0) queueMicrotask(() => this.deliveries.deliver(() => attempt.delivered()))
    else if (!this.deliveries.isIdle) this.deliveries.deliver(() => attempt.delivered())
    else attempt.delivered()
    return attempt
  }

  /** Ends each of `attempts` once what was committed before is delivered, as every backend ends a dropped
   *  incarnation's lanes. */
  endAfterDeliveries(attempts: Set<MemoryAttempt>): void {
    this.queuing++
    queueMicrotask(() => {
      this.queuing--
      this.deliveries.deliver(() => {
        for (const attempt of [...attempts]) attempt.end()
      })
    })
  }

  /** Queues `delivery` from a microtask, so a subscription made meanwhile is ready after it. Returns the turn the caller
   *  waits for while what comes now waits. */
  deliverSoon(delivery: () => void): Promise<void> | null {
    this.queuing++
    queueMicrotask(() => {
      this.queuing--
      this.deliveries.deliver(delivery)
    })
    this.countCommit()
    return this.deliveries.isHolding ? new Promise<void>(nextTurn) : null
  }

  /** Awaited, a commit resolves without a turn, and a listener can answer after an `await` that nobody awaits:
   *  chains of microtasks that would starve the event loop, which only something left waiting ends. A synchronous loop is
   *  no such chain and can't be interrupted anyway, so it goes on. */
  private countCommit(): void {
    if (++this.commitsSinceTurn !== this.commitsBetweenTurns) return
    queueMicrotask(this.holdCommits)
    // Now: a producer's next turn, which it schedules once its loop ends, finds the hold over.
    nextTurn(this.deliveries.drain)
  }

  private readonly holdCommits = (): void => {
    this.commitsSinceTurn = 0
    this.deliveries.hold()
  }
}

/** An in-process subscription: its receiver is in its key's subscribers until it unsubscribes or its incarnation
 *  ends. */
class MemoryAttempt extends DriverAttempt {
  constructor(private readonly remove: () => void) {
    super()
  }

  /** What was committed before it is delivered. */
  delivered(): void {
    this.transition('ready')
  }

  /** Over on purpose, as its incarnation ended; again, nothing. */
  end(): void {
    this.transition('closed')
    this.remove()
  }

  async unsubscribe(): Promise<void> {
    this.end()
  }
}
