export { MemoryBackend }

// The in-process backend.

import {
  DriverAttempt,
  type BackendPayload,
  type BackendReceiver,
  type BroadcastDriver,
  type BroadcastPayload,
  type BroadcastRoute,
  type PublishResult,
} from '../driver.js'
import type { OrderingInfo } from '../driverOrdering.js'
import type { BroadcastKind } from '../../wire/payloads.js'
import { Fifo } from '../../../utils/Fifo.js'
import { utf8ByteLength } from '../../../utils/utf8ByteLength.js'
import { CHANNEL_HOLD_MAX_BYTES } from '../../constants.js'
import { ChannelOverflowError } from '../../channel/channelErrors.js'
import { HELD_SEND_OVERHEAD_BYTES } from '../../channel/server/channelMemory.js'
import { addSubscriber, deliverTo, removeSubscriber, type Subscribers } from './memorySubscribers.js'

/** How deep in-memory publishes in answer to answers may go: 10,000, past the about 1,300 that Node.js's call stack holds
 *  for a recursive delivery. A chain that deep is taken for a listener that answers its own messages without end. */
const MAX_ANSWER_DEPTH = 10_000

type QueuedPublish = { size: number; depth: number; deliver: () => void }

class MemoryBackend implements BroadcastDriver {
  /** The order of each key subscribed here; an unsubscribed key's next publish starts above `broadcastHighest`. */
  private readonly broadcastOrder = new Map<string, OrderingInfo>()
  /** The highest `seq` and `timestamp` a Broadcast publish was given. */
  private readonly broadcastHighest: OrderingInfo = { seq: 0, timestamp: 0 }
  private readonly broadcastSubs: Record<BroadcastKind, Subscribers> = { text: new Map(), binary: new Map() }
  /** Publishes from listeners, waiting for the ones before them, in publish order. */
  private readonly queued = new Fifo<QueuedPublish>()
  /** What `queued` holds: at most `CHANNEL_HOLD_MAX_BYTES`, or one message whatever its size. */
  private queuedBytes = 0
  private delivering = false
  /** How many answers deep the publish being delivered is: 0 for one from outside a listener. */
  private depth = 0

  /** A publish reaches the subscribers its key has as it is made. One from a listener reaches them after the message it
   *  answers, within the publish that started the chain. */
  publish(route: BroadcastRoute, payload: BroadcastPayload): PublishResult | Promise<PublishResult> {
    const targets = this.broadcastSubs[route.kind].get(route.key) ?? []
    if (this.delivering) return this.queue(targets, payload, route.key)
    this.delivering = true
    try {
      return this.deliverNow(targets, payload, route.key)
    } finally {
      this.deliverQueued()
      this.delivering = false
    }
  }

  subscribe({ kind, key }: BroadcastRoute, receiver: BackendReceiver): DriverAttempt {
    addSubscriber(this.broadcastSubs[kind], key, receiver)
    return new MemoryBroadcastAttempt(() => {
      if (removeSubscriber(this.broadcastSubs[kind], key, receiver)) this.released(key)
    })
  }

  published(key: string): number {
    return this.broadcastOrder.get(key)?.seq ?? 0
  }

  /** Delivers what listeners published in answer, and what they publish in answer to that, in order. */
  private deliverQueued(): void {
    for (let publish = this.queued.shift(); publish !== undefined; publish = this.queued.shift()) {
      this.queuedBytes -= publish.size
      this.depth = publish.depth
      publish.deliver()
    }
    this.depth = 0
  }

  private queue(targets: readonly BackendReceiver[], payload: BackendPayload, key: string): Promise<PublishResult> {
    if (this.depth === MAX_ANSWER_DEPTH) return Promise.reject(endlessAnswersError())
    const isText = typeof payload === 'string'
    const size = (isText ? utf8ByteLength(payload) : payload.byteLength) + HELD_SEND_OVERHEAD_BYTES
    // One message alone goes through whatever its size.
    if (this.queuedBytes > 0 && this.queuedBytes + size > CHANNEL_HOLD_MAX_BYTES) {
      return Promise.reject(
        new ChannelOverflowError(
          `Publish rejected: publishes waiting to be delivered exceeded ${CHANNEL_HOLD_MAX_BYTES} bytes; publish less from the listener`,
        ),
      )
    }
    this.queuedBytes += size
    // The caller may reuse or transfer its buffer once the publish returns.
    const held = isText ? payload : new Uint8Array(payload)
    // Its place in the key's order is the publish's: a subscriber that comes while it waits skips it by its seq.
    const mark = this.takeOrdering(key)
    return new Promise((resolve) => {
      const deliver = () => {
        deliverTo(targets, held, mark)
        resolve(mark)
      }
      this.queued.push({ size, depth: this.depth + 1, deliver })
    })
  }

  private deliverNow(targets: readonly BackendReceiver[], payload: BackendPayload, key: string): OrderingInfo {
    const mark = this.takeOrdering(key)
    deliverTo(targets, payload, mark)
    return mark
  }

  private takeOrdering(key: string): OrderingInfo {
    const order = this.broadcastOrder.get(key)
    const mark = nextOrdering(order ?? this.broadcastHighest, Date.now())
    // Only a subscribed key has an order.
    if (order !== undefined || this.broadcastSubscribed(key)) this.broadcastOrder.set(key, mark)
    const highest = this.broadcastHighest
    if (mark.seq > highest.seq) highest.seq = mark.seq
    if (mark.timestamp > highest.timestamp) highest.timestamp = mark.timestamp
    return mark
  }

  /** A key no route of either kind subscribes to forgets its order. */
  private released(key: string): void {
    if (!this.broadcastSubscribed(key)) this.broadcastOrder.delete(key)
  }

  private broadcastSubscribed(key: string): boolean {
    return this.broadcastSubs.text.has(key) || this.broadcastSubs.binary.has(key)
  }
}

/** Ready from the start: a publish reads its key's subscribers when it is made. */
class MemoryBroadcastAttempt extends DriverAttempt {
  private readonly remove: () => void

  constructor(remove: () => void) {
    super()
    this.remove = remove
    this.transition('ready')
  }

  async unsubscribe(): Promise<void> {
    this.transition('closed')
    this.remove()
  }
}

/** `seq` strictly increases per key; `timestamp` is this process's time, clamped so it never goes back. */
function nextOrdering(previous: OrderingInfo | undefined, now: number): OrderingInfo {
  return { seq: (previous?.seq ?? 0) + 1, timestamp: Math.max(now, previous?.timestamp ?? 0) }
}

function endlessAnswersError(): ChannelOverflowError {
  return new ChannelOverflowError(
    `Publish rejected: listeners published in answer to each other's answers ${MAX_ANSWER_DEPTH} deep, far past the about 1,300 a call stack holds, which is taken for a listener that answers its own messages without end; make it skip the messages it published itself`,
  )
}
