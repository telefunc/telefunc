export { PublishHold }
export type { HeldPublish }

import { ChannelOverflowError, getChannelLimits } from 'telefunc/__internal'
import type { RedisSubscriber } from './subscriber.js'

/** How long a publish waits for our own subscriptions on its channels. */
const HOLD_MS = 60_000
/** What a held publish keeps beyond its payload (130–160 bytes measured), so small ones are bounded too. */
const HELD_OVERHEAD_BYTES = 256

/** Where a publish goes, and what it counts against while it waits. */
type HeldPublish = {
  /** Publishes on one queue keep their call order: one waits behind those held before it. */
  queue: string
  /** It waits while one of our subscriptions on these channels is subscribing for the first time. */
  channels: readonly string[]
  /** Its bytes count against this budget's `bufferLimit`, or `bufferLimitBinary` when `binary`. */
  budget: string
  binary: boolean
}

/**
 * Our PUBLISH goes out on the command connection, so it can reach Redis before the SUBSCRIBE our subscriber
 * connection sent just before it, and our own new subscription would miss it. So a publish waits, at most HOLD_MS,
 * until our subscriptions on its channels are ready. Later publishes on its queue wait behind it, in call order. The
 * bytes held per budget are capped; the publish past the cap is refused with `ChannelOverflowError`. A later loss
 * holds nothing: a subscription that was ready once reads where it stands again when it reconnects.
 */
class PublishHold {
  private readonly subscriber: RedisSubscriber
  private readonly queues = new Map<string, { readonly ready: Promise<void>; waiting: number }>()
  private readonly heldBytes = new Map<string, number>()

  constructor({ subscriber }: { subscriber: RedisSubscriber }) {
    this.subscriber = subscriber
  }

  /** False on the fast path, checked before a publish builds anything: nothing of ours is subscribing and nothing
   *  is held. */
  get active(): boolean {
    return this.subscriber.subscribing || this.queues.size > 0
  }

  /** Sends `payload` now, or once the wait ends. */
  whenReady<Payload extends string | Uint8Array, Result>(
    held: HeldPublish,
    payload: Payload,
    send: (payload: Payload) => Promise<Result>,
  ): Promise<Result> {
    let queue = this.queues.get(held.queue)
    if (queue === undefined) {
      const ready = this.subscriber.firstReady(held.channels)
      if (ready === null) return send(payload)
      queue = { ready: withTimeout(ready, HOLD_MS), waiting: 0 }
    }
    const bytes = HELD_OVERHEAD_BYTES + (typeof payload === 'string' ? Buffer.byteLength(payload) : payload.byteLength)
    const total = (this.heldBytes.get(held.budget) ?? 0) + bytes
    // Read only for a held publish: resolving the config is costly.
    const limits = getChannelLimits()
    if (total > (held.binary ? limits.bufferLimitBinary : limits.bufferLimit)) {
      return Promise.reject(new ChannelOverflowError('Redis publish readiness buffer overflow'))
    }
    this.heldBytes.set(held.budget, total)
    this.queues.set(held.queue, queue)
    queue.waiting++
    // The caller may reuse its bytes once this returns.
    const owned = (typeof payload === 'string' ? payload : new Uint8Array(payload)) as Payload
    const current = queue
    // `send` runs in the reaction that ends the wait, so a publish that finds the queue gone can't reach Redis first.
    return current.ready.then(() => {
      if (--current.waiting === 0) this.queues.delete(held.queue)
      const left = this.heldBytes.get(held.budget)! - bytes
      if (left === 0) this.heldBytes.delete(held.budget)
      else this.heldBytes.set(held.budget, left)
      return send(owned)
    })
  }
}

/** Settles when `ready` does or after `ms`, whichever comes first. */
function withTimeout(ready: Promise<void>, ms: number): Promise<void> {
  return new Promise((resolve) => {
    const timer = setTimeout(resolve, ms)
    timer.unref()
    void ready.then(() => {
      clearTimeout(timer)
      resolve()
    })
  })
}
