export { NewSubscriptionWait }
export type { WaitingSend, WaitRule }

import { ChannelOverflowError, createDeferred, getChannelLimits } from 'telefunc/__internal'
import type { RedisSubscriptions } from './RedisSubscriptions.js'

/** How long a send waits for our own new subscriptions on its channels. */
const SEND_WAIT_MS = 60_000

/** A send waits while one of our subscriptions on `channels` is new, behind the waiting sends of its `queue` (what one
 *  seq counts). Its bytes count against `budget`'s `bufferLimit`. */
type WaitRule = {
  queue: string
  channels: readonly string[]
  budget: string
  bufferLimit: 'bufferLimit' | 'bufferLimitBinary'
}

/** The sends of one queue, waiting for our new subscriptions on their channels. */
type Wait = { readonly ready: Promise<void>; sends: number }

/** A send's place in its queue's wait: `await turn` for its own copy of the payload, send that, then `sent()`. A turn
 *  that rejects has left the wait already. */
type WaitingSend<Payload> = { readonly turn: Promise<Payload>; sent(): void }

type NewSubscriptionWaitOptions = { subscriptions: RedisSubscriptions }

/**
 * Our PUBLISH and COMMIT go out on the command connection, so one can reach Redis before the SUBSCRIBE our subscriber
 * connection sent just before it, and our own new subscription would miss it. So a send waits, at most SEND_WAIT_MS,
 * until our subscriptions on its channels were ready once, and a later send of its queue waits behind it. Each budget's
 * waiting payload bytes are bounded by `bufferLimit` (`bufferLimitBinary` for bytes), though one send alone waits
 * whatever its size. When a subscription it waits for fails, as Redis refused its SUBSCRIBE, a send fails with that
 * reason rather than go out unseen by our own subscription; a send made after it has no subscription of ours to miss.
 */
class NewSubscriptionWait {
  private readonly subscriptions: RedisSubscriptions
  /** By queue. */
  private readonly waits = new Map<string, Wait>()
  /** Bytes waiting, by budget. */
  private readonly waitingBytes = new Map<string, number>()

  constructor({ subscriptions }: NewSubscriptionWaitOptions) {
    this.subscriptions = subscriptions
  }

  /** False on the fast path, read before a send builds anything: none of ours is new and no send waits. */
  get sendsMayWait(): boolean {
    return this.subscriptions.establishing || this.waits.size > 0
  }

  /** A send's turn, with its own copy of `payload`: the caller reuses its bytes. Null when it has nothing to wait for.
   *  Throws `ChannelOverflowError` when its bytes would take those waiting past its buffer limit; the config is read
   *  only with others waiting, as resolving it is costly. */
  waitTurn<Payload extends string | Uint8Array>(rule: WaitRule, payload: Payload): WaitingSend<Payload> | null {
    const { queue, channels, budget, bufferLimit } = rule
    let wait = this.waits.get(queue)
    if (wait === undefined && this.subscriptions.establishingOn(channels).length === 0) return null
    // An empty send counts as 1 B, so a flood of them stays bounded.
    const bytes = Math.max(typeof payload === 'string' ? Buffer.byteLength(payload) : payload.byteLength, 1)
    const waiting = this.waitingBytes.get(budget) ?? 0
    if (waiting > 0 && waiting + bytes > getChannelLimits()[bufferLimit]) {
      throw new ChannelOverflowError(`Redis send waiting for a new subscription, past ${bufferLimit}`)
    }
    wait ??= this.openWait(queue, channels)
    this.waits.set(queue, wait)
    wait.sends++
    this.waitingBytes.set(budget, waiting + bytes)
    const owned = (typeof payload === 'string' ? payload : new Uint8Array(payload)) as Payload
    const sent = () => this.endWait(queue, wait, budget, bytes)
    return { turn: this.turnOf(wait, owned, sent), sent }
  }

  /** The payload, once `wait` is over; a failed wait's send counts as gone. */
  private async turnOf<Payload>(wait: Wait, owned: Payload, sent: () => void): Promise<Payload> {
    try {
      await wait.ready
    } catch (error) {
      sent()
      throw error
    }
    return owned
  }

  /** A waiting send went out: the last of its wait's sends unmaps it, so a send made before then still queues behind
   *  the rest. */
  private endWait(queue: string, wait: Wait, budget: string, bytes: number): void {
    if (--wait.sends === 0 && this.waits.get(queue) === wait) this.waits.delete(queue)
    const left = this.waitingBytes.get(budget)! - bytes
    if (left === 0) this.waitingBytes.delete(budget)
    else this.waitingBytes.set(budget, left)
  }

  /** A wait for our new subscriptions on `channels`, including ones made meanwhile, that fails when one does; a failed
   *  wait leaves its queue at once, so the next send there doesn't join it. */
  private openWait(queue: string, channels: readonly string[]): Wait {
    const wait: Wait = { ready: this.whenReady(channels), sends: 0 }
    void this.leaveOnFailure(queue, wait)
    return wait
  }

  private async leaveOnFailure(queue: string, wait: Wait): Promise<void> {
    try {
      await wait.ready
    } catch {
      if (this.waits.get(queue) === wait) this.waits.delete(queue)
    }
  }

  private async whenReady(channels: readonly string[]): Promise<void> {
    const timeout = createDeferred()
    const timer = setTimeout(timeout.resolve, SEND_WAIT_MS)
    timer.unref()
    try {
      await Promise.race([this.readyOn(channels), timeout.promise])
    } finally {
      clearTimeout(timer)
    }
  }

  private async readyOn(channels: readonly string[]): Promise<void> {
    for (let left = this.subscriptions.establishingOn(channels); left.length > 0; ) {
      await Promise.all(left)
      left = this.subscriptions.establishingOn(channels)
    }
  }
}
