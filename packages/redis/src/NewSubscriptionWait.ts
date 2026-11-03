export { NewSubscriptionWait }
export type { WaitingSend, WaitRule }

import { ChannelOverflowError, getChannelLimits } from 'telefunc/__internal'
import type { RedisSubscriptions } from './RedisSubscriptions.js'

/** How long a send waits for our own new subscriptions on its channels. */
const SEND_WAIT_MS = 60_000
/** What a waiting send keeps beyond its payload (130–160 bytes measured), so small ones are bounded too. */
const WAITING_SEND_OVERHEAD_BYTES = 256

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
 * waiting bytes are bounded by `bufferLimit` (`bufferLimitBinary` for bytes). While Redis refuses the SUBSCRIBE of one
 * of its channels, a send fails with that refusal rather than go out unseen by our own subscription; a SUBSCRIBE that
 * succeeds, on the reconnect or by a new subscription, ends it.
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
    return this.subscriptions.subscribing || this.waits.size > 0
  }

  /** A send's turn, with its own copy of `payload`: the caller reuses its bytes. Null when it has nothing to wait for.
   *  Throws `ChannelOverflowError` when its bytes would pass its buffer limit; the config is read only then, as
   *  resolving it is costly. Throws Redis's refusal while it refuses one of its channels. */
  waitTurn<Payload extends string | Uint8Array>(rule: WaitRule, payload: Payload): WaitingSend<Payload> | null {
    const { queue, channels, budget, bufferLimit } = rule
    let wait = this.waits.get(queue)
    if (wait === undefined && this.subscriptions.subscribingOn(channels).length === 0) return null
    const refusal = this.subscriptions.refusalOn(channels)
    if (refusal !== null) throw refusal.error
    const bytes =
      WAITING_SEND_OVERHEAD_BYTES + (typeof payload === 'string' ? Buffer.byteLength(payload) : payload.byteLength)
    const total = (this.waitingBytes.get(budget) ?? 0) + bytes
    if (total > getChannelLimits()[bufferLimit]) {
      throw new ChannelOverflowError(`Redis send waiting for a new subscription, past ${bufferLimit}`)
    }
    wait ??= this.openWait(queue, channels)
    this.waits.set(queue, wait)
    wait.sends++
    this.waitingBytes.set(budget, total)
    const owned = (typeof payload === 'string' ? payload : new Uint8Array(payload)) as Payload
    const sent = () => this.endWait(queue, wait, budget, bytes)
    const turn = wait.ready.then(
      () => owned,
      (error: unknown) => {
        sent()
        throw error
      },
    )
    return { turn, sent }
  }

  /** A waiting send went out: the last of its wait's sends unmaps it, so a send made before then still queues behind
   *  the rest. */
  private endWait(queue: string, wait: Wait, budget: string, bytes: number): void {
    if (--wait.sends === 0 && this.waits.get(queue) === wait) this.waits.delete(queue)
    const left = this.waitingBytes.get(budget)! - bytes
    if (left === 0) this.waitingBytes.delete(budget)
    else this.waitingBytes.set(budget, left)
  }

  /** A wait for our new subscriptions on `channels`, including ones made meanwhile, that fails on Redis's refusal of
   *  one; a refused wait leaves its queue at once, so the next send there doesn't join it. */
  private openWait(queue: string, channels: readonly string[]): Wait {
    let timer!: ReturnType<typeof setTimeout>
    const timeout = new Promise<void>((resolve) => {
      timer = setTimeout(resolve, SEND_WAIT_MS)
      timer.unref()
    })
    let stopListening!: () => void
    const refused = new Promise<never>((_, reject) => {
      stopListening = this.subscriptions.onRefusal(() => {
        const refusal = this.subscriptions.refusalOn(channels)
        if (refusal !== null) reject(refusal.error)
      })
    })
    const ready = Promise.race([this.readyOn(channels), timeout, refused]).finally(() => {
      clearTimeout(timer)
      stopListening()
    })
    const wait: Wait = { ready, sends: 0 }
    ready.catch(() => {
      if (this.waits.get(queue) === wait) this.waits.delete(queue)
    })
    return wait
  }

  private async readyOn(channels: readonly string[]): Promise<void> {
    for (let left = this.subscriptions.subscribingOn(channels); left.length > 0; ) {
      await Promise.all(left)
      left = this.subscriptions.subscribingOn(channels)
    }
  }
}
