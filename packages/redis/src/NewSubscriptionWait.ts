export { NewSubscriptionWait }
export type { WaitingSend, WaitRule }

import {
  ChannelOverflowError,
  createDeferred,
  HELD_SEND_OVERHEAD_BYTES,
  queueMemory,
  unrefTimer,
  type QueueMemory,
} from 'telefunc/__internal'
import type { RedisSubscriptions } from './RedisSubscriptions.js'

/** How long a send waits for our own new subscriptions on its channels. */
const SEND_WAIT_MS = 60_000

/** A send waits while one of our subscriptions on `channels` is new, behind the waiting sends of its `queue` (what one
 *  seq counts), and fails where Redis refuses one on `failsOn`. Its bytes count against `budget`, on the server's memory
 *  ledger. */
type WaitRule = {
  queue: string
  channels: readonly string[]
  failsOn: readonly string[]
  budget: string
}

/** The sends of one queue, waiting for our new subscriptions on their channels; `refused` holds Redis's refusal of each
 *  channel it refused meanwhile. */
type Wait = { readonly ready: Promise<void>; readonly refused: Map<string, unknown>; sends: number }

/** The bytes of one budget's waiting sends, and the ledger's holder for them. */
type WaitingBytes = { bytes: number; readonly memory: QueueMemory }

/** A send's place in its queue's wait: `await turn` for its own copy of the payload, send that, then `sent()`. A turn
 *  that rejects has left the wait already. */
type WaitingSend<Payload> = { readonly turn: Promise<Payload>; sent(): void }

type NewSubscriptionWaitOptions = { subscriptions: RedisSubscriptions }

/**
 * Our PUBLISH and COMMIT go out on the command connection, so one can reach Redis before the SUBSCRIBE our subscriber
 * connection sent just before it, and our own new subscription would miss it. So a send waits, at most SEND_WAIT_MS,
 * until our subscriptions on its channels were ready once, and a later send of its queue waits behind it. Each budget's
 * waiting sends are held on the server's memory ledger, within what `config.channel.maxMemory` admits, though one send
 * alone waits whatever its size. When Redis refuses a subscription it waits for, a send fails with that reason rather
 * than go out unseen by our own subscription; a send made after it has no subscription of ours to miss. A subscription
 * that ends any other way only ends the wait.
 */
class NewSubscriptionWait {
  private readonly subscriptions: RedisSubscriptions
  /** By queue. */
  private readonly waits = new Map<string, Wait>()
  /** By budget. */
  private readonly waitingBytes = new Map<string, WaitingBytes>()

  constructor({ subscriptions }: NewSubscriptionWaitOptions) {
    this.subscriptions = subscriptions
  }

  /** False on the fast path, read before a send builds anything: none of ours is new and no send waits. */
  get sendsMayWait(): boolean {
    return this.subscriptions.establishing || this.waits.size > 0
  }

  /** A send's turn, with the copy `own(payload)` makes of `payload`: the caller reuses its bytes. Null when it has
   *  nothing to wait for. Throws `ChannelOverflowError` when others wait and the server's memory ledger doesn't admit
   *  it. */
  waitTurn<Payload extends string | Uint8Array>(
    rule: WaitRule,
    payload: Payload,
    own: (payload: Payload) => Payload,
  ): WaitingSend<Payload> | null {
    const { queue, channels, failsOn, budget } = rule
    let wait = this.waits.get(queue)
    if (wait === undefined && this.subscriptions.establishingOn(channels).length === 0) return null
    const bytes =
      (typeof payload === 'string' ? Buffer.byteLength(payload) : payload.byteLength) + HELD_SEND_OVERHEAD_BYTES
    const others = this.waitingBytes.get(budget)
    const waiting = others ?? { bytes: 0, memory: queueMemory() }
    // The first waiting send goes whatever the ledger says, charged only where it admits it.
    const charged = waiting.memory.tryHold(bytes) ? bytes : 0
    if (charged === 0 && others !== undefined) {
      throw new ChannelOverflowError(
        "Publish rejected: the publishes waiting for this server's new Redis subscription would pass the most it holds for one channel, or config.channel.maxMemory; await each publish",
      )
    }
    wait ??= this.openWait(queue, channels)
    this.waits.set(queue, wait)
    wait.sends++
    waiting.bytes += charged
    this.waitingBytes.set(budget, waiting)
    const owned = own(payload)
    const sent = () => this.endWait(queue, wait, budget, waiting, charged)
    return { turn: this.turnOf(wait, failsOn, owned, sent), sent }
  }

  /** The payload, once `wait` is over; a send that Redis refused one of its `failsOn` for counts as gone. */
  private async turnOf<Payload>(
    wait: Wait,
    failsOn: readonly string[],
    owned: Payload,
    sent: () => void,
  ): Promise<Payload> {
    await wait.ready
    const refusedOn = failsOn.find((channel) => wait.refused.has(channel))
    if (refusedOn === undefined) return owned
    sent()
    throw wait.refused.get(refusedOn)
  }

  /** A waiting send went out: the last of its wait's sends unmaps it, so a send made before then still queues behind
   *  the rest. */
  private endWait(queue: string, wait: Wait, budget: string, waiting: WaitingBytes, bytes: number): void {
    if (--wait.sends === 0 && this.waits.get(queue) === wait) this.waits.delete(queue)
    waiting.bytes -= bytes
    waiting.memory.letGo(bytes)
    if (waiting.bytes === 0) this.waitingBytes.delete(budget)
  }

  /** A wait for our new subscriptions on `channels`, including ones made meanwhile. Where Redis refuses one, the wait
   *  leaves its queue at once, so the next send there doesn't join it. */
  private openWait(queue: string, channels: readonly string[]): Wait {
    const refused = new Map<string, unknown>()
    const leave = (channel: string, refusal: unknown) => {
      refused.set(channel, refusal)
      if (this.waits.get(queue) === wait) this.waits.delete(queue)
    }
    const wait: Wait = { ready: this.whenReady(channels, leave), refused, sends: 0 }
    return wait
  }

  private async whenReady(
    channels: readonly string[],
    onRefused: (channel: string, refusal: unknown) => void,
  ): Promise<void> {
    const timeout = createDeferred()
    const timer = unrefTimer(setTimeout(timeout.resolve, SEND_WAIT_MS))
    try {
      await Promise.race([this.readyOn(channels, onRefused), timeout.promise])
    } finally {
      clearTimeout(timer)
    }
  }

  /** Once each of ours on `channels` that is new was ready once, ended or was refused. */
  private async readyOn(
    channels: readonly string[],
    onRefused: (channel: string, refusal: unknown) => void,
  ): Promise<void> {
    const settledOn = () =>
      channels.flatMap((channel) =>
        this.subscriptions
          .establishingOn([channel])
          .map((firstReady) => firstReady.catch((refusal: unknown) => onRefused(channel, refusal))),
      )
    for (let left = settledOn(); left.length > 0; left = settledOn()) await Promise.all(left)
  }
}
