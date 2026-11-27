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
 *  channel it refused meanwhile, and `passed` the channels its sends deliver on (their `failsOn`). */
type Wait = {
  readonly ready: Promise<void>
  readonly refused: Map<string, unknown>
  readonly passed: Set<string>
  sends: number
}

/** How many sends of one budget wait, and the ledger's holder for their bytes. */
type WaitingBudget = { sends: number; readonly memory: QueueMemory }

/** A send's place in its queue's wait: `run(send)` sends its own copy of the payload in its turn, and the send leaves
 *  the wait once `send` settled or the turn failed. */
type WaitingSend<Payload> = { run<Result>(send: (payload: Payload) => Promise<Result>): Promise<Result> }

type NewSubscriptionWaitOptions = { subscriptions: RedisSubscriptions }

/**
 * Our PUBLISH and COMMIT go out on the command connection, so one can reach Redis before the SUBSCRIBE our subscriber
 * connection sent just before it, and our own new subscription would miss it. So a send waits, at most SEND_WAIT_MS,
 * until our subscriptions on its channels were ready once, and a later send of its queue waits behind it; past that, it
 * goes out, and each of them tells once ready that it may have missed some. Each budget's waiting sends are held on the
 * server's memory ledger, within what `config.channel.maxMemory` admits, though one send alone waits whatever its size.
 * When Redis refuses a subscription it waits for, a send fails with that reason rather than go out unseen by our own
 * subscription; a send made after it has no subscription of ours to miss. A subscription that ends any other way only
 * ends the wait.
 */
class NewSubscriptionWait {
  private readonly subscriptions: RedisSubscriptions
  /** By queue. */
  private readonly waits = new Map<string, Wait>()
  /** By budget. */
  private readonly budgets = new Map<string, WaitingBudget>()

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
    const others = this.budgets.get(budget)
    const waiting = others ?? { sends: 0, memory: queueMemory() }
    // The first waiting send goes whatever the ledger says, charged only where it admits it.
    const charged = waiting.memory.tryHold(bytes) ? bytes : 0
    if (charged === 0 && others !== undefined) {
      throw new ChannelOverflowError(
        "Publish rejected: the publishes waiting for this server's new Redis subscription would pass the most it holds for one channel, or config.channel.maxMemory; await each publish",
      )
    }
    wait ??= this.openWait(queue, channels)
    this.waits.set(queue, wait)
    for (const channel of failsOn) wait.passed.add(channel)
    wait.sends++
    waiting.sends++
    this.budgets.set(budget, waiting)
    const owned = own(payload)
    return {
      run: async (send) => {
        try {
          await wait.ready
          // A send that Redis refused one of its `failsOn` for counts as gone.
          const refusedOn = failsOn.find((channel) => wait.refused.has(channel))
          if (refusedOn !== undefined) throw wait.refused.get(refusedOn)
          return await send(owned)
        } finally {
          this.endWait(queue, wait, budget, waiting, charged)
        }
      },
    }
  }

  /** A waiting send went out: the last of its wait's sends unmaps it, so a send made before then still queues behind
   *  the rest, and the last of its budget's unmaps that. */
  private endWait(queue: string, wait: Wait, budget: string, waiting: WaitingBudget, bytes: number): void {
    if (--wait.sends === 0 && this.waits.get(queue) === wait) this.waits.delete(queue)
    waiting.memory.letGo(bytes)
    if (--waiting.sends === 0) this.budgets.delete(budget)
  }

  /** A wait for our new subscriptions on `channels`, including ones made meanwhile. Where Redis refuses one, the wait
   *  leaves its queue at once, so the next send there doesn't join it. */
  private openWait(queue: string, channels: readonly string[]): Wait {
    const refused = new Map<string, unknown>()
    const passed = new Set<string>()
    const leave = (channel: string, refusal: unknown) => {
      refused.set(channel, refusal)
      if (this.waits.get(queue) === wait) this.waits.delete(queue)
    }
    const wait: Wait = { ready: this.whenReady(channels, passed, leave), refused, passed, sends: 0 }
    return wait
  }

  /** Once ours on `channels` were ready once, or past SEND_WAIT_MS, where ours on the `passed` channels may have missed
   *  the sends. */
  private async whenReady(
    channels: readonly string[],
    passed: ReadonlySet<string>,
    onRefused: (channel: string, refusal: unknown) => void,
  ): Promise<void> {
    const timeout = createDeferred()
    const timer = unrefTimer(
      setTimeout(() => {
        this.subscriptions.passedBySends([...passed])
        timeout.resolve()
      }, SEND_WAIT_MS),
    )
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
