export { PublishHold }

import type { BroadcastPayload, BroadcastRoute } from 'telefunc/__internal'
import { ChannelOverflowError, getChannelLimits } from 'telefunc/__internal'
import { broadcastChannel, type CommitChannels } from './keys.js'
import type { RedisSubscriptions } from './RedisSubscriptions.js'

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

/** What the publishes held on one queue wait for. */
type Wait = { readonly ready: Promise<void>; ended: boolean }

/**
 * Our PUBLISH goes out on the command connection, so it can reach Redis before the SUBSCRIBE our subscriber
 * connection sent just before it, and our own new subscription would miss it. So a publish waits, at most HOLD_MS,
 * until our subscriptions on its channels are ready. Later publishes on its queue wait behind it, in call order. The
 * bytes held per budget are capped; the publish past the cap is refused with `ChannelOverflowError`. A later loss
 * holds nothing: a subscription that was ready once reads where it stands again when it reconnects.
 */
class PublishHold {
  private readonly subscriptions: RedisSubscriptions
  private readonly prefix: string
  private readonly waits = new Map<string, Wait>()
  private readonly heldBytes = new Map<string, number>()

  constructor({ subscriptions, prefix }: { subscriptions: RedisSubscriptions; prefix: string }) {
    this.subscriptions = subscriptions
    this.prefix = prefix
  }

  /** False on the fast path, checked before a publish builds anything: nothing of ours is subscribing and nothing
   *  is held. */
  get active(): boolean {
    return this.subscriptions.subscribing || this.waits.size > 0
  }

  /** Holds a Broadcast publish while one of our subscriptions on its key subscribes: resolves with the publish's own
   *  copy of `payload` once it may go; null when it may go now. */
  holdBroadcast(route: BroadcastRoute, payload: BroadcastPayload): Promise<BroadcastPayload> | null {
    const text = broadcastChannel(this.prefix, { key: route.key, kind: 'text' })
    const binary = broadcastChannel(this.prefix, { key: route.key, kind: 'binary' })
    // A key's two kinds share its order, so a publish of either waits behind the key's held ones.
    return this.hold(
      {
        queue: text,
        channels: [text, binary],
        budget: route.kind === 'text' ? text : binary,
        binary: route.kind === 'binary',
      },
      payload,
    )
  }

  /** Holds a Room commit while one of our subscriptions on its channels subscribes: resolves with the commit's own
   *  copy of `payload` once it may go; null when it may go now. */
  holdCommit(channels: CommitChannels, payload: Uint8Array): Promise<Uint8Array> | null {
    const { lane, everyTrack, everyMember } = channels
    // Only a binary commit also reaches its member's every-track and its track's every-member channels, and its held
    // bytes count against its member's.
    return this.hold(
      {
        queue: lane,
        channels: [lane, everyTrack, everyMember].filter((channel) => channel !== null),
        budget: everyTrack ?? lane,
        binary: everyTrack !== null,
      },
      payload,
    )
  }

  private hold<Payload extends string | Uint8Array>(held: HeldPublish, payload: Payload): Promise<Payload> | null {
    let wait = this.waits.get(held.queue)
    if (wait === undefined || wait.ended) {
      const ready = this.subscriptions.firstReady(held.channels)
      if (ready === null) {
        if (wait === undefined) return null
        // The publishes the ended wait held send in the microtasks that follow; this one goes after them.
        const owned = ownCopy(payload)
        return wait.ready.then(() => owned)
      }
      wait = { ready: withTimeout(ready, HOLD_MS), ended: false }
    }
    const bytes = HELD_OVERHEAD_BYTES + (typeof payload === 'string' ? Buffer.byteLength(payload) : payload.byteLength)
    const total = (this.heldBytes.get(held.budget) ?? 0) + bytes
    // Read only for a held publish: resolving the config is costly.
    const limits = getChannelLimits()
    if (total > (held.binary ? limits.bufferLimitBinary : limits.bufferLimit)) {
      return Promise.reject(new ChannelOverflowError('Redis publish readiness buffer overflow'))
    }
    this.heldBytes.set(held.budget, total)
    if (this.waits.get(held.queue) !== wait) this.startWait(held.queue, wait)
    const owned = ownCopy(payload)
    return wait.ready.then(() => {
      const left = this.heldBytes.get(held.budget)! - bytes
      if (left === 0) this.heldBytes.delete(held.budget)
      else this.heldBytes.set(held.budget, left)
      return owned
    })
  }

  private startWait(queue: string, wait: Wait): void {
    this.waits.set(queue, wait)
    void wait.ready.then(() => {
      wait.ended = true
      // The publishes it held send in the microtasks that follow, so it is dropped on the event loop's next turn: a
      // publish before then still goes after them.
      setImmediate(() => {
        if (this.waits.get(queue) === wait) this.waits.delete(queue)
      })
    })
  }
}

/** The caller may reuse its bytes once its publish returns. */
function ownCopy<Payload extends string | Uint8Array>(payload: Payload): Payload {
  return (typeof payload === 'string' ? payload : new Uint8Array(payload)) as Payload
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
