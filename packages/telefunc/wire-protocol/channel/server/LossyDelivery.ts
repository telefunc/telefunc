export { LossyDelivery }

import { hold, letGo, type MemoryHolder } from './channelMemory.js'
import type { IndexedPeer, RoomWaiter } from './IndexedPeer.js'
import type { PublishBody } from './PublishBody.js'
import { Fifo } from '../../../utils/Fifo.js'

type LossyDeliveryOptions = {
  /** What the publishes waiting here are charged to. */
  holder: MemoryHolder
  /** A publish waiting here was skipped: the page sees the gap in `info.seq`. */
  onSkip: () => void
}

/** A channel's lossy publishes on their way to its page, held before the wire and handed as its path has room (see
 *  `PathEstimate`). While the path carries what the wire's lossy streams offer, they go in order, every one of them.
 *  Once it doesn't, or while the page has no wire, a newer publish of a stream (`PublishBody.lossyStream`) replaces
 *  that stream's waiting, and what waits goes at the path's pace, so that what reaches the page is young. A `delta`
 *  publish replaces nothing: it waits behind its group, and goes only where its page is due the rest of that group. */
class LossyDelivery implements RoomWaiter {
  private readonly holder: MemoryHolder
  private readonly onSkip: () => void
  /** The page's channel on the wire it last attached on; whether that wire is still the page's. */
  private peer: IndexedPeer | null = null
  private attached = false
  /** In order, what the wire has yet to be handed. */
  private readonly waiting = new Fifo<PublishBody>()
  /** The lossy streams whose newest group this page is due every publish of, so far. */
  private readonly wholeGroups = new Set<string>()

  constructor({ holder, onSkip }: LossyDeliveryOptions) {
    this.holder = holder
    this.onSkip = onSkip
  }

  /** Its newest publish, where no wire has that yet. */
  get newestWaiting(): PublishBody | undefined {
    let newest: PublishBody | undefined
    for (const body of this.waiting) newest = body
    return newest
  }

  /** `body` is of a lossy stream. */
  publish(body: PublishBody): void {
    const stream = body.lossyStream!
    const lane = this.attached ? this.peer!.sender.lossy : null
    lane?.offer(body.frame.byteLength, performance.now())
    if (body.delta && !this.wholeGroups.has(stream)) return this.onSkip()
    hold(this.holder, body)
    if (!body.delta) {
      if (lane === null || lane.pathBound) this.skipWaiting(stream)
      this.wholeGroups.add(stream)
    }
    this.waiting.push(body)
    if (this.attached) this.hand()
  }

  /** The page attached on `peer`'s wire, whose path is measured anew: what waits goes on it. */
  attach(peer: IndexedPeer): void {
    this.peer = peer
    this.attached = true
    if (this.waiting.length > 0) this.hand()
  }

  /** The page's wire went away: until it attaches again, only each stream's newest publish waits. */
  detach(): void {
    this.attached = false
  }

  /** `RoomWaiter`: the wire may have room for what waits. */
  roomMayHaveOpened(): void {
    if (this.attached && this.waiting.length > 0) this.hand()
  }

  /** A graceful close goes behind every publish its page is due: what waits goes on the wire it last attached on. */
  flush(): void {
    if (this.peer === null) return
    while (this.waiting.length > 0) this.handOne(this.peer)
  }

  /** What of `stream` waits goes, unsent, for a newer publish of it to take its place. */
  skipWaiting(stream: string): void {
    let skipped = false
    this.waiting.remove((body) => {
      if (body.lossyStream !== stream) return false
      letGo(this.holder, body)
      skipped = true
      return true
    })
    if (!skipped) return
    this.wholeGroups.delete(stream)
    this.onSkip()
  }

  /** What waits goes, unsent. */
  drop(): void {
    while (this.waiting.length > 0) letGo(this.holder, this.waiting.shift()!)
  }

  /** Hands the wire what waits, in order, while its path has room and its pace allows; the rest waits for either. */
  private hand(): void {
    const peer = this.peer!
    while (this.waiting.length > 0 && peer.sender.lossy.mayHand(performance.now(), this)) this.handOne(peer)
  }

  private handOne(peer: IndexedPeer): void {
    const body = this.waiting.shift()!
    peer.sender.lossy.hand(peer, body)
    letGo(this.holder, body)
  }
}
