export { LaneHistory }
export type { LaneFrame, LaneRoute }

import type { OrderingInfo } from 'telefunc/__internal'
import { unrefTimer } from 'telefunc/__internal'
import { assert } from './assert.js'
import { NO_ID, type LaneStart } from './orderRecord.js'

/** A frame as its route decoded it: its lane key on the route's channel (`''` on a channel of one lane), its place, its
 *  commit's id and the one before it on its lane (`NO_ID` for none), and the commit's fence token, if armed. */
type LaneFrame = {
  lane: string
  place: number
  id: bigint
  prev: bigint
  payload: Uint8Array
  info: OrderingInfo
  token: string | null
  via: LaneRoute
}

/** One subscription on a channel that carries the lane: it delivers what the lane decides to deliver, and reports. */
type LaneRoute = {
  /** The history its last ready start came from: null before its first. */
  readonly history: string | null
  /** `rewound`: the lane follows a frame at or below the last place it delivered. */
  deliver(frame: LaneFrame, rewound: boolean): void
  /** The frame isn't delivered: its fence settles. */
  dropped(frame: LaneFrame): void
  /** What a drop or the reorder wait passed by: `count` frames. */
  reportMissed(count: number): void
  /** The lane can't place `frame` on what it delivered: the route reads where its key stands, as after a cut, and
   *  offers the frame again after that read. */
  forked(frame: LaneFrame): void
  /** The route's start read found the lane's record replaced within the history the route follows. */
  replaced(): void
  /** The wait for a missing frame ended: the route may have missed messages. */
  waitEnded(): void
}

/** What a frame the lane can't place on what it delivered is. `live`: maybe of a change no read has seen yet, which
 *  its route reads for. `accounted`: of a change a start read accounted for. `replaced`: of a record replaced within
 *  its history, which the lane tells once per resume. */
type Unplaced = 'live' | 'accounted' | 'replaced'

/** A frame the lane delivered, or a start (`prev` null): where its history stood. */
type Entry = { place: number; id: bigint; prev: bigint | null; branch: number }

/** How many delivered frames a lane remembers. A copy reaches the lane next to its first copy, as one script publishes a
 *  commit on each of its channels, unless a new route held it while its start read ran: 32 deliveries' time. */
const RING_SIZE = 32
// Redis Cluster forwards a channel's frames from the old and the new slot owner over separate bus links during a
// reshard or a failover, so the old owner's last frame can arrive after the new owner's first.
const REORDER_WAIT_MS = 1_000
const NO_START: LaneStart = { place: 0, id: NO_ID }

/**
 * One lane's order for one consumer, across its subscriptions on the connection that carry it. Redis pub/sub keeps no
 * log and a failover may promote a replica that lacks the last commits, so a frame's place alone can't tell a copy or a
 * late frame from the history that replaced the delivered one. Each frame names its commit and the one before it, and the lane
 * remembers its last `RING_SIZE` deliveries: a frame it can tell is a copy or a counted hole's late original is dropped,
 * a frame that continues what it delivered is delivered, and where it can't tell which history a frame belongs to, its
 * route reads where its key stands first; then the lane delivers and follows the frame. It never picks one history over
 * another, and only a read tells a loss it can't count.
 */
class LaneHistory {
  /** The routes it delivers through: it lives while one holds it. */
  readonly routes = new Set<LaneRoute>()
  private started = false
  /** The history it follows, from its last start or a proof that its key lost nothing in a move: null before its first. */
  follows: string | null = null
  private last = 0
  private tip = NO_ID
  private branch = 0
  private branches = 0
  /** Only once it delivered can a consumer have got what another history replaced. */
  private delivered = false
  /** The highest place the ring let go of: above it, every frame delivered at a place is still in the ring. */
  private evicted = 0
  /** A resume followed a start below `last`: the next delivery is a rewind. */
  private rewindPending = false
  /** The tip is a start a resume followed, whose own commit the lane never delivered. */
  private tipOwed = false
  /** This resume told that the lane's record was replaced. */
  private replacedTold = false
  /** The frames whose route read where its key stands for them, each with the highest place its route had brought the
   *  lane before it: offered again, each is of what that read saw. */
  private readonly reread = new WeakMap<LaneFrame, number>()
  private readonly ring: Entry[] = []
  /** The ring's oldest entry, once it is full. */
  private oldest = 0
  /** The frames past a missing one, by place, until it arrives or the wait ends. */
  private parked: Map<number, LaneFrame[]> | null = null
  private releaseTimer: ReturnType<typeof setTimeout> | null = null

  get done(): boolean {
    return this.routes.size === 0
  }

  /** `route` is ready at `start` (none: the lane had no commit), read from `history`, with the frames that reached it
   *  before. On a first establish, those its start covers are dropped and the rest go out. A start of another history
   *  than the lane follows is followed as a history it never saw; on its own history, a resume compares the start with
   *  what the lane delivered, and a route that joins a running lane drops only what its start covers that the lane is
   *  past. `resumedFrom`: the history of the route's last start, which a lane new to a resuming route had no commit on.
   *  Returns the history it left, if any. */
  establish(
    route: LaneRoute,
    start: LaneStart | undefined,
    early: LaneFrame[],
    resuming: boolean,
    passed: (frame: LaneFrame) => number,
    history: string = this.follows ?? '',
    resumedFrom: string = history,
  ): string | null {
    const first = !this.started
    if (first) {
      this.started = true
      this.follows = resuming ? resumedFrom : history
      // A lane new to a resuming route had no commit when the route was last ready.
      this.follow(resuming ? NO_START : (start ?? NO_START), null, 0)
    }
    const left = this.follows !== history ? this.follows : null
    this.follows = history
    if (left !== null) {
      this.switchTo(start ?? NO_START, early, passed)
      return left
    }
    if (resuming) {
      this.resume(route, start ?? NO_START, early, passed)
      return null
    }
    const covered = start === undefined ? null : coveredBy(start, early)
    for (const frame of early) {
      if (covered?.has(frame) && frame.place <= this.last) route.dropped(frame)
      // The read that started the lane saw these frames' history.
      else if (first) this.offer(frame, Number.POSITIVE_INFINITY, 'accounted')
      else this.offer(frame, passed(frame))
    }
    return null
  }

  /** Frames a route that left brought and the lane didn't decide yet: they go on through a route that stays. */
  adopt(frames: LaneFrame[]): void {
    const [via] = this.routes
    assert(via !== undefined)
    for (const frame of frames) this.offer({ ...frame, via })
  }

  /** A frame, decided by the first row that matches. `passedBefore` is the highest place its route brought the lane
   *  before it; a frame the lane held has passed. `unplaced`: what the frame is if the lane can't place it. */
  offer(frame: LaneFrame, passedBefore = Number.POSITIVE_INFINITY, unplaced: Unplaced = 'live'): void {
    // A lane with no commit when its route went live: every one of its commits is after, on that route's history.
    if (!this.started) {
      this.started = true
      this.follows = frame.via.history
      this.follow(NO_START, null, 0)
    }
    this.decide(frame, passedBefore, unplaced)
    this.drain()
  }

  /** Whether the lane delivered `start`'s commit, or started or stands at it. */
  knows({ place, id }: LaneStart): boolean {
    return (place === this.last && id === this.tip) || this.knowsAt(place, id)
  }

  /** The route is gone: its parked frames go on through a route that stays, as a later route never brings what was
   *  published before it subscribed. */
  leave(route: LaneRoute): void {
    this.routes.delete(route)
    if (this.parked !== null) {
      const [other] = this.routes
      for (const [place, frames] of this.parked) {
        const kept = frames.flatMap((frame) =>
          frame.via !== route ? [frame] : other ? [{ ...frame, via: other }] : [],
        )
        if (kept.length > 0) this.parked.set(place, kept)
        else this.parked.delete(place)
      }
      if (this.parked.size === 0) this.clearParked()
    }
    if (this.done) this.clearParked()
  }

  /** What arrived goes out: what the parked frames wait for can no longer arrive on this connection. */
  releaseParked(): void {
    if (this.parked !== null) this.release()
  }

  private decide(frame: LaneFrame, passedBefore: number, unplaced: Unplaced): void {
    const { place, id, prev, via } = frame
    // copy, or the owed start's own commit
    if (id === this.tip) return this.tipOwed ? this.deliver(frame, this.branch) : via.dropped(frame)
    // next
    if (place === this.last + 1 && prev === this.tip) return this.deliver(frame, this.branch)
    // copy
    if (this.delivers(id)) return via.dropped(frame)
    // old: the late original of a counted hole
    if (this.knowsAt(place, id)) return via.dropped(frame)
    // branch
    const branch = this.branchOf(prev)
    if (branch !== undefined) {
      if (!this.forks(frame, branch)) return this.deliver(frame, branch)
      return this.unplaced(frame, passedBefore, unplaced, () => this.deliver(frame, ++this.branches))
    }
    // unseen: a copy whose first delivery left the ring can't be told from a new frame. A frame at or below its own
    // route's highest place is that route's rewind, one node's in-order stream going back, so it is decided below. A
    // frame offered again after its route's read was brought by that route once, before.
    if (place <= this.evicted && place > (this.reread.get(frame) ?? passedBefore)) {
      return this.unplaced(frame, passedBefore, unplaced, () => via.dropped(frame))
    }
    if (place > this.last + 1) return this.park(frame)
    // unknown
    this.unplaced(frame, passedBefore, unplaced, () => this.deliver(frame, ++this.branches))
  }

  /** A frame the lane can't place, decided by `decide` once a read saw its history. Below a start the lane follows and
   *  never delivered, or offered again after its route's read, it is of what a read saw. */
  private unplaced(frame: LaneFrame, passedBefore: number, unplaced: Unplaced, decide: () => void): void {
    const seen = (this.tipOwed && frame.place < this.last) || this.reread.has(frame)
    if (unplaced === 'live' && !seen) {
      this.reread.set(frame, passedBefore)
      return frame.via.forked(frame)
    }
    if (unplaced === 'replaced' && this.delivered && !this.replacedTold) {
      this.replacedTold = true
      frame.via.replaced()
    }
    decide()
  }

  /** Whether `frame`, which continues a delivered frame on `branch`, splits that history: another delivered frame
   *  continues the same one, or the ring knows another frame at its place on that branch. */
  private forks({ place, id, prev }: LaneFrame, branch: number): boolean {
    for (const entry of this.ring) {
      if (entry.prev === prev && entry.id !== id) return true
      if (entry.branch !== branch) continue
      if (entry.place === place && entry.id !== id) return true
      if (entry.place === place + 1 && entry.prev !== null && entry.prev !== id) return true
    }
    return false
  }

  private deliver(frame: LaneFrame, branch: number): void {
    const rewound = this.rewindPending || frame.place <= this.last
    this.rewindPending = false
    this.delivered = true
    this.follow(frame, frame.prev, branch)
    frame.via.deliver(frame, rewound)
  }

  private follow({ place, id }: LaneStart, prev: bigint | null, branch: number): void {
    this.last = place
    this.tip = id
    this.tipOwed = false
    this.branch = branch
    if (this.ring.length < RING_SIZE) {
      this.ring.push({ place, id, prev, branch })
      return
    }
    const entry = this.ring[this.oldest]!
    this.evicted = Math.max(this.evicted, entry.place)
    entry.place = place
    entry.id = id
    entry.prev = prev
    entry.branch = branch
    this.oldest = (this.oldest + 1) % RING_SIZE
  }

  /** Whether the ring holds a delivered frame with `id`. */
  private delivers(id: bigint): boolean {
    for (const entry of this.ring) if (entry.prev !== null && entry.id === id) return true
    return false
  }

  /** Whether the ring knows `id` at `place`: delivered or started there, or named as the frame before a delivered one. */
  private knowsAt(place: number, id: bigint): boolean {
    for (const entry of this.ring) {
      if (entry.place === place && entry.id === id) return true
      if (entry.place === place + 1 && entry.prev === id) return true
    }
    return false
  }

  /** The branch of the newest entry with `id`, if the ring holds one. */
  private branchOf(id: bigint): number | undefined {
    for (let i = 1; i <= this.ring.length; i++) {
      const entry = this.ring[(this.oldest - i + this.ring.length) % this.ring.length]!
      if (entry.id === id) return entry.branch
    }
    return undefined
  }

  /** The route's lost connection ended at `start`: ahead of the lane, what reached the new subscription goes out and
   *  the rest is counted; at a place the lane knows, the next frame decides; anywhere else another history replaced
   *  what it delivered, and the lane follows the start. */
  private resume(route: LaneRoute, start: LaneStart, early: LaneFrame[], passed: (frame: LaneFrame) => number): void {
    let rest = early
    this.replacedTold = false
    if (start.place > this.last) {
      // From the lowest: below `last`, a history the lane never saw can have reached the new subscription too.
      const arrived = early.filter((frame) => frame.place <= start.place)
      rest = early.filter((frame) => !arrived.includes(frame))
      let missed = 0
      for (const frame of arrived.sort((a, b) => a.place - b.place)) {
        missed += this.skipTo(frame)
        // Below the start, on the history the route followed: one the lane can't place replaced what it delivered.
        this.decide(frame, passed(frame), 'replaced')
      }
      if (start.place > this.last) {
        missed += start.place - this.last
        this.follow(start, null, this.branch)
      }
      if (missed > 0) route.reportMissed(missed)
    } else if (!(start.place === this.last && start.id === this.tip) && !this.knowsAt(start.place, start.id)) {
      // Only a lane that delivered can have got what the record's new history replaced.
      if (this.delivered) route.replaced()
      this.replacedTold = true
      if (start.place < this.last) this.rewindPending = true
      this.follow(start, null, ++this.branches)
      this.tipOwed = start.id !== NO_ID
    }
    for (const frame of rest) this.offer(frame, passed(frame))
  }

  /** The route starts at `start` of another history than the lane followed, which its key was told of: no count holds
   *  across two histories, so the lane follows the start as a history it never saw, unless it stands at the start.
   *  A frame at or below the start is of the change that read saw. */
  private switchTo(start: LaneStart, early: LaneFrame[], passed: (frame: LaneFrame) => number): void {
    if (!(start.place === this.last && start.id === this.tip)) {
      if (start.place < this.last) this.rewindPending = true
      const owed = start.id !== NO_ID && !this.knowsAt(start.place, start.id)
      this.follow(start, null, ++this.branches)
      this.tipOwed = owed
    }
    for (const frame of [...early].sort((a, b) => a.place - b.place)) {
      this.offer(frame, passed(frame), frame.place <= start.place ? 'accounted' : 'live')
    }
  }

  /** Steps past the missing frames before `frame`, unless the ring knows its predecessor: how many it passed. */
  private skipTo(frame: LaneFrame): number {
    if (frame.place <= this.last + 1 || this.branchOf(frame.prev) !== undefined) return 0
    const missed = frame.place - this.last - 1
    this.last = frame.place - 1
    this.tip = frame.prev
    return missed
  }

  private park(frame: LaneFrame): void {
    this.parked ??= new Map()
    const frames = this.parked.get(frame.place)
    if (frames === undefined) this.parked.set(frame.place, [frame])
    else frames.push(frame)
    this.releaseTimer ??= unrefTimer(setTimeout(() => this.waitEnded(), REORDER_WAIT_MS))
  }

  /** The parked frames that now follow in order. */
  private drain(): void {
    while (this.parked !== null && !this.done) {
      const frames = this.parked.get(this.last + 1)
      if (frames === undefined) break
      this.parked.delete(this.last + 1)
      for (const frame of frames) if (this.routes.has(frame.via)) this.decide(frame, Number.POSITIVE_INFINITY, 'live')
    }
    if (this.parked?.size === 0) this.clearParked()
  }

  /** What the lane waited for isn't coming. Its routes say they may have missed messages, which is what makes Room
   *  catch up, and it goes on from the parked frames. */
  private waitEnded(): void {
    this.releaseTimer = null
    for (const route of [...this.routes]) route.waitEnded()
    if (this.parked !== null && !this.done) this.release()
  }

  /** Every parked frame goes out in place order, past what never arrived. */
  private release(): void {
    const frames = [...this.parked!.values()].flat().sort((a, b) => a.place - b.place)
    this.clearParked()
    let missed = 0
    for (const frame of frames) {
      // A consumer may close a route, which its frames leave with.
      if (!this.routes.has(frame.via)) continue
      missed += this.skipTo(frame)
      this.decide(frame, Number.POSITIVE_INFINITY, 'live')
    }
    this.drain()
    if (missed > 0 && !this.done) frames[0]!.via.reportMissed(missed)
  }

  private clearParked(): void {
    if (this.releaseTimer !== null) clearTimeout(this.releaseTimer)
    this.releaseTimer = null
    this.parked = null
  }
}

/** The early frames a start covers: its own frame and every one before it, by the ids each names before it. */
function coveredBy(start: LaneStart, early: LaneFrame[]): Set<LaneFrame> {
  const byId = new Map(early.map((frame) => [frame.id, frame]))
  const covered = new Set<LaneFrame>()
  for (let frame = byId.get(start.id); frame !== undefined; frame = byId.get(frame.prev)) covered.add(frame)
  return covered
}
