export { LaneHistory }
export type { LaneFrame, LaneRoute }

import type { OrderingInfo } from 'telefunc/__internal'
import { unrefTimer } from 'telefunc/__internal'
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
  /** `rewound`: the lane follows a frame at or below the last place it delivered. */
  deliver(frame: LaneFrame, rewound: boolean): void
  /** The frame isn't delivered: its fence settles. */
  dropped(frame: LaneFrame): void
  reportMissed(count: number | null): void
  /** The wait for a missing frame ended: the route may have missed messages. */
  waitEnded(): void
}

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
 * One lane's order across every subscription on the connection that carries it. Redis pub/sub keeps no log and a
 * failover may promote a replica that lacks the last commits, so a frame's place alone can't tell a copy or a late frame
 * from the history that replaced the delivered one. Each frame names its commit and the one before it, and the lane
 * remembers its last `RING_SIZE` deliveries: a frame it can tell is a copy or a counted hole's late original is dropped,
 * a frame that continues what it delivered is delivered, and where it can't tell which history a frame belongs to it
 * reports one loss of an unknown number, then delivers and follows the frame. It never picks one history over another.
 */
class LaneHistory {
  /** The routes it delivers through: it lives while one holds it. */
  readonly routes = new Set<LaneRoute>()
  private started = false
  private last = 0
  private tip = NO_ID
  private branch = 0
  private branches = 0
  /** Only once it delivered can a consumer have got what another history replaced. */
  private delivered = false
  /** The highest place the ring let go of: above it, every frame delivered at a place is still in the ring. */
  private evicted = 0
  private unseenReported = false
  /** A resume followed a start below `last`: the next delivery is a rewind. */
  private rewindPending = false
  /** The tip is a start a resume followed after a report, whose own commit the lane never delivered. */
  private tipOwed = false
  private readonly ring: Entry[] = []
  /** The ring's oldest entry, once it is full. */
  private oldest = 0
  /** The frames past a missing one, by place, until it arrives or the wait ends. */
  private parked: Map<number, LaneFrame[]> | null = null
  private releaseTimer: ReturnType<typeof setTimeout> | null = null

  get done(): boolean {
    return this.routes.size === 0
  }

  /** `route` is ready at `start` (none: the lane had no commit) with the frames that reached it before. On a first
   *  establish, those its start covers are dropped and the rest go out; on a resume, the start is compared with what
   *  the lane delivered. A route that joins a running lane only drops the frames its start covers. */
  establish(
    route: LaneRoute,
    start: LaneStart | undefined,
    early: LaneFrame[],
    resuming: boolean,
    passed: (frame: LaneFrame) => number,
  ): void {
    if (!this.started) {
      this.started = true
      // A lane new to a resuming route had no commit when the route was last ready.
      this.follow(resuming ? NO_START : (start ?? NO_START), null, 0)
      if (!resuming) {
        const covered = start === undefined ? null : coveredBy(start, early)
        for (const frame of early) {
          if (covered?.has(frame)) route.dropped(frame)
          else this.offer(frame)
        }
        return
      }
    }
    if (resuming) return this.resume(route, start ?? NO_START, early, passed)
    const covered = start === undefined ? null : coveredBy(start, early)
    for (const frame of early) {
      if (covered?.has(frame)) route.dropped(frame)
      else this.offer(frame, passed(frame))
    }
  }

  /** A frame, decided by the first row that matches. `passedBefore` is the highest place its route brought the lane
   *  before it; a frame the lane held has passed. */
  offer(frame: LaneFrame, passedBefore = Number.POSITIVE_INFINITY): void {
    // A lane with no commit when its route went live: every one of its commits is after.
    if (!this.started) {
      this.started = true
      this.follow(NO_START, null, 0)
    }
    this.decide(frame, passedBefore)
    this.drain()
  }

  /** The route is gone: its parked frames go with it, as every route of the lane brings each of its frames. */
  leave(route: LaneRoute): void {
    this.routes.delete(route)
    if (this.parked !== null) {
      for (const [place, frames] of this.parked) {
        const kept = frames.filter((frame) => frame.via !== route)
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

  private decide(frame: LaneFrame, passedBefore: number): void {
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
      this.report(via)
      return this.deliver(frame, ++this.branches)
    }
    // unseen: a copy whose first delivery left the ring can't be told from a new frame. A frame at or below its own
    // route's highest place is that route's rewind, one node's in-order stream going back, so it is decided below.
    if (place <= this.evicted && place > passedBefore) {
      if (!this.unseenReported) this.report(via)
      this.unseenReported = true
      return via.dropped(frame)
    }
    if (place > this.last + 1) return this.park(frame)
    // unknown
    this.report(via)
    this.deliver(frame, ++this.branches)
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
    this.unseenReported = false
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

  /** A report of what another history replaced, which only a lane that delivered can have lost. */
  private report(route: LaneRoute): void {
    if (this.delivered) route.reportMissed(null)
  }

  /** The route's lost connection ended at `start`: ahead of the lane, what reached the new subscription goes out and
   *  the rest is counted; at a place the lane knows, the next frame decides; anywhere else another history replaced
   *  what it delivered, and the lane follows the start. */
  private resume(route: LaneRoute, start: LaneStart, early: LaneFrame[], passed: (frame: LaneFrame) => number): void {
    let rest = early
    if (start.place > this.last) {
      const arrived = early.filter((frame) => frame.place > this.last && frame.place <= start.place)
      rest = early.filter((frame) => !arrived.includes(frame))
      let missed = 0
      for (const frame of arrived.sort((a, b) => a.place - b.place)) {
        missed += this.skipTo(frame)
        this.decide(frame, passed(frame))
      }
      if (start.place > this.last) {
        missed += start.place - this.last
        this.follow(start, null, this.branch)
      }
      if (missed > 0) route.reportMissed(missed)
    } else if (!(start.place === this.last && start.id === this.tip) && !this.knowsAt(start.place, start.id)) {
      this.report(route)
      if (start.place < this.last) this.rewindPending = true
      this.follow(start, null, ++this.branches)
      this.tipOwed = start.id !== NO_ID
    }
    for (const frame of rest) this.offer(frame, passed(frame))
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
      for (const frame of frames) if (this.routes.has(frame.via)) this.decide(frame, Number.POSITIVE_INFINITY)
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
      this.decide(frame, Number.POSITIVE_INFINITY)
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
