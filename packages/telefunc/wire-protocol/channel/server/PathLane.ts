export { PathLane }

import type { WireQueue } from '../../flowControl/server/waitingPublishes.js'
import type { IndexedPeer, RoomWaiter } from './IndexedPeer.js'
import type { PublishBody } from './PublishBody.js'

/** A wire's lane for its lossy channels' publishes: whether its path is their limit, and when the next may go, from the
 *  wire's path estimate (see `PathEstimate`). */
class PathLane {
  private readonly queue: WireQueue
  /** The lossy channels waiting for room to hand the page more, and the timer that wakes them at the path's pace. */
  private waiters = new Set<RoomWaiter>()
  private paceTimer: ReturnType<typeof setTimeout> | null = null
  /** The wire closed: no waiter hears from it again. */
  private closed = false

  constructor(queue: WireQueue) {
    this.queue = queue
  }

  /** The lossy channels offered `bytes` at `at`, to hand now or later, or to skip. */
  offer(bytes: number, at: number): void {
    this.queue.path.offer(bytes, at)
  }

  /** The path carries less than the lossy channels offer: a newer publish replaces those waiting. */
  get pathBound(): boolean {
    return this.queue.path.pathBound
  }

  /** Whether a publish may go now: the path has room for it, less what the wire holds back, and its pace allows. If not,
   *  `waiter` hears once that may have changed: at the page's next report, or once the pace allows. */
  mayHand(now: number, waiter: RoomWaiter): boolean {
    const { path } = this.queue
    const paceWait = path.paceWait(now)
    if (paceWait === 0 && path.room - this.queue.unhandedBytes > 0) return true
    this.waitForRoom(waiter, paceWait)
    return false
  }

  hand(peer: IndexedPeer, body: PublishBody): void {
    peer.sendPublish(body)
  }

  /** Tells the waiters there may be room: the page reported what it took, or the pace allows. */
  wake(): void {
    const { waiters } = this
    this.waiters = new Set()
    for (const waiter of waiters) waiter.roomMayHaveOpened()
  }

  close(): void {
    this.closed = true
    this.waiters.clear()
    if (this.paceTimer !== null) clearTimeout(this.paceTimer)
    this.paceTimer = null
  }

  private waitForRoom(waiter: RoomWaiter, paceWait: number): void {
    if (this.closed) return
    this.waiters.add(waiter)
    if (paceWait > 0 && this.paceTimer === null) {
      this.paceTimer = setTimeout(() => {
        this.paceTimer = null
        this.wake()
      }, paceWait)
    }
  }
}
