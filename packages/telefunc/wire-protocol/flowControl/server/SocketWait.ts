export { SocketWait }
export type { SocketWaitOwner }

import { DRAIN_SPAN_MS, DrainRate } from '../DrainRate.js'
import { Waits, type SendWait } from '../SendWait.js'

interface SocketWaitOwner {
  /** All the wire holds, which drains as the frames it is handed arrive. */
  bufferedAmount(): number | undefined
  /** What of that a frame that goes at once counts ahead of it: on SSE the stream's, elsewhere all it holds. */
  queuedAhead(): number | undefined
}

type SocketWaitOptions = {
  owner: SocketWaitOwner
}

/** The wait of a frame that goes at once: what is ahead of it, over the fastest newest rate the wire drained at. */
class SocketWait implements SendWait {
  private readonly owner: SocketWaitOwner
  private readonly drain = new DrainRate()
  /** The span being measured: when it started, what it held then, and the bytes sent since. */
  private spanFrom = 0
  private spanHeld = 0
  private spanHanded = 0
  private readonly waits = new Waits()

  constructor({ owner }: SocketWaitOptions) {
    this.owner = owner
  }

  /** Notes `bytes` about to be sent at `at`, of a frame that goes at once if `atOnce`. */
  handed(bytes: number, atOnce: boolean, at: number): void {
    if (at - this.spanFrom >= DRAIN_SPAN_MS) this.measure(at)
    if (atOnce) this.waits.note(this.waitNow(), at)
    this.spanHanded += bytes
  }

  waitNow(): number {
    const ahead = this.owner.queuedAhead()
    return ahead === undefined || this.drain.rate === 0 ? 0 : ahead / this.drain.rate
  }

  longestWaitSince(at: number): number {
    return Math.max(this.waits.since(at), this.waitNow())
  }

  /** Ends the span: a rate where the socket held data at both its ends. */
  private measure(now: number): void {
    const held = this.owner.bufferedAmount() ?? 0
    if (this.spanHeld > 0 && held > 0)
      this.drain.measured((this.spanHeld + this.spanHanded - held) / (now - this.spanFrom))
    this.spanFrom = now
    this.spanHeld = held
    this.spanHanded = 0
  }
}
