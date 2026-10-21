export { SocketWait }

import { DRAIN_SPAN_MS, DrainRate, Waits, type SendWait } from '../Link.js'

type SocketWaitOptions = {
  /** All the wire holds, which drains as the frames it is handed arrive. */
  held: () => number | undefined
  /** What of that a frame that goes at once counts ahead of it: on SSE the stream's, elsewhere all `held`. */
  ahead: () => number | undefined
}

/** The wait of a frame that goes at once: what `ahead` reads, over the fastest newest rate the wire drained at. */
class SocketWait implements SendWait {
  private readonly held: () => number | undefined
  private readonly ahead: () => number | undefined
  private readonly drain = new DrainRate()
  /** The span being measured: when it started, what it held then, and the bytes sent since. */
  private spanFrom = 0
  private spanHeld = 0
  private spanHanded = 0
  private readonly waits = new Waits()

  constructor({ held, ahead }: SocketWaitOptions) {
    this.held = held
    this.ahead = ahead
  }

  /** Notes `bytes` about to be sent, of a frame that goes at once if `atOnce`. */
  handed(bytes: number, atOnce: boolean): void {
    const now = performance.now()
    if (now - this.spanFrom >= DRAIN_SPAN_MS) this.measure(now)
    if (atOnce) this.waits.note(this.waitNow(), now)
    this.spanHanded += bytes
  }

  waitNow(): number {
    const ahead = this.ahead()
    return ahead === undefined || this.drain.rate === 0 ? 0 : ahead / this.drain.rate
  }

  longestWaitSince(at: number): number {
    return Math.max(this.waits.since(at), this.waitNow())
  }

  /** Ends the span: a rate where the socket held data at both its ends. */
  private measure(now: number): void {
    const held = this.held() ?? 0
    if (this.spanHeld > 0 && held > 0)
      this.drain.measured((this.spanHeld + this.spanHanded - held) / (now - this.spanFrom))
    this.spanFrom = now
    this.spanHeld = held
    this.spanHanded = 0
  }
}
