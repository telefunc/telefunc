export { SocketWait }

import { DRAIN_SPAN_MS, Waits, newestFastest, type SendWait } from '../link.js'

/** The wait of a frame that goes at once: what `ahead` reads, over the fastest newest rate the wire drained at. */
class SocketWait implements SendWait {
  private drainRate = 0
  private readonly drainRates: number[] = []
  /** The span being measured: when it started, what it held then, and the bytes sent since. */
  private spanFrom = 0
  private spanHeld = 0
  private spanHanded = 0
  private readonly waits = new Waits()

  constructor(
    /** All the wire holds, which drains as the frames it is handed arrive. */
    private readonly held: () => number | undefined,
    /** What of that a frame that goes at once counts ahead of it: on SSE the stream's, elsewhere all `held`. */
    private readonly ahead: () => number | undefined,
  ) {}

  /** Notes `bytes` about to be sent, of a frame that goes at once if `atOnce`. */
  handed(bytes: number, atOnce: boolean): void {
    const now = performance.now()
    if (now - this.spanFrom >= DRAIN_SPAN_MS) this.measure(now)
    if (atOnce) this.waits.note(this.waitNow(), now)
    this.spanHanded += bytes
  }

  /** `SendWait`. */
  waitNow(): number {
    const ahead = this.ahead()
    return ahead === undefined || this.drainRate === 0 ? 0 : ahead / this.drainRate
  }

  /** `SendWait`. */
  longestWaitSince(at: number): number {
    return Math.max(this.waits.since(at), this.waitNow())
  }

  /** Ends the span: a rate where the socket held data at both its ends. */
  private measure(now: number): void {
    const held = this.held() ?? 0
    if (this.spanHeld > 0 && held > 0) {
      this.drainRate = newestFastest(this.drainRates, (this.spanHeld + this.spanHanded - held) / (now - this.spanFrom))
    }
    this.spanFrom = now
    this.spanHeld = held
    this.spanHanded = 0
  }
}
