export { SendsOutrun }

import { CREDIT_WINDOW_INITIAL_BYTES, SENDS_OUTRUN_MS } from '../../constants.js'
import { DrainRate } from '../DrainRate.js'

/** Whether a channel's sends outrun its page: each time the page takes some, for `SENDS_OUTRUN_MS`, more is ahead of it
 *  than when that began, and more than a page's first credit and than the page takes in `SENDS_OUTRUN_MS`. */
class SendsOutrun {
  /** The rate the page takes what the channel sends, as the credit it grants and the ack requests it answers. */
  private readonly drain = new DrainRate()
  private drainedAt = performance.now()
  /** Since when, each time the page took some, more was ahead of it than then. */
  private run: { from: number; ahead: number } | null = null

  /** In bytes per ms, 0 before the page took any. */
  get rate(): number {
    return this.drain.rate
  }

  /** The page took `bytes` at `now`, which left `ahead` bytes ahead of it: true where the sends keep outrunning it. */
  drained(bytes: number, ahead: number, now: number): boolean {
    this.drain.spanned(bytes, now - this.drainedAt)
    this.drainedAt = now
    // As much as a page's first credit lets be on the way is what a page cut off for a moment leaves ahead.
    if (ahead <= Math.max(CREDIT_WINDOW_INITIAL_BYTES, this.drain.rate * SENDS_OUTRUN_MS)) {
      this.run = null
      return false
    }
    // A burst that drains has less ahead each time the page takes some.
    if (this.run === null || ahead <= this.run.ahead) {
      this.run = { from: now, ahead }
      return false
    }
    return now - this.run.from >= SENDS_OUTRUN_MS
  }
}
