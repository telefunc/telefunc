export { RoundTrips }

import { SendWaits } from './SendWait.js'

/** One wire's round trips, as this side measures them: every channel on it reads the same. */
class RoundTrips {
  private leastRtt = Number.POSITIVE_INFINITY
  /** Where this side's frames go by batch POST: how long each POST's oldest frame waited for its answer. */
  private readonly postWaits = new SendWaits()

  /** The least round trip an attach's probe or a later ping took on this wire, `Infinity` before a probe's answer. */
  get pathRtt(): number {
    return this.leastRtt
  }

  /** Takes how long a batch POST's oldest frame waited for its answer. */
  postAnswered(wait: number): void {
    this.postWaits.note(wait, performance.now())
  }

  /** The longest a frame waited for its batch POST's answer, of the POSTs answered at or after `at`. */
  longestPostWaitSince(at: number): number {
    return this.postWaits.since(at)
  }

  /** Takes a round trip a probe on this wire took. */
  measured(rtt: number): void {
    if (rtt < this.leastRtt) this.leastRtt = rtt
  }
}
