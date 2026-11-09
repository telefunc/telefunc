export { PathEstimate }
export type { PathEstimateOptions }

import { WIRE_INITIAL_WINDOW_BYTES } from '../constants.js'
import type { PiecesAck } from '../wire/frame.js'
import { Fifo } from '../../utils/Fifo.js'

type PathEstimateOptions = {
  /** The longest a report waits after the bytes it reports arrived, in ms. */
  reportGap: number
}

/** Bytes a wire handed: where they end, of all it handed, and when; what the newest report said then; and whether the
 *  sender held anything back then, so that what they measure is the path's rate and not the sender's. */
type Handed = { end: number; at: number; took: number; tookAt: number; held: boolean }

/** What one wire's path delivers, from its receiver's reports of the bytes it took: the rate, the round trip, the bytes in
 *  flight, and whether the path is the limit. A new wire starts with none of them measured. */
class PathEstimate {
  private readonly reportGap: number
  private handedBytes = 0
  /** What the newest report said the receiver took, and when it came. */
  private took = 0
  private tookAt = 0
  /** What was handed and no report covers yet, at most one entry per ms; the newest of them, null with none. */
  private readonly sent = new Fifo<Handed>()
  private newestSent: Handed | null = null
  /** Whether the sender held anything back since the newest report. */
  private heldSinceReport = false
  /** The fastest rate and the least round trip of this round and the one before: their best is the estimate. */
  private rateNow = 0
  private ratePrev = 0
  private roundTripNow = Number.POSITIVE_INFINITY
  private roundTripPrev = Number.POSITIVE_INFINITY
  /** The round ends once a report covers what was handed when it started; the first starts at the first report. */
  private round: Round | null = null
  /** What the round before delivered per ms, 0 where it is no evidence. */
  private prevRoundRate = 0
  /** The path delivers less than the wire would hand it: proven by one full round of reports. */
  pathBound = false

  constructor({ reportGap }: PathEstimateOptions) {
    this.reportGap = reportGap
  }

  /** In bytes per ms, what the path delivers: 0 before a report measured it. */
  get rate(): number {
    return Math.max(this.rateNow, this.ratePrev)
  }

  /** In ms, the least time the bytes of the latest round took from handed to reported. */
  get roundTrip(): number {
    return Math.min(this.roundTripNow, this.roundTripPrev)
  }

  /** What the path holds that it delivers within a round trip and the wait for its report: 0 before measured. */
  get window(): number {
    const { rate, roundTrip } = this
    return rate === 0 || roundTrip === Number.POSITIVE_INFINITY ? 0 : rate * (roundTrip + this.reportGap)
  }

  get inFlight(): number {
    return this.handedBytes - this.took
  }

  /** The bytes the wire may hand now: a window while the path is the limit, else twice it, as a slow start, and at
   *  least the initial window every TCP sender starts with. */
  get room(): number {
    const bound = this.pathBound ? this.window : Math.max(WIRE_INITIAL_WINDOW_BYTES, 2 * this.window)
    return bound - this.inFlight
  }

  /** The wire handed bytes up to `total`, counted as its receiver counts them, at `at`. */
  handed(total: number, at: number): void {
    if (total <= this.handedBytes) return
    if (this.handedBytes === this.took && this.sent.length === 0) this.tookAt = at
    this.handedBytes = total
    const last = this.newestSent
    if (last !== null && at - last.at < 1 && last.held === this.heldSinceReport) {
      last.end = total
      return
    }
    this.newestSent = { end: total, at, took: this.took, tookAt: this.tookAt, held: this.heldSinceReport }
    this.sent.push(this.newestSent)
  }

  /** The sender held bytes back for want of `room`. */
  held(): void {
    this.heldSinceReport = true
  }

  /** Takes a report that came at `now`: the receiver took `bytes` in all, counted mod 2^32, and sent it `heldMs` after
   *  the newest of them arrived. Returns false if it reports more than was handed. */
  acknowledged({ bytes, heldMs }: PiecesAck, now: number): boolean {
    const took = this.took + ((bytes - this.took) >>> 0)
    if (took > this.handedBytes) return false
    if (took === this.took) return true
    let newest: Handed | undefined
    while (this.sent.length > 0 && this.sent.peek()!.end <= took) newest = this.sent.shift()
    if (newest === this.newestSent) this.newestSent = null
    if (newest !== undefined) this.sample(newest, took, now, heldMs)
    const round = (this.round ??= newRound(this.handedBytes, now, this.took))
    // A round trip with no report says nothing of the path: a stall or a longer path, not a slow one.
    if (now - this.tookAt > this.roundTrip + this.reportGap) round.silent = true
    if (!this.heldSinceReport) round.held = false
    round.largestReport = Math.max(round.largestReport, took - this.took)
    this.took = took
    this.tookAt = now
    this.heldSinceReport = false
    if (took >= round.end) this.endRound(now)
    return true
  }

  private sample(newest: Handed, took: number, now: number, heldMs: number): void {
    const roundTrip = now - heldMs - newest.at
    if (roundTrip < this.roundTripNow) this.roundTripNow = Math.max(0, roundTrip)
    const elapsed = now - newest.tookAt
    if (elapsed <= 0) return
    const rate = (took - newest.took) / elapsed
    // What a sender that held nothing back hands measures its own pace: that can only show the path is faster.
    if ((newest.held || rate > this.rate) && rate > this.rateNow) this.rateNow = rate
  }

  /** The path is the limit where, through a whole round of reports, the sender held bytes back and what the path
   *  delivered per ms rose no more than one report shows. */
  private endRound(now: number): void {
    const round = this.round!
    const ms = now - round.at
    const rate = ms > 0 ? (this.took - round.took) / ms : 0
    const evidence = round.held && !round.silent && this.prevRoundRate > 0
    if (evidence && rate - this.prevRoundRate <= round.largestReport / ms) this.pathBound = true
    this.prevRoundRate = round.silent ? 0 : rate
    if (this.rateNow > 0) this.ratePrev = this.rateNow
    this.rateNow = 0
    if (this.roundTripNow < Number.POSITIVE_INFINITY) this.roundTripPrev = this.roundTripNow
    this.roundTripNow = Number.POSITIVE_INFINITY
    this.round = newRound(this.handedBytes, now, this.took)
  }
}

/** A round of reports: it ends once one covers `end`, what was handed when it started `at`, with `took` reported. */
type Round = { end: number; at: number; took: number; largestReport: number; held: boolean; silent: boolean }

function newRound(end: number, at: number, took: number): Round {
  return { end, at, took, largestReport: 0, held: true, silent: false }
}
