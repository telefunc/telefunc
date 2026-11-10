export { PathEstimate }
export type { PathEstimateOptions }

import { WIRE_INITIAL_WINDOW_BYTES, WIRE_SEGMENT_BYTES } from '../constants.js'
import type { PiecesAck } from '../wire/frame.js'
import { Fifo } from '../../utils/Fifo.js'

type PathEstimateOptions = {
  /** The longest a report waits after the bytes it reports arrived, in ms. */
  reportGap: number
}

/** Bytes a wire handed: where they end, of all it handed, and when. */
type Handed = { end: number; at: number }

/** Lossy bytes offered by the end of the ms `at`, of all offered. */
type Offered = { at: number; total: number }

/** What one wire's path delivers, from its receiver's reports of the bytes it took: the rate, the round trip, the bytes in
 *  flight, and whether the path is the limit of the lossy streams on the wire. A new wire starts with none of them
 *  measured. */
class PathEstimate {
  private readonly reportGap: number
  private handedBytes = 0
  /** What the newest report said the receiver took, when it came, and when the newest byte it covers was handed. */
  private took = 0
  private tookAt = 0
  private tookHandedAt = 0
  /** What was handed and no report covers yet, at most one entry per ms; the newest of them, null with none. */
  private readonly sent = new Fifo<Handed>()
  private newestSent: Handed | null = null
  /** The least round trip of the wire, its path's own: the queue the wire builds and a late report only add to one. */
  private leastRoundTrip = Number.POSITIVE_INFINITY
  /** The least round trip of the reports within the latest round trip, and the least of them at the newest report. */
  private readonly recentRoundTrips = new WindowedBest({ better: (a, b) => a < b })
  private recentRoundTrip = Number.POSITIVE_INFINITY
  /** What the path delivered per ms, and what it carries per ms, each the fastest within the latest two round trips at
   *  the newest report. */
  private readonly deliveredRates = new WindowedBest({ better: (a, b) => a > b })
  private readonly carriedRates = new WindowedBest({ better: (a, b) => a > b })
  private deliveredRate = 0
  private carriedRate = Number.POSITIVE_INFINITY
  /** Where the bytes of the next carried measure start: what a report said the receiver took, when the newest of them
   *  was handed, when the report came less how long it was held, the ms the bytes after it arrived over since, and
   *  whether every report since showed its bytes spaced by the path. */
  private span: { took: number; handedAt: number; reportedAt: number; ms: number; spaced: boolean } | null = null
  /** The lossy bytes offered since the oldest byte in flight was handed, and those offered before it. */
  private readonly offered = new Fifo<Offered>()
  private newestOffered: Offered | null = null
  private offeredBefore = 0
  /** What a TCP sender's slow start under the wire would have grown its window to (RFC 5681 §3.1): the initial window
   *  and every byte reported taken since. It ends, null, once reports show a queue, as HyStart++ ends TCP's (RFC 9406),
   *  so that the wire doesn't flood its path whether or not its lossy streams outrun it: two in a row, each its round
   *  trip above the least by more than a gap and its bytes spaced by the path, as a queue spaces every report and a
   *  path grown longer only one. */
  private slowStart: number | null = WIRE_INITIAL_WINDOW_BYTES
  /** The newest report showed a queue, as `slowStart` reads one. */
  private queueShown = false
  /** Bytes the wire may hand while the path is the limit: they come at its pace, and every byte handed takes some. */
  private tokens = 0
  private tokensAt = 0
  /** The lossy streams offer more than the path carries, and queued what it couldn't. */
  pathBound = false

  constructor({ reportGap }: PathEstimateOptions) {
    this.reportGap = reportGap
  }

  /** In bytes per ms, what the path delivered: 0 before a report measured it. */
  get rate(): number {
    return this.deliveredRate
  }

  /** In ms, the least time bytes of this wire took from handed to reported. */
  get roundTrip(): number {
    return this.leastRoundTrip
  }

  /** In bytes per ms, what the path carries, from bytes that arrived spaced by the path, not by the sender: `Infinity`
   *  before measured. While the path is the limit it stands until measured again, as paced bytes measure nothing. */
  get pathRate(): number {
    return this.carriedRate
  }

  get inFlight(): number {
    return this.handedBytes - this.took
  }

  /** The bytes the wire may hand now: what the path delivers in a round trip and the wait for its report while it is the
   *  limit, else twice that, at least the slow start's window while it lasts, and at least the initial window. */
  get room(): number {
    const { rate, roundTrip } = this
    const window = rate === 0 || roundTrip === Number.POSITIVE_INFINITY ? 0 : rate * (roundTrip + this.reportGap)
    const bound =
      this.pathBound && window > 0 ? window : Math.max(this.slowStart ?? WIRE_INITIAL_WINDOW_BYTES, 2 * window)
    return bound - this.inFlight
  }

  /** In ms, how long the next lossy bytes wait for the path's pace: 0 while it isn't the limit. */
  paceWait(now: number): number {
    if (!this.pathBound) return 0
    this.accrue(now)
    return this.tokens >= 0 ? 0 : -this.tokens / this.paceRate
  }

  /** The wire handed bytes up to `total`, counted as its receiver counts them, at `at`. */
  handed(total: number, at: number): void {
    if (total <= this.handedBytes) return
    if (this.pathBound) {
      this.accrue(at)
      this.tokens -= total - this.handedBytes
    }
    this.handedBytes = total
    const last = this.newestSent
    if (last !== null && at - last.at < 1) {
      last.end = total
      return
    }
    this.newestSent = { end: total, at }
    this.sent.push(this.newestSent)
  }

  /** The wire's lossy streams offered `bytes` at `at`, to hand now or later, or to skip. */
  offer(bytes: number, at: number): void {
    // A report covers bytes handed from now on: what came before them is all that's needed.
    if (this.handedBytes === this.took) this.forgetOffered()
    const last = this.newestOffered
    if (last !== null && at - last.at < 1) {
      last.total += bytes
      return
    }
    this.newestOffered = { at, total: (last?.total ?? this.offeredBefore) + bytes }
    this.offered.push(this.newestOffered)
  }

  /** Takes a report that came at `now`: the receiver took `bytes` in all, counted mod 2^32, sent it `heldMs` after the
   *  newest of them arrived, and the newest data it took arrived `spanUs` after the newest the report before covered.
   *  Returns false if it reports more than was handed. */
  acknowledged({ bytes, heldMs, spanUs }: PiecesAck, now: number): boolean {
    const took = this.took + ((bytes - this.took) >>> 0)
    if (took > this.handedBytes) return false
    if (took === this.took) return true
    let newest: Handed | undefined
    while (this.sent.length > 0 && this.sent.peek()!.end <= took) newest = this.sent.shift()
    if (newest === this.newestSent) this.newestSent = null
    // The newest byte it covers was handed with the entry it ends, or with the next one, which it covers in part.
    const handedAt = newest?.end === took ? newest.at : this.sent.peek()!.at
    const roundTrip = Math.max(0, now - heldMs - handedAt)
    if (roundTrip < this.leastRoundTrip) this.leastRoundTrip = roundTrip
    this.recentRoundTrips.add(roundTrip, now)
    const first = this.tookAt === 0
    const sinceReport = now - this.tookAt
    // What the path delivered since the report before, over the longest of: the gap between the reports, between the
    // handing of the bytes they end at, and between those bytes' arrivals. Two reports that come at once, as Wi-Fi
    // bunches them, or bytes handed at once, then don't read as a fast path (draft-cheng-iccrg-delivery-rate-estimation).
    const spanMs = spanUs / 1000
    // The receiver reports at least every gap while bytes arrive: bytes that arrived over more than two came across a
    // stall, which says nothing of the path's rate, so no measure spans it.
    const paused = spanMs > 2 * this.reportGap
    // Its bytes arrived over longer than they took to hand: the path spaced them, not the sender.
    const spaced = handedAt - this.tookHandedAt < spanMs
    const queue = spaced && roundTrip > this.leastRoundTrip + this.reportGap
    if (queue && this.queueShown) this.slowStart = null
    this.queueShown = queue
    const deliveredMs = Math.max(sinceReport, handedAt - this.tookHandedAt, spanMs)
    if (!first && deliveredMs > 0) this.deliveredRates.add((took - this.took) / deliveredMs, now)
    if (paused) this.span = null
    else this.measureCarried(took, handedAt, now, heldMs, spanMs, spaced)
    if (this.slowStart !== null) this.slowStart += took - this.took
    this.took = took
    this.tookAt = now
    this.tookHandedAt = handedAt
    this.update(now)
    if (!first && !paused) this.judge(roundTrip, handedAt, now, spaced)
    return true
  }

  /** The rates and the recent round trip as of `now`: the latest two round trips' fastest, the latest one's least. */
  private update(now: number): void {
    const since = now - 2 * (this.leastRoundTrip + this.reportGap)
    this.deliveredRate = this.deliveredRates.best(since, 0)
    const carried = this.carriedRates.best(since, 0)
    if (carried > 0) this.carriedRate = carried
    else if (!this.pathBound) this.carriedRate = Number.POSITIVE_INFINITY
    this.recentRoundTrip = this.recentRoundTrips.best(now - this.leastRoundTrip - this.reportGap, this.leastRoundTrip)
  }

  /** Bytes of at least two segments arrived spaced by the path, not by the sender, where every report of them showed
   *  its bytes handed faster than they arrived, while the byte before them still waited in the path's queue: over the
   *  span the receiver took them in, they measure what the path carries. A queue spaces every report; a pause, as a
   *  path grown longer makes, spaces one. The byte before them waited at least until its report came, less how long it
   *  was held and the path's own round trip. */
  private measureCarried(
    took: number,
    handedAt: number,
    now: number,
    heldMs: number,
    spanMs: number,
    spaced: boolean,
  ): void {
    const span = this.span
    if (span !== null) span.spaced &&= spaced
    // A report that came late, as Wi-Fi bunches them, came no later than the next one less their spacing on the
    // receiver's clock.
    if (span !== null && span.ms === 0) span.reportedAt = Math.min(span.reportedAt, now - heldMs - spanMs)
    if (span === null || took - span.took >= 2 * WIRE_SEGMENT_BYTES) {
      const queued = span !== null && handedAt <= span.reportedAt - this.leastRoundTrip
      if (queued && span.spaced) this.carriedRates.add((took - span.took) / (span.ms + spanMs), now)
      this.span = { took, handedAt, reportedAt: now - heldMs, ms: 0, spaced: true }
      return
    }
    span.ms += spanMs
  }

  /** The path is the limit once a report shows the queue it holds, its round trip above the wire's least by more than
   *  the most a late report adds and its bytes spaced by the path, and that the lossy streams offered, over that round
   *  trip, more than the path carries. It stays the limit until one shows they offered no more. */
  private judge(roundTrip: number, handedAt: number, now: number, spaced: boolean): void {
    while (this.offered.length > 0 && this.offered.peek()!.at <= handedAt) {
      this.offeredBefore = this.offered.shift()!.total
    }
    if (this.offered.length === 0) this.newestOffered = null
    const offered = (this.newestOffered?.total ?? this.offeredBefore) - this.offeredBefore
    const offersMore = offered / Math.max(now - handedAt, 1e-3) > this.pathRate
    if (this.pathBound) {
      this.pathBound = offersMore
      return
    }
    if (!offersMore || !spaced || roundTrip <= this.leastRoundTrip + this.reportGap) return
    this.pathBound = true
    this.tokens = 0
    this.tokensAt = now
  }

  /** The bytes per ms the path takes at the pace that drains the queue it holds: what it carries, less the share of the
   *  latest round trip that is queue. */
  private get paceRate(): number {
    const { leastRoundTrip, recentRoundTrip } = this
    return recentRoundTrip > leastRoundTrip ? (this.carriedRate * leastRoundTrip) / recentRoundTrip : this.carriedRate
  }

  /** The tokens of the time since they were last counted, up to two segments, which go back to back. */
  private accrue(now: number): void {
    this.tokens = Math.min(2 * WIRE_SEGMENT_BYTES, this.tokens + this.paceRate * (now - this.tokensAt))
    this.tokensAt = now
  }

  private forgetOffered(): void {
    this.offeredBefore = this.newestOffered?.total ?? this.offeredBefore
    while (this.offered.length > 0) this.offered.shift()
    this.newestOffered = null
  }
}

type WindowedBestOptions = {
  /** Whether `a` beats `b`. */
  better: (a: number, b: number) => boolean
}

/** The best of the values measured since a time: a value goes once it is older, or a newer one beats it. */
class WindowedBest {
  private readonly better: (a: number, b: number) => boolean
  /** From the oldest, each beating the ones after it. */
  private readonly values: { value: number; at: number }[] = []

  constructor({ better }: WindowedBestOptions) {
    this.better = better
  }

  add(value: number, at: number): void {
    const { values } = this
    while (values.length > 0 && !this.better(values[values.length - 1]!.value, value)) values.pop()
    values.push({ value, at })
  }

  /** The best measured at `since` or later, `none` with none. */
  best(since: number, none: number): number {
    const { values } = this
    while (values.length > 0 && values[0]!.at < since) values.shift()
    return values[0]?.value ?? none
  }
}
