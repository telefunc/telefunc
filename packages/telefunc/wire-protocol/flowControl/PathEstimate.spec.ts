import { describe, expect, it } from 'vitest'

import {
  WIRE_INITIAL_WINDOW_BYTES,
  WIRE_PATH_PROBE_GAIN,
  WIRE_PATH_PROBE_ROUNDS,
  WIRE_PIECES_ACK_BYTES,
  WIRE_SEGMENT_BYTES,
} from '../constants.js'
import { PathEstimate } from './PathEstimate.js'

/** A WebSocket's 10 kHz lossy stream: what one value takes on the wire, and the gap between the receiver's reports. */
const VALUE_BYTES = 84
const REPORT_GAP_MS = 50

type Link = {
  /** What the path carries, in bytes per ms; `rateAfter` changes it from a time on. */
  rate: number
  rateAfter?: { at: number; rate: number }
  /** Its one-way delay, in ms; `delayAfter` lengthens it from a time on. */
  oneWay: number
  delayAfter?: { at: number; oneWay: number }
  /** The ms in which it carries nothing. */
  stalled?: (t: number) => boolean
  /** Every other report comes a gap late, with the next one, as acknowledgements bunched by Wi-Fi aggregation do. */
  pairedReports?: boolean
}

/** A lossy sender on a path that carries `link.rate` bytes per ms in order, a millisecond at a time: at each value
 *  published it hands those waiting while `PathEstimate.room` and its pace allow, and once the path is the limit a newer
 *  value replaces those waiting. Its receiver reports as the page does: a gap after the first bytes since its last report, but no sooner
 *  than a gap after that report, and at once once `WIRE_PIECES_ACK_BYTES` arrived since it.
 *
 *  With `kernel`, the sender is instead a TCP sender in slow start with no window of ours: one that starts at the initial
 *  window and grows by every byte acknowledged, its receiver acknowledging every other segment at once (RFC 5681). */
class Sim {
  readonly path = new PathEstimate({ reportGap: REPORT_GAP_MS })
  /** Of the values published in the run's second half, how old each was when it arrived, in ms. */
  readonly lateAges: number[] = []
  skipped = 0
  private readonly link: Link
  private t = 0
  private linkFreeAt = 0
  private handedBytes = 0
  private readonly arrivals: { at: number; end: number }[] = []
  private arrived = 0
  private arrivedAt = 0
  private reportedAt = Number.NEGATIVE_INFINITY
  private reportedNewestAt: number | null = null
  private reported = 0
  private reportCount = 0
  private firstUnreportedAt: number | null = null
  private readonly kernel: boolean
  private readonly valueBytes: number
  private readonly reportsEveryGap: boolean
  /** With `kernel`: its window, and what its receiver acknowledged. */
  private window = WIRE_INITIAL_WINDOW_BYTES
  private acknowledged = 0
  private readonly reports: { bytes: number; heldMs: number; spanUs: number; at: number }[] = []
  private waiting: number[] = []
  private published = 0

  constructor(link: Link, { kernel = false, valueBytes = VALUE_BYTES, reports = 'page' as 'page' | 'everyGap' } = {}) {
    this.link = link
    this.kernel = kernel
    this.valueBytes = valueBytes
    this.reportsEveryGap = reports === 'everyGap'
  }

  /** `perMs` values a ms are published for `ms`, and those published after `lateFrom` count as late. */
  run(ms: number, perMs: number, lateFrom = Number.POSITIVE_INFINITY): this {
    for (const end = this.t + ms; this.t < end; ) this.tick(++this.t, perMs, lateFrom)
    return this
  }

  private rate(t: number): number {
    const { rateAfter } = this.link
    return rateAfter && t >= rateAfter.at ? rateAfter.rate : this.link.rate
  }

  private oneWay(t: number): number {
    const { delayAfter } = this.link
    return delayAfter && t >= delayAfter.at ? delayAfter.oneWay : this.link.oneWay
  }

  private tick(t: number, perMs: number, lateFrom: number): void {
    const { path } = this
    while (this.arrived < this.arrivals.length && this.arrivals[this.arrived]!.at <= t) {
      this.arrivedAt = this.arrivals[this.arrived++]!.at
      this.firstUnreportedAt ??= this.arrivedAt
    }
    const took = this.arrived === 0 ? 0 : this.arrivals[this.arrived - 1]!.end
    if (this.reportDue(t, took)) {
      const late = this.link.pairedReports && this.reportCount++ % 2 === 0 ? REPORT_GAP_MS : 0
      const spanUs = this.reportedNewestAt === null ? 0 : (this.arrivedAt - this.reportedNewestAt) * 1000
      const heldMs = t - this.arrivedAt
      // A stall holds what goes either way: a report sent in it goes once it ends.
      let sentAt = t
      while (this.link.stalled?.(sentAt)) sentAt++
      this.reports.push({ bytes: took, heldMs, spanUs, at: sentAt + this.oneWay(sentAt) + late })
      this.reported = took
      this.reportedAt = t
      this.reportedNewestAt = this.arrivedAt
      this.firstUnreportedAt = null
    }
    while (this.reports.length > 0 && this.reports[0]!.at <= t) {
      const report = this.reports.shift()!
      if (this.kernel) {
        this.window += report.bytes - this.acknowledged
        this.acknowledged = report.bytes
      } else expect(path.acknowledged(report, t)).toBe(true)
      this.hand(t, lateFrom)
    }
    for (this.published += perMs; this.published >= 1; this.published--) {
      path.offer(this.valueBytes, t)
      if (!this.kernel && path.pathBound) {
        this.skipped += this.waiting.length
        this.waiting = []
      }
      this.waiting.push(t)
      this.hand(t, lateFrom)
    }
    this.hand(t, lateFrom)
  }

  private reportDue(t: number, took: number): boolean {
    const first = this.firstUnreportedAt
    if (first === null) return false
    if (this.kernel) return took - this.reported >= 2 * WIRE_SEGMENT_BYTES || this.arrivedAt < t
    if (this.reportsEveryGap) return t - this.reportedAt >= REPORT_GAP_MS
    if (took - this.reported >= WIRE_PIECES_ACK_BYTES) return true
    return t >= (first >= this.reportedAt + REPORT_GAP_MS ? first : this.reportedAt) + REPORT_GAP_MS
  }

  private mayHand(t: number): boolean {
    if (this.kernel) return this.handedBytes - this.acknowledged < this.window
    return this.path.room > 0 && this.path.paceWait(t) === 0
  }

  private hand(t: number, lateFrom: number): void {
    const { path } = this
    while (this.waiting.length > 0 && this.mayHand(t)) {
      const publishedAt = this.waiting.shift()!
      this.handedBytes += this.valueBytes
      path.handed(this.handedBytes, t)
      let start = Math.max(this.linkFreeAt, t)
      while (this.link.stalled?.(Math.floor(start))) start = Math.floor(start) + 1
      this.linkFreeAt = start + this.valueBytes / this.rate(start)
      const at = this.linkFreeAt + this.oneWay(this.linkFreeAt)
      this.arrivals.push({ at, end: this.handedBytes })
      if (publishedAt >= lateFrom) this.lateAges.push(at - publishedAt)
    }
  }

  /** The 99th percentile of `lateAges`. */
  get lateAgeP99(): number {
    const sorted = [...this.lateAges].sort((a, b) => a - b)
    return sorted[Math.floor(sorted.length * 0.99)]!
  }
}

// The links the lossy matrix measures: a and c keep up with 10 kHz, b and d don't.
const a: Link = { rate: 12_500, oneWay: 25 }
const b: Link = { rate: 125, oneWay: 25 }
const c: Link = { rate: 1000, oneWay: 150 }
const d: Link = { rate: 50, oneWay: 450 }
// A path the stream below outruns for a moment: 2.4 Mbit/s, 40 ms one way.
const e: Link = { rate: 300, oneWay: 40 }

describe('PathEstimate', () => {
  it('lets a stream its path carries go whole, through the first rounds of a long path', () => {
    for (const link of [a, c, { ...a, pairedReports: true }, { ...c, pairedReports: true }]) {
      const sim = new Sim(link).run(10_000, 10)
      expect(sim.skipped).toBe(0)
      expect(sim.path.pathBound).toBe(false)
    }
  })

  it('lets a stream go whole that outruns its path for less than a report gap of queue', () => {
    // 100 ms at 4/3 of what the path carries, 100 ms at 1/3: each burst queues 10 KB, 33 ms of the path's time.
    const sim = new Sim(e)
    for (let i = 0; i < 40; i++)
      sim.run(100, (4 / 3) * (e.rate / VALUE_BYTES)).run(100, (1 / 3) * (e.rate / VALUE_BYTES))
    expect(sim.skipped).toBe(0)
  })

  it('ramps up on a path its stream nearly fills no slower than TCP would under it', () => {
    const ours = new Sim(c).run(10_000, 10, 5000)
    const kernel = new Sim(c, { kernel: true }).run(10_000, 10, 5000)
    expect(ours.skipped).toBe(0)
    expect(ours.lateAgeP99).toBeLessThanOrEqual(kernel.lateAgeP99)
  })

  it('lets a stream go whole through a stall and a path grown longer', () => {
    const stalled = (t: number) => (t > 3000 && t <= 4000) || (t > 6000 && t <= 6300)
    expect(new Sim({ ...a, stalled }).run(10_000, 4).skipped).toBe(0)
    expect(new Sim({ ...a, delayAfter: { at: 1500, oneWay: 450 } }).run(10_000, 0.06).skipped).toBe(0)
    // A path grown four times longer: its pause spaces one report, a queue every one. A receiver that reports every gap
    // while bytes arrive brings the paused report sooner, and alone.
    const rows = [
      ['page', a, 2],
      ['page', { ...a, pairedReports: true }, 2],
      ['everyGap', a, 2],
      ['everyGap', { ...a, pairedReports: true }, 2],
      ['everyGap', c, 4],
      ['everyGap', { ...c, pairedReports: true }, 4],
      ['everyGap', c, 10],
    ] as const
    for (const [reports, link, perMs] of rows) {
      const grown = { ...link, delayAfter: { at: 1500, oneWay: 4 * link.oneWay } }
      expect(new Sim(grown, { reports }).run(8000, perMs).skipped).toBe(0)
    }
  })

  // Open (plan.md R3.3): once its slow start ended, a wire on a path grown longer is sized by the path's old least round
  // trip, as c at 4 values a ms grown four times longer with the page's reports: it holds what the path would carry.
  it.todo('lets a stream go whole on a path grown longer once the wire ended its slow start')

  it('keeps what reaches the receiver within two round trips and report gaps of young, on a path it outruns', () => {
    for (const link of [b, { ...b, pairedReports: true }]) {
      const sim = new Sim(link).run(10_000, 10, 5000)
      expect(sim.path.pathBound).toBe(true)
      expect(sim.lateAgeP99).toBeLessThanOrEqual(2 * (2 * link.oneWay + REPORT_GAP_MS))
      // What it carries, read on the receiver's clock.
      expect(sim.path.pathRate).toBeGreaterThan(0.9 * b.rate)
      expect(sim.path.pathRate).toBeLessThan(1.1 * b.rate)
    }
    const long = new Sim(d).run(10_000, 10, 5000)
    expect(long.path.pathBound).toBe(true)
    expect(long.lateAgeP99).toBeLessThanOrEqual(2 * (2 * d.oneWay + REPORT_GAP_MS))
  })

  it('keeps what reaches the receiver young when the stream outruns its path again, after keeping to it', () => {
    const sim = new Sim(b).run(3000, 10).run(1500, 1).run(3000, 10, 4500)
    expect(sim.lateAgeP99).toBeLessThanOrEqual(3 * (2 * b.oneWay + REPORT_GAP_MS))
  })

  it('keeps the bytes in flight within twice a round trip and report gap of the path, when no report judges it', () => {
    // Values of 16 KiB take two gaps and more each on b: every report reads as after a stall, and judges nothing. The
    // least round trip includes one value's time on the path.
    const sim = new Sim(b, { valueBytes: 16 * 1024 }).run(10_000, 1 / 50)
    const window = 2 * b.rate * (sim.path.roundTrip + REPORT_GAP_MS)
    expect(sim.path.inFlight).toBeLessThanOrEqual(window + 16 * 1024)
  })

  it('finds a path that sped up while the stream outran it, a quarter faster every eight round trips', () => {
    // A congested spell: the path carries a quarter of its rate until 1.5 s, less than the stream.
    for (const [link, perMs] of [
      [b, 1],
      [c, 4],
      [d, 0.4],
    ] as const) {
      const sim = new Sim({ ...link, rate: link.rate / 4, rateAfter: { at: 1500, rate: link.rate } }).run(1500, perMs)
      expect(sim.path.pathBound).toBe(true)
      const probes = Math.ceil(Math.log((perMs * VALUE_BYTES) / (link.rate / 4)) / Math.log(WIRE_PATH_PROBE_GAIN))
      sim.run((probes + 1) * WIRE_PATH_PROBE_ROUNDS * (sim.path.roundTrip + REPORT_GAP_MS), perMs)
      expect(sim.path.pathBound).toBe(false)
      const skipped = sim.skipped
      sim.run(3000, perMs)
      expect(sim.skipped).toBe(skipped)
    }
  })

  it('stops skipping once the stream slows to what its path carries', () => {
    const sim = new Sim(b).run(5000, 10)
    expect(sim.path.pathBound).toBe(true)
    const skipped = sim.skipped
    sim.run(5000, 1)
    expect(sim.path.pathBound).toBe(false)
    expect(sim.skipped - skipped).toBeLessThan(10)
  })
})
