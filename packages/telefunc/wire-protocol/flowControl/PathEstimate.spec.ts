import { describe, expect, it } from 'vitest'

import { PathEstimate } from './PathEstimate.js'

/** A WebSocket's 10 kHz lossy stream: what one value takes on the wire, and the gap between the receiver's reports. */
const VALUE_BYTES = 84
const REPORT_GAP_MS = 50

type Link = {
  /** What the path carries, in bytes per ms. */
  rate: number
  /** Its one-way delay, in ms; `delayAfter` lengthens it from a time on. */
  oneWay: number
  delayAfter?: { at: number; oneWay: number }
  /** The ms in which it carries nothing. */
  stalled?: (t: number) => boolean
  /** Every other report comes a gap late, with the next one, as acknowledgements bunched by Wi-Fi aggregation do. */
  pairedReports?: boolean
}

/** A lossy sender on a path that carries `link.rate` bytes per ms in order, a millisecond at a time: it hands the values
 *  published while `PathEstimate.room` and its pace allow, and once the path is the limit a newer value replaces those
 *  waiting. Its receiver reports every `REPORT_GAP_MS` while bytes arrived. */
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
  private reportedAt = 0
  private reportedNewestAt: number | null = null
  private reported = 0
  private reportCount = 0
  private readonly reports: { bytes: number; heldMs: number; spanUs: number; at: number }[] = []
  private waiting: number[] = []
  private published = 0

  constructor(link: Link) {
    this.link = link
  }

  /** `perMs` values a ms are published for `ms`, and those published after `lateFrom` count as late. */
  run(ms: number, perMs: number, lateFrom = Number.POSITIVE_INFINITY): this {
    for (const end = this.t + ms; this.t < end; ) this.tick(++this.t, perMs, lateFrom)
    return this
  }

  private oneWay(t: number): number {
    const { delayAfter } = this.link
    return delayAfter && t >= delayAfter.at ? delayAfter.oneWay : this.link.oneWay
  }

  private tick(t: number, perMs: number, lateFrom: number): void {
    const { path } = this
    while (this.arrived < this.arrivals.length && this.arrivals[this.arrived]!.at <= t) {
      this.arrivedAt = this.arrivals[this.arrived++]!.at
    }
    const took = this.arrived === 0 ? 0 : this.arrivals[this.arrived - 1]!.end
    if (t - this.reportedAt >= REPORT_GAP_MS && took > this.reported) {
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
    }
    while (this.reports.length > 0 && this.reports[0]!.at <= t) {
      expect(path.acknowledged(this.reports.shift()!, t)).toBe(true)
      this.hand(t, lateFrom)
    }
    for (this.published += perMs; this.published >= 1; this.published--) {
      path.offer(VALUE_BYTES, t)
      if (path.pathBound) {
        this.skipped += this.waiting.length
        this.waiting = []
      }
      this.waiting.push(t)
    }
    this.hand(t, lateFrom)
  }

  private hand(t: number, lateFrom: number): void {
    const { path } = this
    while (this.waiting.length > 0 && path.room > 0 && path.paceWait(t) === 0) {
      const publishedAt = this.waiting.shift()!
      this.handedBytes += VALUE_BYTES
      path.handed(this.handedBytes, t)
      let start = Math.max(this.linkFreeAt, t)
      while (this.link.stalled?.(Math.floor(start))) start = Math.floor(start) + 1
      this.linkFreeAt = start + VALUE_BYTES / this.link.rate
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

  it('lets a stream go whole through a stall and a path grown longer', () => {
    const stalled = (t: number) => (t > 3000 && t <= 4000) || (t > 6000 && t <= 6300)
    expect(new Sim({ ...a, stalled }).run(10_000, 1).skipped).toBe(0)
    expect(new Sim({ ...a, delayAfter: { at: 1500, oneWay: 450 } }).run(10_000, 0.06).skipped).toBe(0)
  })

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

  it('stops skipping once the stream slows to what its path carries', () => {
    const sim = new Sim(b).run(5000, 10)
    expect(sim.path.pathBound).toBe(true)
    const skipped = sim.skipped
    sim.run(5000, 1)
    expect(sim.path.pathBound).toBe(false)
    expect(sim.skipped - skipped).toBeLessThan(10)
  })
})
