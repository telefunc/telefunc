export { Link, Waits, newestFastest, DRAIN_SPAN_MS }
export type { SendWait }

import { WIRE_PIECE_RATE_WINDOW_MS } from './constants.js'

/** The least span a rate is measured over: a frame arrives whole, so a shorter span can count one that took longer. */
const DRAIN_SPAN_MS = 100
/** A wire drains at the fastest of this many of the newest such rates. */
const DRAIN_RATES = 8
/** How long a wait stays in the history a download's window reads (see `SendWait`). */
const WAITS_KEPT_MS = 60_000

/** Adds `rate` to `rates`, the newest `DRAIN_RATES`, and returns the fastest of them. */
function newestFastest(rates: number[], rate: number): number {
  rates.push(rate)
  if (rates.length > DRAIN_RATES) rates.shift()
  let fastest = rate
  for (const each of rates) if (each > fastest) fastest = each
  return fastest
}

/** How long the frames this side sends at once wait behind its own data on its wire, where it can tell. */
type SendWait = {
  /** What a frame sent now waits. */
  waitNow(): number
  /** The longest such a frame waited since `at`, on `performance.now()`'s clock, or one sent now waits. */
  longestWaitSince(at: number): number
}

/** One wire, as this side knows it: every channel on it reads the same round trip and rates. */
class Link {
  /** The least round trip an attach's probe or a later ping took on this wire, `Infinity` before a probe's answer. */
  pathRtt = Number.POSITIVE_INFINITY
  /** In bytes per ms, from a frame sent to its arrival, 0 before any; the next measure replaces one that lapsed. */
  fastest = 0
  private roseAt = 0
  /** In bytes per ms, a rate the link carries at least: what an answer, a heartbeat or an acknowledgement showed. */
  floor = 0

  /** Takes a round trip a probe on this wire took. */
  measured(rtt: number): void {
    if (rtt < this.pathRtt) this.pathRtt = rtt
  }

  /** Takes a rate `rate` measured at `at`: it stands if faster, or once the one before lapsed. Returns whether it stood. */
  rise(rate: number, at: number): boolean {
    if (!(rate > this.fastest) && !this.lapsed(at)) return false
    this.fastest = rate
    this.roseAt = at
    return true
  }

  /** Whether no rate stands at `at`. */
  private lapsed(at: number): boolean {
    return this.fastest === 0 || at - this.roseAt > WIRE_PIECE_RATE_WINDOW_MS
  }

  /** Takes a rate the link carried at least. Returns whether it raised the floor. */
  raiseFloor(floor: number): boolean {
    if (!(floor > this.floor)) return false
    this.floor = floor
    return true
  }
}

/** Waits by when they were seen, for `WAITS_KEPT_MS`; each drops the earlier ones no longer than it. */
class Waits {
  private entries: { at: number; wait: number }[] = []
  private head = 0

  note(wait: number, at: number): void {
    while (this.entries.length > this.head && this.entries[this.entries.length - 1]!.wait <= wait) this.entries.pop()
    this.entries.push({ at, wait })
    while (this.entries[this.head]!.at < at - WAITS_KEPT_MS) this.head++
    if (this.head > 16 && this.head >= this.entries.length >>> 1) {
      this.entries = this.entries.slice(this.head)
      this.head = 0
    }
  }

  /** The longest wait seen at or after `at`, 0 for none. */
  since(at: number): number {
    let lo = this.head
    let hi = this.entries.length
    while (lo < hi) {
      const mid = (lo + hi) >>> 1
      if (this.entries[mid]!.at < at) lo = mid + 1
      else hi = mid
    }
    return lo < this.entries.length ? this.entries[lo]!.wait : 0
  }
}
