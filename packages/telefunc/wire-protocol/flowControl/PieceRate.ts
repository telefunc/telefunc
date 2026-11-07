export { PieceRate }

import { WIRE_PIECE_RATE_WINDOW_MS } from '../constants.js'

/** The fastest rate measured, which stands if a faster comes, or until it lapsed, when the next replaces it. */
class StandingRate {
  rate = 0
  private roseAt = 0

  /** Takes `rate` measured at `at`. Returns whether it stood. */
  rise(rate: number, at: number): boolean {
    if (!(rate > this.rate) && !this.lapsed(at)) return false
    this.rate = rate
    this.roseAt = at
    return true
  }

  private lapsed(at: number): boolean {
    return this.rate === 0 || at - this.roseAt > WIRE_PIECE_RATE_WINDOW_MS
  }
}

/** The rates one WebSocket carries this side's frames at, as its piece sender measures them. */
class PieceRate {
  private readonly any = new StandingRate()
  private readonly data = new StandingRate()
  private floorRate = 0

  /** In bytes per ms, from a frame sent to its arrival, 0 before any; the next measure replaces one that lapsed. */
  get fastest(): number {
    return this.any.rate
  }

  /** `fastest` without a probe's measures: its few bytes say how the wire starts, not what it carries. */
  get carried(): number {
    return this.data.rate
  }

  /** In bytes per ms, a rate the wire carries at least: what an answer, a heartbeat or an acknowledgement showed. */
  get floor(): number {
    return this.floorRate
  }

  /** Takes a rate `rate` measured at `at`, by a probe if `probed`. Returns whether `fastest` changed. */
  rise(rate: number, at: number, probed: boolean): boolean {
    if (!probed) this.data.rise(rate, at)
    return this.any.rise(rate, at)
  }

  /** Takes a rate the wire carried at least. Returns whether it raised the floor. */
  raiseFloor(floor: number): boolean {
    if (!(floor > this.floorRate)) return false
    this.floorRate = floor
    return true
  }
}
