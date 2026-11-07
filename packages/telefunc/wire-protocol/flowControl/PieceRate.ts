export { PieceRate }

import { WIRE_PIECE_RATE_WINDOW_MS } from '../constants.js'

/** The rates one WebSocket carries this side's frames at, as its piece sender measures them. */
class PieceRate {
  private fastestRate = 0
  private roseAt = 0
  private floorRate = 0
  /** Whether a probe gave `fastest`: its few bytes say how the wire starts, not what it carries. */
  private probed = false

  /** In bytes per ms, from a frame sent to its arrival, 0 before any; the next measure replaces one that lapsed. */
  get fastest(): number {
    return this.fastestRate
  }

  /** `fastest` once a frame of data gave it, 0 before. */
  get carried(): number {
    return this.probed ? 0 : this.fastestRate
  }

  /** In bytes per ms, a rate the wire carries at least: what an answer, a heartbeat or an acknowledgement showed. */
  get floor(): number {
    return this.floorRate
  }

  /** Takes a rate `rate` measured at `at`: it stands if faster, or once the one before lapsed. Returns whether it stood. */
  rise(rate: number, at: number, probed: boolean): boolean {
    if (!(rate > this.fastestRate) && !this.lapsed(at)) return false
    this.fastestRate = rate
    this.roseAt = at
    this.probed = probed
    return true
  }

  /** Whether no rate stands at `at`. */
  private lapsed(at: number): boolean {
    return this.fastestRate === 0 || at - this.roseAt > WIRE_PIECE_RATE_WINDOW_MS
  }

  /** Takes a rate the wire carried at least. Returns whether it raised the floor. */
  raiseFloor(floor: number): boolean {
    if (!(floor > this.floorRate)) return false
    this.floorRate = floor
    return true
  }
}
