export { ReplayGate, TEXT_LANE_KEY, binaryLaneKey }

import { assertIsNotBrowser } from '../../../utils/assertIsNotBrowser.js'
assertIsNotBrowser()

const TEXT_LANE_KEY = 'text'
function binaryLaneKey(member: string, track: string): string {
  return `${member}\0${track}`
}

/** One holder's retained/live dedup per lane: a retained frame arrives once, never behind a same-or-newer frame, and its live echo is dropped. */
class ReplayGate {
  private readonly _high = new Map<string, number>()
  private readonly _pendingRetained = new Map<string, number>()

  admitLive(lane: string, seq: number): boolean {
    if (this._pendingRetained.get(lane) === seq) {
      this._pendingRetained.delete(lane)
      return false
    }
    if ((this._high.get(lane) ?? 0) < seq) this._high.set(lane, seq)
    return true
  }

  admitRetained(lane: string, seq: number): boolean {
    if ((this._high.get(lane) ?? 0) >= seq) return false
    this._high.set(lane, seq)
    this._pendingRetained.set(lane, seq)
    return true
  }

  forgetMember(member: string): void {
    const prefix = binaryLaneKey(member, '')
    for (const lanes of [this._high, this._pendingRetained])
      for (const key of lanes.keys()) if (key.startsWith(prefix)) lanes.delete(key)
  }
}
