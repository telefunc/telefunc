export { SelfTime }

import { FC_SELF_TIME_WINDOW_MS } from '../constants.js'

/** Telefunc's share of a channel's wall-clock time, which both directions of its credit read. */
class SelfTime {
  // Self-utilisation rolling-sum state. Two buckets, phase-weighted blend.
  private _prevBucket = 0
  private _curBucket = 0
  private _curBucketStart = performance.now()
  private _openedAt = performance.now()

  /** Channel calls this after the sync portion of a send or a receive-dispatch, which started at `startedAt`,
   *  to report how much wall-clock that work consumed. */
  recordSince(startedAt: number): void {
    const now = performance.now()
    this._advanceBuckets(now)
    this._curBucket += Math.min(now - startedAt, FC_SELF_TIME_WINDOW_MS)
  }

  /** Telefunc's share of wall-clock time on this channel over the last
   *  `FC_SELF_TIME_WINDOW_MS`, in [0, 1]. While the channel is younger than the
   *  window, denominates by lifetime instead. */
  utilisation(): number {
    const now = performance.now()
    this._advanceBuckets(now)
    const phase = (now - this._curBucketStart) / FC_SELF_TIME_WINDOW_MS
    const effectiveMs = this._prevBucket * (1 - phase) + this._curBucket
    const denom = Math.min(now - this._openedAt, FC_SELF_TIME_WINDOW_MS)
    if (denom <= 0) return 0
    const util = effectiveMs / denom
    return util > 1 ? 1 : util
  }

  private _advanceBuckets(now: number): void {
    const elapsed = now - this._curBucketStart
    if (elapsed < FC_SELF_TIME_WINDOW_MS) return
    if (elapsed < 2 * FC_SELF_TIME_WINDOW_MS) {
      this._prevBucket = this._curBucket
      this._curBucket = 0
      this._curBucketStart += FC_SELF_TIME_WINDOW_MS
      return
    }
    this._prevBucket = 0
    this._curBucket = 0
    this._curBucketStart = now
  }
}
