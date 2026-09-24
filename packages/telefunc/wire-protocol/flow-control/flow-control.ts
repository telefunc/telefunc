export { FlowControl }
export type { FlowControlEmit }

import { BdpEstimator } from './bdp-estimator.js'
import { macrotaskYield } from './macrotask-yield.js'
import {
  CREDIT_MSG_WINDOW_INITIAL,
  CREDIT_WINDOW_INITIAL_BYTES,
  CREDIT_WINDOW_INITIAL_BYTES_BATCH,
  FC_SELF_TIME_WINDOW_MS,
  FC_SELF_UTIL_THRESHOLD,
} from '../constants.js'

interface FlowControlEmit {
  byteWindowUpdate(bytes: number): void
  msgWindowUpdate(count: number): void
  bdpPing(): void
}

/**
 * Per-channel flow control. Two orthogonal credit axes coordinated by a single
 * BDP probe per RTT, plus a self-utilisation gate that applies symmetrically
 * to growth (receiver-side) and to outbound pacing (sender-side).
 *
 *   1. **Byte credit** (`_peerWindow` / `WINDOW` frame) bounds in-flight memory.
 *   2. **Message-count credit** (`_peerMsgWindow` / `MSG_WINDOW` frame) bounds
 *      in-flight per-RTT *message count* regardless of size.
 *   3. **Self-utilisation gate**: `_recordSelfTime(ms)` accumulates this channel's
 *      sync work into a two-bucket rolling sum. Above `FC_SELF_UTIL_THRESHOLD`,
 *      `onPingAck` refuses to grow either window and `decrement` yields a
 *      macrotask before returning.
 */
class FlowControl {
  private _bdp = new BdpEstimator()
  private _peerWindow: number = CREDIT_WINDOW_INITIAL_BYTES
  private _peerMsgWindow: number = CREDIT_MSG_WINDOW_INITIAL
  private _consumedBytes = 0
  private _consumedMessages = 0
  /** Single deferred shared by all senders blocked on credit. */
  private _creditReady: { promise: Promise<void>; resolve: () => void } | null = null
  private _shutdown = false

  // Self-utilisation rolling-sum state. Two buckets, phase-weighted blend.
  private _prevBucket = 0
  private _curBucket = 0
  private _curBucketStart = performance.now()
  private _openedAt = performance.now()

  constructor(private readonly _emit: FlowControlEmit) {}

  get byteWindow(): number {
    return this._bdp.byteWindow
  }

  get msgWindow(): number {
    return this._bdp.msgWindow
  }

  /** Sender-side: decrement credit for one frame of `bytes`. Returns `void` when
   *  both credit axes have headroom AND our loop utilisation is below the gate.
   *  Otherwise a Promise that resolves on credit refresh (credit-gated) or one
   *  macrotask later (util-gated — single yield, no re-check). */
  decrement(bytes: number): void | Promise<void> {
    this._peerWindow -= bytes
    this._peerMsgWindow -= 1
    if (this._peerWindow <= 0 || this._peerMsgWindow <= 0) return this._waitForCredit()
    if (this._selfUtilisation() > FC_SELF_UTIL_THRESHOLD) {
      return macrotaskYield.yield()
    }
  }

  onPeerByteWindow(bytes: number): void {
    this._peerWindow = bytes
    this._tryWakeCreditWaiters()
  }

  onPeerMessageWindow(count: number): void {
    this._peerMsgWindow = count
    this._tryWakeCreditWaiters()
  }

  /** Receiver-side: account one received frame off the wire. Emits a
   *  `BDP_PING` via the channel's emit callback iff the estimator opens a probe. */
  onReceived(bytes: number): void {
    if (this._bdp.onReceive(bytes)) this._emit.bdpPing()
  }

  /** Receiver-side: account post-callback consumption of one frame. Emits
   *  refresh `WINDOW` / `MSG_WINDOW` frames once a quarter of either window
   *  has been consumed. */
  onConsumed(bytes: number): void {
    this._consumedBytes += bytes
    this._consumedMessages += 1
    if (this._consumedBytes >= this._bdp.byteWindow >> 2) {
      this._consumedBytes = 0
      this._emit.byteWindowUpdate(this._bdp.byteWindow)
    }
    if (this._consumedMessages >= this._bdp.msgWindow >> 2) {
      this._consumedMessages = 0
      this._emit.msgWindowUpdate(this._bdp.msgWindow)
    }
  }

  /** Settle `BDP_PING_ACK`. Each axis grows iff its own sample saturated ≥ 2/3
   *  of its current window AND our own self-utilisation is below threshold.
   *  On growth, the new window is emitted to the peer immediately. */
  onPingAck(): void {
    const suggest = this._bdp.onPingAck()
    if (!suggest.acknowledged) return
    const wantBytes = suggest.bytes === 'grow'
    const wantMsgs = suggest.msgs === 'grow'
    if (!wantBytes && !wantMsgs) return
    if (this._selfUtilisation() > FC_SELF_UTIL_THRESHOLD) return
    if (wantBytes) {
      this._bdp.growBytes()
      this._emit.byteWindowUpdate(this._bdp.byteWindow)
    }
    if (wantMsgs) {
      this._bdp.growMsgs()
      this._emit.msgWindowUpdate(this._bdp.msgWindow)
    }
  }

  /** Channel calls this after the sync portion of a send or a receive-dispatch
   *  to report how much wall-clock that work consumed. */
  _recordSelfTime(durationMs: number): void {
    this._advanceBuckets()
    this._curBucket += Math.min(durationMs, FC_SELF_TIME_WINDOW_MS)
  }

  /** Telefunc's share of wall-clock time on this channel over the last
   *  `FC_SELF_TIME_WINDOW_MS`, in [0, 1]. While the channel is younger than the
   *  window, denominates by lifetime instead. */
  private _selfUtilisation(): number {
    this._advanceBuckets()
    const now = performance.now()
    const phase = (now - this._curBucketStart) / FC_SELF_TIME_WINDOW_MS
    const effectiveMs = this._prevBucket * (1 - phase) + this._curBucket
    const denom = Math.min(now - this._openedAt, FC_SELF_TIME_WINDOW_MS)
    if (denom <= 0) return 0
    const util = effectiveMs / denom
    return util > 1 ? 1 : util
  }

  private _advanceBuckets(): void {
    const now = performance.now()
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

  /** Transport reattach: reset sender-side credit on both axes to initial. */
  reset(): void {
    this._peerWindow = CREDIT_WINDOW_INITIAL_BYTES
    this._peerMsgWindow = CREDIT_MSG_WINDOW_INITIAL
    this._bdp.reset()
    this._tryWakeCreditWaiters()
  }

  /** Bump to the batch-POST initial window and advertise it. Grow-only / idempotent. */
  useBatchTransportInitial(): void {
    const prev = this._bdp.byteWindow
    this._bdp.bumpInitialByteWindow(CREDIT_WINDOW_INITIAL_BYTES_BATCH)
    if (this._bdp.byteWindow > prev) this._emit.byteWindowUpdate(this._bdp.byteWindow)
  }

  shutdown(): void {
    if (this._shutdown) return
    this._shutdown = true
    this._tryWakeCreditWaiters()
  }

  private _tryWakeCreditWaiters(): void {
    if (!this._creditReady) return
    if (!this._shutdown && (this._peerWindow <= 0 || this._peerMsgWindow <= 0)) return
    const ready = this._creditReady
    this._creditReady = null
    ready.resolve()
  }

  private _waitForCredit(): Promise<void> {
    if (this._shutdown || (this._peerWindow > 0 && this._peerMsgWindow > 0)) {
      return resolvedPromise
    }
    if (!this._creditReady) {
      let resolve!: () => void
      const promise = new Promise<void>((r) => {
        resolve = r
      })
      this._creditReady = { promise, resolve }
    }
    return this._creditReady.promise
  }
}

const resolvedPromise = Promise.resolve()
