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

/** Limits and totals go out mod 2^32. */
interface FlowControlEmit {
  byteWindowUpdate(limit: number): void
  msgWindowUpdate(limit: number): void
  sent(bytes: number, messages: number): void
  bdpPing(): void
}

/**
 * Per-channel flow control. Two orthogonal credit axes coordinated by a single
 * BDP probe per RTT, plus a self-utilisation gate that applies symmetrically
 * to growth (receiver-side) and to outbound pacing (sender-side).
 *
 *   1. **Byte credit** (`WINDOW` frame) bounds in-flight memory.
 *   2. **Message-count credit** (`MSG_WINDOW` frame) bounds
 *      in-flight per-RTT *message count* regardless of size.
 *   3. **Self-utilisation gate**: `_recordSelfTime(ms)` accumulates this channel's
 *      sync work into a two-bucket rolling sum. Above `FC_SELF_UTIL_THRESHOLD`,
 *      `onPingAck` refuses to grow either window and `decrement` yields a
 *      macrotask before returning.
 *
 * Limits are cumulative, as QUIC's MAX_DATA: the receiver advertises what it has consumed plus its window, and the
 * sender's credit is that limit minus what it has sent, so what is still in flight counts against it. Totals are exact
 * here and travel mod 2^32: a value off the wire is read as its signed 32-bit distance from the one it updates.
 *
 * Senders waiting on credit get it one at a time, oldest first: while others wait, a send that leaves credit hands it
 * to the next and waits behind them. So senders that each await their sends, once waiting, pass the limit by one
 * frame together. Senders still sending freely as the credit runs out each have a frame out past it.
 */
class FlowControl {
  private _bdp = new BdpEstimator()
  // Sender side: what this side has sent, and the peer's limits.
  private _sentBytes = 0
  private _sentMessages = 0
  private _limitBytes: number = CREDIT_WINDOW_INITIAL_BYTES
  private _limitMessages: number = CREDIT_MSG_WINDOW_INITIAL
  /** `_sentBytes` after the last frame sent while the byte limit was ahead of it. */
  private _sentWithCredit = 0
  // Receiver side: what arrived, what was consumed, and what had been consumed when each limit last went out.
  private _receivedBytes = 0
  private _receivedMessages = 0
  private _consumedBytes = 0
  private _consumedMessages = 0
  private _advertisedBytes = 0
  private _advertisedMessages = 0
  /** Senders waiting on credit, oldest first. */
  private _waiters: Array<() => void> = []
  /** A waiter was handed the credit and no send was counted since. */
  private _released = false
  private _releases = 0
  private _shutdown = false

  // Self-utilisation rolling-sum state. Two buckets, phase-weighted blend.
  private _prevBucket = 0
  private _curBucket = 0
  private _curBucketStart = performance.now()
  private _openedAt = performance.now()

  constructor(private readonly _emit: FlowControlEmit) {
    macrotaskYield.assertSupported()
  }

  get byteWindow(): number {
    return this._bdp.byteWindow
  }

  get msgWindow(): number {
    return this._bdp.msgWindow
  }

  /** Sender-side: no byte credit is left. */
  get isPastByteCredit(): boolean {
    return this._limitBytes - this._sentBytes <= 0
  }

  /** Sender-side: bytes of the frames sent once past the byte limit that are still past it. The frame that crossed it,
   *  sent with credit, is credit flow control's one-frame overshoot and isn't counted. */
  get bytesSentPastCredit(): number {
    return Math.max(0, this._sentBytes - Math.max(this._limitBytes, this._sentWithCredit))
  }

  /** Sender-side: count one frame of `bytes` against credit. Returns `void` when
   *  both credit axes have headroom, no other sender waits, AND our loop utilisation is below the gate.
   *  Otherwise a Promise that resolves at the sender's turn with credit (credit-gated) or one
   *  macrotask later (util-gated — single yield, no re-check). */
  decrement(bytes: number): void | Promise<void> {
    this.countSent(bytes)
    this._released = false
    if (this._isOutOfCredit()) return this._waitForCredit()
    if (this._waiters.length > 0) {
      this._releaseOne()
      return this._waitForCredit()
    }
    if (this._selfUtilisation() > FC_SELF_UTIL_THRESHOLD) {
      return macrotaskYield.yield()
    }
  }

  /** Sender-side: count a frame that went out without a credit gate, one buffered while no peer was attached. */
  countSent(bytes: number): void {
    this._countSentBytes(bytes)
    this._sentMessages += 1
  }

  /** A limit that doesn't raise the current one is stale, and ignored, as in QUIC. */
  onPeerByteWindow(limit: number): void {
    const raise = (limit - this._limitBytes) | 0
    if (raise <= 0) return
    this._limitBytes += raise
    this._tryWakeCreditWaiters()
  }

  onPeerMessageWindow(limit: number): void {
    const raise = (limit - this._limitMessages) | 0
    if (raise <= 0) return
    this._limitMessages += raise
    this._tryWakeCreditWaiters()
  }

  /** Receiver-side: the peer's totals through the frame's seq. What didn't arrive by then was lost beyond its replay
   *  buffer, and counts as consumed, as the final size of a reset QUIC stream does: the peer counted it as sent. */
  onPeerSent(bytes: number, messages: number): void {
    const lostBytes = (bytes - this._receivedBytes) | 0
    const lostMessages = (messages - this._receivedMessages) | 0
    this._receivedBytes += lostBytes
    this._receivedMessages += lostMessages
    this._consume(lostBytes, lostMessages)
  }

  /** Receiver-side: account one received frame off the wire. Emits a
   *  `BDP_PING` via the channel's emit callback iff the estimator opens a probe. */
  onReceived(bytes: number): void {
    this._receivedBytes += bytes
    this._receivedMessages += 1
    if (this._bdp.onReceive(bytes)) this._emit.bdpPing()
  }

  /** Receiver-side: account post-callback consumption of one frame. Emits
   *  refresh `WINDOW` / `MSG_WINDOW` frames once a quarter of either window
   *  has been consumed since that limit last went out. */
  onConsumed(bytes: number): void {
    this._consume(bytes, 1)
  }

  /** Settle `BDP_PING_ACK`. Each axis grows iff its own sample saturated ≥ 2/3
   *  of its current window AND our own self-utilisation is below threshold.
   *  On growth, the new limit goes out to the peer immediately. */
  onPingAck(): void {
    const suggest = this._bdp.onPingAck()
    if (!suggest.acknowledged) return
    const wantBytes = suggest.bytes === 'grow'
    const wantMsgs = suggest.msgs === 'grow'
    if (!wantBytes && !wantMsgs) return
    if (this._selfUtilisation() > FC_SELF_UTIL_THRESHOLD) return
    if (wantBytes) {
      this._bdp.growBytes()
      this._advertiseBytes()
    }
    if (wantMsgs) {
      this._bdp.growMsgs()
      this._advertiseMessages()
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

  /** Attach on another wire than the last. The probe in flight rode the prior wire, and the limits and totals go out
   *  again, which repairs what the prior wire lost of them. Credit carries over: it is cumulative. */
  reattach(): void {
    this._bdp.reset()
    this._advertiseBytes()
    this._advertiseMessages()
    this._emit.sent(this._sentBytes >>> 0, this._sentMessages >>> 0)
  }

  /** Receiver-side: a byte window of at least `bytes`, advertised with the next limit. Grow-only. */
  widenByteWindow(bytes: number): void {
    this._bdp.bumpInitialByteWindow(bytes)
  }

  // A frame counted in bytes only takes no message credit and starts no BDP probe: a broadcast's publish, which
  // nothing waits on.

  countSentBytes(bytes: number): void {
    this._countSentBytes(bytes)
  }

  onReceivedBytes(bytes: number): void {
    this._receivedBytes += bytes
  }

  onConsumedBytes(bytes: number): void {
    this._consume(bytes, 0)
  }

  /** Bump to the batch-POST initial window and advertise it. Grow-only / idempotent. */
  useBatchTransportInitial(): void {
    const prev = this._bdp.byteWindow
    this._bdp.bumpInitialByteWindow(CREDIT_WINDOW_INITIAL_BYTES_BATCH)
    if (this._bdp.byteWindow > prev) this._advertiseBytes()
  }

  shutdown(): void {
    if (this._shutdown) return
    this._shutdown = true
    this._tryWakeCreditWaiters()
  }

  private _consume(bytes: number, messages: number): void {
    this._consumedBytes += bytes
    this._consumedMessages += messages
    if (this._consumedBytes - this._advertisedBytes >= this._bdp.byteWindow >> 2) this._advertiseBytes()
    if (this._consumedMessages - this._advertisedMessages >= this._bdp.msgWindow >> 2) this._advertiseMessages()
  }

  private _advertiseBytes(): void {
    this._advertisedBytes = this._consumedBytes
    this._emit.byteWindowUpdate((this._consumedBytes + this._bdp.byteWindow) >>> 0)
  }

  private _advertiseMessages(): void {
    this._advertisedMessages = this._consumedMessages
    this._emit.msgWindowUpdate((this._consumedMessages + this._bdp.msgWindow) >>> 0)
  }

  private _countSentBytes(bytes: number): void {
    const hadCredit = !this.isPastByteCredit
    this._sentBytes += bytes
    if (hadCredit) this._sentWithCredit = this._sentBytes
  }

  private _isOutOfCredit(): boolean {
    return this._limitBytes - this._sentBytes <= 0 || this._limitMessages - this._sentMessages <= 0
  }

  private _tryWakeCreditWaiters(): void {
    if (this._shutdown) {
      for (const resolve of this._waiters.splice(0)) resolve()
      return
    }
    if (this._released || this._isOutOfCredit()) return
    this._releaseOne()
  }

  /** The oldest waiter's turn. One that hasn't sent by the next macrotask, as one that is done sending, passes it on. */
  private _releaseOne(): void {
    const resolve = this._waiters.shift()
    if (!resolve) return
    this._released = true
    const release = ++this._releases
    resolve()
    if (this._waiters.length === 0) return
    void macrotaskYield.yield().then(() => {
      if (!this._released || this._releases !== release) return
      this._released = false
      this._tryWakeCreditWaiters()
    })
  }

  private _waitForCredit(): Promise<void> {
    if (this._shutdown) return resolvedPromise
    return new Promise<void>((resolve) => this._waiters.push(resolve))
  }
}

const resolvedPromise = Promise.resolve()
