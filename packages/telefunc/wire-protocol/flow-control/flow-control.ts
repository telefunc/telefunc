export { FlowControl, replayWindow }
export type { FlowControlEmit }

import { BdpEstimator } from './bdp-estimator.js'
import { macrotaskYield } from './macrotask-yield.js'
import {
  CREDIT_MSG_WINDOW_INITIAL,
  CREDIT_WINDOW_INITIAL_BYTES,
  CREDIT_WINDOW_INITIAL_BYTES_BATCH,
  CREDIT_WINDOW_MAX_BYTES,
  FC_SELF_TIME_WINDOW_MS,
  FC_SELF_UTIL_THRESHOLD,
} from '../constants.js'
import { Fifo } from '../../utils/Fifo.js'

/** Limits go out mod 2^32. A byte limit goes out with the last seq this side has of the channel, which acknowledges what
 *  arrived (see `constants.ts`). */
interface FlowControlEmit {
  /** `urgent`: past half its window has been consumed since the last urgent limit, so the peer may be close to
   *  blocking: a transport that batches should send it at once. */
  byteWindowUpdate(limit: number, urgent: boolean): void
  msgWindowUpdate(limit: number, urgent: boolean): void
  bdpPing(probe: number): void
}

/** The largest window a receiver grants a sender whose replay buffer's lanes hold `text` and `binary` bytes: half the
 *  smaller, so what credit lets be in flight, and a message up to as large sent as the credit ran out, fit it. At least
 *  a byte, so credit lets a message through each round trip. */
function replayWindow(text: number, binary: number): number {
  return Math.max(1, Math.floor(Math.min(text, binary) / 2))
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
 *      macrotask before returning, as it does once per `yieldBytes` sent.
 *
 * Limits are cumulative, as QUIC's MAX_DATA: the receiver advertises what it has consumed plus its window, and the
 * sender's credit is that limit minus what it has sent, so what is still in flight counts against it. Totals are exact
 * here, and limits travel mod 2^32: a limit off the wire is read as its signed 32-bit distance from the one it updates.
 *
 * Senders waiting on credit get it one at a time, oldest first: while others wait, a send that leaves credit hands it
 * to the next and waits behind them. So senders that each await their sends, once waiting, pass the limit by one
 * frame together. Senders still sending freely as the credit runs out each have a frame out past it.
 *
 * Until `fitReplays` says the replay buffers that bound its windows, it advertises no byte limit, so its peer's
 * assumption of the initial one stands: its peer's replay couldn't keep what a larger window let be in flight.
 */
class FlowControl {
  private _bdp = new BdpEstimator()
  /** The connection's wire at the last `attach`. */
  private _wire: number | null = null
  // Sender side: what this side has sent, and the peer's limits.
  private _sentBytes = 0
  private _sentMessages = 0
  private _limitBytes: number = CREDIT_WINDOW_INITIAL_BYTES
  private _limitMessages: number = CREDIT_MSG_WINDOW_INITIAL
  /** The largest window the peer grants: what this side's replay holds (see `fitReplays`). */
  private _peerByteWindowMax: number = CREDIT_WINDOW_MAX_BYTES
  /** A `WINDOW` raised the byte limit past the one this side assumed the peer starts with. */
  private _limitRaised = false
  /** `fitReplays` said the replay buffers that bound the windows. */
  private _fitted = false
  /** `_sentBytes` after the last frame sent while the byte limit was ahead of it. */
  private _sentWithCredit = 0
  // Receiver side: what arrived, what was consumed, and what had been consumed when each limit last went out.
  private _consumedBytes = 0
  private _consumedMessages = 0
  private _advertisedBytes = 0
  private _advertisedMessages = 0
  /** What had been consumed when the last urgent limit was emitted. */
  private _urgentBytes = 0
  private _urgentMessages = 0
  /** Bytes of the frames credit doesn't count that arrived since the byte limit last went out. */
  private _uncountedBytes = 0
  /** A frame arrived since the byte limit last went out, which acknowledges what arrived. */
  private _arrived = false
  /** The least byte window granted, past the estimator's (see `widenByteWindow`). */
  private _byteWindowFloor = 0
  /** Senders waiting on credit, oldest first. */
  private readonly _waiters = new Fifo<() => void>()
  /** A waiter was handed the credit and no send was counted since. */
  private _released = false
  private _releases = 0
  /** Bytes sent since this side's sender last yielded or waited. */
  private _unyieldedBytes = 0
  private _shutdown = false
  /** Since this side last answered a `BDP_PING`: whether its credit ran out, as more came or as the ping did, while its
   *  wire held nothing. */
  private _starved = false

  // Self-utilisation rolling-sum state. Two buckets, phase-weighted blend.
  private _prevBucket = 0
  private _curBucket = 0
  private _curBucketStart = performance.now()
  private _openedAt = performance.now()

  /** `backlog`: bytes waiting to go out on the channel's wire, `undefined` where the runtime can't tell.
   *  `yieldBytes`: the sender yields a macrotask once it sent that many bytes since it last yielded or waited. A page
   *  sets it: Chromium takes what a page queued on a WebSocket or an upload stream only between tasks, and woken by
   *  credit, a page would send all of it in one. */
  constructor(
    private readonly _emit: FlowControlEmit,
    private readonly _backlog: () => number | undefined,
    private readonly _yieldBytes = Infinity,
  ) {
    macrotaskYield.assertSupported()
  }

  /** Receiver-side: the byte window granted. */
  get byteWindow(): number {
    return Math.max(this._bdp.byteWindow, Math.min(this._byteWindowFloor, this._bdp.byteWindowMax))
  }

  get msgWindow(): number {
    return this._bdp.msgWindow
  }

  /** Sender-side: the largest window the peer grants. */
  get peerByteWindowMax(): number {
    return this._peerByteWindowMax
  }

  /** The round trip of the path the probe of the last `attach` measured, `Infinity` where none did: what this side's
   *  answer to a `BDP_PING` says, for a receiver with no attach of its own to probe. */
  pathRtt(): number {
    return this._wire === null ? Infinity : this._bdp.pathRtt(this._wire)
  }

  /** Keeps what credit lets be in flight within the replay buffers (see `replayWindow`): this side grants its peer
   *  `window` at most, and its peer grants it `peerWindow` at most. The limit this side assumes its peer starts with,
   *  while no `WINDOW` raised it, doesn't pass that. */
  fitReplays(window: number, peerWindow: number): void {
    this._fitted = true
    this._bdp.capByteWindow(window)
    this._peerByteWindowMax = Math.min(CREDIT_WINDOW_MAX_BYTES, peerWindow)
    if (!this._limitRaised) this._limitBytes = Math.min(CREDIT_WINDOW_INITIAL_BYTES, this._peerByteWindowMax)
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
   *  both credit axes have headroom, no other sender waits, AND our loop utilisation is below the gate, nor did the
   *  sender send `yieldBytes` since it last yielded or waited. Otherwise a Promise that resolves at the
   *  sender's turn with credit (credit-gated) or one macrotask later (util- or byte-gated — single yield, no re-check). */
  decrement(bytes: number): void | Promise<void> {
    this.countSent(bytes)
    this._released = false
    if (this._isOutOfCredit()) return this._waitForCredit()
    if (this._waiters.length > 0) {
      this._releaseOne()
      return this._waitForCredit()
    }
    this._unyieldedBytes += bytes
    if (this._selfUtilisation() > FC_SELF_UTIL_THRESHOLD || this._unyieldedBytes >= this._yieldBytes) {
      this._unyieldedBytes = 0
      return macrotaskYield.yield()
    }
  }

  /** Sender-side: count a frame that went out without a credit gate, one buffered while no peer was attached. */
  countSent(bytes: number): void {
    this.countSentBytes(bytes)
    this._sentMessages += 1
  }

  /** A limit that doesn't raise the current one is stale, and ignored, as in QUIC. */
  onPeerByteWindow(limit: number): void {
    const raise = (limit - this._limitBytes) | 0
    if (raise <= 0) return
    this._noteStarved()
    this._limitBytes += raise
    this._limitRaised = true
    this._tryWakeCreditWaiters()
  }

  onPeerMessageWindow(limit: number): void {
    const raise = (limit - this._limitMessages) | 0
    if (raise <= 0) return
    this._noteStarved()
    this._limitMessages += raise
    this._tryWakeCreditWaiters()
  }

  /** Sender-side: answer a `BDP_PING`. Returns whether, since the last one, the window starved the wire: its credit ran
   *  out, as more came or as the ping did, while the wire held nothing, or what the runtime can't tell. A wire that
   *  held a backlog each time was busy, and a larger window would only have queued more on it. */
  onPing(): boolean {
    this._noteStarved()
    const starved = this._starved
    this._starved = false
    return starved
  }

  /** Receiver-side: account one received frame off the wire. Emits a
   *  `BDP_PING` via the channel's emit callback iff the estimator opens a probe. */
  onReceived(bytes: number): void {
    this._arrived = true
    if (this._bdp.onReceive(bytes)) this._emit.bdpPing(this._bdp.probe)
  }

  /** Receiver-side: the number of a probe for the RECONCILE entry of an attach on `wire`, which the peer answers before
   *  any of the channel's frames (see `BdpEstimator`), or `undefined` where that wire's round trip is measured already. */
  probeAttach(wire: number): number | undefined {
    return this._bdp.probeAttach(wire)
  }

  /** Receiver-side: account post-callback consumption of one frame. Emits
   *  refresh `WINDOW` / `MSG_WINDOW` frames once a quarter of the estimator's
   *  byte window, or of the message window, has been consumed since that limit last went out. */
  onConsumed(bytes: number): void {
    this._consume(bytes, 1)
  }

  /** Receiver-side: a frame credit doesn't count, an ack request or its answer, arrived. A `WINDOW` acknowledges it, so
   *  one goes out once a quarter window of these arrived since the last, and the sender's replay lets them go. */
  onReceivedUncounted(bytes: number): void {
    this._arrived = true
    this._uncountedBytes += bytes
    if (this._uncountedBytes >= this._bdp.byteWindow >> 2) this._advertiseBytes()
  }

  /** Receiver-side, at each heartbeat: a `WINDOW` for what arrived since the last, so the sender's replay lets it go
   *  while the channel is quiet. */
  acknowledge(): void {
    if (this._arrived) this._advertiseBytes()
  }

  /** Settle `BDP_PING_ACK`, which says whether the window starved the peer's wire, and the path's round trip as the peer
   *  measured it. Each axis grows iff its own sample saturated ≥ 2/3 of its current window, the byte sample leaving out
   *  the peer's queue (see `BdpEstimator`), AND our own self-utilisation is below threshold.
   *  On growth, the new limit goes out to the peer immediately. */
  onPingAck(probe: number, starved: boolean, peerPathRtt: number): void {
    const suggest = this._bdp.onPingAck(probe, starved, peerPathRtt)
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

  /** Attach on the connection's `wire`. The wire of the last attach lost nothing to repair, and still answers the probe
   *  in flight. */
  attach(wire: number): void {
    if (wire !== this._wire) this.reattach()
    this._wire = wire
  }

  /** Attach on another wire than the last. The probe in flight rode the prior wire, and the limits go out again,
   *  which repairs what the prior wire lost of them. Credit carries over: it is cumulative. */
  reattach(): void {
    this._bdp.reset()
    this._advertiseBytes()
    this._advertiseMessages()
  }

  /** Receiver-side: a byte window of at least `bytes`, advertised with the next limit, while limits still go out once a
   *  quarter of the estimator's window was consumed: a broadcast's page, whose window sets how far behind the server
   *  may have it, acknowledges what it read as often as a stream's does. */
  widenByteWindow(bytes: number): void {
    this._byteWindowFloor = bytes
  }

  // A frame counted in bytes only takes no message credit and starts no BDP probe: a broadcast's publish, which
  // nothing waits on.

  countSentBytes(bytes: number): void {
    const hadCredit = !this.isPastByteCredit
    this._sentBytes += bytes
    if (hadCredit) this._sentWithCredit = this._sentBytes
  }

  onArrived(): void {
    this._arrived = true
  }

  onConsumedBytes(bytes: number): void {
    this._consume(bytes, 0)
  }

  /** Bump to the batch-POST initial window and advertise it. Grow-only / idempotent. */
  useBatchTransportInitial(): void {
    const prev = this.byteWindow
    this._bdp.bumpInitialByteWindow(CREDIT_WINDOW_INITIAL_BYTES_BATCH)
    if (this.byteWindow > prev) this._advertiseBytes()
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
    if (!this._fitted) return
    this._advertisedBytes = this._consumedBytes
    this._uncountedBytes = 0
    this._arrived = false
    const urgent = (this._consumedBytes - this._urgentBytes) * 2 > this.byteWindow
    if (urgent) this._urgentBytes = this._consumedBytes
    this._emit.byteWindowUpdate((this._consumedBytes + this.byteWindow) >>> 0, urgent)
  }

  private _advertiseMessages(): void {
    this._advertisedMessages = this._consumedMessages
    const urgent = (this._consumedMessages - this._urgentMessages) * 2 > this._bdp.msgWindow
    if (urgent) this._urgentMessages = this._consumedMessages
    this._emit.msgWindowUpdate((this._consumedMessages + this._bdp.msgWindow) >>> 0, urgent)
  }

  private _noteStarved(): void {
    if (this._starved || !this._isOutOfCredit()) return
    const backlog = this._backlog()
    this._starved = backlog === undefined || backlog === 0
  }

  private _isOutOfCredit(): boolean {
    return this._limitBytes - this._sentBytes <= 0 || this._limitMessages - this._sentMessages <= 0
  }

  private _tryWakeCreditWaiters(): void {
    if (this._shutdown) {
      for (let resolve = this._waiters.shift(); resolve; resolve = this._waiters.shift()) resolve()
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
    this._unyieldedBytes = 0
    return new Promise<void>((resolve) => this._waiters.push(resolve))
  }
}

const resolvedPromise = Promise.resolve()
