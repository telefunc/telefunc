export { FlowControl, replayWindow }
export type { AttachedWire, ByteWindowKind, FlowControlEmit }

import { BdpEstimator } from './bdp-estimator.js'
import type { Link, SendWait } from '../link.js'
import { macrotaskYield } from './macrotask-yield.js'
import {
  CREDIT_MSG_WINDOW_INITIAL,
  CREDIT_WINDOW_INITIAL_BYTES,
  CREDIT_WINDOW_INITIAL_BYTES_BATCH,
  CREDIT_WINDOW_MAX_BYTES,
  FC_SELF_TIME_WINDOW_MS,
  FC_SELF_UTIL_THRESHOLD,
} from '../constants.js'
import { MESSAGE_FRAME_OVERHEAD_MAX_BYTES, type BdpPingAck } from '../shared-ws.js'
import { Fifo } from '../../utils/Fifo.js'
import { unrefTimer } from '../../utils/unrefTimer.js'

/** A batching wire sends `urgent-flow-control` at once; an `acknowledgement` may ride the next message it sends. */
type ByteWindowKind = 'urgent-flow-control' | 'acknowledgement' | 'flow-control'

/** Limits go out mod 2^32; a byte limit carries the last seq this side has, which acknowledges what arrived. */
interface FlowControlEmit {
  byteWindowUpdate(limit: number, kind: ByteWindowKind): void
  msgWindowUpdate(limit: number, urgent: boolean): void
  bdpPing(probe: number): void
}

/** The page's uplink, as a channel's credit reads it. */
type AttachedWire = SendWait & {
  readonly link: Link
  readonly refreshes: { wait(): number }
  granted(bytes: number): void
  creditArrived(bytes: number): void
  noteWait(wait: number): void
}

/** The largest window a sender whose replay lanes hold `text` and `binary` bytes gets: what it holds in flight fits. */
function replayWindow(text: number, binary: number): number {
  return Math.max(1, Math.floor(Math.min(text, binary) / 2) - MESSAGE_FRAME_OVERHEAD_MAX_BYTES)
}

/** A channel's byte and message credit both ways, as cumulative limits, as QUIC's MAX_DATA. */
class FlowControl {
  private _bdp = new BdpEstimator()
  /** The page's uplink of the last `attach`. */
  private _uplink: AttachedWire | null = null
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
  /** The byte limit this side granted, and the bytes that arrived against it. */
  private _grantedBytes: number = CREDIT_WINDOW_INITIAL_BYTES
  private _receivedBytes = 0
  /** What had been consumed when the last urgent limit was emitted. */
  private _urgentBytes = 0
  private _urgentMessages = 0
  /** Waits for the wire's share to refresh the limits a quarter window of consumption made due (see `Uplink.refreshes`). */
  private _refreshTimer: ReturnType<typeof setTimeout> | null = null
  /** Bytes of the frames credit doesn't count that arrived since the byte limit last went out. */
  private _uncountedBytes = 0
  /** A frame arrived since the byte limit last went out, which acknowledges what arrived. */
  private _arrived = false
  /** It grants the largest byte window its peer's replay allows (see `widenByteWindow`). */
  private _widened = false
  /** It attached on a wire. */
  private _wired = false
  /** Senders waiting on credit, oldest first. */
  private readonly _waiters = new Fifo<() => void>()
  /** A waiter was handed the credit and no send was counted since. */
  private _released = false
  private _releases = 0
  /** Bytes sent since this side's sender last yielded or waited. */
  private _unyieldedBytes = 0
  private _shutdown = false
  /** Since it last answered a `BDP_PING`, the least its wire held as credit ran out; 0: unknown, `Infinity`: none. */
  private _leastBacklog = Infinity

  // Self-utilisation rolling-sum state. Two buckets, phase-weighted blend.
  private _prevBucket = 0
  private _curBucket = 0
  private _curBucketStart = performance.now()
  private _openedAt = performance.now()

  constructor(
    private readonly _emit: FlowControlEmit,
    /** Bytes waiting to go out on the channel's wire, `undefined` where the runtime can't tell. */
    private readonly _backlog: () => number | undefined,
    /** A page's: Chromium takes what a page queued on a socket only between tasks, so its sender yields past this. */
    private readonly _yieldBytes = Infinity,
    /** How long frames that go at once wait behind the wire's own data where no attach tells it, `null` for unknown. */
    private readonly _sendWait: () => SendWait | null = () => null,
  ) {
    macrotaskYield.assertSupported()
  }

  /** Receiver-side: the byte window granted. */
  get byteWindow(): number {
    return this._widened ? this._bdp.byteWindowMax : this._bdp.byteWindow
  }

  get msgWindow(): number {
    return this._bdp.msgWindow
  }

  /** Sender-side: the largest window the peer grants. */
  get peerByteWindowMax(): number {
    return this._peerByteWindowMax
  }

  /** The last `attach`'s wire's path round trip, `Infinity` where unmeasured: what an answer to a `BDP_PING` says. */
  pathRtt(): number {
    return this._uplink?.link.pathRtt ?? Infinity
  }

  /** Keeps what credit lets be in flight within the replay buffers (see `replayWindow`). */
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

  /** Sender-side: bytes sent past the byte limit and still past it, but the one frame that crossed it with credit. */
  get bytesSentPastCredit(): number {
    return Math.max(0, this._sentBytes - Math.max(this._limitBytes, this._sentWithCredit))
  }

  /** Sender-side: counts a frame of `bytes` against credit; a Promise where the sender waits for credit or yields. */
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
    this._noteBacklog()
    this._limitBytes += raise
    this._limitRaised = true
    this._tryWakeCreditWaiters()
  }

  onPeerMessageWindow(limit: number): void {
    const raise = (limit - this._limitMessages) | 0
    if (raise <= 0) return
    this._noteBacklog()
    this._limitMessages += raise
    this._tryWakeCreditWaiters()
  }

  /** Sender-side: answers a `BDP_PING` with the least its wire held whenever its credit ran out since the last. */
  onPing(): number {
    this._noteBacklog()
    const backlog = this._leastBacklog
    this._leastBacklog = Infinity
    return backlog
  }

  /** Receiver-side: account one received frame off the wire, emitting a `BDP_PING` iff the estimator opens a probe. */
  onReceived(bytes: number): void {
    this._arrived = true
    this._receivedBytes += bytes
    this._uplink?.creditArrived(bytes)
    if (this._bdp.onReceive(bytes, this._wireWait())) this._emit.bdpPing(this._bdp.probe)
  }

  /** How long the frames that go at once wait behind the wire's own data: the attached uplink's, else `sendWait`'s. */
  private _wireWait(): SendWait | null {
    return this._uplink ?? this._sendWait()
  }

  /** Receiver-side: a probe number for an attach on `link`'s RECONCILE entry; `undefined` where `link` is measured. */
  probeAttach(link: Link): number | undefined {
    return this._bdp.probeAttach(link)
  }

  /** Receiver-side: the RECONCILED answered the probe of an attach's RECONCILE entry (see `probeAttach`). */
  onProbeAnswered(probe: number): void {
    this._bdp.settleAttachProbe(probe)
  }

  /** Receiver-side: account post-callback consumption of one frame. */
  onConsumed(bytes: number): void {
    this._consume(bytes, 1)
  }

  /** Receiver-side: a frame credit doesn't count, an ack request or its answer, arrived; a `WINDOW` acknowledges it. */
  onReceivedUncounted(bytes: number): void {
    this._arrived = true
    this._uncountedBytes += bytes
    if (this._uncountedBytes >= this._bdp.byteWindow >> 2) this._advertiseBytes('acknowledgement')
  }

  /** Receiver-side, at each heartbeat: a `WINDOW` for what arrived since the last, which the sender's replay frees. */
  acknowledge(): void {
    if (this._arrived) this._advertiseBytes('acknowledgement')
  }

  /** Receiver-side: this side answered the newest frame that arrived, so no `WINDOW` is owed for what arrived. */
  answered(): void {
    this._uncountedBytes = 0
    this._arrived = false
  }

  /** Settles a `BDP_PING_ACK`; `deliveredBefore(ms)`: what the wire delivered in the `ms` before the ack arrived. */
  onPingAck(ack: BdpPingAck, deliveredBefore: (ms: number) => number): void {
    const suggest = this._bdp.onPingAck(ack, deliveredBefore, this._wireWait())
    if (!suggest.acknowledged) return
    const wantBytes = suggest.bytes === 'grow'
    const wantMsgs = suggest.msgs === 'grow'
    if (!wantBytes && !wantMsgs) return
    if (this._selfUtilisation() > FC_SELF_UTIL_THRESHOLD) return
    if (wantBytes) {
      this._bdp.growBytes()
      this._advertiseBytes('flow-control')
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

  /** Attach on `wire`. The wire of the last attach lost nothing to repair, and still answers the probe in flight. */
  attach(wire: AttachedWire): void {
    if (wire !== this._uplink) {
      this._uplink = wire
      wire.granted(this._grantedBytes - this._receivedBytes)
      this.reattach()
    }
    this._bdp.attach(wire.link)
  }

  /** Attach on another wire than the last, or the first: the limits go out again, which repairs what the prior lost. */
  reattach(): void {
    this._bdp.reset()
    if (!this._wired) {
      this._wired = true
      if (this._arrived || this._consumedBytes + this.byteWindow > this._grantedBytes) {
        this._advertiseBytes('flow-control')
      }
      return
    }
    this._advertiseBytes('flow-control')
    this._advertiseMessages()
  }

  /** Receiver-side: the largest byte window its peer's replay allows, advertised with the next limit. */
  widenByteWindow(): void {
    this._widened = true
  }

  /** Sender-side: counts a frame in bytes only, a broadcast's publish, which nothing waits on. */
  countSentBytes(bytes: number): void {
    const hadCredit = !this.isPastByteCredit
    this._sentBytes += bytes
    if (hadCredit) this._sentWithCredit = this._sentBytes
  }

  /** Receiver-side: a frame counted in bytes only arrived, which starts no BDP probe. */
  onArrived(): void {
    this._arrived = true
  }

  /** Receiver-side: such a frame of `bytes` was consumed, which takes no message credit. */
  onConsumedBytes(bytes: number): void {
    this._consume(bytes, 0)
  }

  /** Bump to the batch-POST initial window and advertise it. Grow-only / idempotent. */
  useBatchTransportInitial(): void {
    const prev = this.byteWindow
    this._bdp.raiseByteWindow(CREDIT_WINDOW_INITIAL_BYTES_BATCH)
    if (this.byteWindow > prev) this._advertiseBytes('flow-control')
  }

  shutdown(): void {
    if (this._shutdown) return
    this._shutdown = true
    if (this._refreshTimer !== null) clearTimeout(this._refreshTimer)
    this._tryWakeCreditWaiters()
  }

  private _consume(bytes: number, messages: number): void {
    this._consumedBytes += bytes
    this._consumedMessages += messages
    this._refresh()
  }

  private _refresh(): void {
    const bytes = this._consumedBytes - this._advertisedBytes >= this._bdp.byteWindow >> 2
    const messages = this._consumedMessages - this._advertisedMessages >= this._bdp.msgWindow >> 2
    if (bytes || messages) this._advertiseDue(bytes, messages)
  }

  /** Sends the limits due, once the wire's share allows. */
  private _advertiseDue(bytes: boolean, messages: boolean): void {
    const wait = this._uplink?.refreshes.wait() ?? 0
    if (wait > 0) {
      this._uplink!.noteWait(wait)
      this._refreshTimer ??= unrefTimer(
        setTimeout(() => {
          this._refreshTimer = null
          this._refresh()
        }, wait),
      )
      return
    }
    if (bytes) this._advertiseBytes('flow-control')
    if (messages) this._advertiseMessages()
  }

  private _advertiseBytes(kind: 'acknowledgement' | 'flow-control'): void {
    if (!this._fitted) return
    this._advertisedBytes = this._consumedBytes
    const limit = this._consumedBytes + this.byteWindow
    if (limit > this._grantedBytes) {
      this._uplink?.granted(limit - this._grantedBytes)
      this._grantedBytes = limit
    }
    this._uncountedBytes = 0
    this._arrived = false
    let sent: ByteWindowKind = kind
    if ((this._consumedBytes - this._urgentBytes) * 2 > this.byteWindow) {
      this._urgentBytes = this._consumedBytes
      sent = 'urgent-flow-control'
    }
    this._emit.byteWindowUpdate(limit >>> 0, sent)
  }

  private _advertiseMessages(): void {
    this._advertisedMessages = this._consumedMessages
    const urgent = (this._consumedMessages - this._urgentMessages) * 2 > this._bdp.msgWindow
    if (urgent) this._urgentMessages = this._consumedMessages
    this._emit.msgWindowUpdate((this._consumedMessages + this._bdp.msgWindow) >>> 0, urgent)
  }

  private _noteBacklog(): void {
    if (this._leastBacklog === 0 || !this._isOutOfCredit()) return
    this._leastBacklog = Math.min(this._leastBacklog, this._backlog() ?? 0)
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
