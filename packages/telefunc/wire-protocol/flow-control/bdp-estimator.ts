export { BdpEstimator }
export type { GrowDecision, AxisDecision }

import {
  BDP_PING_MAX_INTERVAL_MS,
  BDP_PING_MIN_INTERVAL_MS,
  CREDIT_MSG_WINDOW_INITIAL,
  CREDIT_MSG_WINDOW_MAX,
  CREDIT_WINDOW_INITIAL_BYTES,
  CREDIT_WINDOW_MAX_BYTES,
} from '../constants.js'
import type { Link, SendWait } from '../link.js'

/** Per-axis verdict on a settled probe. `grow` = the sample saturated ≥ 2/3 of the current window, which is not at
 *  cap, and for the byte window, the sample leaves the sender's queue out: taken at the path's round trip where an
 *  attach measured it, the receiver's or the sender's, and where none did, the window starved the sender's wire.
 *  `sample-too-small` = the sample didn't. `wire-busy` = the sender's wire held an eighth of the window or more whenever
 *  its credit ran out.
 *  `window-grew` = the probe went out after a window grew, so what the sender says covers the smaller one. */
type AxisDecision = 'grow' | 'sample-too-small' | 'wire-busy' | 'window-grew' | 'at-cap'

/** `acknowledged` is true when a ping was in flight to settle (false for
 *  a stale ack arriving after a transport reset, and for an attach's probe). Per-axis decisions are
 *  independent — caller commits each axis's grow separately. */
type GrowDecision = { acknowledged: boolean; bytes: AxisDecision; msgs: AxisDecision }

const NOT_SETTLED: GrowDecision = { acknowledged: false, bytes: 'sample-too-small', msgs: 'sample-too-small' }

/** A channel's receive window: it grows, as gRPC's does, while what crosses in a round trip fills two thirds of it. */
class BdpEstimator {
  // Byte axis
  private _byteWindow: number = CREDIT_WINDOW_INITIAL_BYTES
  /** The largest the byte window gets: `CREDIT_WINDOW_MAX_BYTES`, or less where the sender's replay holds less. */
  private _byteWindowMax: number = CREDIT_WINDOW_MAX_BYTES
  private _bytesAtPingSent = 0
  private _bytesReceived = 0
  // Message-count axis
  private _msgWindow: number = CREDIT_MSG_WINDOW_INITIAL
  private _msgsAtPingSent = 0
  private _msgsReceived = 0
  /** The message window before its last growth, until the next ping settles or the wire is replaced. */
  private _msgWindowBeforeGrowth = 0
  // Shared probe
  /** Probes started, attach probes included: each takes the next number. */
  private _probes = 0
  /** The number of the ping in flight, or the last one. */
  private _ping = 0
  private _pingInFlight = false
  private _pingSentAt = 0
  /** Attach probes not answered yet. Each measures its wire's round trip and settles no growth, so none holds up a ping:
   *  one lost with its wire, or with an upgrade that didn't happen, costs nothing. */
  private _attachProbes: { probe: number; sentAt: number; link: Link }[] = []
  /** The wire it attached on last, whose round trip a sample counts at. */
  private _link: Link | null = null
  /** The round trip a round the window held back took, which a queue can't have lengthened, since the wire was
   *  replaced: lowered as the path's is. `Infinity` before. */
  private _boundRtt = Infinity
  private _lastPingAt = Number.NEGATIVE_INFINITY
  /** How long the ping in flight waited behind this side's own data on its wire. */
  private _pingWait = 0
  /** What the next `growBytes` takes the byte window to where the last settled probe sized it, else 0: it doubles. */
  private _grownWindow = 0
  /** The byte window grew since the last ping went out. */
  private _grewSincePing = false
  /** The ping in flight went out after the byte window grew. The sender answers on the credit it had since it answered the
   *  one before, which the smaller window granted. */
  private _pingFollowsGrowth = false
  /** Adaptive probe interval: snaps to `MIN` on grow, doubles up to `MAX` on
   *  non-grow. Slows but never freezes — a real rate change rediscovers. */
  private _probeIntervalMs = BDP_PING_MIN_INTERVAL_MS

  /** The byte window the estimator sets, which `FlowControl` grants unless it grants more. */
  get byteWindow(): number {
    return this._byteWindow
  }

  /** The largest the byte window gets (see `capByteWindow`). */
  get byteWindowMax(): number {
    return this._byteWindowMax
  }

  /** Currently advertised message-count window. */
  get msgWindow(): number {
    return this._msgWindow
  }

  /** The number of the ping in flight, which its `BDP_PING` carries. */
  get probe(): number {
    return this._ping
  }

  /** Record one received frame (pre-app-processing), received at `at` on `performance.now()`'s clock. Returns true iff
   *  a `BDP_PING`, of the number `probe` reads, should be emitted: caller fires `sendBdpPing()` synchronously, and it
   *  waits for what `sendWait` says. A single probe collects samples for both axes — `onPingAck` then derives
   *  independent byte-sample / msg-sample saturation decisions. */
  onReceive(bytes: number, at: number, sendWait: SendWait | null = null): boolean {
    let probe = false
    // Skip probe only when *both* axes have already hit their cap — otherwise one of them
    // might still want to grow.
    if (!this._pingInFlight && !(this._byteWindow >= this._byteWindowMax && this._msgWindow >= CREDIT_MSG_WINDOW_MAX)) {
      if (at - this._lastPingAt >= this._probeIntervalMs) {
        // Snapshot BEFORE crediting the triggering frame — it counts as the first
        // in-flight byte. Otherwise a window-bound producer always samples 0.
        this._sendPing()
        this._pingWait = sendWait?.waitNow() ?? 0
        this._pingFollowsGrowth = this._grewSincePing
        this._grewSincePing = false
        this._lastPingAt = at
        probe = true
      }
    }
    this._bytesReceived += bytes
    this._msgsReceived += 1
    return probe
  }

  /** Start a probe that goes out with an attach on `link`, and return its number, which the RECONCILE entry carries, or
   *  `undefined` where that wire's round trip is measured already. Its answer measures the wire's round trip, lowers it
   *  if measured, and one for a wire gone since measures nothing anyone reads. */
  probeAttach(link: Link): number | undefined {
    if (link.pathRtt < Infinity) return undefined
    this._attachProbes = this._attachProbes.filter((attach) => attach.link === link || attach.link === this._link)
    this._probes = (this._probes + 1) >>> 0
    this._attachProbes.push({ probe: this._probes, sentAt: performance.now(), link })
    return this._probes
  }

  /** Settles `probe` if it is an attach's, whose answer measures its wire's round trip. Returns whether it was. */
  settleAttachProbe(probe: number): boolean {
    const attach = this._attachProbes.find((pending) => pending.probe === probe)
    if (!attach) return false
    this._attachProbes = this._attachProbes.filter((pending) => pending !== attach)
    attach.link.measured(performance.now() - attach.sentAt)
    return true
  }

  /** It attached on `link`. */
  attach(link: Link): void {
    this._link = link
  }

  /** Settle the `BDP_PING` in flight against its `BDP_PING_ACK`, which says the least the sender's wire held whenever its
   *  credit ran out, and the path's round trip as the sender measured it, `Infinity` where it measured none.
   *  `deliveredBefore(ms)`: the bytes the wire delivered, from any channel, in the `ms` before the ack arrived.
   *  `sendWait`: how long this side's frames waited in the round. Returns
   *  per-axis grow suggestions. Caller decides whether to actually `growBytes()` / `growMsgs()` (e.g. after applying the
   *  CPU-lag gate). */
  onPingAck(
    probe: number,
    senderBacklog: number,
    senderPathRtt: number,
    deliveredBefore: (ms: number) => number,
    sendWait: SendWait | null = null,
  ): GrowDecision {
    if (this.settleAttachProbe(probe)) return NOT_SETTLED
    if (!this._pingInFlight || probe !== this._ping) return NOT_SETTLED
    this._pingInFlight = false
    const now = performance.now()
    const rtt = now - this._pingSentAt
    // An attach's probe may have waited behind other channels' frames, or on the channel's registration: a later ping
    // that took less shows the path takes no more.
    const link = this._link
    if (link !== null && link.pathRtt < Infinity) link.measured(rtt)
    const roundWait = sendWait?.longestWaitSince(this._pingSentAt) ?? 0
    const roundRtt = Math.max(1, rtt - this._pingWait)
    if (this._boundRtt < Infinity && roundRtt < this._boundRtt) this._boundRtt = roundRtt
    const received = this._bytesReceived - this._bytesAtPingSent
    const starved = senderBacklog * 8 < this._byteWindow
    // The window held the sender back, and in the eighth of the round trip before the ack the wire delivered under half
    // what the channel did on average. A queue the ack waits behind delivers at the link's rate up to it, at least the
    // channel's rate, another channel's frames too. So the round trip, less what the ping waited behind on this side's
    // wire, is the path's, however much less one measured before.
    if (starved && received * 3 >= this._byteWindow * 2 && deliveredBefore(rtt / 8) * 16 < received)
      this._boundRtt = roundRtt
    // Where an attach measured the path's round trip, here or at the sender, that tells a queue from the path, wherever
    // the queue is, as does a round the window held back, which took longer if the path's rose since. Else only the sender
    // can, where its runtime reports what its wire holds.
    let pathRtt = Math.min(link?.pathRtt ?? Infinity, senderPathRtt)
    if (this._boundRtt < Infinity) pathRtt = pathRtt < Infinity ? Math.max(pathRtt, this._boundRtt) : this._boundRtt
    const measured = pathRtt < Infinity
    const loopRtt = pathRtt + roundWait
    const atPathRtt = loopRtt < rtt ? loopRtt / rtt : 1
    const byteSample = received * atPathRtt
    const msgSample = this._msgsReceived - this._msgsAtPingSent
    const bytes = this._byteDecision(byteSample, measured, starved)
    const msgWindow = (starved && this._msgWindowBeforeGrowth) || this._msgWindow
    this._msgWindowBeforeGrowth = 0
    const msgs: AxisDecision =
      this._msgWindow >= CREDIT_MSG_WINDOW_MAX ? 'at-cap' : msgSample * 3 < msgWindow * 2 ? 'sample-too-small' : 'grow'
    this._grownWindow =
      bytes === 'grow' && roundWait > 0
        ? Math.min(2 * this._byteWindow, Math.max(this._byteWindow, Math.ceil(1.5 * byteSample)))
        : 0
    // Cadence: snap to MIN on grow (more headroom may exist), exponential
    // backoff on a verdict against growing (converged or temporarily quiet).
    if (bytes === 'grow' || msgs === 'grow') {
      this._probeIntervalMs = BDP_PING_MIN_INTERVAL_MS
    } else if (bytes !== 'window-grew') {
      this._probeIntervalMs = Math.min(BDP_PING_MAX_INTERVAL_MS, this._probeIntervalMs * 2)
    }
    return { acknowledged: true, bytes, msgs }
  }

  /** The byte axis's verdict on a sample of `byteSample` bytes: taken at the path's round trip if `measured`, and in a
   *  round the window held the sender back if `starved`. */
  private _byteDecision(byteSample: number, measured: boolean, starved: boolean): AxisDecision {
    if (this._byteWindow >= this._byteWindowMax) return 'at-cap'
    if (byteSample * 3 < this._byteWindow * 2) return 'sample-too-small'
    if (measured) return 'grow'
    if (this._pingFollowsGrowth) return 'window-grew'
    return starved ? 'grow' : 'wire-busy'
  }

  /** Commit a byte-window growth: a doubling, or less as the last settled probe says. Idempotent at the cap. */
  growBytes(): void {
    this._byteWindow = Math.min(this._byteWindowMax, this._grownWindow || this._byteWindow * 2)
    this._grownWindow = 0
    this._grewSincePing = true
  }

  /** Raises the byte window to `bytes`, within its cap; never lowers it. */
  raiseByteWindow(bytes: number): void {
    const window = Math.min(this._byteWindowMax, bytes)
    if (window <= this._byteWindow) return
    this._byteWindow = window
    this._grewSincePing = true
  }

  /** The byte window gets to `bytes` at most, `CREDIT_WINDOW_MAX_BYTES` if more, and is lowered to it. */
  capByteWindow(bytes: number): void {
    this._byteWindowMax = Math.min(CREDIT_WINDOW_MAX_BYTES, bytes)
    this._byteWindow = Math.min(this._byteWindow, this._byteWindowMax)
  }

  /** Commit a message-window doubling. Idempotent at the cap. */
  growMsgs(): void {
    this._msgWindowBeforeGrowth = this._msgWindow
    this._msgWindow = Math.min(CREDIT_MSG_WINDOW_MAX, this._msgWindow * 2)
  }

  /** Drop the ping in flight (its ack rode the prior wire). Preserves window AND
   *  cadence — both are link properties; a real rate change rediscovers. */
  reset(): void {
    this._pingInFlight = false
    this._boundRtt = Infinity
    this._msgWindowBeforeGrowth = 0
    this._bytesAtPingSent = this._bytesReceived
    this._msgsAtPingSent = this._msgsReceived
  }

  private _sendPing(): void {
    this._probes = (this._probes + 1) >>> 0
    this._ping = this._probes
    this._pingInFlight = true
    this._pingSentAt = performance.now()
    this._bytesAtPingSent = this._bytesReceived
    this._msgsAtPingSent = this._msgsReceived
  }
}
