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
import type { Link, SendWait } from './Link.js'
import type { BdpPingAck } from '../wire/connectionFrame.js'

/** Per-axis verdict on a settled probe; `grow` needs the sample to saturate ≥ 2/3 of a window not at cap. */
type AxisDecision = 'grow' | 'sample-too-small' | 'wire-busy' | 'window-grew' | 'at-cap'

/** `acknowledged`: a ping was in flight to settle, not a stale ack or an attach's probe's; each axis grows apart. */
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
  /** Attach probes not answered yet: each settles no growth, so none holds up a ping. */
  private _attachProbes: { probe: number; sentAt: number; link: Link }[] = []
  /** The wire it attached on last, whose round trip a sample counts at. */
  private _link: Link | null = null
  /** A round trip the window held back, which no queue lengthened, since the wire was replaced; `Infinity` before. */
  private _boundRtt = Infinity
  private _lastPingAt = Number.NEGATIVE_INFINITY
  /** How long the ping in flight waited behind this side's own data on its wire. */
  private _pingWait = 0
  /** What the next `growBytes` takes the byte window to where the last settled probe sized it, else 0: it doubles. */
  private _grownWindow = 0
  /** The byte window grew since the last ping went out. */
  private _grewSincePing = false
  /** The ping in flight went out after the byte window grew: its answer covers credit the smaller window granted. */
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

  /** Records one received frame; true iff the caller should send a `BDP_PING` of the number `probe` reads, at once. */
  onReceive(bytes: number, sendWait: SendWait | null = null): boolean {
    const now = performance.now()
    const probe = this._pingDue(now)
    // Snapshot BEFORE crediting the triggering frame — it counts as the first
    // in-flight byte. Otherwise a window-bound producer always samples 0.
    if (probe) this._sendPing(now, sendWait)
    this._bytesReceived += bytes
    this._msgsReceived += 1
    return probe
  }

  /** Starts a probe that goes out with an attach on `link`; `undefined` where that wire's round trip is measured. */
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

  /** Settles the ping in flight; `deliveredBefore(ms)`: what the wire delivered from any channel in the `ms` before. */
  onPingAck(
    { probe, backlog: senderBacklog, pathRtt: senderPathRtt }: BdpPingAck,
    deliveredBefore: (ms: number) => number,
    sendWait: SendWait | null = null,
  ): GrowDecision {
    if (this.settleAttachProbe(probe)) return NOT_SETTLED
    if (!this._pingInFlight || probe !== this._ping) return NOT_SETTLED
    this._pingInFlight = false
    const rtt = performance.now() - this._lastPingAt
    // An attach's probe may have waited behind other frames: a later ping that took less shows the path takes no more.
    const link = this._link
    if (link !== null && link.pathRtt < Infinity) link.measured(rtt)
    const roundWait = sendWait?.longestWaitSince(this._lastPingAt) ?? 0
    const received = this._bytesReceived - this._bytesAtPingSent
    const starved = senderBacklog * 8 < this._byteWindow
    this._boundRoundTrip(rtt, received, starved, deliveredBefore)
    const pathRtt = this._pathRtt(senderPathRtt)
    const loopRtt = pathRtt + roundWait
    const byteSample = received * (loopRtt < rtt ? loopRtt / rtt : 1)
    const bytes = this._byteDecision(byteSample, pathRtt < Infinity, starved)
    const msgs = this._msgDecision(this._msgsReceived - this._msgsAtPingSent, starved)
    this._msgWindowBeforeGrowth = 0
    this._grownWindow =
      bytes === 'grow' && roundWait > 0
        ? Math.min(2 * this._byteWindow, Math.max(this._byteWindow, Math.ceil(1.5 * byteSample)))
        : 0
    this._pace(bytes, msgs)
    return { acknowledged: true, bytes, msgs }
  }

  /** This round, less its wait behind this side's data, bounds the round trip where shorter, or where no queue held it. */
  private _boundRoundTrip(
    rtt: number,
    received: number,
    starved: boolean,
    deliveredBefore: (ms: number) => number,
  ): void {
    const roundRtt = Math.max(1, rtt - this._pingWait)
    if (this._boundRtt < Infinity && roundRtt < this._boundRtt) this._boundRtt = roundRtt
    // Starved, the wire near idle before the ack: no queue held it, so the round trip less its wait is the path's.
    if (starved && received * 3 >= this._byteWindow * 2 && deliveredBefore(rtt / 8) * 16 < received) {
      this._boundRtt = roundRtt
    }
  }

  /** A measured path round trip, or a round the window held back, tells a queue from the path; else only the sender. */
  private _pathRtt(senderPathRtt: number): number {
    const pathRtt = Math.min(this._link?.pathRtt ?? Infinity, senderPathRtt)
    if (this._boundRtt === Infinity) return pathRtt
    return pathRtt < Infinity ? Math.max(pathRtt, this._boundRtt) : this._boundRtt
  }

  /** The byte axis's verdict on `byteSample`, taken at the path's round trip if `measured`, starved if `starved`. */
  private _byteDecision(byteSample: number, measured: boolean, starved: boolean): AxisDecision {
    if (this._byteWindow >= this._byteWindowMax) return 'at-cap'
    if (byteSample * 3 < this._byteWindow * 2) return 'sample-too-small'
    if (measured) return 'grow'
    if (this._pingFollowsGrowth) return 'window-grew'
    return starved ? 'grow' : 'wire-busy'
  }

  /** The message axis's verdict on `msgSample`; starved, against the window before its last growth. */
  private _msgDecision(msgSample: number, starved: boolean): AxisDecision {
    if (this._msgWindow >= CREDIT_MSG_WINDOW_MAX) return 'at-cap'
    const msgWindow = (starved && this._msgWindowBeforeGrowth) || this._msgWindow
    return msgSample * 3 < msgWindow * 2 ? 'sample-too-small' : 'grow'
  }

  /** Cadence: snap to MIN on grow (more headroom may exist), exponential backoff on a verdict against growing. */
  private _pace(bytes: AxisDecision, msgs: AxisDecision): void {
    if (bytes === 'grow' || msgs === 'grow') {
      this._probeIntervalMs = BDP_PING_MIN_INTERVAL_MS
    } else if (bytes !== 'window-grew') {
      this._probeIntervalMs = Math.min(BDP_PING_MAX_INTERVAL_MS, this._probeIntervalMs * 2)
    }
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

  private _pingDue(now: number): boolean {
    if (this._pingInFlight) return false
    // Skip probe only when *both* axes have already hit their cap — otherwise one of them
    // might still want to grow.
    if (this._byteWindow >= this._byteWindowMax && this._msgWindow >= CREDIT_MSG_WINDOW_MAX) return false
    return now - this._lastPingAt >= this._probeIntervalMs
  }

  private _sendPing(now: number, sendWait: SendWait | null): void {
    this._probes = (this._probes + 1) >>> 0
    this._ping = this._probes
    this._pingInFlight = true
    this._lastPingAt = now
    this._pingWait = sendWait?.waitNow() ?? 0
    this._pingFollowsGrowth = this._grewSincePing
    this._grewSincePing = false
    this._bytesAtPingSent = this._bytesReceived
    this._msgsAtPingSent = this._msgsReceived
  }
}
