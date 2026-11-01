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
import type { RoundTrips } from './RoundTrips.js'
import type { SendWait } from './SendWait.js'
import type { BdpPingAck } from '../wire/frame.js'
import type { CreditOwner } from './credit.js'

/** Per-axis verdict on a settled probe; `grow` needs the sample to saturate ≥ 2/3 of a window not at cap. */
type AxisDecision = 'grow' | 'sample-too-small' | 'wire-busy' | 'window-grew' | 'at-cap'

/** Each axis grows apart. */
type GrowDecision = { bytes: AxisDecision; msgs: AxisDecision }

const NOT_SETTLED: GrowDecision = { bytes: 'sample-too-small', msgs: 'sample-too-small' }

type AttachProbe = { probe: number; sentAt: number; roundTrips: RoundTrips }

const NO_ATTACH_PROBES: readonly AttachProbe[] = []

/** A channel's receive window: it grows, as gRPC's does, while what crosses in a round trip fills two thirds of it. */
class BdpEstimator {
  // Byte axis
  private currentByteWindow: number = CREDIT_WINDOW_INITIAL_BYTES
  /** The largest the byte window gets: `CREDIT_WINDOW_MAX_BYTES`, or less where the sender's replay holds less. */
  private byteWindowCap: number = CREDIT_WINDOW_MAX_BYTES
  /** The byte window `raiseByteWindow` replaced, until it grows or the raise is undone; 0 otherwise. */
  private byteWindowBeforeRaise = 0
  private bytesAtPingSent = 0
  private bytesReceived = 0
  // Message-count axis
  private currentMsgWindow: number = CREDIT_MSG_WINDOW_INITIAL
  private msgsAtPingSent = 0
  private msgsReceived = 0
  /** The message window before its last growth, until the next ping settles or the wire is replaced. */
  private msgWindowBeforeGrowth = 0
  // Shared probe
  /** Probes started, attach probes included: each takes the next number. */
  private probes = 0
  /** The number of the ping in flight, or the last one. */
  private ping = 0
  private pingInFlight = false
  /** Attach probes not answered yet: each settles no growth, so none holds up a ping. */
  private attachProbes: readonly AttachProbe[] = NO_ATTACH_PROBES
  /** The wire it attached on last, whose round trip a sample counts at. */
  private roundTrips: RoundTrips | null = null
  /** A round trip the window held back, which no queue lengthened, since the wire was replaced; `Infinity` before. */
  private boundRtt = Infinity
  private lastPingAt = Number.NEGATIVE_INFINITY
  /** How long the ping in flight waited behind this side's own data on its wire. */
  private pingWait = 0
  /** What the next `growBytes` takes the byte window to where the last settled probe sized it, else 0: it doubles. */
  private grownWindow = 0
  /** The byte window grew since the last ping went out. */
  private grewSincePing = false
  /** The ping in flight went out after the byte window grew: its answer covers credit the smaller window granted. */
  private pingFollowsGrowth = false
  /** The probe interval, which `pace` sets: the minimum after a grow, kept after `'window-grew'`, doubled up to the
   *  maximum otherwise. It slows but never stops, so a real rate change is found again. */
  private probeIntervalMs = BDP_PING_MIN_INTERVAL_MS

  /** The byte window the estimator sets, which `ReceiveCredit` grants unless it grants more. */
  get byteWindow(): number {
    return this.currentByteWindow
  }

  /** The largest the byte window gets (see `capByteWindow`). */
  get byteWindowMax(): number {
    return this.byteWindowCap
  }

  /** The message window the estimator sets, which `ReceiveCredit` advertises. */
  get msgWindow(): number {
    return this.currentMsgWindow
  }

  /** The number of the ping in flight, which its `BDP_PING` carries. */
  get probe(): number {
    return this.ping
  }

  /** Records one received frame; true iff the caller should send a `BDP_PING` of the number `probe` reads, at once. */
  onReceive(bytes: number, sendWait: SendWait | null = null): boolean {
    const now = performance.now()
    const probe = this.pingDue(now)
    // Snapshot BEFORE crediting the triggering frame — it counts as the first
    // in-flight byte. Otherwise a window-bound producer always samples 0.
    if (probe) this.sendPing(now, sendWait)
    this.bytesReceived += bytes
    this.msgsReceived += 1
    return probe
  }

  /** Starts a probe that goes out with an attach on `roundTrips`' wire; `undefined` where its round trip is measured. */
  probeAttach(roundTrips: RoundTrips): number | undefined {
    if (roundTrips.pathRtt < Infinity) return undefined
    const attachProbes = this.attachProbes.filter(
      (attach) => attach.roundTrips === roundTrips || attach.roundTrips === this.roundTrips,
    )
    this.probes = (this.probes + 1) >>> 0
    attachProbes.push({ probe: this.probes, sentAt: performance.now(), roundTrips })
    this.attachProbes = attachProbes
    return this.probes
  }

  /** Settles `probe` if it is an attach's, whose answer measures its wire's round trip. Returns whether it was. */
  settleAttachProbe(probe: number): boolean {
    const attach = this.attachProbes.find((pending) => pending.probe === probe)
    if (!attach) return false
    this.attachProbes = this.attachProbes.filter((pending) => pending !== attach)
    attach.roundTrips.measured(performance.now() - attach.sentAt)
    return true
  }

  /** It attached on the wire of `roundTrips`. */
  attach(roundTrips: RoundTrips): void {
    this.roundTrips = roundTrips
  }

  /** Settles the ping in flight; `wire._deliveredBefore(ms)` is what the wire delivered from any channel in the `ms`
   *  before. */
  onPingAck(
    { probe, backlog: senderBacklog, pathRtt: senderPathRtt }: BdpPingAck,
    wire: Pick<CreditOwner, '_deliveredBefore'>,
    sendWait: SendWait | null = null,
  ): GrowDecision {
    if (this.settleAttachProbe(probe)) return NOT_SETTLED
    if (!this.pingInFlight || probe !== this.ping) return NOT_SETTLED
    this.pingInFlight = false
    const rtt = performance.now() - this.lastPingAt
    // An attach's probe may have waited behind other frames: a later ping that took less shows the path takes no more.
    const roundTrips = this.roundTrips
    if (roundTrips !== null && roundTrips.pathRtt < Infinity) roundTrips.measured(rtt)
    const roundWait = sendWait?.longestWaitSince(this.lastPingAt) ?? 0
    const received = this.bytesReceived - this.bytesAtPingSent
    const starved = senderBacklog * 8 < this.currentByteWindow
    this.boundRoundTrip(rtt, received, starved, wire)
    const pathRtt = this.pathRtt(senderPathRtt)
    // A round of credit on a batch-POST wire also waits for its POST.
    const loopRtt = pathRtt + roundWait + (roundTrips?.longestPostWaitSince(this.lastPingAt) ?? 0)
    const byteSample = received * (loopRtt < rtt ? loopRtt / rtt : 1)
    const bytes = this.byteDecision(byteSample, pathRtt < Infinity, starved)
    const msgs = this.msgDecision(this.msgsReceived - this.msgsAtPingSent, starved)
    this.msgWindowBeforeGrowth = 0
    this.grownWindow =
      bytes === 'grow' && roundWait > 0
        ? Math.min(2 * this.currentByteWindow, Math.max(this.currentByteWindow, Math.ceil(1.5 * byteSample)))
        : 0
    this.pace(bytes, msgs)
    return { bytes, msgs }
  }

  /** This round, less its wait behind this side's data, bounds the round trip where shorter, or where no queue held it. */
  private boundRoundTrip(
    rtt: number,
    received: number,
    starved: boolean,
    wire: Pick<CreditOwner, '_deliveredBefore'>,
  ): void {
    const roundRtt = Math.max(1, rtt - this.pingWait)
    if (this.boundRtt < Infinity && roundRtt < this.boundRtt) this.boundRtt = roundRtt
    // Starved, the wire near idle before the ack: no queue held it, so the round trip less its wait is the path's.
    if (starved && received * 3 >= this.currentByteWindow * 2 && wire._deliveredBefore(rtt / 8) * 16 < received) {
      this.boundRtt = roundRtt
    }
  }

  /** A measured path round trip, or a round the window held back, tells a queue from the path; else only the sender. */
  private pathRtt(senderPathRtt: number): number {
    const pathRtt = Math.min(this.roundTrips?.pathRtt ?? Infinity, senderPathRtt)
    if (this.boundRtt === Infinity) return pathRtt
    return pathRtt < Infinity ? Math.max(pathRtt, this.boundRtt) : this.boundRtt
  }

  /** The byte axis's verdict on `byteSample`, taken at the path's round trip if `measured`, starved if `starved`. */
  private byteDecision(byteSample: number, measured: boolean, starved: boolean): AxisDecision {
    if (this.currentByteWindow >= this.byteWindowCap) return 'at-cap'
    if (byteSample * 3 < this.currentByteWindow * 2) return 'sample-too-small'
    if (measured) return 'grow'
    if (this.pingFollowsGrowth) return 'window-grew'
    return starved ? 'grow' : 'wire-busy'
  }

  /** The message axis's verdict on `msgSample`; starved, against the window before its last growth. */
  private msgDecision(msgSample: number, starved: boolean): AxisDecision {
    if (this.currentMsgWindow >= CREDIT_MSG_WINDOW_MAX) return 'at-cap'
    const msgWindow = (starved && this.msgWindowBeforeGrowth) || this.currentMsgWindow
    return msgSample * 3 < msgWindow * 2 ? 'sample-too-small' : 'grow'
  }

  /** Sets the probe interval: back to the minimum after a grow, since more headroom may exist; kept after a
   *  `'window-grew'` byte decision, whose ping followed the window's growth; doubled, up to the maximum, after any
   *  other verdict against growing. */
  private pace(bytes: AxisDecision, msgs: AxisDecision): void {
    if (bytes === 'grow' || msgs === 'grow') {
      this.probeIntervalMs = BDP_PING_MIN_INTERVAL_MS
    } else if (bytes !== 'window-grew') {
      this.probeIntervalMs = Math.min(BDP_PING_MAX_INTERVAL_MS, this.probeIntervalMs * 2)
    }
  }

  /** Grows the byte window: doubles it, or less where the last settled probe says so; no change at the cap. */
  growBytes(): void {
    this.currentByteWindow = Math.min(this.byteWindowCap, this.grownWindow || this.currentByteWindow * 2)
    this.grownWindow = 0
    this.grewSincePing = true
    this.byteWindowBeforeRaise = 0
  }

  /** Raises the byte window to `bytes`, within its cap; never lowers it. */
  raiseByteWindow(bytes: number): void {
    const window = Math.min(this.byteWindowCap, bytes)
    if (window <= this.currentByteWindow) return
    if (this.byteWindowBeforeRaise === 0) this.byteWindowBeforeRaise = this.currentByteWindow
    this.currentByteWindow = window
    this.grewSincePing = true
  }

  /** Back to the byte window before `raiseByteWindow`, within its cap, where it didn't grow since. */
  undoByteWindowRaise(): void {
    if (this.byteWindowBeforeRaise === 0) return
    this.currentByteWindow = Math.min(this.byteWindowCap, this.byteWindowBeforeRaise)
    this.byteWindowBeforeRaise = 0
  }

  /** The byte window gets to `bytes` at most, `CREDIT_WINDOW_MAX_BYTES` if more, and is lowered to it. */
  capByteWindow(bytes: number): void {
    this.byteWindowCap = Math.min(CREDIT_WINDOW_MAX_BYTES, bytes)
    this.currentByteWindow = Math.min(this.currentByteWindow, this.byteWindowCap)
  }

  /** Doubles the message window; no change at the cap. */
  growMsgs(): void {
    this.msgWindowBeforeGrowth = this.currentMsgWindow
    this.currentMsgWindow = Math.min(CREDIT_MSG_WINDOW_MAX, this.currentMsgWindow * 2)
  }

  /** Drops the ping in flight, whose ack rode a prior wire or counts frames held back, and starts the next round here.
   *  Keeps the windows and the cadence: both are the link's, and a real rate change rediscovers them. */
  reset(): void {
    this.pingInFlight = false
    this.boundRtt = Infinity
    this.msgWindowBeforeGrowth = 0
    this.bytesAtPingSent = this.bytesReceived
    this.msgsAtPingSent = this.msgsReceived
  }

  private pingDue(now: number): boolean {
    if (this.pingInFlight) return false
    // A probe is skipped only when both windows are at their cap: otherwise one may still grow.
    if (this.currentByteWindow >= this.byteWindowCap && this.currentMsgWindow >= CREDIT_MSG_WINDOW_MAX) return false
    return now - this.lastPingAt >= this.probeIntervalMs
  }

  private sendPing(now: number, sendWait: SendWait | null): void {
    this.probes = (this.probes + 1) >>> 0
    this.ping = this.probes
    this.pingInFlight = true
    this.lastPingAt = now
    this.pingWait = sendWait?.waitNow() ?? 0
    this.pingFollowsGrowth = this.grewSincePing
    this.grewSincePing = false
    this.bytesAtPingSent = this.bytesReceived
    this.msgsAtPingSent = this.msgsReceived
  }
}
