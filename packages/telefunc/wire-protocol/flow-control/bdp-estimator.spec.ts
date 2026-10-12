import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'

import { BdpEstimator } from './bdp-estimator.js'
import { Link } from '../link.js'
import {
  BDP_PING_MAX_INTERVAL_MS,
  BDP_PING_MIN_INTERVAL_MS,
  CREDIT_MSG_WINDOW_INITIAL,
  CREDIT_MSG_WINDOW_MAX,
  CREDIT_WINDOW_INITIAL_BYTES,
  CREDIT_WINDOW_MAX_BYTES,
} from '../constants.js'

// `performance.now()` drives the min-interval throttle; fake time so tests can pin the
// "ping is allowed to fire" / "throttled" boundary deterministically.
beforeEach(() => vi.useFakeTimers({ now: 1_000_000 }))
afterEach(() => vi.useRealTimers())

/** What the sender's wire held each time its credit ran out: nothing, or a window and more. */
const EMPTY = 0
const HELD = CREDIT_WINDOW_MAX_BYTES
/** What the wire delivered before an ack: right up to it, as a queue does, or nothing. */
const upToAck = () => Infinity
const nothing = () => 0

// Sample shape: `bytesAtPingSent` is snapshotted *after* the first `onReceive`
// increments the byte counter, so the sample is the bytes that arrive
// *between* ping-send and ack-arrival — modelling the real protocol where a
// sender pushes new data while waiting for the ack to come back. Helper drives
// the full cycle: kick off, accumulate, settle. Commits whichever axis (or
// both) the suggestion endorsed so subsequent cycles see the grown windows.
function cycle(bdp: BdpEstimator, sampleBytes: number): boolean {
  bdp.onReceive(1, performance.now()) // fires ping; bytesReceived=1, msgsReceived=1
  bdp.onReceive(sampleBytes, performance.now()) // accumulates into byte sample; msgsReceived=2
  const dec = bdp.onPingAck(bdp.probe, EMPTY, Infinity, upToAck)
  if (dec.bytes === 'grow') bdp.growBytes()
  if (dec.msgs === 'grow') bdp.growMsgs()
  return dec.bytes === 'grow'
}

describe('BdpEstimator', () => {
  it('initial window equals CREDIT_WINDOW_INITIAL_BYTES', () => {
    const bdp = new BdpEstimator()
    expect(bdp.byteWindow).toBe(CREDIT_WINDOW_INITIAL_BYTES)
  })

  // First inbound byte after construction must trigger a ping — that's how the
  // estimator starts measuring. Returning false here would mean we never sample.
  it('fires a BDP_PING on the first onReceive', () => {
    const bdp = new BdpEstimator()
    expect(bdp.onReceive(1024, performance.now())).toBe(true)
  })

  // The "one ping in flight" invariant: subsequent receives during the same RTT
  // window must NOT fire additional pings. Catches a missing `_pingInFlight` guard.
  it('does not fire another ping while one is in flight', () => {
    const bdp = new BdpEstimator()
    bdp.onReceive(1024, performance.now())
    expect(bdp.onReceive(1024, performance.now())).toBe(false)
    expect(bdp.onReceive(1024, performance.now())).toBe(false)
  })

  // After the previous sample completes, the next byte should fire again — only
  // gated by the min-interval throttle, which we advance past here. Sample must
  // saturate (`grow` verdict) or the estimator settles and stops probing.
  it('fires another ping after the previous ack and the min interval has elapsed', () => {
    const bdp = new BdpEstimator()
    bdp.onReceive(CREDIT_WINDOW_INITIAL_BYTES, performance.now()) // saturating: sample → grow, no settle
    bdp.onPingAck(bdp.probe, EMPTY, Infinity, upToAck)
    vi.advanceTimersByTime(BDP_PING_MIN_INTERVAL_MS)
    expect(bdp.onReceive(1024, performance.now())).toBe(true)
  })

  // Min-interval throttle: even with the previous ack settled, we should not fire
  // again until BDP_PING_MIN_INTERVAL_MS has elapsed. This is the loopback
  // protection — prevents thousands of pings/sec on sub-ms RTT.
  it('throttles pings to at most one per BDP_PING_MIN_INTERVAL_MS', () => {
    const bdp = new BdpEstimator()
    bdp.onReceive(CREDIT_WINDOW_INITIAL_BYTES, performance.now()) // saturating: keeps probing alive
    bdp.onPingAck(bdp.probe, EMPTY, Infinity, upToAck)
    vi.advanceTimersByTime(BDP_PING_MIN_INTERVAL_MS - 1)
    expect(bdp.onReceive(1024, performance.now())).toBe(false)
    vi.advanceTimersByTime(1)
    expect(bdp.onReceive(1024, performance.now())).toBe(true)
  })

  // Sample saturates → window doubles. The sample is the bytes arriving while
  // the ping was in flight, so we have to accumulate them between ping-send and
  // ack-arrival (see `cycle` helper).
  it('doubles the window when the sample saturates ≥ 2/3 W', () => {
    const bdp = new BdpEstimator()
    expect(cycle(bdp, CREDIT_WINDOW_INITIAL_BYTES)).toBe(true)
    expect(bdp.byteWindow).toBe(CREDIT_WINDOW_INITIAL_BYTES * 2)
  })

  // Sample below 2/3 W means the wire/producer wasn't the bottleneck — leave W
  // alone. This is the "don't grow on quiet samples" property.
  it('does not grow the window when the sample is < 2/3 W', () => {
    const bdp = new BdpEstimator()
    expect(cycle(bdp, Math.floor(CREDIT_WINDOW_INITIAL_BYTES / 4))).toBe(false)
    expect(bdp.byteWindow).toBe(CREDIT_WINDOW_INITIAL_BYTES)
  })

  // Boundary check: exactly at 2/3 W grows (the predicate is `sample * 3 < W * 2`,
  // so equality counts as saturation). Catches off-by-one in the comparator.
  it('grows when sample equals the 2/3 W boundary', () => {
    const bdp = new BdpEstimator()
    const boundary = Math.ceil((CREDIT_WINDOW_INITIAL_BYTES * 2) / 3)
    expect(cycle(bdp, boundary)).toBe(true)
  })

  // The ack waits behind what the sender's wire still holds, so a sender that fills its window faster than its wire
  // drains saturates any sample. Only a wire the window left idle says the window, not the path, was the limit. What
  // waits on the wire the byte window bounds, so the message window grows on its sample as before.
  it('does not grow a byte window whose sender always had a backlog on its wire, however saturated the sample', () => {
    const bdp = new BdpEstimator()
    bdp.onReceive(1, performance.now())
    bdp.onReceive(CREDIT_WINDOW_INITIAL_BYTES, performance.now())
    for (let i = 0; i < CREDIT_MSG_WINDOW_INITIAL; i++) bdp.onReceive(1, performance.now())
    expect(bdp.onPingAck(bdp.probe, HELD, Infinity, upToAck)).toEqual({
      acknowledged: true,
      bytes: 'wire-busy',
      msgs: 'grow',
    })
    expect(bdp.byteWindow).toBe(CREDIT_WINDOW_INITIAL_BYTES)
  })

  // The sender says whether its credit ran out on an empty wire since it answered the ping before. A ping that went
  // out after a growth still covers credit the smaller window granted, so only the one after it judges the new window.
  it('judges a grown window only by a probe that went out after the first one following the growth', () => {
    const bdp = new BdpEstimator()
    expect(cycle(bdp, CREDIT_WINDOW_INITIAL_BYTES)).toBe(true)
    vi.advanceTimersByTime(BDP_PING_MIN_INTERVAL_MS)
    bdp.onReceive(1, performance.now())
    bdp.onReceive(2 * CREDIT_WINDOW_INITIAL_BYTES, performance.now())
    expect(bdp.onPingAck(bdp.probe, EMPTY, Infinity, upToAck).bytes).toBe('window-grew')
    // The cadence stays at the floor.
    vi.advanceTimersByTime(BDP_PING_MIN_INTERVAL_MS)
    expect(cycle(bdp, 2 * CREDIT_WINDOW_INITIAL_BYTES)).toBe(true)
    expect(bdp.byteWindow).toBe(4 * CREDIT_WINDOW_INITIAL_BYTES)
  })

  /** A ping whose ack comes `rttMs` after it, with a window's worth arriving meanwhile, up to the ack, as a queue
   *  delivers it. */
  function probeAfter(bdp: BdpEstimator, rttMs: number, backlog: number, senderPathRtt = Infinity) {
    vi.advanceTimersByTime(BDP_PING_MAX_INTERVAL_MS)
    expect(bdp.onReceive(1, performance.now())).toBe(true)
    vi.advanceTimersByTime(rttMs)
    const window = bdp.byteWindow
    bdp.onReceive(window, performance.now())
    return bdp.onPingAck(bdp.probe, backlog, senderPathRtt, () => window)
  }

  /** A ping that waited `pingWait` behind the receiver's own data, and whose ack comes `rttMs` after it, with a window's
   *  worth arriving at once and, of what the wire delivers, only what `deliveredBefore` says after: the window held back
   *  its sender, whose wire held `backlog` as its credit ran out. The receiver's frames waited `roundWait` at most since
   *  the ping went out. */
  function heldBack(
    bdp: BdpEstimator,
    rttMs: number,
    { backlog = EMPTY, deliveredBefore = nothing, pingWait = 0, roundWait = 0 } = {},
  ) {
    const sendWait = { waitNow: () => pingWait, longestWaitSince: () => roundWait }
    vi.advanceTimersByTime(BDP_PING_MAX_INTERVAL_MS)
    expect(bdp.onReceive(1, performance.now(), sendWait)).toBe(true)
    bdp.onReceive(bdp.byteWindow, performance.now())
    vi.advanceTimersByTime(rttMs)
    return bdp.onPingAck(bdp.probe, backlog, Infinity, deliveredBefore, sendWait)
  }

  /** An attach on `link`, whose probe is answered `rttMs` after it went out. */
  function attach(bdp: BdpEstimator, rttMs: number, link = new Link()) {
    const probe = bdp.probeAttach(link)!
    bdp.attach(link)
    vi.advanceTimersByTime(rttMs)
    return bdp.onPingAck(probe, Infinity, Infinity, upToAck)
  }

  // An attach's probe goes out ahead of the channel's frames, so its round trip is the path's. A later probe that took
  // ten times as long waited behind a queue, and its sample counts a tenth: a window that fills the path grows no more.
  it("counts a probe's sample at the round trip an attach's probe took", () => {
    const bdp = new BdpEstimator()
    // It settles no growth.
    expect(attach(bdp, 50).acknowledged).toBe(false)
    expect(probeAfter(bdp, 500, EMPTY).bytes).toBe('sample-too-small')
    // One that took the path's round trip counts in full, whatever the sender says.
    expect(probeAfter(bdp, 50, HELD).bytes).toBe('grow')
  })

  // A path whose round trip rose since the attach measured it: a round the window held back took the path's, as no queue
  // held its ack, so samples count at it, until a round takes less. A new wire may take another path.
  it('counts samples at the round trip of a round the window held back, where it is more than the one an attach measured', () => {
    const bdp = new BdpEstimator()
    attach(bdp, 10)
    expect(probeAfter(bdp, 200, EMPTY).bytes).toBe('sample-too-small')
    expect(heldBack(bdp, 200).bytes).toBe('grow')
    expect(probeAfter(bdp, 200, HELD).bytes).toBe('grow')
    expect(probeAfter(bdp, 400, EMPTY).bytes).toBe('sample-too-small')
    expect(probeAfter(bdp, 100, HELD).bytes).toBe('grow')
    expect(probeAfter(bdp, 200, EMPTY).bytes).toBe('sample-too-small')
    bdp.reset()
    expect(probeAfter(bdp, 100, EMPTY).bytes).toBe('sample-too-small')
  })

  // Other channels' messages, a few bytes in either side's socket and arriving just before the ack, are no queue: a queue
  // delivers at the link's rate up to the ack, at least the channel's own rate.
  it('counts a round the window held back at its round trip, whatever few frames of other channels its wire held and delivered', () => {
    const bdp = new BdpEstimator()
    attach(bdp, 10)
    // In the last eighth of the round trip, an eighth of what the channel got in it: a queue at the channel's rate.
    expect(heldBack(bdp, 200, { deliveredBefore: () => bdp.byteWindow / 8 }).bytes).toBe('sample-too-small')
    expect(heldBack(bdp, 200, { backlog: bdp.byteWindow / 8 }).bytes).toBe('sample-too-small')
    expect(heldBack(bdp, 200, { backlog: 100, deliveredBefore: () => 100 }).bytes).toBe('grow')
  })

  // A ping that waited behind the receiver's own upload took the path's round trip and that wait, which the window's
  // refreshes wait too: a round counts at the path's round trip and the longest the receiver's frames waited in it.
  it("counts a round at the path's round trip and the longest the receiver's own frames waited in it", () => {
    const bdp = new BdpEstimator()
    attach(bdp, 10)
    // The path takes 100 ms of the 200: the sample counts half.
    expect(heldBack(bdp, 200, { pingWait: 100 }).bytes).toBe('sample-too-small')
    expect(heldBack(bdp, 200, { pingWait: 100, roundWait: 100 }).bytes).toBe('grow')
  })

  // A window past the loop, with the receiver's wait in it, only queues at the sender: grown on such a round, it is what
  // the sample fills two thirds of, at most double.
  it("grows a window whose round counts the receiver's wait to one and a half times the sample, at most double", () => {
    const bdp = new BdpEstimator()
    const window = bdp.byteWindow
    attach(bdp, 10)
    expect(heldBack(bdp, 200, { pingWait: 100, roundWait: 100 }).bytes).toBe('grow')
    bdp.growBytes()
    // The sample is the window and the ping's byte.
    expect(bdp.byteWindow).toBe(Math.ceil(1.5 * (window + 1)))
    expect(heldBack(bdp, 200).bytes).toBe('grow')
    bdp.growBytes()
    expect(bdp.byteWindow).toBe(2 * Math.ceil(1.5 * (window + 1)))
  })

  // Where nothing measured the path, a sender whose wire held under an eighth of the window each time its credit ran
  // out had it empty soon after: the window held it back.
  it("grows a byte window nothing measured the path for once its sender's wire held under an eighth of it each time its credit ran out", () => {
    const bdp = new BdpEstimator()
    expect(probeAfter(bdp, 50, CREDIT_WINDOW_INITIAL_BYTES / 8).bytes).toBe('wire-busy')
    expect(probeAfter(bdp, 50, CREDIT_WINDOW_INITIAL_BYTES / 8 - 1).bytes).toBe('grow')
  })

  // A server's receive side has no attach of its own to probe: the page's ack says the round trip its own attach's
  // probe took, and counts as that would.
  it("counts a probe's sample at the round trip its sender's attach measured, where it measured none itself", () => {
    const bdp = new BdpEstimator()
    expect(probeAfter(bdp, 500, EMPTY, 50).bytes).toBe('sample-too-small')
    expect(probeAfter(bdp, 50, HELD, 50).bytes).toBe('grow')
  })

  // A later wire, as one of batched POSTs whose attach isn't probed, may take another path, or wait for its POSTs.
  it("measures the round trip of the wire an attach's probe went on, and of no other", () => {
    const link = new Link()
    attach(new BdpEstimator(), 50, link)
    expect(link.pathRtt).toBe(50)
    expect(new Link().pathRtt).toBe(Infinity)
  })

  // Every channel on a wire takes the path its round trip measures: one attach's probe measures it for all.
  it("counts samples at the round trip another channel's attach measured on its wire, and probes that wire no more", () => {
    const link = new Link()
    attach(new BdpEstimator(), 50, link)
    const bdp = new BdpEstimator()
    expect(bdp.probeAttach(link)).toBeUndefined()
    bdp.attach(link)
    expect(probeAfter(bdp, 500, EMPTY).bytes).toBe('sample-too-small')
    expect(probeAfter(bdp, 50, HELD).bytes).toBe('grow')
  })

  // A registration's RECONCILE lists every channel again, on the same wire, where the answer waits behind what the
  // wire holds: a wire whose round trip is measured is probed no more. One on another wire measures another path.
  it("probes a wire's round trip once, and that of the next wire", () => {
    const bdp = new BdpEstimator()
    const link = new Link()
    attach(bdp, 50, link)
    expect(bdp.probeAttach(link)).toBeUndefined()
    expect(probeAfter(bdp, 500, EMPTY).bytes).toBe('sample-too-small')
    attach(bdp, 500)
    expect(probeAfter(bdp, 500, HELD).bytes).toBe('grow')
  })

  // A registration's RECONCILE may go out before the first attach's probe is answered: that one's answer, which the
  // channel's frames don't hold up, counts.
  it('takes the least round trip of the probes of attaches on one wire in flight together', () => {
    const bdp = new BdpEstimator()
    const link = new Link()
    const first = bdp.probeAttach(link)!
    const second = bdp.probeAttach(link)!
    bdp.attach(link)
    vi.advanceTimersByTime(50)
    bdp.onPingAck(first, HELD, Infinity, upToAck)
    vi.advanceTimersByTime(450)
    bdp.onPingAck(second, HELD, Infinity, upToAck)
    expect(probeAfter(bdp, 500, EMPTY).bytes).toBe('sample-too-small')
  })

  // One answered late, after a later wire's, rode a wire that is gone.
  it("ignores the answer to an earlier wire's probe once a later wire's round trip is measured", () => {
    const bdp = new BdpEstimator()
    const gone = new Link()
    const early = bdp.probeAttach(gone)!
    bdp.attach(gone)
    attach(bdp, 500)
    bdp.onPingAck(early, HELD, Infinity, upToAck)
    expect(probeAfter(bdp, 500, HELD).bytes).toBe('grow')
  })

  // An attach's probe is lost with its wire, or with an upgrade whose barrier never went out: the next wire measures its
  // own path.
  it("keeps probing while an attach's probe goes unanswered, and measures the path of the next wire", () => {
    const bdp = new BdpEstimator()
    attach(bdp, 50)
    const next = new Link()
    bdp.probeAttach(next)
    expect(probeAfter(bdp, 50, EMPTY).bytes).toBe('grow')
    attach(bdp, 500, next)
    expect(probeAfter(bdp, 500, HELD).bytes).toBe('grow')
  })

  // The attach that resets the old wire's ping sent its probe on the new wire.
  it("measures the round trip of an attach's probe answered after the reset of that attach", () => {
    const bdp = new BdpEstimator()
    const link = new Link()
    const probe = bdp.probeAttach(link)!
    bdp.attach(link)
    bdp.reset()
    vi.advanceTimersByTime(50)
    bdp.onPingAck(probe, HELD, Infinity, upToAck)
    expect(probeAfter(bdp, 500, EMPTY).bytes).toBe('sample-too-small')
  })

  // A probe dropped by a reset may still be answered, as when its ping waited for the attach that reset it: another
  // probe's ack settles nothing.
  it('ignores an ack that answers another probe than the one in flight', () => {
    const bdp = new BdpEstimator()
    bdp.onReceive(1, performance.now())
    const stale = bdp.probe
    bdp.reset()
    vi.advanceTimersByTime(BDP_PING_MIN_INTERVAL_MS)
    bdp.onReceive(1, performance.now())
    bdp.onReceive(CREDIT_WINDOW_INITIAL_BYTES, performance.now())
    expect(bdp.onPingAck(stale, EMPTY, Infinity, upToAck).acknowledged).toBe(false)
    expect(bdp.onPingAck(bdp.probe, EMPTY, Infinity, upToAck).bytes).toBe('grow')
  })

  // Hard cap. Once window hits CREDIT_WINDOW_MAX_BYTES, growth stops there —
  // bounded receiver memory per channel.
  it('clamps growth at CREDIT_WINDOW_MAX_BYTES', () => {
    const bdp = new BdpEstimator()
    while (bdp.byteWindow < CREDIT_WINDOW_MAX_BYTES) {
      cycle(bdp, bdp.byteWindow)
      vi.advanceTimersByTime(BDP_PING_MIN_INTERVAL_MS)
    }
    expect(bdp.byteWindow).toBe(CREDIT_WINDOW_MAX_BYTES)
  })

  // Once BOTH axes are at MAX, no more pings should fire — they'd be pure
  // overhead. While either axis can still grow, probes continue so its sample
  // can be observed; this checks the combined-cap behaviour.
  it('stops issuing pings once both windows are at MAX', () => {
    const bdp = new BdpEstimator()
    // Saturate both axes by alternating large byte samples (grow bytes) with
    // many-message samples (grow msgs). Once bytes hits cap, the byte cycle's
    // verdict is `at-cap + sample-too-small` which trips the cadence backoff;
    // advancing MAX between cycles keeps subsequent probes firing regardless.
    let safety = 200
    while ((bdp.byteWindow < CREDIT_WINDOW_MAX_BYTES || bdp.msgWindow < CREDIT_MSG_WINDOW_MAX) && safety-- > 0) {
      // One byte-saturating cycle.
      cycle(bdp, bdp.byteWindow)
      vi.advanceTimersByTime(BDP_PING_MAX_INTERVAL_MS)
      // One msg-saturating cycle: many small frames in flight.
      bdp.onReceive(1, performance.now())
      for (let i = 0; i < bdp.msgWindow; i++) bdp.onReceive(1, performance.now())
      const dec = bdp.onPingAck(bdp.probe, EMPTY, Infinity, upToAck)
      if (dec.bytes === 'grow') bdp.growBytes()
      if (dec.msgs === 'grow') bdp.growMsgs()
      vi.advanceTimersByTime(BDP_PING_MAX_INTERVAL_MS)
    }
    expect(bdp.byteWindow).toBe(CREDIT_WINDOW_MAX_BYTES)
    expect(bdp.msgWindow).toBe(CREDIT_MSG_WINDOW_MAX)
    expect(bdp.onReceive(1024, performance.now())).toBe(false)
  })

  // Adaptive cadence: a non-grow verdict doubles the probe interval — so a
  // converged link stops spamming probes — but never freezes probing entirely.
  // After the next probe lands and grows, the cadence snaps back to the floor.
  it('backs off the probe interval after a non-grow verdict', () => {
    const bdp = new BdpEstimator()
    expect(cycle(bdp, Math.floor(CREDIT_WINDOW_INITIAL_BYTES / 4))).toBe(false)
    // Interval doubled to 2 × MIN — at MIN it must NOT fire yet.
    vi.advanceTimersByTime(BDP_PING_MIN_INTERVAL_MS)
    expect(bdp.onReceive(1024, performance.now())).toBe(false)
    // Past 2 × MIN it fires.
    vi.advanceTimersByTime(BDP_PING_MIN_INTERVAL_MS)
    expect(bdp.onReceive(1024, performance.now())).toBe(true)
  })

  // A transient slow-second shouldn't permanently freeze growth. After a non-grow
  // verdict the cadence is slower, but the very next probe — if it saturates —
  // grows the window AND snaps the cadence back to the floor for fast catch-up.
  it('resumes fast probing after a grow following a backoff', () => {
    const bdp = new BdpEstimator()
    // Transient: tiny sample → non-grow → interval doubles.
    expect(cycle(bdp, Math.floor(CREDIT_WINDOW_INITIAL_BYTES / 4))).toBe(false)
    // Wait long enough for the backed-off interval, then deliver a saturating sample.
    vi.advanceTimersByTime(2 * BDP_PING_MIN_INTERVAL_MS)
    expect(cycle(bdp, CREDIT_WINDOW_INITIAL_BYTES)).toBe(true)
    // Next probe should fire at the floor (MIN) again, not the backed-off interval.
    vi.advanceTimersByTime(BDP_PING_MIN_INTERVAL_MS)
    expect(bdp.onReceive(1024, performance.now())).toBe(true)
  })

  // Reset preserves the backed-off cadence: BDP is a property of the link, not
  // the wire — a reconnect over the same path shouldn't re-burst-probe.
  it('reset preserves the backed-off probe cadence', () => {
    const bdp = new BdpEstimator()
    expect(cycle(bdp, Math.floor(CREDIT_WINDOW_INITIAL_BYTES / 4))).toBe(false)
    bdp.reset()
    // Cadence stays at the backed-off value (2 × MIN), so a probe at MIN doesn't fire.
    vi.advanceTimersByTime(BDP_PING_MIN_INTERVAL_MS)
    expect(bdp.onReceive(1024, performance.now())).toBe(false)
    vi.advanceTimersByTime(BDP_PING_MIN_INTERVAL_MS)
    expect(bdp.onReceive(1024, performance.now())).toBe(true)
  })

  // onPingAck with no ping in flight is a no-op on both axes — defends against
  // stray ACKs arriving after `reset()` dropped the previous in-flight state.
  it('onPingAck without an outstanding ping returns no-grow for both axes', () => {
    const bdp = new BdpEstimator()
    expect(bdp.onPingAck(bdp.probe, EMPTY, Infinity, upToAck)).toEqual({
      acknowledged: false,
      bytes: 'sample-too-small',
      msgs: 'sample-too-small',
    })
  })

  // reset() drops the in-flight ping (its ack rode the prior wire and won't
  // arrive) but PRESERVES the grown window — BDP is a property of the link, not
  // of a single wire instance. Catches a reset that accidentally clobbers W.
  it('reset drops the in-flight ping but preserves the grown window', () => {
    const bdp = new BdpEstimator()
    // Grow once.
    cycle(bdp, CREDIT_WINDOW_INITIAL_BYTES)
    const grown = bdp.byteWindow
    expect(grown).toBe(CREDIT_WINDOW_INITIAL_BYTES * 2)

    // Issue a fresh ping, then reset before its ack — the ack should be ignored.
    vi.advanceTimersByTime(BDP_PING_MIN_INTERVAL_MS)
    bdp.onReceive(1, performance.now()) // fires a new ping
    bdp.onReceive(grown, performance.now()) // would have saturated if we let it settle
    bdp.reset()

    expect(bdp.byteWindow).toBe(grown) // preserved
    expect(bdp.onPingAck(bdp.probe, EMPTY, Infinity, upToAck)).toEqual({
      acknowledged: false,
      bytes: 'sample-too-small',
      msgs: 'sample-too-small',
    }) // stale ack ignored
    expect(bdp.byteWindow).toBe(grown) // and didn't grow from it

    // A fresh ping can fire after reset.
    vi.advanceTimersByTime(BDP_PING_MIN_INTERVAL_MS)
    expect(bdp.onReceive(1024, performance.now())).toBe(true)
  })

  // ── Message-count axis ────────────────────────────────────────────────────

  it('initial msgWindow equals CREDIT_MSG_WINDOW_INITIAL', () => {
    const bdp = new BdpEstimator()
    expect(bdp.msgWindow).toBe(CREDIT_MSG_WINDOW_INITIAL)
  })

  // The two axes are independent: a single big byte-frame saturates bytes but
  // not msgs (only 2 msgs received), so `suggest.bytes` is true and
  // `suggest.msgs` is false.
  it('byte-saturating sample grows bytes only, not msgs', () => {
    const bdp = new BdpEstimator()
    bdp.onReceive(1, performance.now())
    bdp.onReceive(CREDIT_WINDOW_INITIAL_BYTES, performance.now()) // huge byte sample, msg sample = 2
    const dec = bdp.onPingAck(bdp.probe, EMPTY, Infinity, upToAck)
    expect(dec).toEqual({ acknowledged: true, bytes: 'grow', msgs: 'sample-too-small' })
  })

  // And the dual: many small frames saturate the msg sample but barely the byte
  // sample, so `suggest.msgs` fires alone.
  it('msg-saturating sample grows msgs only, not bytes', () => {
    const bdp = new BdpEstimator()
    bdp.onReceive(1, performance.now()) // fires ping
    // CREDIT_MSG_WINDOW_INITIAL more single-byte frames → msg sample fills,
    // byte sample stays tiny (1 byte each).
    for (let i = 0; i < CREDIT_MSG_WINDOW_INITIAL; i++) bdp.onReceive(1, performance.now())
    const dec = bdp.onPingAck(bdp.probe, EMPTY, Infinity, upToAck)
    expect(dec.msgs).toBe('grow')
    expect(dec.bytes).toBe('sample-too-small')
    bdp.growMsgs()
    expect(bdp.msgWindow).toBe(CREDIT_MSG_WINDOW_INITIAL * 2)
    expect(bdp.byteWindow).toBe(CREDIT_WINDOW_INITIAL_BYTES) // untouched
  })

  function msgCycleOf(bdp: BdpEstimator, msgs: number, backlog: number) {
    vi.advanceTimersByTime(BDP_PING_MIN_INTERVAL_MS)
    bdp.onReceive(1, performance.now())
    for (let i = 0; i < msgs; i++) bdp.onReceive(1, performance.now())
    const dec = bdp.onPingAck(bdp.probe, backlog, Infinity, upToAck)
    if (dec.msgs === 'grow') bdp.growMsgs()
    return dec.msgs
  }

  // The sender answers a ping that went out after a growth on the credit the smaller window granted, so its sample
  // can't saturate the larger one. It counts against the window before, and only for a sender whose credit ran out on an
  // empty wire: a wire that held a backlog filled its sample with what the byte window let queue.
  it('judges a ping that went out after a message window growth by the window before it, for a starved sender', () => {
    const bdp = new BdpEstimator()
    const msgCycle = (msgs: number, backlog: number) => msgCycleOf(bdp, msgs, backlog)
    expect(msgCycle(CREDIT_MSG_WINDOW_INITIAL, EMPTY)).toBe('grow')
    expect(bdp.msgWindow).toBe(CREDIT_MSG_WINDOW_INITIAL * 2)
    // The sample saturates the window before the growth: a starved sender's ping counts.
    expect(msgCycle(CREDIT_MSG_WINDOW_INITIAL, EMPTY)).toBe('grow')
    expect(bdp.msgWindow).toBe(CREDIT_MSG_WINDOW_INITIAL * 4)
    // A sample of the window before doesn't pass the one it grew to.
    expect(msgCycle(CREDIT_MSG_WINDOW_INITIAL, EMPTY)).toBe('sample-too-small')
  })

  it('judges a ping that went out after a message window growth by the grown window, for a sender whose wire held a backlog', () => {
    const bdp = new BdpEstimator()
    const msgCycle = (msgs: number, backlog: number) => msgCycleOf(bdp, msgs, backlog)
    expect(msgCycle(CREDIT_MSG_WINDOW_INITIAL, HELD)).toBe('grow')
    // A sample of the window before doesn't pass the one it grew to.
    expect(msgCycle(CREDIT_MSG_WINDOW_INITIAL, HELD)).toBe('sample-too-small')
    expect(bdp.msgWindow).toBe(CREDIT_MSG_WINDOW_INITIAL * 2)
  })

  it('judges a ping on a replaced wire by the grown message window', () => {
    const bdp = new BdpEstimator()
    expect(msgCycleOf(bdp, CREDIT_MSG_WINDOW_INITIAL, EMPTY)).toBe('grow')
    bdp.reset()
    expect(msgCycleOf(bdp, CREDIT_MSG_WINDOW_INITIAL, EMPTY)).toBe('sample-too-small')
  })

  it('judges only the ping that follows a message window growth by the window before it', () => {
    const bdp = new BdpEstimator()
    expect(msgCycleOf(bdp, CREDIT_MSG_WINDOW_INITIAL, EMPTY)).toBe('grow')
    expect(msgCycleOf(bdp, 0, EMPTY)).toBe('sample-too-small')
    vi.advanceTimersByTime(BDP_PING_MAX_INTERVAL_MS)
    expect(msgCycleOf(bdp, CREDIT_MSG_WINDOW_INITIAL, EMPTY)).toBe('sample-too-small')
  })

  it('judges the byte window by a ping that went out after a message window growth', () => {
    const bdp = new BdpEstimator()
    expect(msgCycleOf(bdp, CREDIT_MSG_WINDOW_INITIAL, EMPTY)).toBe('grow')
    vi.advanceTimersByTime(BDP_PING_MIN_INTERVAL_MS)
    bdp.onReceive(1, performance.now())
    bdp.onReceive(CREDIT_WINDOW_INITIAL_BYTES, performance.now())
    expect(bdp.onPingAck(bdp.probe, EMPTY, Infinity, upToAck).bytes).toBe('grow')
  })

  // Cap clamping is independent per axis.
  it('clamps msgWindow growth at CREDIT_MSG_WINDOW_MAX', () => {
    const bdp = new BdpEstimator()
    let safety = 200
    while (bdp.msgWindow < CREDIT_MSG_WINDOW_MAX && safety-- > 0) {
      bdp.onReceive(1, performance.now())
      for (let i = 0; i < bdp.msgWindow; i++) bdp.onReceive(1, performance.now())
      const dec = bdp.onPingAck(bdp.probe, EMPTY, Infinity, upToAck)
      if (dec.msgs === 'grow') bdp.growMsgs()
      vi.advanceTimersByTime(BDP_PING_MIN_INTERVAL_MS)
    }
    expect(bdp.msgWindow).toBe(CREDIT_MSG_WINDOW_MAX)
  })
})
