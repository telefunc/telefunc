import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'

import { BdpEstimator } from './bdp-estimator.js'
import {
  BDP_PING_MAX_INTERVAL_MS,
  BDP_PING_MIN_INTERVAL_MS,
  CREDIT_MSG_WINDOW_INITIAL,
  CREDIT_MSG_WINDOW_MAX,
  CREDIT_WINDOW_INITIAL_BYTES,
  CREDIT_WINDOW_MAX_BYTES,
} from '../constants.js'

// `Date.now()` drives the min-interval throttle; fake time so tests can pin the
// "ping is allowed to fire" / "throttled" boundary deterministically.
beforeEach(() => vi.useFakeTimers({ now: 1_000_000 }))
afterEach(() => vi.useRealTimers())

// Sample shape: `bytesAtPingSent` is snapshotted *after* the first `onReceive`
// increments the byte counter, so the sample is the bytes that arrive
// *between* ping-send and ack-arrival — modelling the real protocol where a
// sender pushes new data while waiting for the ack to come back. Helper drives
// the full cycle: kick off, accumulate, settle. Commits whichever axis (or
// both) the suggestion endorsed so subsequent cycles see the grown windows.
function cycle(bdp: BdpEstimator, sampleBytes: number): boolean {
  bdp.onReceive(1) // fires ping; bytesReceived=1, msgsReceived=1
  bdp.onReceive(sampleBytes) // accumulates into byte sample; msgsReceived=2
  const dec = bdp.onPingAck()
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
    expect(bdp.onReceive(1024)).toBe(true)
  })

  // The "one ping in flight" invariant: subsequent receives during the same RTT
  // window must NOT fire additional pings. Catches a missing `_pingInFlight` guard.
  it('does not fire another ping while one is in flight', () => {
    const bdp = new BdpEstimator()
    bdp.onReceive(1024)
    expect(bdp.onReceive(1024)).toBe(false)
    expect(bdp.onReceive(1024)).toBe(false)
  })

  // After the previous sample completes, the next byte should fire again — only
  // gated by the min-interval throttle, which we advance past here. Sample must
  // saturate (`grow` verdict) or the estimator settles and stops probing.
  it('fires another ping after the previous ack and the min interval has elapsed', () => {
    const bdp = new BdpEstimator()
    bdp.onReceive(CREDIT_WINDOW_INITIAL_BYTES) // saturating: sample → grow, no settle
    bdp.onPingAck()
    vi.advanceTimersByTime(BDP_PING_MIN_INTERVAL_MS)
    expect(bdp.onReceive(1024)).toBe(true)
  })

  // Min-interval throttle: even with the previous ack settled, we should not fire
  // again until BDP_PING_MIN_INTERVAL_MS has elapsed. This is the loopback
  // protection — prevents thousands of pings/sec on sub-ms RTT.
  it('throttles pings to at most one per BDP_PING_MIN_INTERVAL_MS', () => {
    const bdp = new BdpEstimator()
    bdp.onReceive(CREDIT_WINDOW_INITIAL_BYTES) // saturating: keeps probing alive
    bdp.onPingAck()
    vi.advanceTimersByTime(BDP_PING_MIN_INTERVAL_MS - 1)
    expect(bdp.onReceive(1024)).toBe(false)
    vi.advanceTimersByTime(1)
    expect(bdp.onReceive(1024)).toBe(true)
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
      bdp.onReceive(1)
      for (let i = 0; i < bdp.msgWindow; i++) bdp.onReceive(1)
      const dec = bdp.onPingAck()
      if (dec.bytes === 'grow') bdp.growBytes()
      if (dec.msgs === 'grow') bdp.growMsgs()
      vi.advanceTimersByTime(BDP_PING_MAX_INTERVAL_MS)
    }
    expect(bdp.byteWindow).toBe(CREDIT_WINDOW_MAX_BYTES)
    expect(bdp.msgWindow).toBe(CREDIT_MSG_WINDOW_MAX)
    expect(bdp.onReceive(1024)).toBe(false)
  })

  // Adaptive cadence: a non-grow verdict doubles the probe interval — so a
  // converged link stops spamming probes — but never freezes probing entirely.
  // After the next probe lands and grows, the cadence snaps back to the floor.
  it('backs off the probe interval after a non-grow verdict', () => {
    const bdp = new BdpEstimator()
    expect(cycle(bdp, Math.floor(CREDIT_WINDOW_INITIAL_BYTES / 4))).toBe(false)
    // Interval doubled to 2 × MIN — at MIN it must NOT fire yet.
    vi.advanceTimersByTime(BDP_PING_MIN_INTERVAL_MS)
    expect(bdp.onReceive(1024)).toBe(false)
    // Past 2 × MIN it fires.
    vi.advanceTimersByTime(BDP_PING_MIN_INTERVAL_MS)
    expect(bdp.onReceive(1024)).toBe(true)
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
    expect(bdp.onReceive(1024)).toBe(true)
  })

  // Reset preserves the backed-off cadence: BDP is a property of the link, not
  // the wire — a reconnect over the same path shouldn't re-burst-probe.
  it('reset preserves the backed-off probe cadence', () => {
    const bdp = new BdpEstimator()
    expect(cycle(bdp, Math.floor(CREDIT_WINDOW_INITIAL_BYTES / 4))).toBe(false)
    bdp.reset()
    // Cadence stays at the backed-off value (2 × MIN), so a probe at MIN doesn't fire.
    vi.advanceTimersByTime(BDP_PING_MIN_INTERVAL_MS)
    expect(bdp.onReceive(1024)).toBe(false)
    vi.advanceTimersByTime(BDP_PING_MIN_INTERVAL_MS)
    expect(bdp.onReceive(1024)).toBe(true)
  })

  // onPingAck with no ping in flight is a no-op on both axes — defends against
  // stray ACKs arriving after `reset()` dropped the previous in-flight state.
  it('onPingAck without an outstanding ping returns no-grow for both axes', () => {
    const bdp = new BdpEstimator()
    expect(bdp.onPingAck()).toEqual({ acknowledged: false, bytes: 'sample-too-small', msgs: 'sample-too-small' })
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
    bdp.onReceive(1) // fires a new ping
    bdp.onReceive(grown) // would have saturated if we let it settle
    bdp.reset()

    expect(bdp.byteWindow).toBe(grown) // preserved
    expect(bdp.onPingAck()).toEqual({ acknowledged: false, bytes: 'sample-too-small', msgs: 'sample-too-small' }) // stale ack ignored
    expect(bdp.byteWindow).toBe(grown) // and didn't grow from it

    // A fresh ping can fire after reset.
    vi.advanceTimersByTime(BDP_PING_MIN_INTERVAL_MS)
    expect(bdp.onReceive(1024)).toBe(true)
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
    bdp.onReceive(1)
    bdp.onReceive(CREDIT_WINDOW_INITIAL_BYTES) // huge byte sample, msg sample = 2
    const dec = bdp.onPingAck()
    expect(dec).toEqual({ acknowledged: true, bytes: 'grow', msgs: 'sample-too-small' })
  })

  // And the dual: many small frames saturate the msg sample but barely the byte
  // sample, so `suggest.msgs` fires alone.
  it('msg-saturating sample grows msgs only, not bytes', () => {
    const bdp = new BdpEstimator()
    bdp.onReceive(1) // fires ping
    // CREDIT_MSG_WINDOW_INITIAL more single-byte frames → msg sample fills,
    // byte sample stays tiny (1 byte each).
    for (let i = 0; i < CREDIT_MSG_WINDOW_INITIAL; i++) bdp.onReceive(1)
    const dec = bdp.onPingAck()
    expect(dec.msgs).toBe('grow')
    expect(dec.bytes).toBe('sample-too-small')
    bdp.growMsgs()
    expect(bdp.msgWindow).toBe(CREDIT_MSG_WINDOW_INITIAL * 2)
    expect(bdp.byteWindow).toBe(CREDIT_WINDOW_INITIAL_BYTES) // untouched
  })

  // Cap clamping is independent per axis.
  it('clamps msgWindow growth at CREDIT_MSG_WINDOW_MAX', () => {
    const bdp = new BdpEstimator()
    let safety = 200
    while (bdp.msgWindow < CREDIT_MSG_WINDOW_MAX && safety-- > 0) {
      bdp.onReceive(1)
      for (let i = 0; i < bdp.msgWindow; i++) bdp.onReceive(1)
      const dec = bdp.onPingAck()
      if (dec.msgs === 'grow') bdp.growMsgs()
      vi.advanceTimersByTime(BDP_PING_MIN_INTERVAL_MS)
    }
    expect(bdp.msgWindow).toBe(CREDIT_MSG_WINDOW_MAX)
  })
})
