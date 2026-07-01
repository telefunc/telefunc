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

/** Per-axis verdict on a settled probe. `grow` = sample saturated ≥ 2/3 of
 *  current window (or saturation gate disabled) and not at cap. */
type AxisDecision = 'grow' | 'sample-too-small' | 'at-cap'

/** `acknowledged` is true when a probe was in flight to settle (false for
 *  a stale ack arriving after a transport reset). Per-axis decisions are
 *  independent — caller commits each axis's grow separately. */
type GrowDecision = { acknowledged: boolean; bytes: AxisDecision; msgs: AxisDecision }

/**
 * gRPC-style adaptive flow-control window. Maintained per channel on the receive
 * side; the resulting `byteWindow` is advertised to the peer via the existing
 * `WINDOW` frame so the sender stops blocking once the wire's BDP is reached.
 *
 *   1. On the first byte received after a previous sample completes, issue a
 *      `BDP_PING` and snapshot `bytesReceived`. Only one ping is in flight at a time
 *      so the cadence self-paces to ~1 ping per RTT during active traffic and zero
 *      while idle. `BDP_PING_MIN_INTERVAL_MS` further caps the rate on loopback /
 *      sub-ms RTT where unthrottled gRPC pacing would churn the event loop.
 *   2. The sender echoes `BDP_PING_ACK` immediately (queue-jumping any data) — the
 *      gap between snapshot and ack is the in-flight byte count, which IS the BDP
 *      sample for that round-trip.
 *   3. If the sample saturates ≥ 2/3 of the currently-advertised window, the window
 *      was the bottleneck → `onPingAck` returns `true`; caller may call `grow()` to
 *      double it (clamped to `CREDIT_WINDOW_MAX_BYTES`). The caller applies any
 *      additional gates (e.g. runtime saturation in `FlowControl`) before committing.
 *      Otherwise leave it; producer or wire is the limit, not us.
 *   4. Once `window` reaches the cap, probing stops entirely — no further growth
 *      is possible, so the round-trips would be pure overhead.
 *
 * Scope: pure bytes/RTT (BDP). Cross-cutting policies — CPU saturation, congestion,
 * fairness — live above this layer (see `FlowControl`), which can refuse to commit
 * a `grow()` even when this estimator says the link has byte-room.
 *
 * Grow-only by design (same as gRPC). Oversized `W` costs nothing as long as the
 * sender doesn't fill it — receiver memory = unconsumed bytes, not `W`. Shrinking
 * adds oscillation risk and decay-tuning complexity for no real win; the only
 * downside of an oversized `W` is the worst-case admissible buffering, capped at MAX.
 *
 * Why a dedicated `BDP_PING` frame rather than piggybacking on the user-level ping
 * (`CHANNEL_PING_INTERVAL_MS`): the user-ping is configurable for connection health
 * (defaults to 5 s, often set to 30 s+); BDP needs RTT-scale cadence to adapt within
 * a handful of round-trips.
 */
class BdpEstimator {
  // Byte axis
  private _byteWindow: number = CREDIT_WINDOW_INITIAL_BYTES
  private _bytesAtPingSent = 0
  private _bytesReceived = 0
  // Message-count axis
  private _msgWindow: number = CREDIT_MSG_WINDOW_INITIAL
  private _msgsAtPingSent = 0
  private _msgsReceived = 0
  // Shared probe
  private _pingInFlight = false
  private _lastPingAt = 0
  /** Adaptive probe interval: snaps to `MIN` on grow, doubles up to `MAX` on
   *  non-grow. Slows but never freezes — a real rate change rediscovers. */
  private _probeIntervalMs = BDP_PING_MIN_INTERVAL_MS

  /** Currently advertised byte-credit window. */
  get byteWindow(): number {
    return this._byteWindow
  }

  /** Currently advertised message-count window. */
  get msgWindow(): number {
    return this._msgWindow
  }

  /** Record one received frame (pre-app-processing). Returns true iff a `BDP_PING`
   *  should be emitted: caller fires `sendBdpPing()` synchronously. A single probe
   *  collects samples for both axes — `onPingAck` then derives independent
   *  byte-sample / msg-sample saturation decisions. */
  onReceive(bytes: number): boolean {
    let probe = false
    // Skip probe only when *both* axes have already hit their cap — otherwise one of them
    // might still want to grow.
    if (
      !this._pingInFlight &&
      !(this._byteWindow >= CREDIT_WINDOW_MAX_BYTES && this._msgWindow >= CREDIT_MSG_WINDOW_MAX)
    ) {
      const now = Date.now()
      if (now - this._lastPingAt >= this._probeIntervalMs) {
        // Snapshot BEFORE crediting the triggering frame — it counts as the first
        // in-flight byte. Otherwise a window-bound producer always samples 0.
        this._bytesAtPingSent = this._bytesReceived
        this._msgsAtPingSent = this._msgsReceived
        this._pingInFlight = true
        this._lastPingAt = now
        probe = true
      }
    }
    this._bytesReceived += bytes
    this._msgsReceived += 1
    return probe
  }

  /** Settle an outstanding `BDP_PING` against its `BDP_PING_ACK`. Returns per-axis
   *  grow suggestions: each is `true` iff that axis's sample saturated ≥ 2/3 of
   *  its current window. Caller decides whether to actually `growBytes()` /
   *  `growMsgs()` (e.g. after applying the CPU-lag gate). */
  onPingAck(): GrowDecision {
    if (!this._pingInFlight) return { acknowledged: false, bytes: 'sample-too-small', msgs: 'sample-too-small' }
    this._pingInFlight = false
    const byteSample = this._bytesReceived - this._bytesAtPingSent
    const msgSample = this._msgsReceived - this._msgsAtPingSent
    const bytes: AxisDecision =
      this._byteWindow >= CREDIT_WINDOW_MAX_BYTES
        ? 'at-cap'
        : byteSample * 3 >= this._byteWindow * 2
          ? 'grow'
          : 'sample-too-small'
    const msgs: AxisDecision =
      this._msgWindow >= CREDIT_MSG_WINDOW_MAX
        ? 'at-cap'
        : msgSample * 3 >= this._msgWindow * 2
          ? 'grow'
          : 'sample-too-small'
    // Cadence: snap to MIN on grow (more headroom may exist), exponential
    // backoff on non-grow (converged or temporarily quiet).
    if (bytes === 'grow' || msgs === 'grow') {
      this._probeIntervalMs = BDP_PING_MIN_INTERVAL_MS
    } else {
      this._probeIntervalMs = Math.min(BDP_PING_MAX_INTERVAL_MS, this._probeIntervalMs * 2)
    }
    return { acknowledged: true, bytes, msgs }
  }

  /** Commit a byte-window doubling. Idempotent at the cap. */
  growBytes(): void {
    this._byteWindow = Math.min(CREDIT_WINDOW_MAX_BYTES, this._byteWindow * 2)
  }

  /** Grow-only bump of the byte window (clamped to MAX). */
  bumpInitialByteWindow(bytes: number): void {
    if (bytes <= this._byteWindow) return
    this._byteWindow = Math.min(CREDIT_WINDOW_MAX_BYTES, bytes)
  }

  /** Commit a message-window doubling. Idempotent at the cap. */
  growMsgs(): void {
    this._msgWindow = Math.min(CREDIT_MSG_WINDOW_MAX, this._msgWindow * 2)
  }

  /** Drop the in-flight ping (its ack rode the prior wire). Preserves window AND
   *  cadence — both are link properties; a real rate change rediscovers. */
  reset(): void {
    this._pingInFlight = false
    this._bytesAtPingSent = this._bytesReceived
    this._msgsAtPingSent = this._msgsReceived
  }
}
