export { testUpgrade }

import { page, test, expect, autoRetry, getServerUrl } from '@brillout/test-e2e'
import { navigate, getResult, getCleanupState, resetCleanupState } from '../../e2e-utils'

/**
 * SSE→WS barrier-upgrade e2e.
 *
 * What this proves: the upgrade really completes in a real browser against a real build (dev AND
 * preview), and a connection that has undergone one delivers exactly once in both directions —
 * upstream burst, 1 Hz acked ticks, two independently-ticking channels, a 1 MB binary round-trip.
 *
 * What it does NOT prove, measured rather than assumed: that a frame in flight AT the barrier turn
 * survives it. `maybeStartUpgrade` runs on the first settled RECONCILED, which lands during
 * `#channel-connect` — instrumenting the commit counter at each step of this test showed
 * `committed=1` already before `#channel-test-upstream-open` was clicked, so every click below
 * happens on the ALREADY-upgraded wire. Nothing here is in flight across the handoff.
 *
 * That is not a fixable oversight, and deliberately no sleep is tuned in to chase it: a browser
 * cannot deterministically park a frame mid-`recvChain`, so any such gate would be timing-luck and
 * a flake generator. The happens-before edge is gated where it can be gated deterministically —
 * the park/burst rows in `upgrade.barrier-server.spec.ts`, which park a real frame and are
 * mutation-verified. This file
 * gates the things only a browser can: that the upgrade fires at all, exactly once, and leaves a
 * healthy connection behind.
 *
 * Completion is asserted from the SERVER, via the barrier-commit counter on `/api/cleanup-state`.
 * The available client-side signal — `page.waitForEvent('websocket')` — fires on the PROBE socket
 * and is therefore satisfied by an upgrade that probes and then aborts. Using it here would be a
 * gate that cannot fail.
 *
 * The `['sse']`-only variants run this same body as the CONTROL: identical traffic, no upgrade
 * available, so the commit counter must stay at zero. Neither polarity is a gate on its own — the
 * positive alone passes if the counter is stuck high, the control alone passes if it is stuck low.
 */
function testUpgrade(isDev: boolean) {
  const transports = parseChannelTransports(process.env.PUBLIC_ENV__CHANNEL_TRANSPORTS)
  // The upgrade is enabled by the transport list alone: the connection starts on `transports[0]`
  // and probes for a later one. `['sse']` cannot upgrade; `['sse','ws']` does.
  const expectsUpgrade = transports.length > 1 && transports[transports.length - 1] === 'ws'
  const label = expectsUpgrade ? 'upgrade' : 'upgrade control (sse-only)'

  const UPSTREAM_SENDS = 25

  test(`${label}: the SSE→WS upgrade commits once and the upgraded wire delivers exactly once [${transports.join(',')}]`, async () => {
    // Zeroes the barrier counters too, so the counts below belong to THIS page load. Done before
    // navigating: the connection is created by the first channel open, which is a click away.
    await resetCleanupState()
    await navigate(`${getServerUrl()}/channel`)

    // Main channel: 1 Hz server ticks, each one a full round trip the client acks.
    await page.click('#channel-connect')
    await autoRetry(async () => {
      const state = await getResult<UpgradeChannelState>('#channel-state')
      expect(state.connected).toBe(true)
      expect(state.welcomeReceived).toBe(true)
    })

    // Upstream channel: client→server sequential integers, server-side gap detection.
    await page.click('#channel-test-upstream-open')
    let channelId: string | null = null
    await autoRetry(async () => {
      const state = await getResult<UpgradeChannelState>('#channel-state')
      expect(state.upstreamReconnectChannelId).not.toBe(null)
      channelId = state.upstreamReconnectChannelId
    })

    // Load on the upgraded wire. Measured: the commit has already landed by here (see the header),
    // so this is traffic AFTER the handoff, not across it — it proves the upgraded connection is
    // healthy and exactly-once, which is a different claim from surviving the barrier turn.
    await page.click('#channel-test-multi')
    await page.click('#channel-test-binary')
    for (let i = 0; i < UPSTREAM_SENDS; i++) {
      await page.click('#channel-test-upstream-send')
    }

    // ── The upgrade itself ──────────────────────────────────────────────
    if (expectsUpgrade) {
      await autoRetry(async () => {
        const ss = await getCleanupState()
        expect(ss.upgrade_observerPresent).toBe('true')
        expect(ss.upgrade_committedCount).toBe('1')
      })
    }

    // ── Upstream (client→server) exactly-once ───────────────────────────
    // `hasGap === 'false'` WITH the full count is the exactly-once proof: a gap flags reordering or
    // a skip, and the count flags loss or duplication. Either alone is satisfiable by the other's
    // failure mode.
    await autoRetry(async () => {
      const ss = await getCleanupState()
      expect(ss[`upstream_${channelId}_receivedCount`]).toBe(String(UPSTREAM_SENDS))
      expect(ss[`upstream_${channelId}_lastSeq`]).toBe(String(UPSTREAM_SENDS))
      expect(ss[`upstream_${channelId}_hasGap`]).toBe('false')
    })

    // ── Downstream (server→client) exactly-once + mux integrity ─────────
    await autoRetry(async () => {
      const state = await getResult<UpgradeChannelState>('#channel-state')
      // tickCount > lastTickServerCount → duplicates; < → loss.
      expect(state.tickCount).toBe(state.lastTickServerCount)
      expect(state.tickWentBackward).toBe(false)
      // Two independent channels on the same mux, each strictly monotonic across the handoff.
      expect(state.multiCh1IsMonotonic).toBe(true)
      expect(state.multiCh2IsMonotonic).toBe(true)
      // 1 MB round trip, byte-verified — a real payload draining across the wire boundary.
      expect(state.binaryRoundTripOk).toBe(true)
      expect(state.binaryByteCount).toBe(256 * 4096)
    })

    // ── Final upgrade accounting ────────────────────────────────────────
    // Asserted last, once all the traffic above has settled: by this point an upgrade has had every
    // opportunity to fire, so the control's zero is a real observation rather than a premature read.
    const ss = await getCleanupState()
    expect(ss.upgrade_observerPresent).toBe('true')
    if (expectsUpgrade) {
      expect(ss.upgrade_committedCount).toBe('1')
      // No retry-thrash: a healthy upgrade prepares exactly once. A count above 1 means attempts
      // are aborting and re-probing, which every assertion above would otherwise survive.
      expect(ss.upgrade_preparedCount).toBe('1')
    } else {
      // Control: same traffic, same oracle, no upgrade to be had.
      expect(ss.upgrade_committedCount).toBe('0')
      expect(ss.upgrade_preparedCount).toBe('0')
    }
  })

  // `isDev` is accepted for symmetry with `testChannel(isDev)`; this body runs identically in dev
  // and preview, which is the point — the handoff must not depend on the bundling mode.
  void isDev
}

function parseChannelTransports(raw: string | undefined): string[] {
  if (!raw) return ['sse']
  return JSON.parse(raw) as string[]
}

type UpgradeChannelState = {
  connected: boolean
  welcomeReceived: boolean
  tickCount: number
  lastTickServerCount: number | null
  tickWentBackward: boolean
  binaryRoundTripOk: boolean | null
  binaryByteCount: number | null
  multiCh1IsMonotonic: boolean | null
  multiCh2IsMonotonic: boolean | null
  upstreamReconnectChannelId: string | null
}
