// The PRIMARY behavioural oracle for the SSE→WS upgrade: a real browser, a real upgrade, real
// frames in flight across it. The paired sse-only run is the control — same assertions, no upgrade
// available — so an assertion that cannot fail shows up as the control passing when it should not.
//
// The server-side counters are read rather than `page.waitForEvent('websocket')`, which fires on the
// PROBE socket and so is satisfied by an upgrade that aborts.
export { testUpgrade }

import { page, test, expect, autoRetry, getServerUrl } from '@brillout/test-e2e'
import { navigate, getResult, getCleanupState, resetCleanupState } from '../../e2e-utils'

function testUpgrade() {
  const transports = parseChannelTransports(process.env.PUBLIC_ENV__CHANNEL_TRANSPORTS)
  const expectsUpgrade = transports.length > 1 && transports[transports.length - 1] === 'ws'
  const label = expectsUpgrade ? 'upgrade' : 'upgrade control (sse-only)'

  const UPSTREAM_SENDS = 25

  test(`${label}: the SSE→WS upgrade commits once and the upgraded wire delivers exactly once [${transports.join(',')}]`, async () => {
    await resetCleanupState()
    await navigate(`${getServerUrl()}/channel`)

    await page.click('#channel-connect')
    await autoRetry(async () => {
      const state = await getResult<UpgradeChannelState>('#channel-state')
      expect(state.connected).toBe(true)
      expect(state.welcomeReceived).toBe(true)
    })

    await page.click('#channel-test-upstream-open')
    let channelId: string | null = null
    await autoRetry(async () => {
      const state = await getResult<UpgradeChannelState>('#channel-state')
      expect(state.upstreamReconnectChannelId).not.toBe(null)
      channelId = state.upstreamReconnectChannelId
    })

    await page.click('#channel-test-multi')
    await page.click('#channel-test-binary')
    for (let i = 0; i < UPSTREAM_SENDS; i++) {
      await page.click('#channel-test-upstream-send')
    }

    if (expectsUpgrade) {
      await autoRetry(async () => {
        const ss = await getCleanupState()
        expect(ss.upgrade_observerPresent).toBe('true')
        expect(ss.upgrade_committedCount).toBe('1')
      })
    }

    await autoRetry(async () => {
      const ss = await getCleanupState()
      expect(ss[`upstream_${channelId}_receivedCount`]).toBe(String(UPSTREAM_SENDS))
      expect(ss[`upstream_${channelId}_lastSeq`]).toBe(String(UPSTREAM_SENDS))
      expect(ss[`upstream_${channelId}_hasGap`]).toBe('false')
    })

    await autoRetry(async () => {
      const state = await getResult<UpgradeChannelState>('#channel-state')
      expect(state.tickCount).toBe(state.lastTickServerCount)
      expect(state.tickWentBackward).toBe(false)
      expect(state.multiCh1IsMonotonic).toBe(true)
      expect(state.multiCh2IsMonotonic).toBe(true)
      expect(state.binaryRoundTripOk).toBe(true)
      expect(state.binaryByteCount).toBe(256 * 4096)
    })

    const ss = await getCleanupState()
    expect(ss.upgrade_observerPresent).toBe('true')
    if (expectsUpgrade) {
      expect(ss.upgrade_committedCount).toBe('1')
      expect(ss.upgrade_preparedCount).toBe('1')
    } else {
      expect(ss.upgrade_committedCount).toBe('0')
      expect(ss.upgrade_preparedCount).toBe('0')
    }
  })
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
