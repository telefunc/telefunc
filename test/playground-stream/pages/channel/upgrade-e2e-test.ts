// The PRIMARY behavioural oracle for the SSE→WS upgrade, kept fully black-box: a real browser, a real
// upgrade, real frames observed only through Playwright's public WebSocket inspection. Register
// `page.on('websocket')` before connecting and count each `/_telefunc` socket's received data frames.
// A committed upgrade routes the channel's ongoing traffic onto a WebSocket, so that socket's
// received-frame count climbs past a threshold and keeps climbing as ticks arrive. An aborted upgrade
// fires the same 'websocket' event on its probe socket, but that socket only carries the tiny
// PREPARE/READY/PING exchange before closing — so a frame threshold, not the event alone, separates
// commit from abort. The paired sse-only run is the control: no `/_telefunc` WebSocket may ever open.
export { testUpgrade }

import { page, test, expect, autoRetry, getServerUrl } from '@brillout/test-e2e'
import type { WebSocket } from 'playwright-chromium'
import { navigate, getResult, getCleanupState, resetCleanupState } from '../../e2e-utils'

type FrameCounts = { received: number; sent: number }

function testUpgrade() {
  const transports = parseChannelTransports(process.env.PUBLIC_ENV__CHANNEL_TRANSPORTS)
  const expectsUpgrade = transports.length > 1 && transports[transports.length - 1] === 'ws'
  const isSseOnly = transports.length === 1 && transports[0] === 'sse'
  // ws-only (['ws']): no SSE to start from, so data-on-WS is normal and asserting it would be vacuous
  // — the existing channel suite already covers pure-WS delivery. Run only the upgrade path + control.
  if (!expectsUpgrade && !isSseOnly) return

  const label = expectsUpgrade ? 'upgrade' : 'upgrade control (sse-only)'
  const UPSTREAM_SENDS = 25
  // Comfortably above an aborted probe's handshake frames, comfortably below steady-state traffic.
  const DATA_FRAME_THRESHOLD = 10

  test(`${label}: the SSE→WS upgrade commits once and the upgraded wire delivers exactly once [${transports.join(',')}]`, async () => {
    // Count frames per `/_telefunc` socket. Registered BEFORE connect so the probe and the upgraded
    // wire are both observed; Vite's dev HMR socket is excluded by the URL filter.
    const sockets: FrameCounts[] = []
    const onWebSocket = (ws: WebSocket) => {
      if (!ws.url().includes('/_telefunc')) return
      const counts: FrameCounts = { received: 0, sent: 0 }
      sockets.push(counts)
      ws.on('framereceived', () => counts.received++)
      ws.on('framesent', () => counts.sent++)
    }
    page.on('websocket', onWebSocket)
    try {
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
        // A `/_telefunc` WebSocket must carry the channel's ongoing traffic: its received data-frame
        // count crosses the threshold and is still climbing as the app's tick counter advances.
        let carrier: FrameCounts | undefined
        await autoRetry(async () => {
          carrier = sockets.find((s) => s.received > DATA_FRAME_THRESHOLD)
          expect(carrier).not.toBe(undefined)
        })
        const receivedBefore = carrier!.received
        const tickBefore = (await getResult<UpgradeChannelState>('#channel-state')).tickCount
        await autoRetry(async () => {
          const state = await getResult<UpgradeChannelState>('#channel-state')
          expect(state.tickCount).greaterThan(tickBefore)
          expect(carrier!.received).greaterThan(receivedBefore)
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

      if (!expectsUpgrade) {
        // Control: sse-only carries everything over SSE, so no `/_telefunc` WebSocket ever opens. This
        // assertion CAN fail — it fails the instant an upgrade wrongly fires on the sse-only path.
        expect(sockets.length).toBe(0)
      }
    } finally {
      page.off('websocket', onWebSocket)
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
