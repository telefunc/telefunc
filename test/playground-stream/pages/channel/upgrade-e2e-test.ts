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
import type { Page, WebSocket, WebSocketRoute } from 'playwright-chromium'
import { navigate, getResult, getCleanupState, resetCleanupState } from '../../e2e-utils'

type FrameCounts = { received: number }

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
      const counts: FrameCounts = { received: 0 }
      sockets.push(counts)
      ws.on('framereceived', () => counts.received++)
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

  if (!expectsUpgrade) return

  // The server moves a page's publishes to the WebSocket at the barrier, and the page takes that wire over once the
  // barrier's POST answers. Holding that answer lands a publish between the two, whole or with its body's end after.
  for (const { bytes, what, bodyHeld } of [
    { bytes: 1024, what: 'arrives whole', bodyHeld: false },
    { bytes: 4 * 1024 * 1024, what: 'starts arriving', bodyHeld: true },
  ]) {
    test(`${label}: a publish that ${what} before the page takes the new wire over reaches the page [${transports.join(',')}]`, async () => {
      const context = await page.context().browser()!.newContext()
      const routed = await context.newPage()
      const acrossFlip = async () => {
        const state = JSON.parse((await routed.textContent('#channel-state'))!) as UpgradeChannelState
        return state.acrossFlip
      }
      try {
        const gate = await holdFlip(routed, bytes / 2)
        await navigate(`${getServerUrl()}/channel`, undefined, routed)
        await routed.click('#channel-test-across-flip')
        await autoRetry(async () => expect(await acrossFlip()).not.toBe(null))
        await gate.committed
        gate.countFromHere()
        await routed.evaluate((n) => (window as unknown as AcrossFlipPage).publishAcrossFlip(n), bytes)
        void routed.evaluate(() => (window as unknown as AcrossFlipPage).sendAcrossFlip())
        await autoRetry(() => expect(gate.passedBytes()).greaterThan(bytes / 2))
        expect((await gate.flip()) > 0).toBe(bodyHeld)
        await autoRetry(async () => expect((await acrossFlip())?.received).deep.equal([bytes]))
      } finally {
        await context.close()
      }
    })
  }
}

/** Routes `target`'s wires through the test: the barrier's POST is answered only by `flip()`, which then passes what it
 *  held of the page's WebSocket past `heldPast` bytes, once the page's first send after the flip shows it took the wire. */
async function holdFlip(target: Page, heldPast: number) {
  const held: Buffer[] = []
  let counted = false
  let passed = 0
  let toPage: WebSocketRoute | undefined
  let allowFlip: () => void
  const flipAllowed = new Promise<void>((resolve) => (allowFlip = resolve))
  let tookWire: () => void
  const tookWireP = new Promise<void>((resolve) => (tookWire = resolve))
  let serverCommitted: () => void
  const committed = new Promise<void>((resolve) => (serverCommitted = resolve))
  await target.route(/\/_telefunc/, async (route) => {
    if (!route.request().postDataBuffer()?.includes('upgradeId')) return route.fallback()
    const answer = await route.fetch()
    serverCommitted()
    await flipAllowed
    await route.fulfill({ response: answer })
  })
  await target.routeWebSocket(/\/_telefunc/, (ws) => {
    const server = ws.connectToServer()
    toPage = ws
    ws.onMessage((message) => {
      if (Buffer.from(message as Buffer).includes('after-flip')) tookWire()
      server.send(message)
    })
    server.onMessage((message) => {
      const bytes = Buffer.from(message as Buffer)
      if (counted && passed > heldPast) return void held.push(bytes)
      if (counted) passed += bytes.length
      ws.send(message)
    })
  })
  return {
    committed,
    countFromHere: () => void (counted = true),
    passedBytes: () => passed,
    flip: async () => {
      allowFlip()
      await tookWireP
      counted = false
      for (const message of held) toPage!.send(message)
      return held.length
    },
  }
}

function parseChannelTransports(raw: string | undefined): string[] {
  if (!raw) return ['sse']
  return JSON.parse(raw) as string[]
}

type AcrossFlipPage = { publishAcrossFlip(bytes: number): Promise<void>; sendAcrossFlip(): Promise<void> }

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
  acrossFlip: { received: number[] } | null
}
