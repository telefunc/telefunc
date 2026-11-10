export { testWebTransport }

import { page, test, expect, autoRetry, getServerUrl } from '@brillout/test-e2e'
import type { WebSocket } from 'playwright-chromium'
import { navigate, getResult } from '../../e2e-utils'
import type { WtState } from './WebTransportTest'

/** Counts the page's WebTransport sessions; with `unreachable`, none ever opens, as where UDP to the server is dropped. */
function watchWebTransport(unreachable: boolean) {
  return page.addInitScript((unreachable) => {
    const counts = { sessions: 0 }
    Object.assign(window, { wtCounts: counts })
    const Real = window.WebTransport
    window.WebTransport = class extends Real {
      constructor(url: string | URL, options?: WebTransportOptions) {
        counts.sessions++
        super(unreachable ? 'https://127.0.0.1:9/' : url, unreachable ? {} : options)
        if (!unreachable) return
        // A handshake whose packets are dropped never settles: the page bounds the wait itself.
        for (const pending of ['ready', 'closed'])
          Object.defineProperty(this, pending, { value: new Promise(() => {}) })
      }
    }
  }, unreachable)
}

/** The page's `/_telefunc` WebSockets. */
function watchWebSockets(): WebSocket[] {
  const sockets: WebSocket[] = []
  page.on('websocket', (ws) => {
    if (ws.url().includes('/_telefunc')) sockets.push(ws)
  })
  return sockets
}

async function ticksAfter(ticks: number): Promise<WtState['twoChannels']> {
  let state: WtState['twoChannels'] = null
  await autoRetry(async () => {
    state = (await getResult<WtState>('#wt-state')).twoChannels
    expect(state?.ticks).greaterThan(ticks)
  })
  return state
}

function testWebTransport() {
  test('webtransport: channels move to WebTransport, and one stalled by its reader holds back none beside it', async () => {
    await watchWebTransport(false)
    const sockets = watchWebSockets()
    await navigate(`${getServerUrl()}/webtransport`)
    await page.click('#wt-two-channels')
    // The page climbs SSE, then a WebSocket, then WebTransport, which retires the WebSocket.
    await autoRetry(async () => {
      expect(
        await page.evaluate(() => (window as unknown as { wtCounts: { sessions: number } }).wtCounts.sessions),
      ).toBe(1)
      expect(sockets.length).greaterThan(0)
      expect(sockets.every((ws) => ws.isClosed())).toBe(true)
    })
    const stalledAt = (await ticksAfter(0))!
    // The stalled channel's credit ran out long before: its count stays while the ticks go on.
    const later = (await ticksAfter(stalledAt.ticks + 25))!
    expect(later.stalledReceived).toBe(1)
  })

  test('webtransport: a lossy broadcast faster than its subscriber keeps the newest, in order', async () => {
    await watchWebTransport(false)
    await navigate(`${getServerUrl()}/webtransport`)
    await page.click('#wt-lossy')
    await autoRetry(async () => {
      const lossy = (await getResult<WtState>('#wt-state')).lossy
      expect(lossy?.at(-1)).toBe(2000 - 1)
      expect(lossy!.every((ordinal, i) => i === 0 || ordinal > lossy![i - 1]!)).toBe(true)
    })
  })

  test('webtransport: where UDP never reaches the server, channels stay on the WebSocket', async () => {
    await watchWebTransport(true)
    const sockets = watchWebSockets()
    await navigate(`${getServerUrl()}/webtransport`)
    await page.click('#wt-two-channels')
    // Each probe ends unanswered, and the page tries again only as its channels change.
    await autoRetry(async () => {
      expect(
        await page.evaluate(() => (window as unknown as { wtCounts: { sessions: number } }).wtCounts.sessions),
      ).greaterThan(0)
    })
    const before = (await ticksAfter(0))!
    await ticksAfter(before.ticks + 25)
    expect(sockets.some((ws) => !ws.isClosed())).toBe(true)
  })
}
