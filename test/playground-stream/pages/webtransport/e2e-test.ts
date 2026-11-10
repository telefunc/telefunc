export { testWebTransport }

import { page, expect, autoRetry, getServerUrl } from '@brillout/test-e2e'
import type { Page } from 'playwright-chromium'
import { navigate, getResult } from '../../e2e-utils'
import { test, expectWire } from '../../laneWire'
import { readWire } from '../../wireProbe'
import type { WtState } from './WebTransportTest'

/** A page of its own on which no WebTransport session ever opens, as where UDP to the server is dropped. */
async function pageWithoutWebTransport(): Promise<Page> {
  const tab = await page.context().browser()!.newPage()
  await tab.addInitScript(() => {
    const Real = window.WebTransport
    window.WebTransport = class extends Real {
      constructor() {
        super('https://127.0.0.1:9/')
        // A handshake whose packets are dropped never settles: the page bounds the wait itself.
        for (const pending of ['ready', 'closed'])
          Object.defineProperty(this, pending, { value: new Promise(() => {}) })
      }
    }
  })
  return tab
}

async function ticksAfter(ticks: number, target = page): Promise<WtState['twoChannels']> {
  let state: WtState['twoChannels'] = null
  await autoRetry(async () => {
    state = (JSON.parse((await target.textContent('#wt-state'))!) as WtState).twoChannels
    expect(state?.ticks).greaterThan(ticks)
  })
  return state
}

function testWebTransport() {
  test('webtransport: channels move to WebTransport, and one stalled by its reader holds back none beside it', async () => {
    await navigate(`${getServerUrl()}/webtransport`)
    await page.click('#wt-two-channels')
    // The page climbs SSE, then a WebSocket, then WebTransport, which retires the WebSocket.
    await autoRetry(async () => {
      const wire = (await page.evaluate(readWire))!
      expect(wire.webtransport.sessions).toBe(1)
      expect(wire.ws.sockets).greaterThan(0)
      expect(wire.live.join()).toBe('webtransport')
    })
    const stalledAt = (await ticksAfter(0))!
    // The stalled channel's credit ran out long before: its count stays while the ticks go on.
    const later = (await ticksAfter(stalledAt.ticks + 25))!
    expect(later.stalledReceived).toBe(1)
  })

  test('webtransport: a lossy broadcast faster than its subscriber keeps the newest, in order', async () => {
    await navigate(`${getServerUrl()}/webtransport`)
    await page.click('#wt-lossy')
    await autoRetry(async () => {
      const lossy = (await getResult<WtState>('#wt-state')).lossy
      expect(lossy?.at(-1)).toBe(2000 - 1)
      expect(lossy!.every((ordinal, i) => i === 0 || ordinal > lossy![i - 1]!)).toBe(true)
    })
  })

  test('webtransport: where UDP never reaches the server, channels stay on the WebSocket', async () => {
    expectWire({ wire: 'ws' })
    const tab = await pageWithoutWebTransport()
    await navigate(`${getServerUrl()}/webtransport`, undefined, tab)
    await tab.click('#wt-two-channels')
    // Each probe ends unanswered, and the page tries again only as its channels change.
    await autoRetry(async () => {
      expect((await tab.evaluate(readWire))!.webtransport.sessions).greaterThan(0)
    })
    const before = (await ticksAfter(0, tab))!
    await ticksAfter(before.ticks + 25, tab)
  })
}
