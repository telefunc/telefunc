export { testSlowLink }

import { page, test, expect, autoRetry, getServerUrl } from '@brillout/test-e2e'
import { navigate, getResult, countConnections } from '../../e2e-utils'
import { startBlackHoleProxy } from '../../black-hole-proxy'

type SlowLinkState = {
  linkShare: { pingMs: number[]; downloaded: number } | null
  slowUplink: { uploaded: number; opened: string; longestTickGapMs: number } | null
}

// Preview only: the page loads through a proxy in the test process, which Vite's HMR socket would share.
function testSlowLink(isDev: boolean) {
  if (isDev) return
  // With two transports the page opens an SSE stream, then a WebSocket: wire-e2e-test.ts covers a slow link there.
  if (JSON.parse(process.env.PUBLIC_ENV__CHANNEL_TRANSPORTS!).length > 1) return

  test("channel: on a 1 MB/s link, a channel's acknowledged sends beside another channel's download answer, on the connection they started on", async () => {
    const proxy = await startBlackHoleProxy(getServerUrl())
    try {
      await navigate(`${proxy.url}/channel`)
      proxy.pace({ downBytesPerSecond: 1_000_000, upBytesPerSecond: 1_000_000, queueBytes: 64 * 1024 })
      const connections = countConnections()
      await page.click('#channel-test-link-share')
      let linkShare: SlowLinkState['linkShare'] = null
      await autoRetry(
        async () => {
          linkShare = (await getResult<SlowLinkState>('#channel-state')).linkShare
          expect(linkShare).not.toBe(null)
        },
        { timeout: 50_000 },
      )
      for (const ms of linkShare!.pingMs) expect(ms).lessThan(20_000)
      expect(connections.stop()).toBe(1)
    } finally {
      // The page leaves before the proxy closes, which would cut its connections.
      await page.goto('about:blank')
      await proxy.close()
    }
  })

  test('channel: on a 125 kB/s uplink, a 2 MiB upload arrives and keeps the download coming on the connection it started on, and a channel opened meanwhile opens on it', async () => {
    const proxy = await startBlackHoleProxy(getServerUrl())
    try {
      await navigate(`${proxy.url}/channel`)
      proxy.pace({ downBytesPerSecond: 10_000_000, upBytesPerSecond: 125_000, queueBytes: 64 * 1024 })
      const connections = countConnections()
      await page.click('#channel-test-slow-uplink')
      let slowUplink: SlowLinkState['slowUplink'] = null
      await autoRetry(
        async () => {
          slowUplink = (await getResult<SlowLinkState>('#channel-state')).slowUplink
          expect(slowUplink).not.toBe(null)
        },
        { timeout: 55_000 },
      )
      expect(slowUplink!.uploaded).toBe(2 * 1024 * 1024)
      expect(slowUplink!.opened).toBe('answer')
      expect(slowUplink!.longestTickGapMs).lessThan(2000)
      expect(connections.stop()).toBe(1)
    } finally {
      // The page leaves before the proxy closes, which would cut its connections.
      await page.goto('about:blank')
      await proxy.close()
    }
  })
}
