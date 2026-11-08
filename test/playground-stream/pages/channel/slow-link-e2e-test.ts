export { testSlowLink }

import { page, test, expect, autoRetry, getServerUrl } from '@brillout/test-e2e'
import { navigate, getResult, countConnections, serverArrayBuffers, sleep } from '../../e2e-utils'
import { startBlackHoleProxy } from '../../black-hole-proxy'
import { withNetworkConditions } from '../../../utils'

type SlowLinkState = {
  linkShare: { pingMs: number[]; downloaded: number } | null
  slowUplink: { uploaded: number; opened: string; longestTickGapMs: number } | null
  publishesBehind: { pingMs: number; publishesAhead: number; closed: string; closeMs: number } | null
}

// Preview only: the page loads through a proxy in the test process, which Vite's HMR socket would share.
function testSlowLink(isDev: boolean) {
  if (isDev) return
  const upgrades = JSON.parse(process.env.PUBLIC_ENV__CHANNEL_TRANSPORTS!).length > 1

  test(upgrades
    ? "channel: on a 1 MB/s link, once the page moved from SSE to a WebSocket, a channel's acknowledged sends beside another channel's download answer as on a WebSocket from the start"
    : "channel: on a 1 MB/s link, a channel's acknowledged sends beside another channel's download answer, on the connection they started on", async () => {
    const proxy = await startBlackHoleProxy(getServerUrl())
    try {
      await navigate(`${proxy.url}/channel`)
      proxy.pace({ downBytesPerSecond: 1_000_000, upBytesPerSecond: 1_000_000, queueBytes: 64 * 1024 })
      const connections = upgrades ? null : countConnections()
      await page.click('#channel-test-link-share')
      let linkShare: SlowLinkState['linkShare'] = null
      await autoRetry(
        async () => {
          linkShare = (await getResult<SlowLinkState>('#channel-state')).linkShare
          expect(linkShare).not.toBe(null)
        },
        { timeout: 50_000 },
      )
      if (upgrades) {
        // The last send goes after the move: ~2 s where the window went back to a WebSocket's, ~7.5 s where it stayed.
        expect(linkShare!.pingMs.at(-1)).lessThan(5500)
      } else {
        for (const ms of linkShare!.pingMs) expect(ms).lessThan(20_000)
        expect(connections!.stop()).toBe(1)
      }
    } finally {
      // The page leaves before the proxy closes, which would cut its connections.
      await page.goto('about:blank')
      await proxy.close()
    }
  })

  // With two transports the page opens an SSE stream, then moves to a WebSocket: wire-e2e-test.ts covers the rest of a
  // slow link there.
  if (upgrades) return

  if (process.env.PUBLIC_ENV__CHANNEL_TRANSPORTS === '["sse"]')
    test('channel: SSE wires the page gave up on hold nothing of the 32 MiB waiting for it, and all of it arrives', async () => {
      await navigate(`${getServerUrl()}/channel`)
      const before = await serverArrayBuffers()
      await withNetworkConditions({ downloadThroughput: 1000, uploadThroughput: -1 }, async () => {
        await page.click('#channel-test-binary-burst-unawaited-32')
        // Time for the page to give up on a few wires, each with what waited on it.
        await sleep(15_000)
        // The replay of 32 MiB and, in base64, what waits on the current wire: 75 MiB.
        expect((await serverArrayBuffers()) - before).lessThan(96 * 1024 * 1024)
      })
      await autoRetry(
        async () => {
          const { binaryBurst } = await getResult<{
            binaryBurst: { received: number; inOrder: boolean; closeErr: string | null } | null
          }>('#channel-state')
          expect(binaryBurst).deep.equal({ ...binaryBurst, received: 32, inOrder: true, closeErr: null })
        },
        { timeout: 30_000 },
      )
    })

  // 32 s to cross, past the 10 s a page waits for an answer to its ping.
  test('channel: on a 1 Mbit/s link, a 4 MB message from the server arrives on the connection it started on', async () => {
    const proxy = await startBlackHoleProxy(getServerUrl())
    try {
      await navigate(`${proxy.url}/channel`)
      // In 40 ms bursts, as a cell link delivers: a wire's first two pieces reach the page together.
      proxy.pace({ downBytesPerSecond: 125_000, upBytesPerSecond: 125_000, queueBytes: 64 * 1024, burstMs: 40 })
      const connections = countConnections()
      await page.click('#wire-large-slow-link')
      await autoRetry(
        async () => {
          const { large } = await getResult<{ large: number | null }>('#wire-state')
          expect(large).toBe(4_000_000)
        },
        { timeout: 55_000 },
      )
      expect(connections.stop()).toBe(1)
    } finally {
      // The page leaves before the proxy closes, which would cut its connections.
      await page.goto('about:blank')
      await proxy.close()
    }
  })

  // The four bodies still to cross take at least 16 s, and 22 s on SSE.
  test("channel: on a 1 MB/s link, publishes falling behind hold up neither another channel's answer nor the server's close, and the answer to the page's own publish costs it none of them", async () => {
    const proxy = await startBlackHoleProxy(getServerUrl())
    try {
      await navigate(`${proxy.url}/channel`)
      proxy.pace({ downBytesPerSecond: 1_000_000, upBytesPerSecond: 1_000_000, queueBytes: 64 * 1024 })
      await page.click('#channel-test-publishes-behind')
      let behind: SlowLinkState['publishesBehind'] = null
      await autoRetry(
        async () => {
          behind = (await getResult<SlowLinkState>('#channel-state')).publishesBehind
          expect(behind).not.toBe(null)
        },
        { timeout: 45_000 },
      )
      // An event goes whole: 4 MiB in base64 takes about 5.6 s here.
      const upToMs = JSON.parse(process.env.PUBLIC_ENV__CHANNEL_TRANSPORTS!).includes('sse') ? 14_000 : 4000
      expect(behind!.pingMs).lessThan(upToMs)
      expect(behind!.publishesAhead).toBe(1)
      expect(behind!.closed).toBe('0')
      expect(behind!.closeMs).lessThan(upToMs)
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
