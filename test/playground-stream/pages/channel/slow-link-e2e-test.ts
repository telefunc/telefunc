// The slow-link cards. Each one with a deadline a stall can miss runs through `onSlowLink` (slowLinkCard.ts), which
// states the rule that makes a failed run void rather than a fail.
export { testSlowLink }

import { page, test, expect, autoRetry, getServerUrl } from '@brillout/test-e2e'
import {
  navigate,
  withPacedLink,
  getResult,
  countConnections,
  serverArrayBuffers,
  sleep,
  defined,
} from '../../e2e-utils'
import { withNetworkConditions } from '../../../utils'
import { parseChannelTransports } from '../../channelTransports'
import { onSlowLink } from '../../slowLinkCard'

/** The playground's `pingInterval` (+server.ts): a wire whose page or server stops for about that long may be taken for
 *  dead. */
const PING_INTERVAL_MS = 1000

type SlowLinkState = {
  linkShare: { pingMs: number[]; downloaded: number } | null
  slowUplink: { uploaded: number; opened: string; longestTickGapMs: number } | null
  publishesBehind: {
    pingMs: number
    publishesAhead: number
    closed: string
    received: number
    closeErr: string
    abortReceived: number
    abortErr: string
  } | null
  lossyTicker: { received: number; last: number | undefined; worstMs: number; closed: string | null } | null
}

// Preview only: the page loads through a proxy in the test process, which Vite's HMR socket would share.
function testSlowLink(isDev: boolean) {
  if (isDev) return
  const upgrades = parseChannelTransports(process.env.PUBLIC_ENV__CHANNEL_TRANSPORTS).length > 1

  test(upgrades
    ? "channel: on a 1 MB/s link, once the page moved from SSE to a WebSocket, a channel's acknowledged sends beside another channel's download answer as on a WebSocket from the start"
    : "channel: on a 1 MB/s link, a channel's acknowledged sends beside another channel's download answer, on the connection they started on", async () => {
    await withPacedLink(
      { downBytesPerSecond: 1_000_000, upBytesPerSecond: 1_000_000, queueBytes: 64 * 1024 },
      async () => {
        const connections = upgrades ? null : countConnections()
        await page.click('#channel-test-link-share')
        const linkShare = await autoRetry(
          async () => defined((await getResult<SlowLinkState>('#channel-state')).linkShare, 'the link-share row'),
          { timeout: 50_000 },
        )
        if (upgrades) {
          // The last send goes after the move: ~2 s where the window went back to a WebSocket's, ~7.5 s where it stayed.
          expect(linkShare.pingMs.at(-1)).lessThan(5500)
        } else {
          for (const ms of linkShare.pingMs) expect(ms).lessThan(20_000)
          expect(defined(connections, 'the connection count').stop()).toBe(1)
        }
      },
    )
  })

  // With two transports the page opens an SSE stream, then moves to a WebSocket: wire-e2e-test.ts covers the rest of a
  // slow link there.
  if (upgrades) return

  if (parseChannelTransports(process.env.PUBLIC_ENV__CHANNEL_TRANSPORTS).join() === 'sse')
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

  // 32 s to cross, past the deadline a page waits for an answer to its ping. A process on the path that the machine
  // stops for a ping interval may cost the wire, so such a run is void (`onSlowLink`).
  test('channel: on a 1 Mbit/s link, a 4 MB message from the server arrives on the connection it started on', async () => {
    await onSlowLink(
      {
        // In 40 ms bursts, as a cell link delivers: a wire's first two pieces reach the page together.
        link: { downBytesPerSecond: 125_000, upBytesPerSecond: 125_000, queueBytes: 64 * 1024, burstMs: 40 },
        slackMs: PING_INTERVAL_MS,
        // It checks that the message arrived on one connection: a stop can only make that harder.
        aStopCouldPassIt: false,
      },
      async () => {
        const connections = countConnections()
        await page.click('#wire-large-slow-link')
        const seen: { large: number | null } = { large: null }
        await settles(() =>
          autoRetry(
            async () => {
              seen.large = (await getResult<{ large: number | null }>('#wire-state')).large
              expect(seen.large).toBe(4_000_000)
            },
            { timeout: 55_000 },
          ),
        )
        return { large: seen.large, connections: connections.stop() }
      },
      (got) => expect(got).deep.equal({ large: 4_000_000, connections: 1 }),
    )
  })

  // Up to three bodies of 4 MiB still wait when the server closes: about 12.6 s, 16.8 s on SSE; three of 1 MiB when it
  // aborts.
  test("channel: on a 1 MB/s link, publishes falling behind don't hold up another channel's answer, the answer to the page's own publish costs it none of them, a graceful close delivers them all, and so does an abort", async () => {
    await withPacedLink(
      { downBytesPerSecond: 1_000_000, upBytesPerSecond: 1_000_000, queueBytes: 64 * 1024 },
      async () => {
        await page.click('#channel-test-publishes-behind')
        const behind = await autoRetry(
          async () =>
            defined((await getResult<SlowLinkState>('#channel-state')).publishesBehind, 'the publishes-behind row'),
          { timeout: 55_000 },
        )
        // An event goes whole: 4 MiB in base64 takes about 5.6 s here.
        const upToMs = parseChannelTransports(process.env.PUBLIC_ENV__CHANNEL_TRANSPORTS).includes('sse')
          ? 14_000
          : 4000
        expect(behind.pingMs).lessThan(upToMs)
        expect(behind.publishesAhead).toBe(1)
        // The abort comes behind the three publishes made before it, as the close does.
        expect(behind).deep.include({
          closed: 'clean',
          received: 5,
          closeErr: 'none',
          abortReceived: 3,
          abortErr: 'Abort',
        })
      },
    )
  })

  // 240 KB/s of 4 KiB messages for 5 s: the link holds a second of them at most, not all of them in turn.
  test('channel: on a 125 kB/s link, a lossy broadcast faster than the link replaces what waits, so the newest stays recent and the last arrives', async () => {
    await withPacedLink({ downBytesPerSecond: 125_000, upBytesPerSecond: 125_000, queueBytes: 64 * 1024 }, async () => {
      await page.click('#channel-test-lossy-ticker')
      const ticker = await autoRetry(
        async () => defined((await getResult<SlowLinkState>('#channel-state')).lossyTicker, 'the lossy-ticker row'),
        { timeout: 40_000 },
      )
      expect(ticker.last).toBe(299)
      expect(ticker.received).lessThan(300)
      expect(ticker.worstMs).lessThan(3000)
    })
  })

  // 120 messages of 4 KiB at 60/s, then the server closes the broadcast: whatever was replaced, the last arrives first.
  test('channel: on a 125 kB/s link, a lossy broadcast closed gracefully delivers its newest message before it closes', async () => {
    await withPacedLink(
      { downBytesPerSecond: 125_000, upBytesPerSecond: 125_000, queueBytes: 64 * 1024, delayMs: 25 },
      async () => {
        await page.click('#channel-test-lossy-ticker-close')
        const ticker = await autoRetry(
          async () => defined((await getResult<SlowLinkState>('#channel-state')).lossyTicker, 'the lossy-ticker row'),
          { timeout: 40_000 },
        )
        expect(ticker).deep.equal({ ...ticker, last: 119, closed: 'none' })
      },
    )
  })

  // 60/s of 100 B for 5 s over 450 ms each way: a link that keeps up, whose confirmations take 900 ms. A stop of a
  // process on the path about as long as `LOSSY_CONFIRM_SLACK_MS` (300 ms) delays them past what the server allows a
  // page that keeps up, and that run may lose messages, so a run with one of 100 ms or more may be void (`onSlowLink`).
  test('channel: on a 900 ms round trip that keeps up, a lossy broadcast delivers every message', async () => {
    await onSlowLink(
      {
        link: { downBytesPerSecond: 12_500_000, upBytesPerSecond: 12_500_000, queueBytes: 256 * 1024, delayMs: 450 },
        slackMs: 100,
        // It checks that every message arrived.
        aStopCouldPassIt: false,
      },
      runSmallTicker,
      (ticker) => expect(ticker).deep.equal({ ...ticker, received: 300, last: 299 }),
    )
  })

  // The server ticks every 100 ms while the page uploads for about 17 s: a gap in the ticks is a stall of a process on
  // the path, never the download waiting for the upload.
  test('channel: on a 125 kB/s uplink, a 2 MiB upload arrives and keeps the download coming on the connection it started on, and a channel opened meanwhile opens on it', async () => {
    await onSlowLink(
      {
        link: { downBytesPerSecond: 10_000_000, upBytesPerSecond: 125_000, queueBytes: 64 * 1024 },
        slackMs: PING_INTERVAL_MS,
        // Its tick gap is checked less the longest stall: a stop takes it off.
        aStopCouldPassIt: true,
      },
      async () => {
        const connections = countConnections()
        await page.click('#channel-test-slow-uplink')
        const seen: { slowUplink: SlowLinkState['slowUplink'] } = { slowUplink: null }
        await settles(() =>
          autoRetry(
            async () => {
              seen.slowUplink = (await getResult<SlowLinkState>('#channel-state')).slowUplink
              expect(seen.slowUplink).not.toBe(null)
            },
            { timeout: 55_000 },
          ),
        )
        return { ...seen, connections: connections.stop() }
      },
      ({ slowUplink, connections }, stalls) => {
        expect({ slowUplink, connections }).deep.equal({
          slowUplink: slowUplink && { ...slowUplink, uploaded: 2 * 1024 * 1024, opened: 'answer' },
          connections: 1,
        })
        const longestStallMs = Math.max(0, ...stalls.map((stall) => stall.ms))
        expect((slowUplink?.longestTickGapMs ?? Infinity) - longestStallMs).lessThan(PING_INTERVAL_MS)
      },
    )
  })
}

/** Runs the page's lossy ticker of small messages and returns what the page got. */
async function runSmallTicker() {
  await page.click('#channel-test-lossy-ticker-small')
  // The click clears the last run's result.
  await autoRetry(async () => expect((await getResult<SlowLinkState>('#channel-state')).lossyTicker).toBe(null))
  return autoRetry(
    async () => {
      const { lossyTicker } = await getResult<SlowLinkState>('#channel-state')
      if (!lossyTicker) throw new Error('The ticker is still running')
      return lossyTicker
    },
    { timeout: 40_000 },
  )
}

/** Waits for `wait`, which a card's poll for its result is, and carries on where it times out: the check then says what
 *  the page got. */
async function settles(wait: () => Promise<unknown>): Promise<void> {
  try {
    await wait()
  } catch {}
}
