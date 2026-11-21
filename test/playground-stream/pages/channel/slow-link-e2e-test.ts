export { testSlowLink }

import { Worker } from 'node:worker_threads'
import { page, test, expect, autoRetry, getServerUrl } from '@brillout/test-e2e'
import { navigate, withPacedLink, getResult, countConnections, serverArrayBuffers, sleep } from '../../e2e-utils'
import { withNetworkConditions } from '../../../utils'

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
    abortMs: number
  } | null
  lossyTicker: { received: number; last: number | undefined; worstMs: number; closed: string | null } | null
}

// Preview only: the page loads through a proxy in the test process, which Vite's HMR socket would share.
function testSlowLink(isDev: boolean) {
  if (isDev) return
  const upgrades = JSON.parse(process.env.PUBLIC_ENV__CHANNEL_TRANSPORTS!).length > 1

  test(upgrades
    ? "channel: on a 1 MB/s link, once the page moved from SSE to a WebSocket, a channel's acknowledged sends beside another channel's download answer as on a WebSocket from the start"
    : "channel: on a 1 MB/s link, a channel's acknowledged sends beside another channel's download answer, on the connection they started on", async () => {
    await withPacedLink(
      { downBytesPerSecond: 1_000_000, upBytesPerSecond: 1_000_000, queueBytes: 64 * 1024 },
      async () => {
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
      },
    )
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
    // In 40 ms bursts, as a cell link delivers: a wire's first two pieces reach the page together.
    await withPacedLink(
      { downBytesPerSecond: 125_000, upBytesPerSecond: 125_000, queueBytes: 64 * 1024, burstMs: 40 },
      async () => {
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
      },
    )
  })

  // Up to three bodies still wait when the server closes: about 12.6 s, and 16.8 s on SSE.
  test("channel: on a 1 MB/s link, publishes falling behind don't hold up another channel's answer, the answer to the page's own publish costs it none of them, a graceful close delivers them all, and an abort ends at once without them", async () => {
    await withPacedLink(
      { downBytesPerSecond: 1_000_000, upBytesPerSecond: 1_000_000, queueBytes: 64 * 1024 },
      async () => {
        await page.click('#channel-test-publishes-behind')
        let behind: SlowLinkState['publishesBehind'] = null
        await autoRetry(
          async () => {
            behind = (await getResult<SlowLinkState>('#channel-state')).publishesBehind
            expect(behind).not.toBe(null)
          },
          { timeout: 55_000 },
        )
        // An event goes whole: 4 MiB in base64 takes about 5.6 s here.
        const upToMs = JSON.parse(process.env.PUBLIC_ENV__CHANNEL_TRANSPORTS!).includes('sse') ? 14_000 : 4000
        expect(behind!.pingMs).lessThan(upToMs)
        expect(behind!.publishesAhead).toBe(1)
        expect(behind).deep.equal({ ...behind!, closed: '0', received: 5, closeErr: 'none' })
        expect(behind!.abortErr).not.toBe('none')
        // Of the three publishes the abort comes behind, only the one already going out may arrive before it.
        expect(behind!.abortReceived).lessThan(2)
        expect(behind!.abortMs).lessThan(upToMs)
      },
    )
  })

  // 240 KB/s of 4 KiB messages for 5 s: the link holds a second of them at most, not all of them in turn.
  test('channel: on a 125 kB/s link, a lossy broadcast faster than the link replaces what waits, so the newest stays recent and the last arrives', async () => {
    await withPacedLink({ downBytesPerSecond: 125_000, upBytesPerSecond: 125_000, queueBytes: 64 * 1024 }, async () => {
      await page.click('#channel-test-lossy-ticker')
      let ticker: SlowLinkState['lossyTicker'] = null
      await autoRetry(
        async () => {
          ticker = (await getResult<SlowLinkState>('#channel-state')).lossyTicker
          expect(ticker).not.toBe(null)
        },
        { timeout: 40_000 },
      )
      expect(ticker!.last).toBe(299)
      expect(ticker!.received).lessThan(300)
      expect(ticker!.worstMs).lessThan(3000)
    })
  })

  // 120 messages of 4 KiB at 60/s, then the server closes the broadcast: whatever was replaced, the last arrives first.
  test('channel: on a 125 kB/s link, a lossy broadcast closed gracefully delivers its newest message before it closes', async () => {
    await withPacedLink(
      { downBytesPerSecond: 125_000, upBytesPerSecond: 125_000, queueBytes: 64 * 1024, delayMs: 25 },
      async () => {
        await page.click('#channel-test-lossy-ticker-close')
        let ticker: SlowLinkState['lossyTicker'] = null
        await autoRetry(
          async () => {
            ticker = (await getResult<SlowLinkState>('#channel-state')).lossyTicker
            expect(ticker).not.toBe(null)
          },
          { timeout: 40_000 },
        )
        expect(ticker).deep.equal({ ...ticker!, last: 119, closed: 'none' })
      },
    )
  })

  // 60/s of 100 B for 5 s over 450 ms each way: a link that keeps up, whose confirmations take 900 ms. A pause of the
  // whole machine about as long as `LOSSY_CONFIRM_SLACK_MS` (300 ms) delays them past what the server allows a page that
  // keeps up, and that run may lose messages. A run that lost some while a thread of the test's own woke 100 ms late or
  // more, a stall of the machine, runs again, up to three times in all; any other run's count is the card's.
  test('channel: on a 900 ms round trip that keeps up, a lossy broadcast delivers every message', async () => {
    await withPacedLink(
      { downBytesPerSecond: 12_500_000, upBytesPerSecond: 12_500_000, queueBytes: 256 * 1024, delayMs: 450 },
      async () => {
        let run = await whileTimingStalls(runSmallTicker)
        for (let runs = 1; runs < 3 && run.result.received < 300 && run.stalledMs >= 100; runs++)
          run = await whileTimingStalls(runSmallTicker)
        expect(run.result).deep.equal({ ...run.result, received: 300, last: 299 })
      },
    )
  })

  test('channel: on a 125 kB/s uplink, a 2 MiB upload arrives and keeps the download coming on the connection it started on, and a channel opened meanwhile opens on it', async () => {
    await withPacedLink(
      { downBytesPerSecond: 10_000_000, upBytesPerSecond: 125_000, queueBytes: 64 * 1024 },
      async () => {
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

/** Runs `run` while a thread of the test's own wakes every 10 ms, and returns its result with the longest that thread
 *  overslept meanwhile: a stall of the machine, which the channel, running in other processes and threads, can't cause. */
async function whileTimingStalls<T>(run: () => Promise<T>): Promise<{ result: T; stalledMs: number }> {
  const timer = new Worker(
    `const { parentPort } = require('node:worker_threads')
    let last = performance.now()
    let stalledMs = 0
    setInterval(() => {
      const now = performance.now()
      stalledMs = Math.max(stalledMs, now - last - 10)
      last = now
    }, 10)
    parentPort.on('message', () => parentPort.postMessage(stalledMs))`,
    { eval: true },
  )
  try {
    const result = await run()
    const stalledMs = await new Promise<number>((resolve) => {
      timer.once('message', resolve)
      timer.postMessage(null)
    })
    return { result, stalledMs }
  } finally {
    await timer.terminate()
  }
}
