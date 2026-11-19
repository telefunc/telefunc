export { testSlowLink }

import { page, test, expect, autoRetry, getServerUrl } from '@brillout/test-e2e'
import { navigate, withPacedLink, getResult, countConnections, serverArrayBuffers, sleep } from '../../e2e-utils'
import { newCDPSession, withNetworkConditions } from '../../../utils'
import { parseChannelTransports } from '../../channelTransports'
import { StallClock, STALL_TICK_MS, byMachine, formatStalls, type Stall } from '../../stalls'

declare global {
  interface Window {
    /** The page's clock of its stalls, while a card runs (`clockPageStalls`). */
    stallClock?: { stalls: { endedAt: number; ms: number }[]; stop: () => void }
  }
}

/** The playground's `pingInterval` (+server.ts): a wire whose page or server stops for about that long may be taken for
 *  dead. */
const PING_INTERVAL_MS = 1000
/** The shortest stall the clocks keep. */
const STALL_MIN_MS = 50

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
  const upgrades = parseChannelTransports(process.env.PUBLIC_ENV__CHANNEL_TRANSPORTS).length > 1

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
  // stops for a ping interval may cost the wire, so such a run is void (`onlyTelefuncFails`).
  test('channel: on a 1 Mbit/s link, a 4 MB message from the server arrives on the connection it started on', async () => {
    await onlyTelefuncFails(
      PING_INTERVAL_MS,
      () =>
        // In 40 ms bursts, as a cell link delivers: a wire's first two pieces reach the page together.
        withPacedLink(
          { downBytesPerSecond: 125_000, upBytesPerSecond: 125_000, queueBytes: 64 * 1024, burstMs: 40 },
          () =>
            whileClockingStalls(async () => {
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
            }),
        ),
      (got) => expect(got).deep.equal({ large: 4_000_000, connections: 1 }),
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
        const upToMs = parseChannelTransports(process.env.PUBLIC_ENV__CHANNEL_TRANSPORTS).includes('sse')
          ? 14_000
          : 4000
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

  // 60/s of 100 B for 5 s over 450 ms each way: a link that keeps up, whose confirmations take 900 ms. A stop of a
  // process on the path about as long as `LOSSY_CONFIRM_SLACK_MS` (300 ms) delays them past what the server allows a
  // page that keeps up, and that run may lose messages, so a run with one of 100 ms or more is void (`onlyTelefuncFails`).
  test('channel: on a 900 ms round trip that keeps up, a lossy broadcast delivers every message', async () => {
    await withPacedLink(
      { downBytesPerSecond: 12_500_000, upBytesPerSecond: 12_500_000, queueBytes: 256 * 1024, delayMs: 450 },
      () =>
        onlyTelefuncFails(
          100,
          () => whileClockingStalls(runSmallTicker),
          (ticker) => expect(ticker).deep.equal({ ...ticker, received: 300, last: 299 }),
        ),
    )
  })

  // The server ticks every 100 ms while the page uploads for about 17 s: a gap in the ticks is a stall of a process on
  // the path, never the download waiting for the upload.
  test('channel: on a 125 kB/s uplink, a 2 MiB upload arrives and keeps the download coming on the connection it started on, and a channel opened meanwhile opens on it', async () => {
    await onlyTelefuncFails(
      PING_INTERVAL_MS,
      () =>
        withPacedLink({ downBytesPerSecond: 10_000_000, upBytesPerSecond: 125_000, queueBytes: 64 * 1024 }, () =>
          whileClockingStalls(async () => {
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
          }),
        ),
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

/** Runs `check` on what `run` got. A run whose check fails is void where a process on the path stalled `slackMs` or more
 *  and every such stall was the machine's, the process stopped, not computing (`byMachine`): it runs again, twice at
 *  most, and a third void run fails the card with its stalls, as a machine that can't run it. A check that fails with no
 *  such stall, or with one a process spent computing, fails the card, with the stalls and what the page got. */
async function onlyTelefuncFails<T>(
  slackMs: number,
  run: () => Promise<{ result: T; stalls: Stall[] }>,
  check: (result: T, stalls: Stall[]) => void,
): Promise<void> {
  const voided: string[] = []
  for (let runs = 1; ; runs++) {
    const { result, stalls } = await run()
    try {
      check(result, stalls)
      return
    } catch (err) {
      const past = stalls.filter((stall) => stall.ms >= slackMs)
      const got = `The page got ${JSON.stringify(result)}; the stalls of ${STALL_MIN_MS} ms or more: ${formatStalls(stalls)}`
      if (past.length === 0 || !past.every(byMachine)) throw new Error(`${String(err)}\n${got}`, { cause: err })
      voided.push(`run ${runs}: ${formatStalls(past)}`)
      console.log(`[slow-link] void ${voided.at(-1)}; ${got}`)
      if (runs === 3)
        throw new Error(
          `Three runs were void, each with a process on the path stopped by the machine for ${slackMs} ms or more: ${voided.join('; ')}`,
          { cause: err },
        )
    }
  }
}

/** Waits for `wait`, which a card's poll for its result is, and carries on where it times out: the check then says what
 *  the page got. */
async function settles(wait: () => Promise<unknown>): Promise<void> {
  try {
    await wait()
  } catch {}
}

/** Runs `run` while the page, the server and this test process clock their stalls of `STALL_MIN_MS` or more, and returns
 *  its result with them. */
async function whileClockingStalls<T>(run: () => Promise<T>): Promise<{ result: T; stalls: Stall[] }> {
  const testClock = new StallClock('test', STALL_MIN_MS)
  testClock.start()
  await fetch(`${getServerUrl()}/api/stalls/start?minMs=${STALL_MIN_MS}`, { method: 'POST' })
  const pageClock = await clockPageStalls()
  try {
    const result = await run()
    return { result, stalls: [...(await pageClock.stop()), ...(await serverStalls()), ...testClock.stop()] }
  } finally {
    testClock.stop()
  }
}

async function serverStalls(): Promise<Stall[]> {
  const resp = await fetch(`${getServerUrl()}/api/stalls/stop`, { method: 'POST' })
  return (await resp.json()) as Stall[]
}

/** The page clocks its stalls on its main thread; the CPU time that thread spent across each is read from Chrome
 *  (`Performance.getMetrics`'s `ThreadTime`) about every 50 ms meanwhile, a read before the stall to one after it. */
async function clockPageStalls() {
  const cdp = await newCDPSession()
  await cdp.send('Performance.enable')
  const samples: { at: number; cpuMs: number }[] = []
  const sample = async () => {
    const { metrics } = await cdp.send('Performance.getMetrics')
    const thread = metrics.find((metric) => metric.name === 'ThreadTime')
    if (thread) samples.push({ at: Date.now(), cpuMs: thread.value * 1000 })
  }
  await sample()
  await page.evaluate(
    ({ minMs, tickMs }) => {
      const stalls: { endedAt: number; ms: number }[] = []
      let last = performance.now()
      const timer = setInterval(() => {
        const now = performance.now()
        if (now - last - tickMs >= minMs) stalls.push({ endedAt: Date.now(), ms: Math.round(now - last) })
        last = now
      }, tickMs)
      window.stallClock = { stalls, stop: () => clearInterval(timer) }
    },
    { minMs: STALL_MIN_MS, tickMs: STALL_TICK_MS },
  )
  let sampling = true
  const sampler = (async () => {
    while (sampling) {
      await sample()
      await sleep(50)
    }
  })()
  return {
    async stop(): Promise<Stall[]> {
      const clocked = await page.evaluate(() => {
        window.stallClock?.stop()
        return window.stallClock?.stalls ?? []
      })
      sampling = false
      await sampler
      await sample()
      await cdp.detach()
      return clocked.map(({ endedAt, ms }) => {
        const before = samples.findLast((s) => s.at <= endedAt - ms) ?? samples[0]
        const after = samples.find((s) => s.at >= endedAt) ?? samples[samples.length - 1]
        const cpuMs = before && after ? Math.round(after.cpuMs - before.cpuMs) : ms
        return { who: 'page', endedAt, ms, cpuMs }
      })
    },
  }
}
