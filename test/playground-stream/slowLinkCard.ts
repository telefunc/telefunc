// A card with a deadline under a slow link states its link, its slack (the longest stall of a process on the path that
// its deadline absorbs) and whether a stop could make its check pass, and runs through `onSlowLink`. The page, the
// server and this test process then each clock where their timers ran 50 ms or more late, and a sleeper beside them, a
// process that only sleeps, clocks where the machine stopped. A stall is the machine's where the sleeper's stops cover
// all of it but less than the slack. A failed check is void where every stall of the slack or more was the machine's,
// and runs again, three runs at most: then the card is "Not run". Any other failed check fails the card. A passed check
// passes, unless a stop could make it pass and the machine stopped for the slack or more: that run is void too. Its page
// is the playground's `/channel` (`withPacedLink`), and its server serves `/api/stalls/start` and `/api/stalls/stop`.
export { onSlowLink }

import { spawn } from 'node:child_process'
import { createInterface } from 'node:readline'
import { page, getServerUrl } from '@brillout/test-e2e'
import { withPacedLink } from './e2e-utils'
import {
  StallClock,
  STALL_TICK_MS,
  isVoid,
  stoppedFor,
  formatStalls,
  stallsOf,
  stopOf,
  type Stall,
  type Stop,
} from './stalls'

declare global {
  interface Window {
    /** The page's clock of its stalls, while a card runs. */
    stallClock?: { stalls: { endedAt: number; ms: number }[]; stop: () => void }
  }
}

/** The shortest stall the clocks keep. */
const STALL_MIN_MS = 50
const RUNS_MAX = 3

/** Runs `run` over `card.link`, then `check` on what it got, by the rule above. Each void or failed run logs its stalls
 *  and the sleeper's stops as JSON (`[slow-link] stalls`), and the card logs its outcome (`[slow-link] outcome`). */
async function onSlowLink<T>(
  card: { link: Parameters<typeof withPacedLink>[0]; slackMs: number; aStopCouldPassIt: boolean },
  run: () => Promise<T>,
  check: (result: T, stalls: Stall[]) => void,
): Promise<void> {
  const { link, slackMs, aStopCouldPassIt } = card
  const voided: string[] = []
  const outcome = (verdict: 'pass' | 'fail' | 'not run', runs: number) =>
    console.log(`[slow-link] outcome ${JSON.stringify({ verdict, runs, slackMs, aStopCouldPassIt, voided })}`)
  for (let runs = 1; runs <= RUNS_MAX; runs++) {
    const { result, stalls, stops } = await withPacedLink(link, () => whileClockingStalls(run))
    const got = `The page got ${JSON.stringify(result)}; the stalls of ${STALL_MIN_MS} ms or more: ${formatStalls(stalls)}; the sleeper's stops: ${formatStalls(stops)}`
    let failure: unknown = null
    try {
      check(result, stalls)
    } catch (err) {
      failure = err
    }
    if (failure === null && !(aStopCouldPassIt && stoppedFor(slackMs, stops))) return outcome('pass', runs)
    console.log(`[slow-link] stalls ${JSON.stringify({ slackMs, stalls, stops })}`)
    if (failure !== null && !isVoid(slackMs, stalls, stops)) {
      outcome('fail', runs)
      throw new Error(`${String(failure)}\n${got}`, { cause: failure })
    }
    voided.push(`run ${runs} ${failure === null ? 'passed' : 'failed'} under the machine's stops: ${got}`)
    console.log(`[slow-link] void ${voided.at(-1)}`)
  }
  outcome('not run', RUNS_MAX)
  throw new Error(
    `Not run: ${RUNS_MAX} runs were void, the machine stopped for ${slackMs} ms or more in each. ${voided.join('; ')}`,
  )
}

/** Runs `run` while the page, the server, this test process and the sleeper clock their stalls of `STALL_MIN_MS` or
 *  more, and returns its result with them. */
async function whileClockingStalls<T>(run: () => Promise<T>): Promise<{ result: T; stalls: Stall[]; stops: Stop[] }> {
  const sleeper = await startSleeper()
  const testClock = new StallClock('test', STALL_MIN_MS)
  testClock.start()
  try {
    await fetch(`${getServerUrl()}/api/stalls/start?minMs=${STALL_MIN_MS}`, { method: 'POST' })
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
    const result = await run()
    const pageStalls = await page.evaluate(() => {
      window.stallClock?.stop()
      return window.stallClock?.stalls ?? []
    })
    const serverStalls = stallsOf(await (await fetch(`${getServerUrl()}/api/stalls/stop`, { method: 'POST' })).json())
    const stalls = [
      ...pageStalls.map((stall) => ({ who: 'page' as const, ...stall })),
      ...serverStalls,
      ...testClock.stop(),
    ]
    return { result, stalls, stops: await sleeper.stop() }
  } finally {
    testClock.stop()
    sleeper.kill()
  }
}

/** A process that runs nothing but a clock of its own stops: none of Telefunc's code, none of this test's. */
async function startSleeper() {
  const child = spawn(
    process.execPath,
    [
      '-e',
      `let last = performance.now()
      console.log('ready')
      setInterval(() => {
        const now = performance.now()
        if (now - last - ${STALL_TICK_MS} >= ${STALL_MIN_MS}) console.log(JSON.stringify({ endedAt: Date.now(), ms: Math.round(now - last) }))
        last = now
      }, ${STALL_TICK_MS})`,
    ],
    { stdio: ['ignore', 'pipe', 'inherit'] },
  )
  const stops: Stop[] = []
  const lines = createInterface({ input: child.stdout })
  await new Promise<void>((resolve, reject) => {
    lines.once('line', () => resolve())
    child.once('error', reject)
  })
  lines.on('line', (line) => stops.push(stopOf(JSON.parse(line))))
  return {
    async stop(): Promise<Stop[]> {
      // Its output is read whole once its pipes close.
      const closed = new Promise((resolve) => child.once('close', resolve))
      child.kill()
      await closed
      return stops
    },
    kill: () => child.kill(),
  }
}
