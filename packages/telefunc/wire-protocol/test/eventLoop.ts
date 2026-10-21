export { mark, ran, timeOutsideGc, longestGapWhile }

type Mark = { wall: number; cpu: NodeJS.CpuUsage }
const mark = (): Mark => ({ wall: performance.now(), cpu: process.threadCpuUsage() })
/** Milliseconds the main thread ran from `from` to `to`: wall time, but no more than its CPU time, which the machine's
 *  load, holding the thread off a CPU, does not add to. */
function ran(from: Mark, to: Mark): number {
  const cpu = to.cpu.user - from.cpu.user + to.cpu.system - from.cpu.system
  return Math.min(to.wall - from.wall, cpu / 1000)
}
/** How long `run` took, less the garbage collections during it: their pauses grow with the live heap, not with what
 *  `run` does. */
async function timeOutsideGc(run: () => Promise<void>): Promise<number> {
  const gc = recordGc()
  const started = performance.now()
  await run()
  const ended = performance.now()
  await gc.stop()
  return ended - started - gc.pausedWithin(started, ended)
}
/** The longest event-loop gap while `run` runs, counting the time the main thread ran in it less the garbage
 *  collections in it, whose pauses grow with the live heap, and what `run` returned. */
async function longestGapWhile<T>(run: () => Promise<T>): Promise<{ gap: number; value: T }> {
  const gc = recordGc()
  const ticks: { from: Mark; to: Mark }[] = []
  let last = mark()
  const timer = setInterval(() => {
    const now = mark()
    ticks.push({ from: last, to: now })
    last = now
  }, 1)
  let value: T
  try {
    value = await run()
    ticks.push({ from: last, to: mark() })
  } finally {
    clearInterval(timer)
    await gc.stop()
  }
  const gap = ticks.reduce(
    (longest, { from, to }) => Math.max(longest, ran(from, to) - gc.pausedWithin(from.wall, to.wall)),
    0,
  )
  return { gap, value }
}
/** The garbage collections from now until `stop()`, which waits for the last one's entry: it arrives after the
 *  collection. */
function recordGc(): { stop(): Promise<void>; pausedWithin(from: number, to: number): number } {
  const collections: PerformanceEntry[] = []
  const observer = new PerformanceObserver((entries) => {
    for (const entry of entries.getEntries()) collections.push(entry)
  })
  observer.observe({ entryTypes: ['gc'] })
  return {
    async stop() {
      await new Promise((resolve) => setTimeout(resolve, 0))
      observer.disconnect()
    },
    /** Milliseconds of collection pauses between two `performance.now()` times. */
    pausedWithin: (from, to) =>
      collections.reduce(
        (total, entry) =>
          total + Math.max(0, Math.min(entry.startTime + entry.duration, to) - Math.max(entry.startTime, from)),
        0,
      ),
  }
}
