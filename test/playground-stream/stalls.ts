export { StallClock, processCpuMs, byMachine, formatStalls, STALL_TICK_MS, type Stall, type StallSource }

/** Which process on a card's path stalled: the page (Chrome's renderer), the server, or the test process, which runs the
 *  paced link. */
type StallSource = 'page' | 'server' | 'test'

/** A stretch in which a process ran none of its timers, and the CPU time it spent across it. */
type Stall = { who: StallSource; endedAt: number; ms: number; cpuMs: number }

const STALL_TICK_MS = 10

/** A stall is the machine's where the process spent at most a quarter of it on the CPU: it was stopped, not computing. */
function byMachine(stall: Stall): boolean {
  return stall.cpuMs <= stall.ms / 4
}

function formatStalls(stalls: Stall[]): string {
  if (stalls.length === 0) return 'none'
  return stalls.map((s) => `${s.who} ${s.ms} ms (${s.cpuMs} ms on the CPU)`).join(', ')
}

/** The CPU time this process has spent, its threads together, in milliseconds. */
function processCpuMs(): number {
  const { user, system } = process.cpuUsage()
  return (user + system) / 1000
}

/** Wakes every `STALL_TICK_MS` and keeps each wake `minMs` or more late as a stall, with the CPU time `cpuMs()` counts
 *  across it. */
class StallClock {
  private readonly who: StallSource
  private readonly minMs: number
  private readonly cpuMs: () => number
  private readonly stalls: Stall[] = []
  private timer: ReturnType<typeof setInterval> | null = null

  constructor(who: StallSource, minMs: number, cpuMs: () => number = processCpuMs) {
    this.who = who
    this.minMs = minMs
    this.cpuMs = cpuMs
  }

  start(): void {
    let last = performance.now()
    let lastCpu = this.cpuMs()
    this.timer = setInterval(() => {
      const now = performance.now()
      const cpu = this.cpuMs()
      if (now - last - STALL_TICK_MS >= this.minMs)
        this.stalls.push({
          who: this.who,
          endedAt: Date.now(),
          ms: Math.round(now - last),
          cpuMs: Math.round(cpu - lastCpu),
        })
      last = now
      lastCpu = cpu
    }, STALL_TICK_MS)
  }

  stop(): Stall[] {
    if (this.timer) clearInterval(this.timer)
    this.timer = null
    return this.stalls
  }
}
