export { StallClock, isVoid, stoppedFor, formatStalls, STALL_TICK_MS, type Stall, type StallSource, type Stop }

/** Which process on a card's path stalled: the page (Chrome's renderer), the server, or the test process, which runs the
 *  paced link. */
type StallSource = 'page' | 'server' | 'test'

/** A stretch in which a process ran none of its timers. */
type Stall = { who: StallSource; endedAt: number; ms: number }

/** A stop of the sleeper, a process beside the run that only sleeps a tick at a time: it stops only when the machine
 *  stops it. */
type Stop = { endedAt: number; ms: number }

const STALL_TICK_MS = 10

/** Whether a run that failed its check is void: a process on the path stalled `slackMs` or more, and the machine made
 *  each such stall (`byMachine`). */
function isVoid(slackMs: number, stalls: Stall[], stops: Stop[]): boolean {
  const past = stalls.filter((stall) => stall.ms >= slackMs)
  return past.length > 0 && past.every((stall) => byMachine(stall, stops, slackMs))
}

/** Whether the machine stopped for `slackMs` or more during the run. */
function stoppedFor(slackMs: number, stops: Stop[]): boolean {
  return stops.some((stop) => stop.ms >= slackMs)
}

/** A stall is the machine's where the sleeper's stops cover all of it but less than `slackMs`, which its deadline
 *  absorbs. */
function byMachine(stall: Stall, stops: Stop[], slackMs: number): boolean {
  const from = stall.endedAt - stall.ms
  // One clock's stops follow one another, so they cover a stall each by its own overlap.
  const covered = stops.reduce(
    (sum, stop) => sum + Math.max(0, Math.min(stop.endedAt, stall.endedAt) - Math.max(stop.endedAt - stop.ms, from)),
    0,
  )
  return stall.ms - covered < slackMs
}

function formatStalls(stalls: (Stall | Stop)[]): string {
  if (stalls.length === 0) return 'none'
  return stalls
    .map((s) => `${'who' in s ? s.who : 'sleeper'} ${s.ms} ms to ${new Date(s.endedAt).toISOString().slice(11, 23)}Z`)
    .join(', ')
}

/** Wakes every `STALL_TICK_MS` and keeps each wake `minMs` or more late as a stall. */
class StallClock {
  private readonly who: StallSource
  private readonly minMs: number
  private readonly stalls: Stall[] = []
  private timer: ReturnType<typeof setInterval> | null = null

  constructor(who: StallSource, minMs: number) {
    this.who = who
    this.minMs = minMs
  }

  start(): void {
    let last = performance.now()
    this.timer = setInterval(() => {
      const now = performance.now()
      if (now - last - STALL_TICK_MS >= this.minMs)
        this.stalls.push({ who: this.who, endedAt: Date.now(), ms: Math.round(now - last) })
      last = now
    }, STALL_TICK_MS)
  }

  stop(): Stall[] {
    if (this.timer) clearInterval(this.timer)
    this.timer = null
    return this.stalls
  }
}
