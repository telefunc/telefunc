export { staleSignals }

import { performance } from 'node:perf_hooks'

/** How often the pace is adjusted, in ms. */
const TICK = 20
/** The share of the event loop's time it may be busy before stale signals slow down. */
const BUSY = 0.7
/** Signals per second at the start and at the least: a few per tick even when the server is busy. */
const SLOWEST = 100

/**
 * Stale signals, sent as fast as this server can serve the refetches they cause. Each signal makes a client run a
 * telefunction again: when thousands of results change at once (a public feed, a burst of writes), sending every
 * signal at once queues all those runs in front of every other request. So signals go out at once while the event
 * loop has time to spare, and wait their turn when it doesn't: the server keeps answering, and under load results
 * update as often as it can afford, a client's result never staying stale for good.
 *
 * Bun and Deno report no event loop use, so there signals always go out at once.
 */
class StaleSignals {
  private readonly waiting: (() => void)[] = []
  /** Signals per second: rises while signals wait and the event loop has time to spare, halves while it doesn't. */
  private rate = SLOWEST
  /** Signals that may go out now. */
  private allowance = 1
  private last = performance.now()
  private usage = performance.eventLoopUtilization()
  private ticking: ReturnType<typeof setInterval> | undefined
  private measuresLoad = false

  send(signal: () => void): void {
    if (!this.measuresLoad) {
      // Node measures the event loop's time once its loop runs; Bun and Deno never do: there's no pace to keep.
      const { idle, active } = performance.eventLoopUtilization()
      this.measuresLoad = idle + active > 0
      if (!this.measuresLoad) return signal()
    }
    this.refill()
    if (this.waiting.length === 0 && this.allowance >= 1) {
      this.allowance--
      signal()
    } else this.waiting.push(signal)
    if (this.ticking) return
    this.usage = performance.eventLoopUtilization()
    this.ticking = setInterval(() => this.tick(), TICK).unref()
  }

  private tick(): void {
    const usage = performance.eventLoopUtilization()
    const busy = performance.eventLoopUtilization(usage, this.usage).utilization
    this.usage = usage
    if (busy > BUSY) this.rate = Math.max(SLOWEST, this.rate / 2)
    else if (this.waiting.length > 0) this.rate *= 1.5
    this.refill()
    while (this.waiting.length > 0 && this.allowance >= 1) {
      this.allowance--
      this.waiting.shift()!()
    }
    if (this.waiting.length === 0 && busy <= BUSY) {
      clearInterval(this.ticking)
      this.ticking = undefined
    }
  }

  private refill(): void {
    const now = performance.now()
    // Up to one tick's worth saved: a burst after a quiet spell goes out at the pace measured last.
    this.allowance = Math.min(this.allowance + ((now - this.last) * this.rate) / 1000, (this.rate * TICK) / 1000 + 1)
    this.last = now
  }
}

const staleSignals = new StaleSignals()
