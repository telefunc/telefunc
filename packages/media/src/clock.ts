export { audioContext, contextTimeOf, MemberClock }

import { ticksSince, wallClock } from './wire.js'

let context: AudioContext | null = null

/** The page's one audio context, at Opus's rate. A browser starts it suspended until the page is used, so each call
 *  asks it to run. */
function audioContext(): AudioContext {
  context ??= new AudioContext({ sampleRate: 48_000, latencyHint: 'interactive' })
  if (context.state === 'suspended') void context.resume().catch(() => {})
  return context
}

/** The context's time, in seconds, at which a sample plays at wall-clock tick `tick`. The output's latency comes after,
 *  the same for every sample. */
function contextTimeOf(audio: AudioContext, tick: number): number {
  return audio.currentTime + ticksSince(tick, wallClock()) / 1000
}

/** How long one member's units take to arrive, up to the gap between its clock and this page's: the least over
 *  `MemberClock.WINDOW_MS` of arrival less capture time, so a clock that drifts is followed. Its audio and video share
 *  it, which keeps them in step. */
class MemberClock {
  /** Long enough to hold a unit that waited in no queue, short enough that clocks 100 ppm apart drift 1 ms in it. */
  static readonly WINDOW_MS = 10_000
  private least = Number.POSITIVE_INFINITY
  private leastAt = 0
  private runnerUp = Number.POSITIVE_INFINITY

  /** A unit captured at tick `captured` arrived now. Returns how much later than the least it is, in ms. */
  arrived(captured: number): number {
    const now = performance.now()
    const transit = ticksSince(wallClock(), captured)
    // The least of two halves of the window: the older half's ages out, the newer half's takes its place.
    if (now - this.leastAt > MemberClock.WINDOW_MS / 2) {
      this.least = this.runnerUp
      this.runnerUp = Number.POSITIVE_INFINITY
      this.leastAt = now
    }
    this.runnerUp = Math.min(this.runnerUp, transit)
    this.least = Math.min(this.least, transit)
    return transit - this.least
  }

  /** The wall-clock tick here at which a unit captured at `captured` arrives if it waits in no queue. */
  earliestArrival(captured: number): number {
    return (captured + Math.round(this.least * 10)) >>> 0
  }
}
