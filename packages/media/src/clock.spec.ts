import { afterEach, describe, expect, it, vi } from 'vitest'
import { MemberClock } from './clock.js'
import { ticksSince, wallClock } from './wire.js'

/** ITU-R BT.1359-1: sound from 45 ms early to 125 ms late of its picture goes unnoticed. */
const UNDETECTABLE = { early: 45, late: -125 }

describe('MemberClock', () => {
  afterEach(() => vi.restoreAllMocks())

  // A clock that runs slow makes each unit seem later than the one before: a least that never ages out goes stale.
  it.each([100, -100])(
    'keeps a member’s sound and picture in step for 10 minutes while its clock drifts %i ppm',
    (ppm) => {
      let now = 0
      vi.spyOn(performance, 'now').mockImplementation(() => now)
      const clock = new MemberClock()
      // Each unit as `AudioReceiver` and `VideoReceiver` schedule it: due its earliest arrival and the sound's buffer.
      const bufferMs = 60
      const minutes = 10
      const random = seeded(1)
      const units: { kind: 'audio' | 'video'; captured: number; arrives: number }[] = []
      for (let t = 0; t < minutes * 60_000; t += 20) units.push(unit('audio', t, 20 + random() * 30))
      for (let t = 0; t < minutes * 60_000; t += 1000 / 30) units.push(unit('video', t, 30 + random() * 40))
      units.sort((a, b) => a.arrives - b.arrives)

      let heardAfter: number | null = null
      let late = 0
      const offsets: number[] = []
      for (const { kind, captured, arrives } of units) {
        now = arrives
        clock.arrived(captured)
        const due = (clock.earliestArrival(captured) + bufferMs * 10) >>> 0
        const wait = Math.max(0, ticksSince(due, wallClock()))
        if (ticksSince(due, wallClock()) < 0) late++
        const after = ticksSince(wallClock(), captured) + wait
        if (kind === 'audio') heardAfter = after
        else if (heardAfter !== null) offsets.push(after - heardAfter)
      }

      expect(Math.min(...offsets)).toBeGreaterThanOrEqual(UNDETECTABLE.late)
      expect(Math.max(...offsets)).toBeLessThanOrEqual(UNDETECTABLE.early)
      // The clock follows the drift: what comes in its usual time is never due before it comes.
      expect(late / units.length).toBeLessThan(0.01)

      /** A unit captured at `t` ms on the member's clock, `ppm` off this page's, that arrives `transitMs` later here. */
      function unit(kind: 'audio' | 'video', t: number, transitMs: number) {
        const captured = Math.round((performance.timeOrigin + t * (1 + ppm * 1e-6)) * 10) >>> 0
        return { kind, captured, arrives: t + transitMs }
      }
    },
  )
})

/** Deterministic numbers in [0, 1). */
function seeded(seed: number): () => number {
  let state = seed
  return () => {
    state = (state * 1_103_515_245 + 12_345) % 2 ** 31
    return state / 2 ** 31
  }
}
