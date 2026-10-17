import { expect, test } from 'vitest'

import { Waits } from './link.js'

/** A seeded generator of numbers in [0, 1). */
function random(seed: number) {
  return () => {
    seed = (seed + 0x6d2b79f5) | 0
    let t = Math.imul(seed ^ (seed >>> 15), 1 | seed)
    t = (t + Math.imul(t ^ (t >>> 7), 61 | t)) ^ t
    return ((t ^ (t >>> 14)) >>> 0) / 2 ** 32
  }
}

test("the longest wait since an instant is the longest of the last minute's noted at or after it, through expiry and compaction", () => {
  const next = random(1)
  const waits = new Waits()
  const noted: { wait: number; at: number }[] = []
  let at = 0
  // Waits that mostly shorten keep many in the history, so a minute's expiry drops enough of them to compact it.
  for (let i = 0; i < 500; i++) {
    at += Math.floor(next() * 720)
    const wait = (500 - i) * 4 + Math.floor(next() * 40)
    waits.note(wait, at)
    noted.push({ wait, at })
    for (let q = 0; q < 5; q++) {
      const since = at - Math.floor(next() * 90_000) + 1_000
      let longest = 0
      for (const each of noted)
        if (each.at >= since && each.at >= at - 60_000 && each.wait > longest) longest = each.wait
      expect(waits.since(since)).toBe(longest)
    }
  }
  expect(at).toBeGreaterThan(170_000)
})
