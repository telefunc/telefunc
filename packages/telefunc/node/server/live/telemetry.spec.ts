import { afterEach, describe, expect, it } from 'vitest'
import { incrementCounter, snapshotCounters, _resetCountersForTesting } from './telemetry.js'
import type { LiveCounterName } from './telemetry.js'

afterEach(() => _resetCountersForTesting())

describe('telemetry counter registry (§3.J)', () => {
  it('T1.J1 increment accumulates known names; an out-of-set name asserts and does not allocate', () => {
    expect(snapshotCounters()['live.tagHub.publishFailure']).toBe(0)
    incrementCounter('live.tagHub.publishFailure')
    incrementCounter('live.tagHub.publishFailure', 2)
    expect(snapshotCounters()['live.tagHub.publishFailure']).toBe(3)

    const keysBefore = Object.keys(snapshotCounters()).length
    expect(() => incrementCounter('made.up.name' as unknown as LiveCounterName)).toThrow()
    // An INHERITED object key must also be rejected (a naive `name in counters` would accept it).
    expect(() => incrementCounter('toString' as unknown as LiveCounterName)).toThrow()
    expect(() => incrementCounter('constructor' as unknown as LiveCounterName)).toThrow()
    expect(Object.keys(snapshotCounters()).length).toBe(keysBefore) // no allocation, no growth
    expect(Object.keys(snapshotCounters())).not.toContain('toString')
  })

  it('T1.J2 snapshot is a bounded copy of a fixed-size record', () => {
    const snap = snapshotCounters()
    snap['live.tagHub.publishFailure'] = 999
    expect(snapshotCounters()['live.tagHub.publishFailure']).toBe(0) // mutating the copy never leaks

    incrementCounter('live.tagHub.publishFailure', 5)
    incrementCounter('live.tagHub.journalOverflow', 7)
    expect(Object.keys(snapshotCounters()).length).toBe(2) // key set never grows at runtime
  })

  it('T1.J6 the closed name set is exactly the two tagHub counters', () => {
    expect(Object.keys(snapshotCounters()).sort()).toEqual([
      'live.tagHub.journalOverflow',
      'live.tagHub.publishFailure',
    ])
  })
})
