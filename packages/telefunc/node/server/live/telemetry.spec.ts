import { afterEach, describe, expect, it } from 'vitest'
import { incrementCounter, _resetCountersForTesting, _snapshotCountersForTesting } from './telemetry.js'

afterEach(() => _resetCountersForTesting())

describe('internal live-subsystem correctness counters', () => {
  it('increment accumulates the two tagHub counters', () => {
    expect(_snapshotCountersForTesting()['live.tagHub.publishFailure']).toBe(0)
    incrementCounter('live.tagHub.publishFailure')
    incrementCounter('live.tagHub.publishFailure', 2)
    incrementCounter('live.tagHub.journalOverflow', 7)
    const snap = _snapshotCountersForTesting()
    expect(snap['live.tagHub.publishFailure']).toBe(3)
    expect(snap['live.tagHub.journalOverflow']).toBe(7)
  })

  it('the snapshot is a bounded copy — mutating it never leaks, key set is the two fixed counters', () => {
    const snap = _snapshotCountersForTesting()
    snap['live.tagHub.publishFailure'] = 999
    expect(_snapshotCountersForTesting()['live.tagHub.publishFailure']).toBe(0)
    expect(Object.keys(_snapshotCountersForTesting()).sort()).toEqual([
      'live.tagHub.journalOverflow',
      'live.tagHub.publishFailure',
    ])
  })

  it('reset zeroes the counters', () => {
    incrementCounter('live.tagHub.publishFailure', 5)
    _resetCountersForTesting()
    expect(_snapshotCountersForTesting()['live.tagHub.publishFailure']).toBe(0)
  })
})
