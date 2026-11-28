import { expect, test, vi } from 'vitest'
import { DeadlineTimer } from './DeadlineTimer.js'

test('a timer that runs before its time on the performance clock waits again with no negative delay, and fires once the clock is there', () => {
  vi.useFakeTimers({ toFake: ['setTimeout', 'clearTimeout'] })
  // The `performance` clock at each read: when armed; when its timer runs, still before 100; when it arms the next, as
  // the turn worked on, past 100; when that one runs.
  const reads = [0, 99.5, 100.5, 100.5]
  const now = vi.spyOn(performance, 'now').mockImplementation(() => reads.shift() ?? Number.NaN)
  const setTimeoutSpy = vi.spyOn(globalThis, 'setTimeout')
  try {
    let fired = 0
    new DeadlineTimer().arm(100, () => fired++)
    vi.advanceTimersToNextTimer()
    expect(fired).toBe(0)
    vi.advanceTimersToNextTimer()
    expect(fired).toBe(1)
    expect(setTimeoutSpy.mock.calls.map(([, delay]) => delay)).toEqual([100, 0])
    expect(reads).toEqual([])
  } finally {
    setTimeoutSpy.mockRestore()
    now.mockRestore()
    vi.useRealTimers()
  }
})
