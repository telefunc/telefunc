// The records a session registry keeps are freed past their instant: by its one timer, and, on a host that drops the
// timers of a request once it answered it (workerd), by the next record written after the instant. On a fake clock.

import { afterAll, beforeAll, expect, test, vi } from 'vitest'
import { SessionRegistry } from './SessionRegistry.js'

beforeAll(() => void vi.useFakeTimers({ toFake: ['setTimeout', 'clearTimeout', 'performance'] }))
afterAll(() => void vi.useRealTimers())

const KEPT_MS = 250

/** The ids of the records the registry's map holds, whether a lookup sees them or not. */
function held(registry: SessionRegistry): unknown[] {
  const kept: unknown = Reflect.get(registry, 'kept')
  if (!(kept instanceof Map)) throw new Error('no records')
  return [...kept.keys()]
}

// Each row's name comes first, for its title, then whether the host drops the timer.
test.each([
  ['whose timers run', false],
  ['that drops the timers of a request it answered', true],
] as const)(
  'on a host %s, a record past its instant is freed by the next record written, at the latest',
  async (_host, dropsTimers) => {
    const registry = new SessionRegistry({ retiredLeavesAt: () => performance.now() + KEPT_MS })
    // The host drops every timer the registry arms, as workerd drops those of a request it answered, and nothing tells
    // the registry.
    if (dropsTimers) Reflect.set(registry, 'sweep', { arm() {}, disarm() {} })
    const keep = (sessionId: string) => registry.keepEnd(sessionId, 'unreadable', 1, performance.now() + KEPT_MS)
    keep('first')
    await vi.advanceTimersByTimeAsync(KEPT_MS * 2)
    const pastFirst = held(registry)
    keep('second')
    const afterSecond = held(registry)
    await vi.advanceTimersByTimeAsync(KEPT_MS * 2)
    keep('third')
    expect({ pastFirst, afterSecond, afterThird: held(registry) }).toEqual({
      pastFirst: dropsTimers ? ['first'] : [],
      afterSecond: ['second'],
      afterThird: ['third'],
    })
  },
)
