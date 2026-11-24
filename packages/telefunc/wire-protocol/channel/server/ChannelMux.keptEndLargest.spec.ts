// The instant a session's record leaves at the largest knobs the server accepts, whose keep is longer than one timer
// waits: on the real mux and a fake clock, a reopen 1 ms before it reads the end, and one at it finds no record.

import { afterAll, beforeAll, expect, test, vi } from 'vitest'
import { config } from '../../../node/server/serverConfig.js'
import { TIMER_DELAY_MAX_MS } from '../../constants.js'
import { keptEndWindowOf, PING_INTERVAL_MAX_MS } from '../../wire/pingDeadline.js'
import { endedSession, reopen } from './serverPage.fixture.js'

config.channel = { pingInterval: PING_INTERVAL_MAX_MS, reconnectTimeout: TIMER_DELAY_MAX_MS }
const WINDOW = keptEndWindowOf(PING_INTERVAL_MAX_MS, TIMER_DELAY_MAX_MS)

beforeAll(() => void vi.useFakeTimers({ toFake: ['setTimeout', 'clearTimeout', 'performance'] }))
afterAll(() => void vi.useRealTimers())

const at = async (instant: number) => vi.advanceTimersByTimeAsync(instant - performance.now())

test('at the largest knobs, a reopen 1 ms before the instant reads the end, and one at the instant finds no record', async () => {
  expect(WINDOW).toBeGreaterThan(TIMER_DELAY_MAX_MS)
  const before = await endedSession()
  const atInstant = await endedSession()
  await at(before.decidedAt + WINDOW - 1)
  const early = await reopen(before.sessionId, 2)
  await at(atInstant.decidedAt + WINDOW)
  const late = await reopen(atInstant.sessionId, 2)
  expect([early, late]).toEqual(['unreadable', 'took'])
})
