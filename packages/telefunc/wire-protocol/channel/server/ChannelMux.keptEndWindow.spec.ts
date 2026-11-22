// How long the server keeps what it knows of a session that left it, on the real mux and a fake clock: from the time it
// decided, for `keptEndWindowOf` its two knobs, written once. A reopen it answers changes nothing of that time; one at
// or after the instant finds no record, and is answered as a session the server doesn't have. One timer frees the
// records, oldest first.

import { afterAll, beforeAll, expect, test, vi } from 'vitest'
import { CHANNEL_PING_INTERVAL_MS, CHANNEL_RECONNECT_TIMEOUT_MS } from '../../constants.js'
import { encode } from '../../wire/frame.js'
import { keptEndWindowOf } from '../../wire/pingDeadline.js'
import { getChannelMux } from './ChannelMux.js'
import { ServerChannel } from './ServerChannel.js'
import { endedSession, openWebSocket, reopen } from './serverPage.fixture.js'

/** The time a session's record is kept from the decision, at the default knobs. */
const WINDOW = keptEndWindowOf(CHANNEL_PING_INTERVAL_MS, CHANNEL_RECONNECT_TIMEOUT_MS)

beforeAll(() => void vi.useFakeTimers({ toFake: ['setTimeout', 'clearTimeout', 'performance'] }))
afterAll(() => void vi.useRealTimers())

const at = async (instant: number) => vi.advanceTimersByTimeAsync(instant - performance.now())

test('at the default knobs, a reopen 1 ms before the instant reads the end, and one at the instant finds no record', async () => {
  const before = await endedSession()
  const atInstant = await endedSession()
  await at(before.decidedAt + WINDOW - 1)
  const early = await reopen(before.sessionId, 2)
  await at(atInstant.decidedAt + WINDOW)
  const late = await reopen(atInstant.sessionId, 2)
  expect([early, late]).toEqual(['unreadable', 'took'])
})

test("a reopen the end answers doesn't keep it longer: one before the instant reads the end, a second at the instant, less than one window after the first, finds no record", async () => {
  const { sessionId, decidedAt } = await endedSession()
  await at(decidedAt + WINDOW - 1)
  const first = await reopen(sessionId, 2)
  await at(decidedAt + WINDOW)
  const second = await reopen(sessionId, 3)
  expect([first, second]).toEqual(['unreadable', 'took'])
})

test('the reopens the end answers raise its highest attempt and write no record: an older attempt read after a newer one takes nothing, and a reopen answered across the instant leaves none past it', async () => {
  const { sessionId, decidedAt } = await endedSession()
  await at(decidedAt + WINDOW - 5)
  const newer = await reopen(sessionId, 3)
  const older = await reopen(sessionId, 2)
  // A channel the next reopen names: the end it is given runs the app's callback, which sees the clock pass the instant.
  const named = new ServerChannel<string, string>()
  getChannelMux().registerChannel(named)
  const during: string[] = []
  named.onClose(() => {
    vi.advanceTimersByTime(2)
    during.push(performance.now() >= decidedAt + WINDOW ? 'past the instant' : 'before it')
  })
  await at(decidedAt + WINDOW - 1)
  const across = await reopen(sessionId, 5, [{ id: named.id, ix: 0 }])
  const after = await reopen(sessionId, 6)
  expect({ newer, older, across, during, after }).toEqual({
    newer: 'unreadable',
    older: 'transient',
    across: 'unreadable',
    during: ['past the instant'],
    after: 'took',
  })
})

test('records leaving at 100 a second are freed by one timer, each visited once as it goes and once ahead', async () => {
  const sessions = Reflect.get(getChannelMux(), 'sessions')
  if (typeof sessions !== 'object' || sessions === null) throw new Error('no registry')
  // The registry's records, counted as they are visited: each step of an iteration over them.
  let visits = 0
  class CountedMap<K, V> extends Map<K, V> {
    override *[Symbol.iterator](): MapIterator<[K, V]> {
      for (const entry of super.entries()) {
        visits++
        yield entry
      }
    }
  }
  const kept: unknown = Reflect.get(sessions, 'kept')
  if (!(kept instanceof Map)) throw new Error('no records')
  // The rows before leave theirs, in their order.
  Reflect.set(sessions, 'kept', new CountedMap(kept))
  const departures = async (count: number) => {
    for (let i = 0; i < count; i++) {
      const ws = openWebSocket()
      await ws.mux.onWireMessage(ws.wire, encode.reconcile({ sessionId: crypto.randomUUID(), attempt: 1, open: [] }))
      ws.close('transient')
      await vi.advanceTimersByTimeAsync(10)
    }
  }
  // Filled to the window's steady state, then measured over 10 s of the same churn.
  await departures(Math.ceil(WINDOW / 10))
  const counted = Reflect.get(sessions, 'kept')
  if (!(counted instanceof Map)) throw new Error('no records')
  const heldBefore = counted.size
  visits = 0
  const timersBefore = vi.getTimerCount()
  await departures(1_000)
  const timers = vi.getTimerCount()
  const freed = heldBefore + 1_000 - counted.size
  expect({ freed: freed >= 990, visitsPerFreed: visits / freed <= 2, timers: timers - timersBefore <= 1 }).toEqual({
    freed: true,
    visitsPerFreed: true,
    timers: true,
  })
})
