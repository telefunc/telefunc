import { afterEach, describe, expect, it } from 'vitest'
import {
  reserveLiveQuery,
  LiveQuotaExceededError,
  _resetLimiterForTesting,
  _liveConnectionCountForTesting,
  _liveReservationCountForTesting,
} from './resource-limiter.js'
import { config } from '../../node/server/serverConfig.js'
import { snapshotCounters, _resetCountersForTesting } from '../../node/server/live/telemetry.js'

/** A fake channel recording its `onClose` callbacks so the test can fire permanent close. */
function fakeChannel() {
  const callbacks: Array<() => void> = []
  return {
    onClose(cb: () => void) {
      callbacks.push(cb)
    },
    fireClose() {
      for (const cb of callbacks) cb()
    },
  }
}

afterEach(() => {
  _resetLimiterForTesting()
  _resetCountersForTesting()
  config.channel = {} // drop any cap override → back to the default 100
})

describe('resource limiter — per-connection live-query reservations (§3.F)', () => {
  it('T1.F1 reserve-before-acquire returns a reservation and increments the connection', () => {
    const reservation = reserveLiveQuery('c1')
    expect(typeof reservation.transferToChannel).toBe('function')
    expect(_liveReservationCountForTesting('c1')).toBe(1)
  })

  it('T1.F2/F3/F11/J3 default cap 100 — 100 accepted, 101st typed rejection + counter', () => {
    for (let i = 0; i < 100; i++) reserveLiveQuery('c1')
    expect(_liveReservationCountForTesting('c1')).toBe(100)
    expect(() => reserveLiveQuery('c1')).toThrowError(LiveQuotaExceededError)
    expect(_liveReservationCountForTesting('c1')).toBe(100) // unchanged
    expect(snapshotCounters()['live.limiter.rejected']).toBe(1)
  })

  it('T1.F4 configurable cap', () => {
    config.channel.liveQueryPerConnection = 2
    reserveLiveQuery('c1')
    reserveLiveQuery('c1')
    expect(() => reserveLiveQuery('c1')).toThrowError(LiveQuotaExceededError)
  })

  it('T1.F5 transfer exactly once', () => {
    const reservation = reserveLiveQuery('c1')
    const channel = fakeChannel()
    reservation.transferToChannel(channel)
    expect(() => reservation.transferToChannel(channel)).toThrow()
  })

  it('T1.F6 release exactly once on permanent close', () => {
    const reservation = reserveLiveQuery('c1')
    const channel = fakeChannel()
    reservation.transferToChannel(channel)
    channel.fireClose()
    expect(_liveReservationCountForTesting('c1')).toBe(0)
    channel.fireClose() // second permanent-close fire must not double-release
    expect(_liveReservationCountForTesting('c1')).toBe(0)
  })

  it('T1.F7 reconnect (no onClose) does not release or double-reserve', () => {
    const reservation = reserveLiveQuery('c1')
    reservation.transferToChannel(fakeChannel()) // channel survives a transient disconnect: onClose NOT fired
    expect(_liveReservationCountForTesting('c1')).toBe(1)
  })

  it('T1.F8 unwind on channel-creation failure (explicit release before transfer)', () => {
    const reservation = reserveLiveQuery('c1')
    expect(_liveReservationCountForTesting('c1')).toBe(1)
    reservation.release()
    expect(_liveReservationCountForTesting('c1')).toBe(0)
    // slot reusable
    reserveLiveQuery('c1')
    expect(_liveReservationCountForTesting('c1')).toBe(1)
  })

  it('T1.F9 per-connection isolation', () => {
    config.channel.liveQueryPerConnection = 1
    reserveLiveQuery('a')
    expect(() => reserveLiveQuery('a')).toThrowError(LiveQuotaExceededError)
    expect(() => reserveLiveQuery('b')).not.toThrow() // b is independent of a
    expect(_liveReservationCountForTesting('b')).toBe(1)
  })

  it('T1.F10 release below cap re-opens a slot', () => {
    config.channel.liveQueryPerConnection = 100
    const reservations = Array.from({ length: 100 }, () => reserveLiveQuery('c1'))
    expect(() => reserveLiveQuery('c1')).toThrow()
    reservations[0]!.release()
    expect(() => reserveLiveQuery('c1')).not.toThrow()
    expect(_liveReservationCountForTesting('c1')).toBe(100)
  })

  it('T1.F12 idempotent cleanup after transfer — explicit release + onClose release exactly once', () => {
    config.channel.liveQueryPerConnection = 2
    const reservation = reserveLiveQuery('c1')
    const channel = fakeChannel()
    reservation.transferToChannel(channel)
    expect(_liveReservationCountForTesting('c1')).toBe(1)
    reservation.release() // explicit unwind
    channel.fireClose() // and the channel close — order-independent, must not double-release
    expect(_liveReservationCountForTesting('c1')).toBe(0)
  })

  it('T1.F13 connection state is bounded — no leaked map entries after reserve/release cycles', () => {
    for (let i = 0; i < 50; i++) {
      const reservation = reserveLiveQuery(`conn-${i}`)
      reservation.release()
    }
    expect(_liveConnectionCountForTesting()).toBe(0)
  })
})
