// The slow link's pacing, on a fake clock: a direction sends what the time since its last tick allows at its rate,
// whether the tick ran on time or late, and an idle link sends no burst once it has data again. Its ticks are driven
// by hand, so a late tick is one the clock moved past without a tick in between.

import { afterEach, beforeEach, describe, expect, test, vi } from 'vitest'
import type * as net from 'node:net'

import { PacedDirection } from './black-hole-proxy'

const RATE = 125_000
const BURST_MS = 5

let now = 0
beforeEach(() => {
  now = 0
  vi.spyOn(performance, 'now').mockImplementation(() => now)
})
afterEach(() => vi.restoreAllMocks())

/** A direction of a 1 Mbit/s link and the bytes its receiver got; its own timer is stopped, and `tick` runs it. */
function link() {
  const direction = new PacedDirection(RATE, 64 * 1024, BURST_MS, 0)
  direction.stop()
  let received = 0
  const to = { destroyed: false, write: (chunk: Buffer) => void (received += chunk.length) } as unknown as net.Socket
  const from = { pause() {}, resume() {} } as unknown as net.Socket
  return {
    send: (bytes: number) => direction.enqueue(from, to, Buffer.alloc(bytes)),
    /** The clock moves `ms`, then the direction's timer fires once. */
    tick(ms: number) {
      now += ms
      ;(direction as any).drain()
    },
    received: () => received,
  }
}

describe("a slow link's pacing", () => {
  test('ticks on time each send the bytes of one tick', () => {
    const l = link()
    l.send(1_000_000)
    for (let i = 1; i <= 10; i++) {
      l.tick(BURST_MS)
      expect(l.received()).toBe((i * RATE * BURST_MS) / 1000)
    }
  })

  test('a tick 500 ms late sends the bytes of the 500 ms, and one 2 s late those of a second', () => {
    const l = link()
    l.send(1_000_000)
    l.tick(BURST_MS)
    const before = l.received()
    l.tick(500)
    expect(l.received() - before).toBe((RATE * 500) / 1000)
    l.tick(2_000)
    expect(l.received() - before).toBe((RATE * 500) / 1000 + RATE)
  })

  test('over 10 s of ticks, some of them up to 300 ms late, it carries its rate within 2 %', () => {
    const l = link()
    l.send(2 * RATE * 10)
    let elapsed = 0
    for (let i = 0; elapsed < 10_000; i++) {
      const ms = i % 7 === 6 ? 300 : BURST_MS
      l.tick(ms)
      elapsed += ms
    }
    expect(Math.abs(l.received() / (elapsed / 1000) - RATE) / RATE).toBeLessThan(0.02)
  })

  test('a link idle for a second sends no burst once it has data again', () => {
    const l = link()
    l.tick(1_000)
    l.send(1_000_000)
    l.tick(BURST_MS)
    expect(l.received()).toBe((RATE * 2 * BURST_MS) / 1000)
  })
})
