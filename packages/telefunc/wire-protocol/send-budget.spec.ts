import { afterEach, expect, test, vi } from 'vitest'
import { CREDIT_WINDOW_MAX_BYTES, WIRE_QUEUE_DELAY_MS, WIRE_SEND_AHEAD_MIN_BYTES } from './constants.js'
import { PieceSender } from './pieces.js'
import { SendBudget } from './send-budget.js'
import { TAG } from './shared-ws.js'

afterEach(() => void vi.restoreAllMocks())

const data = (bytes: number) => Object.assign(new Uint8Array(bytes), { 0: TAG.BINARY })
const flowControl = (bytes: number) => Object.assign(new Uint8Array(bytes), { 0: TAG.WINDOW })

/** A budget on a clock the test sets. */
function open() {
  const clock = { now: 0 }
  vi.spyOn(performance, 'now').mockImplementation(() => clock.now)
  const sender = new PieceSender(
    () => {},
    () => 0,
  )
  const budget = new SendBudget(sender)
  let sent = 0
  /** Sends a data frame and has the server acknowledge it `spanMs` after the one before it. The allowance, with nothing
   *  unacknowledged. */
  const exchange = (bytes: number, spanMs: number) => {
    budget.send(data(bytes), 5_000)
    sent += bytes
    clock.now += 10
    expect(budget.acknowledged(sent, 0, spanMs * 1_000)).toBe(true)
    return budget.room
  }
  return { budget, clock, exchange }
}

test('data goes out until the allowance is sent, and more as the server acknowledges it', () => {
  const { budget } = open()
  expect(budget.room).toBe(WIRE_SEND_AHEAD_MIN_BYTES)
  budget.send(data(40_000), 5_000)
  expect(budget.room).toBeGreaterThan(0)
  budget.send(data(40_000), 5_000)
  expect(budget.room).toBeLessThan(0)
  expect(budget.acknowledged(40_000, 0, 0)).toBe(true)
  expect(budget.room).toBeGreaterThan(0)
})

test('the frames that go at once count against the room, and are not dated', () => {
  const { budget } = open()
  budget.send(flowControl(1_000), 5_000)
  expect(budget.room).toBe(WIRE_SEND_AHEAD_MIN_BYTES - 1_000)
  expect(budget.acknowledged(1_000, 0, 0)).toBe(true)
  expect(budget.room).toBe(WIRE_SEND_AHEAD_MIN_BYTES)
})

test('the allowance grows by what each acknowledgement covers while no frame waits, to the largest credit window', () => {
  const { exchange } = open()
  expect(exchange(32_000, 0)).toBe(WIRE_SEND_AHEAD_MIN_BYTES + 32_000)
  expect(exchange(32_000, 10)).toBe(WIRE_SEND_AHEAD_MIN_BYTES + 2 * 32_000)
  let allowance = 0
  for (let i = 0; i < 80; i++) allowance = exchange(1_000_000, 10)
  expect(allowance).toBe(CREDIT_WINDOW_MAX_BYTES)
})

test('the allowance falls while frames wait more than the target, to the least', () => {
  const { exchange } = open()
  for (let i = 0; i < 20; i++) exchange(32_000, 10)
  // Each frame reaches the server 5 ms later than the one before it, and 10 ms after it.
  let allowance = exchange(32_000, 10)
  let i = 0
  while (allowance > WIRE_SEND_AHEAD_MIN_BYTES && i < 200) {
    const before = allowance
    allowance = exchange(32_000, 15)
    i++
    if (i * 5 > WIRE_QUEUE_DELAY_MS + 5) expect(allowance).toBeLessThanOrEqual(before)
  }
  expect(allowance).toBe(WIRE_SEND_AHEAD_MIN_BYTES)
})

test('a wait that stays over the target with the least allowance is the path having slowed, not a queue', () => {
  const { exchange } = open()
  exchange(32_000, 0)
  // The path now takes 200 ms more, for all that follows.
  exchange(32_000, 10 + 200)
  let allowance = 0
  for (let i = 0; i < 20; i++) allowance = exchange(32_000, 10)
  expect(allowance).toBeGreaterThan(2 * WIRE_SEND_AHEAD_MIN_BYTES)
})
