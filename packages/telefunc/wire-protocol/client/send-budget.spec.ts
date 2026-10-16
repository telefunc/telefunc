import { afterEach, expect, test, vi } from 'vitest'
import {
  CREDIT_WINDOW_MAX_BYTES,
  WIRE_PIECES_ACK_BYTES,
  WIRE_PROBE_FIRST_BYTES,
  WIRE_PROBE_PAIR_BYTES,
  WIRE_QUEUE_DELAY_MS,
  WIRE_SEND_AHEAD_MIN_BYTES,
} from '../constants.js'
import { PieceSender } from '../pieces.js'
import { SendBudget } from './send-budget.js'
import { Uplink } from './uplink.js'
import { TAG } from '../shared-ws.js'

afterEach(() => void vi.restoreAllMocks())

const data = (bytes: number) => Object.assign(new Uint8Array(bytes), { 0: TAG.BINARY })
const flowControl = (bytes: number) => Object.assign(new Uint8Array(bytes), { 0: TAG.WINDOW })

/** The bytes of the probe by which its sender measured its rate, so it hands the socket what it is sent at once. */
const PROBE = 5_000

/** A budget on a wire whose rate a probe measured, on a clock the test sets. */
function open() {
  const clock = { now: 0 }
  vi.spyOn(performance, 'now').mockImplementation(() => clock.now)
  const uplink = new Uplink()
  const prober = new PieceSender(
    () => {},
    () => 0,
    5_000,
    uplink.link,
  )
  prober.send(data(PROBE))
  prober.acknowledged({ bytes: WIRE_PROBE_FIRST_BYTES + WIRE_PROBE_PAIR_BYTES, heldMs: 0, spanUs: 0 })
  prober.acknowledged({ bytes: PROBE, heldMs: 0, spanUs: 0 })
  const budget = new SendBudget(
    () => {},
    () => 0,
    5_000,
    uplink,
    () => {},
  )
  let sent = 0
  /** Sends a data frame and has the server acknowledge it `spanMs` after the one before it. The allowance, with nothing
   *  unacknowledged. */
  const exchange = (bytes: number, spanMs: number) => {
    budget.send(data(bytes))
    budget.sender.flush() // as its turn ends
    sent += bytes
    clock.now += 10
    expect(budget.acknowledged({ bytes: sent, heldMs: 0, spanUs: spanMs * 1_000 })).toBe(true)
    return budget.sendRoom
  }
  return { budget, uplink, clock, exchange }
}

// The server acknowledges what arrived once `WIRE_PIECES_ACK_BYTES` did, so that much may have arrived unacknowledged.
test('data goes out until the allowance and what the server holds before it acknowledges are sent, and more as it acknowledges', () => {
  const { budget } = open()
  expect(budget.sendRoom).toBe(WIRE_SEND_AHEAD_MIN_BYTES)
  budget.send(data(40_000))
  expect(budget.sendRoom).toBe(WIRE_SEND_AHEAD_MIN_BYTES + WIRE_PIECES_ACK_BYTES - 40_000)
  budget.send(data(40_000))
  budget.send(data(40_000))
  expect(budget.sendRoom).toBeLessThan(0)
  expect(budget.acknowledged({ bytes: 80_000, heldMs: 0, spanUs: 0 })).toBe(true)
  expect(budget.sendRoom).toBeGreaterThan(0)
})

test('the frames that go at once count against the room, and are not dated', () => {
  const { budget } = open()
  budget.send(flowControl(WIRE_PIECES_ACK_BYTES + 1_000))
  expect(budget.sendRoom).toBe(WIRE_SEND_AHEAD_MIN_BYTES - 1_000)
  expect(budget.acknowledged({ bytes: WIRE_PIECES_ACK_BYTES + 1_000, heldMs: 0, spanUs: 0 })).toBe(true)
  expect(budget.sendRoom).toBe(WIRE_SEND_AHEAD_MIN_BYTES)
})

test('the allowance grows by what each acknowledgement covers while no frame waits, to the largest credit window', () => {
  const { exchange } = open()
  expect(exchange(32_000, 0)).toBe(WIRE_SEND_AHEAD_MIN_BYTES + 32_000)
  expect(exchange(32_000, 10)).toBe(WIRE_SEND_AHEAD_MIN_BYTES + 2 * 32_000)
  // A frame past the room would go in pieces as the server acknowledges it: each takes the room, up to 1 MB.
  let allowance = WIRE_SEND_AHEAD_MIN_BYTES + 2 * 32_000
  for (let i = 0; i < 100; i++) allowance = exchange(Math.min(allowance, 1_000_000), 10)
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

// A rate measured while the page sent little lags one it sends more at: the data acknowledged is out of the uplink anyway.
test('a frame that goes at once waits behind none of the data the server acknowledged, however slow the rate measured so far', () => {
  const { budget, uplink, clock, exchange } = open()
  // 10 KB a frame, 100 ms apart: the server took 100 B/ms.
  for (let i = 0; i < 2; i++) exchange(10_000, 100)
  budget.send(data(20_000))
  expect(uplink.waitNow()).toBeGreaterThan(0)
  clock.now += 10
  expect(budget.acknowledged({ bytes: 40_000, heldMs: 0, spanUs: 10_000 })).toBe(true)
  expect(uplink.waitNow()).toBe(0)
})

// A burst under the room keeps a slow uplink as busy as one past it: the frames that go at once wait behind either.
test('the longest wait since an instant is what a frame that went at once after it waited, whether the room ran out or not', () => {
  const { budget, uplink, clock, exchange } = open()
  // 10 KB a frame, 100 ms apart: the server took 100 B/ms.
  for (let i = 0; i < 2; i++) exchange(10_000, 100)
  const start = clock.now
  budget.send(data(20_000))
  budget.sender.flush() // as its turn ends
  expect(budget.sendRoom).toBeGreaterThan(0)
  budget.send(flowControl(100))
  // 20 KB at 100 B/ms: it waited 200 ms, and a frame sent now waits behind its 100 B too.
  expect(uplink.longestWaitSince(start)).toBe(201)
  clock.now += 150
  // The uplink drained 15 KB since: a frame sent now waits 51 ms, the one before still waited 200.
  expect(uplink.waitNow()).toBe(51)
  expect(uplink.longestWaitSince(start)).toBe(200)
  // Since a later instant, only what a frame sent then waits.
  expect(uplink.longestWaitSince(clock.now)).toBe(51)
})

// The server held what arrived before it acknowledged: the acknowledgement came back no later than one it sent at once.
test('an acknowledgement the server held leaves the room as one it sent at once', () => {
  const roomAfter = (heldMs: number) => {
    const { budget, clock, exchange } = open()
    for (let i = 0; i < 20; i++) exchange(32_000, 10)
    budget.send(data(32_000))
    budget.sender.flush() // as its turn ends
    clock.now += 10 + heldMs
    expect(budget.acknowledged({ bytes: 21 * 32_000, heldMs, spanUs: 10_000 })).toBe(true)
    clock.now += 40 - heldMs
    budget.send(data(32_000))
    budget.sender.flush() // as its turn ends
    // The uplink carried it, so how late acknowledgements come back decides what may not have arrived.
    clock.now += 1_000
    return budget.sendRoom
  }
  expect(roomAfter(40)).toBe(roomAfter(0))
})
