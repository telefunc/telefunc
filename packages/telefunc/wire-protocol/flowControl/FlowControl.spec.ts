import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'

import { FlowControl } from './FlowControl.js'
import type { FlowControlOptions } from './FlowControl.js'
import { BDP_PING_MIN_INTERVAL_MS, CREDIT_WINDOW_INITIAL_BYTES, CREDIT_WINDOW_MAX_BYTES } from '../constants.js'

beforeEach(() => vi.useFakeTimers({ now: 1_000_000 }))
afterEach(() => vi.useRealTimers())

/** What the wire delivered before an ack: right up to it, as a queue does. */
const upToAck = () => Infinity

// The async `_waitForGates` re-check loop adds a couple of microtask hops
// between a waiter-drain and the outer `decrement` Promise resolving. Tests
// that observe post-drain resolution must flush enough microtasks to settle
// the chain — `Promise.resolve()` once isn't enough.
async function flushMicrotasks(): Promise<void> {
  for (let i = 0; i < 6; i++) await Promise.resolve()
}

// Spy emit — captures calls so tests can assert which wire frames the flow
// would have produced. Each test starts with a fresh `Emit` so the spec
// doesn't need cross-test reset hygiene.
type Emit = FlowControlOptions & {
  windowCalls: number[]
  msgWindowCalls: number[]
  bdpPingCalls: number
  /** The number of the last probe sent. */
  probe: number
}
function makeEmit(): Emit {
  const e: Emit = {
    windowCalls: [],
    msgWindowCalls: [],
    bdpPingCalls: 0,
    probe: 0,
    byteWindowUpdate(b) {
      e.windowCalls.push(b)
    },
    msgWindowUpdate(c) {
      e.msgWindowCalls.push(c)
    },
    bdpPing(probe) {
      e.bdpPingCalls++
      e.probe = probe
    },
    backlog: () => 0,
  }
  return e
}
function makeFlow() {
  const emit = makeEmit()
  return { emit, flow: fitted(new FlowControl(emit)) }
}
/** Fitted to the default replay buffers, which allow the largest window either way. */
function fitted(flow: FlowControl): FlowControl {
  flow.fitReplays(CREDIT_WINDOW_MAX_BYTES, CREDIT_WINDOW_MAX_BYTES)
  return flow
}

describe('FlowControl — sender-side credit', () => {
  // The advertised value starts at INITIAL — pessimistic until the peer's BDP
  // estimator decides to grow. Both sides agree on this initial floor.
  it('initial window equals CREDIT_WINDOW_INITIAL_BYTES', () => {
    const { flow } = makeFlow()
    expect(flow.byteWindow).toBe(CREDIT_WINDOW_INITIAL_BYTES)
  })

  // Decrement with credit remaining returns void → caller's `await` resolves
  // synchronously (one microtask), no real wait. This is the fast path.
  it('decrement returns void when credit remains', () => {
    const { flow } = makeFlow()
    expect(flow.decrement(1024)).toBeUndefined()
  })

  // When the decrement zeroes-or-crosses-below credit, decrement returns a
  // Promise → caller's next `await` blocks until the peer's WINDOW arrives.
  // The gate is `_peerWindow > 0`, so exactly draining to 0 also blocks.
  it('decrement returns a Promise once credit is depleted', () => {
    const { flow } = makeFlow()
    expect(flow.decrement(CREDIT_WINDOW_INITIAL_BYTES - 1)).toBeUndefined()
    const gate = flow.decrement(1) // drains the final byte to 0
    expect(gate).toBeInstanceOf(Promise)
  })
})

describe('FlowControl — receiver-side consumption (byte axis)', () => {
  // Below the W/4 threshold: tick the counter but emit no WINDOW frame.
  it('does not emit WINDOW below the W/4 threshold', () => {
    const { flow, emit } = makeFlow()
    const sub = Math.floor(CREDIT_WINDOW_INITIAL_BYTES / 4) - 1
    flow.onConsumed(sub)
    expect(emit.windowCalls).toHaveLength(0)
  })

  // At the threshold, emit WINDOW(window). Counter resets so the next refresh
  // requires another W/4 of consumption.
  it('emits WINDOW at the W/4 threshold and resets the counter', () => {
    const { flow, emit } = makeFlow()
    const quarter = Math.floor(CREDIT_WINDOW_INITIAL_BYTES / 4)
    flow.onConsumed(quarter)
    expect(emit.windowCalls).toEqual([quarter + CREDIT_WINDOW_INITIAL_BYTES])
    flow.onConsumed(quarter - 1)
    expect(emit.windowCalls).toEqual([quarter + CREDIT_WINDOW_INITIAL_BYTES])
  })

  // Counter accumulates across calls.
  it('emits WINDOW once the cumulative quarter is reached across multiple calls', () => {
    const { flow, emit } = makeFlow()
    const quarter = Math.floor(CREDIT_WINDOW_INITIAL_BYTES / 4)
    const chunk = Math.floor(quarter / 4)
    flow.onConsumed(chunk)
    flow.onConsumed(chunk)
    flow.onConsumed(chunk)
    expect(emit.windowCalls).toHaveLength(0)
    flow.onConsumed(chunk + 4)
    expect(emit.windowCalls).toEqual([4 * chunk + 4 + CREDIT_WINDOW_INITIAL_BYTES])
  })

  it('byte-refresh threshold scales with the adaptive window', () => {
    const { flow, emit } = makeFlow()
    flow.onReceived(1)
    flow.onReceived(CREDIT_WINDOW_INITIAL_BYTES)
    flow.onPingAck({ probe: emit.probe, backlog: 0, pathRtt: Infinity }, upToAck)
    expect(flow.byteWindow).toBe(CREDIT_WINDOW_INITIAL_BYTES * 2)
    // BDP growth itself emits a WINDOW with the new value; reset the spy so the
    // consumption assertions below are clean.
    emit.windowCalls.length = 0

    const oldQuarter = Math.floor(CREDIT_WINDOW_INITIAL_BYTES / 4)
    flow.onConsumed(oldQuarter)
    expect(emit.windowCalls).toHaveLength(0)
    const newQuarter = Math.floor((CREDIT_WINDOW_INITIAL_BYTES * 2) / 4)
    flow.onConsumed(newQuarter - oldQuarter)
    expect(emit.windowCalls).toEqual([newQuarter + CREDIT_WINDOW_INITIAL_BYTES * 2])
  })
})

describe('FlowControl — BDP integration', () => {
  // The sample is bytes arriving *between* ping-send and ack-arrival. Cycle
  // fires the probe (one onReceived), accumulates (a second), settles.
  const cycle = ({ flow, emit }: { flow: FlowControl; emit: Emit }, sampleBytes: number) => {
    flow.onReceived(1)
    flow.onReceived(sampleBytes)
    flow.onPingAck({ probe: emit.probe, backlog: 0, pathRtt: Infinity }, upToAck)
  }

  it('emits BDP_PING on the first onReceived', () => {
    const { flow, emit } = makeFlow()
    flow.onReceived(1024)
    expect(emit.bdpPingCalls).toBe(1)
  })

  it('does not emit a second BDP_PING while one is in flight', () => {
    const { flow, emit } = makeFlow()
    flow.onReceived(1024)
    flow.onReceived(1024)
    expect(emit.bdpPingCalls).toBe(1)
  })

  it('grows the byte window when sample saturates', () => {
    const { flow, emit } = makeFlow()
    cycle({ flow, emit }, CREDIT_WINDOW_INITIAL_BYTES)
    expect(flow.byteWindow).toBe(CREDIT_WINDOW_INITIAL_BYTES * 2)
    expect(emit.windowCalls).toContain(CREDIT_WINDOW_INITIAL_BYTES * 2)
  })

  it('does not grow or emit WINDOW on quiet samples', () => {
    const { flow, emit } = makeFlow()
    cycle({ flow, emit }, 100)
    expect(flow.byteWindow).toBe(CREDIT_WINDOW_INITIAL_BYTES)
    expect(emit.windowCalls).toHaveLength(0)
  })

  // Eventually growth stops at the cap so per-channel memory is bounded.
  it('byte-window growth caps at CREDIT_WINDOW_MAX_BYTES', () => {
    const { flow, emit } = makeFlow()
    while (flow.byteWindow < CREDIT_WINDOW_MAX_BYTES) {
      cycle({ flow, emit }, flow.byteWindow)
      vi.advanceTimersByTime(BDP_PING_MIN_INTERVAL_MS)
    }
    expect(flow.byteWindow).toBe(CREDIT_WINDOW_MAX_BYTES)
  })
})

describe('FlowControl — reset (transport reattach)', () => {
  // The BDP estimator's in-flight ping is dropped on reset (its ack rode the
  // prior wire and won't arrive). A fresh ping must be allowed to fire next.
  it('drops the in-flight BDP ping so the next receive can fire a fresh one', () => {
    const { flow, emit } = makeFlow()
    flow.onReceived(1024) // first ping in flight
    expect(emit.bdpPingCalls).toBe(1)
    flow.reattach()
    vi.advanceTimersByTime(BDP_PING_MIN_INTERVAL_MS)
    flow.onReceived(1024)
    expect(emit.bdpPingCalls).toBe(2)
  })
})

describe('FlowControl — shutdown', () => {
  // Channel close: anyone blocked on credit must be released so their Promise
  // settles (lets the caller observe the closed state via the surrounding
  // error path). A stuck await here would leak a hang.
  it('shutdown releases all blocked senders', async () => {
    const { flow } = makeFlow()
    flow.decrement(CREDIT_WINDOW_INITIAL_BYTES)
    const gates = [flow.decrement(100), flow.decrement(100)]
    for (const g of gates) expect(g).toBeInstanceOf(Promise)

    const resolved: boolean[] = [false, false]
    gates.forEach((g, i) => {
      void (g as Promise<void>).then(() => {
        resolved[i] = true
      })
    })

    flow.shutdown()
    await flushMicrotasks()
    expect(resolved).toEqual([true, true])
  })
})
