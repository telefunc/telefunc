import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'

import { FlowControl } from './flow-control.js'
import type { FlowControlEmit } from './flow-control.js'
import {
  BDP_PING_MIN_INTERVAL_MS,
  CREDIT_MSG_WINDOW_INITIAL,
  CREDIT_WINDOW_INITIAL_BYTES,
  CREDIT_WINDOW_INITIAL_BYTES_BATCH,
  CREDIT_WINDOW_MAX_BYTES,
} from '../constants.js'
import { decode, encode } from '../shared-ws.js'

beforeEach(() => vi.useFakeTimers({ now: 1_000_000 }))
afterEach(() => vi.useRealTimers())

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
type Emit = FlowControlEmit & {
  windowCalls: number[]
  msgWindowCalls: number[]
  sentCalls: [number, number][]
  bdpPingCalls: number
}
function makeEmit(): Emit {
  const e: Emit = {
    windowCalls: [],
    msgWindowCalls: [],
    sentCalls: [],
    bdpPingCalls: 0,
    byteWindowUpdate(b) {
      e.windowCalls.push(b)
    },
    msgWindowUpdate(c) {
      e.msgWindowCalls.push(c)
    },
    sent(bytes, messages) {
      e.sentCalls.push([bytes, messages])
    },
    bdpPing() {
      e.bdpPingCalls++
    },
  }
  return e
}
function makeFlow() {
  const emit = makeEmit()
  return { emit, flow: new FlowControl(emit) }
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
  // The gate is credit > 0, so exactly draining to 0 also blocks.
  it('decrement returns a Promise once credit is depleted', () => {
    const { flow } = makeFlow()
    expect(flow.decrement(CREDIT_WINDOW_INITIAL_BYTES - 1)).toBeUndefined()
    const gate = flow.decrement(1) // drains the final byte to 0
    expect(gate).toBeInstanceOf(Promise)
  })

  // A limit is cumulative: what the receiver has consumed plus its window. Credit is that limit minus what
  // was sent, so a limit that only covers what is already in flight leaves a sender blocked.
  it('a limit wakes a pending sender only once it exceeds what was sent', async () => {
    const { flow } = makeFlow()
    flow.decrement(CREDIT_WINDOW_INITIAL_BYTES)
    const gate = flow.decrement(100)
    expect(gate).toBeInstanceOf(Promise)

    let resolved = false
    void (gate as Promise<void>).then(() => {
      resolved = true
    })

    flow.onPeerByteWindow(CREDIT_WINDOW_INITIAL_BYTES + 100)
    await flushMicrotasks()
    expect(resolved).toBe(false)
    flow.onPeerByteWindow(2 * CREDIT_WINDOW_INITIAL_BYTES)
    await flushMicrotasks()
    expect(resolved).toBe(true)
  })

  // Multiple senders blocked on the same depletion event all wake when WINDOW
  // arrives — catches a "splice but only resolve first" bug.
  it('onPeerByteWindow wakes all blocked senders', async () => {
    const { flow } = makeFlow()
    flow.decrement(CREDIT_WINDOW_INITIAL_BYTES)

    const gates = [flow.decrement(100), flow.decrement(100), flow.decrement(100)]
    for (const g of gates) expect(g).toBeInstanceOf(Promise)

    const resolved: boolean[] = [false, false, false]
    gates.forEach((g, i) => {
      void (g as Promise<void>).then(() => {
        resolved[i] = true
      })
    })

    flow.onPeerByteWindow(2 * CREDIT_WINDOW_INITIAL_BYTES)
    await flushMicrotasks()
    expect(resolved).toEqual([true, true, true])
  })

  // Limits can arrive late or twice: a refresh queued while the wire couldn't take it, one a reattach repeats.
  it('ignores a limit that does not raise the current one', async () => {
    const { flow } = makeFlow()
    flow.onPeerByteWindow(3 * CREDIT_WINDOW_INITIAL_BYTES)
    flow.onPeerByteWindow(2 * CREDIT_WINDOW_INITIAL_BYTES)
    expect(flow.decrement(3 * CREDIT_WINDOW_INITIAL_BYTES - 1)).toBeUndefined()
    expect(flow.decrement(1)).toBeInstanceOf(Promise)
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

  // At the threshold, emit WINDOW(consumed + window). The next refresh requires another W/4 of consumption.
  it('emits WINDOW at the W/4 threshold and again only a quarter later', () => {
    const { flow, emit } = makeFlow()
    const quarter = Math.floor(CREDIT_WINDOW_INITIAL_BYTES / 4)
    flow.onConsumed(quarter)
    expect(emit.windowCalls).toEqual([quarter + CREDIT_WINDOW_INITIAL_BYTES])
    flow.onConsumed(quarter - 1)
    expect(emit.windowCalls).toEqual([quarter + CREDIT_WINDOW_INITIAL_BYTES])
    flow.onConsumed(1)
    expect(emit.windowCalls).toEqual([quarter + CREDIT_WINDOW_INITIAL_BYTES, 2 * quarter + CREDIT_WINDOW_INITIAL_BYTES])
  })

  // Consumption accumulates across calls.
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
    flow.onPingAck()
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
  const cycle = (flow: FlowControl, sampleBytes: number) => {
    flow.onReceived(1)
    flow.onReceived(sampleBytes)
    flow.onPingAck()
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
    cycle(flow, CREDIT_WINDOW_INITIAL_BYTES)
    expect(flow.byteWindow).toBe(CREDIT_WINDOW_INITIAL_BYTES * 2)
    expect(emit.windowCalls).toContain(CREDIT_WINDOW_INITIAL_BYTES * 2)
  })

  it('does not grow or emit WINDOW on quiet samples', () => {
    const { flow, emit } = makeFlow()
    cycle(flow, 100)
    expect(flow.byteWindow).toBe(CREDIT_WINDOW_INITIAL_BYTES)
    expect(emit.windowCalls).toHaveLength(0)
  })

  // Eventually growth stops at the cap so per-channel memory is bounded.
  it('byte-window growth caps at CREDIT_WINDOW_MAX_BYTES', () => {
    const { flow } = makeFlow()
    while (flow.byteWindow < CREDIT_WINDOW_MAX_BYTES) {
      cycle(flow, flow.byteWindow)
      vi.advanceTimersByTime(BDP_PING_MIN_INTERVAL_MS)
    }
    expect(flow.byteWindow).toBe(CREDIT_WINDOW_MAX_BYTES)
  })
})

describe('FlowControl — reattach', () => {
  // Credit is cumulative, so a reattach keeps it: resetting it to the initial window let a sender run a window
  // ahead of what was in flight, and stalled one whose receiver waits for a quarter of its grown window. The
  // receive window grown by BDP is kept too (the link's BDP doesn't change across transport hiccups).
  it('keeps credit and the grown receive window, and advertises the limits and the totals again', () => {
    const { flow, emit } = makeFlow()
    // Grow W via BDP first — fire ping, accumulate saturating sample, settle.
    flow.onReceived(1)
    flow.onReceived(CREDIT_WINDOW_INITIAL_BYTES)
    flow.onPingAck()
    const grownWindow = flow.byteWindow
    expect(grownWindow).toBeGreaterThan(CREDIT_WINDOW_INITIAL_BYTES)
    flow.onConsumed(100)
    emit.windowCalls.length = 0
    emit.msgWindowCalls.length = 0

    // Deplete sender-side credit.
    expect(flow.decrement(CREDIT_WINDOW_INITIAL_BYTES)).toBeInstanceOf(Promise)

    flow.reattach()
    expect(flow.byteWindow).toBe(grownWindow)
    expect(flow.decrement(1)).toBeInstanceOf(Promise)
    expect(emit.windowCalls).toEqual([100 + grownWindow])
    expect(emit.msgWindowCalls).toEqual([1 + CREDIT_MSG_WINDOW_INITIAL])
    expect(emit.sentCalls).toEqual([[CREDIT_WINDOW_INITIAL_BYTES, 1]])
  })

  // A sender blocked across a reattach wakes on the limit the peer advertises again on its end, the one a refresh
  // lost with the prior wire would have raised. Catches a reattach that leaks waiters.
  it('wakes a sender blocked across it on the limit the peer advertises again', async () => {
    const { flow } = makeFlow()
    flow.decrement(CREDIT_WINDOW_INITIAL_BYTES)
    const gate = flow.decrement(100)
    expect(gate).toBeInstanceOf(Promise)

    let resolved = false
    void (gate as Promise<void>).then(() => {
      resolved = true
    })

    flow.reattach()
    await flushMicrotasks()
    expect(resolved).toBe(false)
    flow.onPeerByteWindow(CREDIT_WINDOW_INITIAL_BYTES + CREDIT_WINDOW_INITIAL_BYTES)
    await flushMicrotasks()
    expect(resolved).toBe(true)
  })

  // The BDP estimator's in-flight ping is dropped on reattach (its ack rode the
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

  // An SSE wire's upload can fall back to batch POSTs after its first attach, and a later attach on that same wire
  // skips the reattach, so the larger window goes out on its own.
  it('advertises the batch-POST window as it grows, once', () => {
    const { flow, emit } = makeFlow()
    flow.onConsumed(100)
    flow.useBatchTransportInitial()
    flow.useBatchTransportInitial()
    expect(emit.windowCalls).toEqual([100 + CREDIT_WINDOW_INITIAL_BYTES_BATCH])
  })

  // Frames the sender counted but the receiver never gets, lost beyond the replay buffer on a reattach, count as
  // consumed once the sender's totals arrive: otherwise their credit never returns, and a loss of most of a
  // window stalls the stream for good.
  it('counts what the sender sent and never arrived as consumed', () => {
    const { flow, emit } = makeFlow()
    flow.onReceived(1_000)
    flow.onConsumed(1_000)
    flow.onPeerSent(1_000 + CREDIT_WINDOW_INITIAL_BYTES, CREDIT_MSG_WINDOW_INITIAL)
    expect(emit.windowCalls).toEqual([1_000 + CREDIT_WINDOW_INITIAL_BYTES + CREDIT_WINDOW_INITIAL_BYTES])
    expect(emit.msgWindowCalls).toEqual([CREDIT_MSG_WINDOW_INITIAL + CREDIT_MSG_WINDOW_INITIAL])
    // Totals already accounted for, as the next reattach repeats them, count nothing twice.
    flow.onPeerSent(1_000 + CREDIT_WINDOW_INITIAL_BYTES, CREDIT_MSG_WINDOW_INITIAL)
    flow.onConsumed(CREDIT_WINDOW_INITIAL_BYTES / 4 - 1)
    expect(emit.windowCalls).toHaveLength(1)
  })
})

/** A sender and a receiver linked by the u32 wire, as `WINDOW`, `MSG_WINDOW` and `SENT` frames link a channel's ends.
 *  BDP pings go unanswered, so the windows stay at their initial size. */
function makePair() {
  const toSender: FlowControlEmit = {
    byteWindowUpdate: (limit) => sender.onPeerByteWindow((decode(encode.window(0, limit)) as { bytes: number }).bytes),
    msgWindowUpdate: (limit) =>
      sender.onPeerMessageWindow((decode(encode.msgWindow(0, limit)) as { count: number }).count),
    sent: () => {},
    bdpPing: () => {},
  }
  const toReceiver: FlowControlEmit = {
    byteWindowUpdate: () => {},
    msgWindowUpdate: () => {},
    sent: (bytes, messages) => {
      const frame = decode(encode.sent(0, 0, bytes, messages)) as { bytes: number; messages: number }
      receiver.onPeerSent(frame.bytes, frame.messages)
    },
    bdpPing: () => {},
  }
  const receiver = new FlowControl(toSender)
  const sender = new FlowControl(toReceiver)
  return { sender, receiver }
}

describe('FlowControl — 32-bit wraparound', () => {
  // Limits and totals travel mod 2^32. A stream past 4 GiB must keep what is in flight within the window, and
  // keep flowing, as its wire values wrap.
  it('keeps what is in flight within the window as the byte totals cross 2^32', async () => {
    const { sender, receiver } = makePair()
    const size = 1 << 20
    let sent = 0
    let consumed = 0
    let inFlight = 0
    let blocked = false
    while (consumed < 2 ** 32 + 2 * CREDIT_WINDOW_INITIAL_BYTES) {
      if (!blocked) {
        const gate = sender.decrement(size)
        sent += size
        receiver.onReceived(size)
        inFlight = Math.max(inFlight, sent - consumed)
        if (gate) {
          blocked = true
          void gate.then(() => (blocked = false))
        }
        continue
      }
      expect(sent - consumed, 'blocked with nothing in flight').toBeGreaterThan(0)
      receiver.onConsumed(size)
      consumed += size
      await flushMicrotasks()
    }
    expect(inFlight).toBe(CREDIT_WINDOW_INITIAL_BYTES)
  })

  // The sender's totals wrap as well: what was lost past the wrap still counts as consumed.
  it('counts a loss as consumed when the totals have wrapped', async () => {
    const { sender, receiver } = makePair()
    const size = 1 << 20
    for (let n = 0; n < 2 ** 32 / size; n++) {
      sender.decrement(size)
      receiver.onReceived(size)
      receiver.onConsumed(size)
    }
    // A window's worth is sent, and lost with the wire.
    for (let n = 0; n < CREDIT_WINDOW_INITIAL_BYTES / size; n++) sender.decrement(size)
    const gate = sender.decrement(size)
    expect(gate).toBeInstanceOf(Promise)
    let resolved = false
    void gate!.then(() => (resolved = true))
    sender.reattach()
    await flushMicrotasks()
    expect(resolved).toBe(true)
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
