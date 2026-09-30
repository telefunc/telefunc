import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'

import { FlowControl, replayWindow } from './flow-control.js'
import type { FlowControlEmit } from './flow-control.js'
import {
  BDP_PING_MIN_INTERVAL_MS,
  CREDIT_MSG_WINDOW_INITIAL,
  CREDIT_WINDOW_INITIAL_BYTES,
  CREDIT_WINDOW_INITIAL_BYTES_BATCH,
  CREDIT_WINDOW_MAX_BYTES,
} from '../constants.js'
import { decode, encode, type SeqReader } from '../shared-ws.js'

/** A receiver with nothing of any channel: each seq reads as its low 32 bits. */
const wireSeqs: SeqReader = { received: () => 0, sent: () => 0 }

beforeEach(() => vi.useFakeTimers({ now: 1_000_000 }))
afterEach(() => vi.useRealTimers())

// The async `_waitForGates` re-check loop adds a couple of microtask hops
// between a waiter-drain and the outer `decrement` Promise resolving. Tests
// that observe post-drain resolution must flush enough microtasks to settle
// the chain — `Promise.resolve()` once isn't enough.
async function flushMicrotasks(): Promise<void> {
  for (let i = 0; i < 6; i++) await Promise.resolve()
}

/** Resolves on the next macrotask, which fake timers don't hold back. */
function nextMacrotask(): Promise<void> {
  const { port1, port2 } = new MessageChannel()
  return new Promise<void>((resolve) => {
    port1.onmessage = () => {
      port1.close()
      resolve()
    }
    port2.postMessage(null)
  })
}

/** Whether each promise has resolved, as of now. */
function watch(promises: (void | Promise<void>)[]): boolean[] {
  const resolved = promises.map(() => false)
  promises.forEach((p, i) => void (p as Promise<void>).then(() => (resolved[i] = true)))
  return resolved
}

// Spy emit — captures calls so tests can assert which wire frames the flow
// would have produced. Each test starts with a fresh `Emit` so the spec
// doesn't need cross-test reset hygiene.
type Emit = FlowControlEmit & {
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
  }
  return e
}
/** `backlog` is what its wire holds, nothing unless a test says otherwise. */
function makeFlow(backlog: () => number | undefined = () => 0) {
  const emit = makeEmit()
  return { emit, flow: fitted(new FlowControl(emit, backlog)) }
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
    flow.decrement(CREDIT_WINDOW_INITIAL_BYTES - 1)
    const gate = flow.decrement(100)
    expect(gate).toBeInstanceOf(Promise)
    const resolved = watch([gate])

    flow.onPeerByteWindow(CREDIT_WINDOW_INITIAL_BYTES + 99)
    await flushMicrotasks()
    expect(resolved).toEqual([false])
    flow.onPeerByteWindow(2 * CREDIT_WINDOW_INITIAL_BYTES)
    await flushMicrotasks()
    expect(resolved).toEqual([true])
  })

  // Woken together, senders that each await their sends would all send, each past the limit by a frame.
  it('a limit wakes one blocked sender, and each send it makes within credit hands the credit to the next', async () => {
    const { flow } = makeFlow()
    const gates = [flow.decrement(CREDIT_WINDOW_INITIAL_BYTES), flow.decrement(100), flow.decrement(100)]
    for (const g of gates) expect(g).toBeInstanceOf(Promise)
    const resolved = watch(gates)

    flow.onPeerByteWindow(3 * CREDIT_WINDOW_INITIAL_BYTES)
    await flushMicrotasks()
    expect(resolved).toEqual([true, false, false])
    // The first sends again, within credit: the second's turn, and the first waits behind the third.
    const again = flow.decrement(100)
    expect(again).toBeInstanceOf(Promise)
    await flushMicrotasks()
    expect([...resolved, ...watch([again])]).toEqual([true, true, false, false])
  })

  it('a sender woken to its turn that sends nothing passes the credit on a macrotask later', async () => {
    const { flow } = makeFlow()
    const gates = [flow.decrement(CREDIT_WINDOW_INITIAL_BYTES), flow.decrement(100)]
    const resolved = watch(gates)
    flow.onPeerByteWindow(3 * CREDIT_WINDOW_INITIAL_BYTES)
    await flushMicrotasks()
    expect(resolved).toEqual([true, false])
    await nextMacrotask()
    await flushMicrotasks()
    expect(resolved).toEqual([true, true])
  })

  it('a send within credit resolves at once while no other sender waits', () => {
    const { flow } = makeFlow()
    for (let n = 0; n < 10; n++) expect(flow.decrement(1024)).toBeUndefined()
  })

  // The frame that crosses the limit is credit flow control's normal overshoot; what is sent after it is not.
  it('counts as sent past credit only frames sent once none was left, until a limit covers them', () => {
    const { flow } = makeFlow()
    flow.decrement(CREDIT_WINDOW_INITIAL_BYTES - 10)
    flow.decrement(1_000_000) // crosses the limit by 999 990 bytes
    expect(flow.isPastByteCredit).toBe(true)
    expect(flow.bytesSentPastCredit).toBe(0)
    flow.decrement(100)
    flow.decrement(100)
    expect(flow.bytesSentPastCredit).toBe(200)
    // A limit covering the crossing frame and half of the next.
    flow.onPeerByteWindow(CREDIT_WINDOW_INITIAL_BYTES + 999_990 + 50)
    expect(flow.bytesSentPastCredit).toBe(150)
    flow.onPeerByteWindow(2 * CREDIT_WINDOW_INITIAL_BYTES)
    expect(flow.isPastByteCredit).toBe(false)
    expect(flow.bytesSentPastCredit).toBe(0)
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
    flow.onPingAck(emit.probe, true, Infinity)
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
    flow.onPingAck(emit.probe, true, Infinity)
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

describe('FlowControl — whether the window starved the wire', () => {
  // A probe's ack waits behind what the sender's wire holds, so its sample can't tell a window that left the wire
  // idle from one the sender filled faster than its wire drains. The sender can: whether its wire was empty as its
  // credit ran out.
  it('says so when the credit ran out while the wire held nothing', () => {
    const { flow } = makeFlow(() => 0)
    flow.decrement(CREDIT_WINDOW_INITIAL_BYTES)
    flow.onPeerByteWindow(2 * CREDIT_WINDOW_INITIAL_BYTES)
    expect(flow.onPing()).toBe(true)
    // Answered, it starts over.
    expect(flow.onPing()).toBe(false)
  })

  it('does not when the wire still held a backlog each time the credit ran out', () => {
    const { flow } = makeFlow(() => 64 * 1024)
    flow.decrement(CREDIT_WINDOW_INITIAL_BYTES)
    flow.onPeerByteWindow(2 * CREDIT_WINDOW_INITIAL_BYTES)
    flow.decrement(CREDIT_WINDOW_INITIAL_BYTES)
    expect(flow.onPing()).toBe(false)
  })

  it('does not while credit is left, however empty the wire', () => {
    const { flow } = makeFlow(() => 0)
    flow.decrement(CREDIT_WINDOW_INITIAL_BYTES - 1)
    flow.onPeerByteWindow(2 * CREDIT_WINDOW_INITIAL_BYTES)
    expect(flow.onPing()).toBe(false)
  })

  it('does when the message credit ran out on an empty wire', () => {
    const { flow } = makeFlow(() => 0)
    for (let n = 0; n < CREDIT_MSG_WINDOW_INITIAL; n++) flow.decrement(1)
    expect(flow.onPing()).toBe(true)
  })

  // Where the runtime can't tell what its wire holds, the window is taken to limit it once its credit runs out.
  it('does when the credit ran out on a wire that cannot tell what it holds', () => {
    const { flow } = makeFlow(() => undefined)
    flow.decrement(CREDIT_WINDOW_INITIAL_BYTES)
    expect(flow.onPing()).toBe(true)
  })

  it('grows its own window only on an ack that says the window starved the peer', () => {
    const { flow, emit } = makeFlow()
    flow.onReceived(1)
    flow.onReceived(CREDIT_WINDOW_INITIAL_BYTES)
    flow.onPingAck(emit.probe, false, Infinity)
    expect(flow.byteWindow).toBe(CREDIT_WINDOW_INITIAL_BYTES)
  })
})

describe('FlowControl — reattach', () => {
  // Credit is cumulative, so a reattach keeps it: resetting it to the initial window let a sender run a window
  // ahead of what was in flight, and stalled one whose receiver waits for a quarter of its grown window. The
  // receive window grown by BDP is kept too (the link's BDP doesn't change across transport hiccups).
  it('keeps credit and the grown receive window, and advertises the limits again', () => {
    const { flow, emit } = makeFlow()
    // Grow W via BDP first — fire ping, accumulate saturating sample, settle.
    flow.onReceived(1)
    flow.onReceived(CREDIT_WINDOW_INITIAL_BYTES)
    flow.onPingAck(emit.probe, true, Infinity)
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
  })

  // A sender blocked across a reattach wakes on the limit the peer advertises again on its end, the one a refresh
  // lost with the prior wire would have raised. Catches a reattach that leaks waiters.
  it('wakes a sender blocked across it on the limit the peer advertises again', async () => {
    const { flow } = makeFlow()
    flow.decrement(CREDIT_WINDOW_INITIAL_BYTES - 1)
    const gate = flow.decrement(100)
    expect(gate).toBeInstanceOf(Promise)
    const resolved = watch([gate])

    flow.reattach()
    await flushMicrotasks()
    expect(resolved).toEqual([false])
    flow.onPeerByteWindow(CREDIT_WINDOW_INITIAL_BYTES + CREDIT_WINDOW_INITIAL_BYTES)
    await flushMicrotasks()
    expect(resolved).toEqual([true])
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
})

describe('FlowControl — replay buffers', () => {
  it("a receiver's window is half the smaller lane of its peer's replay buffer, at least a byte", () => {
    expect(replayWindow(4 * 1024 * 1024, 2 * 1024 * 1024)).toBe(1024 * 1024)
    expect(replayWindow(1, 0)).toBe(1)
  })

  // What credit lets be in flight, and a message as large sent as the credit ran out, must fit the sender's replay.
  it("a window grows no larger than its peer's replay allows, whatever asks it to", () => {
    const { flow } = makeFlow()
    flow.fitReplays(1024 * 1024, CREDIT_WINDOW_MAX_BYTES)
    expect(flow.byteWindow).toBe(1024 * 1024)
    flow.useBatchTransportInitial()
    flow.widenByteWindow(CREDIT_WINDOW_MAX_BYTES)
    expect(flow.byteWindow).toBe(1024 * 1024)
    flow.fitReplays(4 * CREDIT_WINDOW_MAX_BYTES, CREDIT_WINDOW_MAX_BYTES)
    flow.widenByteWindow(4 * CREDIT_WINDOW_MAX_BYTES)
    expect(flow.byteWindow).toBe(CREDIT_WINDOW_MAX_BYTES)
  })

  // A page learns the server's replay buffer from its first RECONCILED. A limit it advertised before could let the server
  // have more in flight than that holds.
  it('until it knows the replay buffers, a receiver advertises no byte limit, and a sender assumes the initial window', () => {
    const emit = makeEmit()
    const flow = new FlowControl(emit, () => 0)
    expect(flow.decrement(CREDIT_WINDOW_INITIAL_BYTES - 1)).toBeUndefined()
    flow.onReceived(CREDIT_WINDOW_INITIAL_BYTES)
    flow.onConsumed(CREDIT_WINDOW_INITIAL_BYTES)
    flow.reattach()
    expect(emit.windowCalls).toEqual([])
    flow.fitReplays(64 * 1024, CREDIT_WINDOW_MAX_BYTES)
    flow.reattach()
    expect(emit.windowCalls).toEqual([CREDIT_WINDOW_INITIAL_BYTES + 64 * 1024])
  })

  // The peer assumes the same initial window its own replay allows, so a sender's credit starts there.
  it("a sender's credit starts at what its own replay allows, until a WINDOW raises it", () => {
    const { flow } = makeFlow()
    flow.fitReplays(CREDIT_WINDOW_MAX_BYTES, 512 * 1024)
    expect(flow.peerByteWindowMax).toBe(512 * 1024)
    expect(flow.decrement(512 * 1024)).toBeInstanceOf(Promise)
    flow.onPeerByteWindow(4 * 1024 * 1024)
    flow.fitReplays(CREDIT_WINDOW_MAX_BYTES, 256 * 1024)
    expect(flow.decrement(1024)).toBeUndefined()
  })

  // Ack requests and their answers take no credit, so no limit goes out for them: a WINDOW acknowledges them instead.
  it('a quarter window of what credit does not count sends a WINDOW, which acknowledges it, and one sent for any reason starts the count again', () => {
    const { flow, emit } = makeFlow()
    const quarter = CREDIT_WINDOW_INITIAL_BYTES / 4
    flow.onReceivedUncounted(quarter - 1)
    expect(emit.windowCalls).toEqual([])
    flow.onReceivedUncounted(1)
    expect(emit.windowCalls).toEqual([CREDIT_WINDOW_INITIAL_BYTES])
    flow.onReceivedUncounted(quarter - 1)
    flow.onConsumed(quarter)
    flow.onReceivedUncounted(1)
    expect(emit.windowCalls).toEqual([CREDIT_WINDOW_INITIAL_BYTES, quarter + CREDIT_WINDOW_INITIAL_BYTES])
  })

  // A quiet channel's last frames are acknowledged at the next heartbeat, and a heartbeat with nothing new sends nothing.
  it('a heartbeat sends a WINDOW only for what arrived since the last', () => {
    const { flow, emit } = makeFlow()
    flow.acknowledge()
    expect(emit.windowCalls).toEqual([])
    flow.onReceived(100)
    flow.onConsumed(100)
    flow.acknowledge()
    flow.acknowledge()
    expect(emit.windowCalls).toEqual([100 + CREDIT_WINDOW_INITIAL_BYTES])
    flow.onReceivedUncounted(10)
    flow.acknowledge()
    expect(emit.windowCalls).toHaveLength(2)
  })
})

/** A sender and a receiver linked by the u32 wire, as `WINDOW` and `MSG_WINDOW` frames link a channel's ends.
 *  BDP pings go unanswered, so the windows stay at their initial size. */
function makePair() {
  const toSender: FlowControlEmit = {
    byteWindowUpdate: (limit) =>
      sender.onPeerByteWindow((decode(encode.window(0, limit, 0), wireSeqs) as { bytes: number }).bytes),
    msgWindowUpdate: (limit) =>
      sender.onPeerMessageWindow((decode(encode.msgWindow(0, limit), wireSeqs) as { count: number }).count),
    bdpPing: () => {},
  }
  const toReceiver: FlowControlEmit = {
    byteWindowUpdate: () => {},
    msgWindowUpdate: () => {},
    bdpPing: () => {},
  }
  const receiver = fitted(new FlowControl(toSender, () => 0))
  const sender = fitted(new FlowControl(toReceiver, () => 0))
  return { sender, receiver }
}

describe('FlowControl — 32-bit wraparound', () => {
  // Limits travel mod 2^32. A stream past 4 GiB must keep what is in flight within the window, and
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
})

/** A stream over a path of `bytesPerMs` with `delayMs` each way, a millisecond at a time. What the sender sends waits in
 *  its wire, which reports what it holds as a socket's `bufferedAmount` does, then in `hiddenBytes` below it that it
 *  doesn't report, as a kernel's send buffer, then on the link. The receiver attaches, with a probe unless `attach` is
 *  false, which the sender answers ahead of the channel's frames. Where `senderAttach`, the sender probed the path with
 *  an attach of its own, as a page does where the server receives, answered ahead of all it sends. One producer awaits
 *  every send of `frameBytes`, and the receiver consumes each frame as it arrives. Frames toward the sender only take
 *  `delayMs`. */
async function runPath({
  bytesPerMs,
  delayMs,
  frameBytes,
  ms,
  hiddenBytes = 0,
  attach = true,
  senderAttach = false,
}: {
  bytesPerMs: number
  delayMs: number
  frameBytes: number
  ms: number
  hiddenBytes?: number
  attach?: boolean
  senderAttach?: boolean
}) {
  type Ack = { probe: number; starved: boolean; pathRtt: number }
  type Frame = { bytes: number } | Ack
  const size = (frame: Frame) => ('bytes' in frame ? frame.bytes : 0)
  const wire: Frame[] = []
  let wireBytes = 0
  const hidden: Frame[] = []
  let hiddenQueued = 0
  const onLink: { at: number; frame: Frame }[] = []
  const toSender: { at: number; deliver: () => void }[] = []
  let now = 0
  const upstream = (deliver: () => void) => toSender.push({ at: now + delayMs, deliver })
  const sender = fitted(new FlowControl({ byteWindowUpdate() {}, msgWindowUpdate() {}, bdpPing() {} }, () => wireBytes))
  const answer = (probe: number, starved: boolean, pathRtt: number) =>
    wire.push(decode(encode.bdpPingAck(0, probe, starved, pathRtt), wireSeqs) as Ack)
  const receiver = fitted(
    new FlowControl(
      {
        byteWindowUpdate: (limit) => upstream(() => sender.onPeerByteWindow(limit)),
        msgWindowUpdate: (limit) => upstream(() => sender.onPeerMessageWindow(limit)),
        bdpPing: (probe) => upstream(() => answer(probe, sender.onPing(), sender.pathRtt(0))),
      },
      () => 0,
    ),
  )
  let attached = false
  const attachProbe = attach ? receiver.probeAttach(0)! : undefined
  const senderProbe = senderAttach ? sender.probeAttach(0)! : undefined
  upstream(() => {
    if (attachProbe !== undefined) answer(attachProbe, false, Infinity)
    attached = true
  })
  if (senderProbe !== undefined)
    toSender.push({ at: 2 * delayMs, deliver: () => sender.onPingAck(senderProbe, false, Infinity) })
  let blocked = false
  let budget = 0
  let delivered = 0
  let deliveredLastSecond = 0
  for (now = 0; now < ms; now++) {
    vi.advanceTimersByTime(1)
    while (toSender[0] && toSender[0].at <= now) toSender.shift()!.deliver()
    await flushMicrotasks()
    while (attached && !blocked) {
      wire.push({ bytes: frameBytes })
      wireBytes += frameBytes
      const gate = sender.decrement(frameBytes)
      if (gate) {
        blocked = true
        void gate.then(() => (blocked = false))
      }
    }
    while (wire[0] && hiddenQueued + size(wire[0]) <= hiddenBytes) {
      const frame = wire.shift()!
      wireBytes -= size(frame)
      hiddenQueued += size(frame)
      hidden.push(frame)
    }
    budget += bytesPerMs
    for (;;) {
      const from = hidden.length > 0 ? hidden : wire
      if (!from[0] || size(from[0]) > budget) break
      const frame = from.shift()!
      budget -= size(frame)
      if (from === hidden) hiddenQueued -= size(frame)
      else wireBytes -= size(frame)
      onLink.push({ at: now + delayMs, frame })
    }
    if (hidden.length === 0 && wire.length === 0) budget = 0
    while (onLink[0] && onLink[0].at <= now) {
      const { frame } = onLink.shift()!
      if ('starved' in frame) {
        receiver.onPingAck(frame.probe, frame.starved, frame.pathRtt)
        continue
      }
      receiver.onReceived(frame.bytes)
      receiver.onConsumed(frame.bytes)
      delivered += frame.bytes
      if (now >= ms - 1_000) deliveredLastSecond += frame.bytes
    }
  }
  return {
    byteWindow: receiver.byteWindow,
    msgWindow: receiver.msgWindow,
    deliveredBytesPerMs: delivered / ms,
    lastSecondBytesPerMs: deliveredLastSecond / 1_000,
  }
}

describe('FlowControl — window on a path', () => {
  // The ack of a probe waits behind what the sender holds for the link, so what arrived before it saturated any window
  // the sender filled faster than the link drained: the window doubled on every probe, to the 64 MiB cap.
  it('keeps its initial window on a path slower than that window per RTT, and fills the path', async () => {
    const path = await runPath({ bytesPerMs: 4_000, delayMs: 25, frameBytes: 64 * 1024, ms: 5_000 })
    expect(path.byteWindow).toBe(CREDIT_WINDOW_INITIAL_BYTES)
    expect(path.deliveredBytesPerMs).toBeGreaterThan(0.95 * 4_000)
  })

  // Without an attach's probe, as behind a page's batched POSTs or on a server's receive side, the sender says its
  // wire held a backlog each time its credit ran out.
  it('keeps its initial window on that path without an attach probe, where the sender sees its queue', async () => {
    const path = await runPath({ bytesPerMs: 4_000, delayMs: 25, frameBytes: 64 * 1024, ms: 5_000, attach: false })
    expect(path.byteWindow).toBe(CREDIT_WINDOW_INITIAL_BYTES)
    expect(path.deliveredBytesPerMs).toBeGreaterThan(0.95 * 4_000)
  })

  // The queue a sender can't see, as in a kernel send buffer that takes the whole window, still lengthens the round
  // trip past the one the attach's probe took.
  it('keeps its initial window on that path when the queue is where the sender cannot see it', async () => {
    const path = await runPath({
      bytesPerMs: 4_000,
      delayMs: 25,
      frameBytes: 64 * 1024,
      ms: 5_000,
      hiddenBytes: CREDIT_WINDOW_INITIAL_BYTES,
    })
    expect(path.byteWindow).toBe(CREDIT_WINDOW_INITIAL_BYTES)
    expect(path.deliveredBytesPerMs).toBeGreaterThan(0.95 * 4_000)
  })

  // A server's receive side has no attach of its own, and a browser's socket reports none of what its kernel holds: the
  // page says the round trip its own attach's probe took, and the sample counts at that.
  it('keeps its initial window on that path without an attach of its own, where the sender cannot see its queue but measured the path', async () => {
    const path = await runPath({
      bytesPerMs: 4_000,
      delayMs: 25,
      frameBytes: 64 * 1024,
      ms: 5_000,
      hiddenBytes: CREDIT_WINDOW_INITIAL_BYTES,
      attach: false,
      senderAttach: true,
    })
    expect(path.byteWindow).toBe(CREDIT_WINDOW_INITIAL_BYTES)
    expect(path.deliveredBytesPerMs).toBeGreaterThan(0.95 * 4_000)
  })

  // A window grows while a sample fills two thirds of it, so it doubles at most once past 1.5 times what the path holds.

  it('grows its window past the bandwidth-delay product of a fast path, and settles within three times it', async () => {
    // 400 MB/s × 50 ms, with a 4 MiB send buffer, the most Linux gives one by default.
    const bdp = 20_000_000
    const path = await runPath({
      bytesPerMs: 400_000,
      delayMs: 25,
      frameBytes: 64 * 1024,
      ms: 3_000,
      hiddenBytes: 4 * 1024 * 1024,
    })
    expect(path.byteWindow).toBeGreaterThan(bdp)
    expect(path.byteWindow).toBeLessThanOrEqual(3 * bdp)
    expect(path.lastSecondBytesPerMs).toBeGreaterThan(0.95 * 400_000)
  })

  it('grows its window past the bandwidth-delay product of a fast path at the round trip its sender measured, and settles within three times it', async () => {
    const bdp = 20_000_000
    const path = await runPath({
      bytesPerMs: 400_000,
      delayMs: 25,
      frameBytes: 64 * 1024,
      ms: 3_000,
      hiddenBytes: 4 * 1024 * 1024,
      attach: false,
      senderAttach: true,
    })
    expect(path.byteWindow).toBeGreaterThan(bdp)
    expect(path.byteWindow).toBeLessThanOrEqual(3 * bdp)
    expect(path.lastSecondBytesPerMs).toBeGreaterThan(0.95 * 400_000)
  })

  // The message window grows on its sample, queue and all: what waits on the wire the byte window bounds.
  it('keeps its byte window on a path of small messages, and grows its message window only as far as that lets be in flight', async () => {
    // 4 MB/s × 50 ms, of 1 KiB messages
    const path = await runPath({ bytesPerMs: 4_000, delayMs: 25, frameBytes: 1024, ms: 5_000 })
    expect(path.byteWindow).toBe(CREDIT_WINDOW_INITIAL_BYTES)
    expect(path.msgWindow).toBeGreaterThan(CREDIT_WINDOW_INITIAL_BYTES / 1024)
    expect(path.msgWindow).toBeLessThanOrEqual((2 * CREDIT_WINDOW_INITIAL_BYTES) / 1024)
    expect(path.lastSecondBytesPerMs).toBeGreaterThan(0.95 * 4_000)
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
