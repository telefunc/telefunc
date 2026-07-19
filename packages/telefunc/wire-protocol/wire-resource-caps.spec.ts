// T7 — INBOUND WIRE RESOURCE CAPS (D3). Bounds a misbehaving client's cheapest allocations at the
// two inbound entry points: the raw frame itself, and what it can leave QUEUED on one connection.
//
// Same two-kinds-of-test discipline `upgrade.admission-caps.spec.ts` established:
//
//   MECHANISM — a small limit is injected into the REAL `ChannelMux`, so the real dispatch path
//     enforces it. An accountant test standing beside the code stays green when production stops
//     consulting the limit, which is the exact failure these guard against.
//   VALUE — the shipped default is pinned by name, plus a BRIDGE per constant proving a
//     default-constructed mux actually reads it. Without the bridge both kinds could be true of a
//     mux rewired to ignore the constants entirely.
//
// Every over-limit row asserts three things: the offending wire is really terminated, the frame that
// overran was NOT delivered, and a live SSE sentinel on a different connection still routes. The
// third is the independent one — "rejected" is otherwise satisfied by an implementation that
// rejected by tearing everything down, which is precisely what a cap must not do.
//
// ── The park, and why it is the only honest one ──────────────────────────────────────────────
// Backlog only exists while a turn is unfinished, so a backlog test needs something genuinely
// awaited. The ONLY such seam reachable from `handleFrame` is `attach`'s
// `waitForChannelRegistration`, entered by a RECONCILE naming an `initial: true` channel that is not
// yet registered. Async listeners park NOTHING (`_dispatchFrame` is `void`), so a slow-listener test
// here could not fail. Each backlog test therefore opens with an instrument check that the park is
// real — the queued frames are provably still charged at assertion time.

import { afterEach, describe, expect, test } from 'vitest'

import { ServerChannel } from './server/channel.js'
import { getChannelMux } from './server/mux.js'
import { OversizeFrameError, StreamReader } from './server/request/StreamReader.js'
import { handleSseChannelRequest } from './server/sse.js'
import { encodeSseRequestMetadata } from './sse-request.js'
import { encodeU32 } from './frame.js'
import { encode } from './shared-ws.js'
import { createMuxHarness, reconcileFrame, settle, textFrame } from './upgrade-mux-harness.js'
import {
  UPGRADE_MAX_FRAME_BYTES,
  UPGRADE_MAX_ID_BYTES,
  UPGRADE_MAX_OPEN_ENTRIES,
  UPGRADE_MAX_STAGED_BYTES,
  UPGRADE_MAX_STAGED_RECORDS,
  UPGRADE_STAGE_TTL_MS,
  WIRE_MAX_RAW_FRAME_BYTES,
  WIRE_MAX_RECV_BACKLOG_BYTES,
  WIRE_MAX_RECV_BACKLOG_FRAMES,
} from './constants.js'

let harness: ReturnType<typeof createMuxHarness> | null = null
afterEach(() => {
  harness?.dispose()
  harness = null
})

type Harness = ReturnType<typeof createMuxHarness>

/** Bring a wire up with its own registered channel at ix 0. */
async function connect(h: Harness, wire: Harness['sse'], id: string) {
  const channel = h.registerChannel(id)
  await wire.deliver(reconcileFrame({ open: [{ id, ix: 0, lastSeq: 0, initial: true }] }))
  expect(wire.sessionId()).toBeTypeOf('string')
  return channel
}

let sentinelSeq = 6_000
let sentinelValue = 7_000
afterEach(() => {
  sentinelSeq = 6_000
  sentinelValue = 7_000
})

/** The independent oracle: the OTHER connection is untouched and still delivering. */
async function expectSentinelAlive(h: Harness, chA: { received: number[] }) {
  expect(h.sse.terminated()).toBe(false)
  const value = ++sentinelValue
  await h.sse.deliver(textFrame(0, ++sentinelSeq, value))
  expect(chA.received.at(-1)).toBe(value)
}

// ── Raw frame ceiling, WS path (`dispatchInbound`, pre-decode) ───────────────────────────────

describe('raw frame ceiling — mechanism', () => {
  test('a frame over the ceiling terminates its wire and is never dispatched', async () => {
    const h = (harness = createMuxHarness({ maxRawFrameBytes: 512 }))
    const chA = await connect(h, h.sse, 'A')
    const chW = await connect(h, h.ws, 'W')

    await h.ws.deliver(encode.text(0, JSON.stringify('x'.repeat(2_000)), 1))

    expect(h.ws.terminated()).toBe(true)
    // Refused BEFORE decode and before dispatch — not decoded and then discarded.
    expect(chW.received).toEqual([])
    await expectSentinelAlive(h, chA)
  })

  test('a frame AT the ceiling is delivered — the control for the row above', async () => {
    // Without this pairing, "over the ceiling is refused" is satisfied by a ceiling of zero, which
    // would refuse every frame forever and still look like working enforcement.
    const h = (harness = createMuxHarness({ maxRawFrameBytes: 512 }))
    const chW = await connect(h, h.ws, 'W')

    const frame = encode.text(0, JSON.stringify(42), 1)
    expect(frame.byteLength).toBeLessThanOrEqual(512)
    await h.ws.deliver(frame)

    expect(h.ws.terminated()).toBe(false)
    expect(chW.received).toEqual([42])
  })
})

// ── Per-connection recv-chain backlog ────────────────────────────────────────────────────────

describe('recv-chain backlog — frame count', () => {
  test('overflowing the queued-frame count terminates the offending wire only', async () => {
    const h = (harness = createMuxHarness({ maxRecvBacklogFrames: 3 }))
    const chA = await connect(h, h.sse, 'A')
    const parked = h.createChannel('P')

    // Park: a reconcile naming an unregistered `initial: true` channel awaits its registration.
    void h.ws.deliver(reconcileFrame({ open: [{ id: 'P', ix: 0, lastSeq: 0, initial: true }] }))
    void h.ws.deliver(textFrame(0, 1, 11))
    void h.ws.deliver(textFrame(0, 2, 22))
    await settle()

    // INSTRUMENT CHECK: the park is real. Three frames are admitted and still charged, and the
    // enforcement fields say so — without this the overflow below could be an artifact of frames
    // that already drained.
    expect(h.ws.backlog()).toMatchObject({ frames: 3 })
    expect(h.ws.terminated()).toBe(false)

    // The fourth cannot be admitted.
    void h.ws.deliver(textFrame(0, 3, 33))
    await settle()

    expect(h.ws.terminated()).toBe(true)
    // Rejected, not queued: the charge is unchanged by the frame that overran.
    expect(h.ws.backlog()).toMatchObject({ frames: 3 })
    await expectSentinelAlive(h, chA)

    // Releasing the park drains the chain and REFUNDS every charge. This is the decrement observed
    // directly on the field enforcement reads.
    h.register(parked)
    await settle()
    expect(h.ws.backlog()).toMatchObject({ frames: 0, bytes: 0 })
  })

  test('frames far past the cap flow freely when the chain is never parked', async () => {
    // The decrement's own row. With the refund skipped, the counter ratchets and this wire dies
    // partway through — even though it never has more than one frame in flight at a time.
    const h = (harness = createMuxHarness({ maxRecvBacklogFrames: 3 }))
    const chW = await connect(h, h.ws, 'W')

    for (let i = 1; i <= 12; i++) await h.ws.deliver(textFrame(0, i, i))

    expect(h.ws.terminated()).toBe(false)
    expect(chW.received).toEqual([1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12])
    expect(h.ws.backlog()).toMatchObject({ frames: 0, bytes: 0 })
  })
})

describe('recv-chain backlog — bytes', () => {
  test('overflowing the queued BYTE budget terminates the offending wire only', async () => {
    // Sized from the real frames, so the row proves the byte unit specifically: the frame cap is at
    // its 50k default throughout and only four frames are ever in flight.
    const park = reconcileFrame({ open: [{ id: 'P', ix: 0, lastSeq: 0, initial: true }] })
    const big = encode.text(0, JSON.stringify('x'.repeat(4_000)), 1)
    const h = (harness = createMuxHarness({ maxRecvBacklogBytes: park.byteLength + big.byteLength }))
    const chA = await connect(h, h.sse, 'A')
    const parked = h.createChannel('P')

    void h.ws.deliver(park)
    void h.ws.deliver(big)
    await settle()

    // INSTRUMENT CHECK: both are charged and the budget is exactly full — the next frame is refused
    // for its bytes and nothing else.
    expect(h.ws.backlog()).toMatchObject({ bytes: park.byteLength + big.byteLength, frames: 2 })
    expect(h.ws.terminated()).toBe(false)

    void h.ws.deliver(encode.text(0, JSON.stringify('y'.repeat(4_000)), 2))
    await settle()

    expect(h.ws.terminated()).toBe(true)
    expect(h.ws.backlog()).toMatchObject({ frames: 2 })
    await expectSentinelAlive(h, chA)

    h.register(parked)
    await settle()
    expect(h.ws.backlog()).toMatchObject({ frames: 0, bytes: 0 })
  })
})

// ── SSE framing: the DECLARED length, before the body is pulled ──────────────────────────────

/** A source that reports how many chunks the reader actually pulled from it. The pull count is the
 *  whole point: a cap enforced after the read bounds what is RETAINED while still letting a hostile
 *  u32 drive the allocation, and only this counter can tell the two apart. */
function countingSource(chunks: Uint8Array<ArrayBuffer>[]) {
  let pulls = 0
  const iterator: AsyncIterator<Uint8Array<ArrayBuffer>> = {
    next: async () => {
      pulls++
      const value = chunks.shift()
      return value ? { value, done: false } : { value: undefined, done: true }
    },
  }
  const source = { [Symbol.asyncIterator]: () => iterator } as unknown as ReadableStream<Uint8Array>
  return { source, pulls: () => pulls }
}

describe('SSE framing — declared length', () => {
  test('a declared length over the cap is refused without pulling one body byte', async () => {
    // The prefix alone is delivered. No body follows it, and none is asked for.
    const { source, pulls } = countingSource([encodeU32(64) as Uint8Array<ArrayBuffer>])
    const reader = new StreamReader(source)

    await expect(reader.readLengthPrefixedBytesOrNull(63)).rejects.toBeInstanceOf(OversizeFrameError)
    // One pull: the length prefix. Reading the declared body would need a second.
    expect(pulls()).toBe(1)
  })

  test('a declared length AT the cap is read normally', async () => {
    const body = new Uint8Array(64) as Uint8Array<ArrayBuffer>
    body[0] = 7
    const { source, pulls } = countingSource([encodeU32(64) as Uint8Array<ArrayBuffer>, body])
    const reader = new StreamReader(source)

    const frame = await reader.readLengthPrefixedBytesOrNull(64)

    expect(frame?.byteLength).toBe(64)
    expect(frame?.[0]).toBe(7)
    expect(pulls()).toBe(2)
  })
})

// ── The same cap through the REAL SSE transport, at the shipped default ──────────────────────

const lengthPrefixed = (bytes: Uint8Array<ArrayBuffer>): Uint8Array<ArrayBuffer> => {
  const out = new Uint8Array(4 + bytes.length) as Uint8Array<ArrayBuffer>
  out.set(encodeU32(bytes.length), 0)
  out.set(bytes, 4)
  return out
}

const registered: ServerChannel<number, never>[] = []
afterEach(() => {
  for (const channel of registered) channel._onPeerClose()
  registered.length = 0
})

function registerChannel(id: string): ServerChannel<number, never> {
  const channel = new ServerChannel<number, never>({ id })
  getChannelMux().registerChannel(channel)
  registered.push(channel)
  return channel
}

async function openDownstream(connId: string, id: string): Promise<void> {
  const reconcile = encode.reconcile({ open: [{ id, ix: 0, lastSeq: 0, initial: true }] })
  const body = new Blob([encodeSseRequestMetadata({ connId, streamResponse: true }), lengthPrefixed(reconcile)])
  const response = await handleSseChannelRequest(new Request('http://test.local/_telefunc', { method: 'POST', body }))
  expect(response?.statusCode).toBe(200)
  await settle()
}

describe('SSE framing — the real transport at the shipped default', () => {
  test('a body declaring more than the ceiling kills that connection and no other', async () => {
    const victimId = crypto.randomUUID()
    const sentinelId = crypto.randomUUID()
    const chVictim = registerChannel(`V-${victimId}`)
    const chSentinel = registerChannel(`S-${sentinelId}`)
    await openDownstream(victimId, chVictim.id)
    await openDownstream(sentinelId, chSentinel.id)

    // FOUR BYTES on the wire announce 64 MiB + 1. Nothing follows, and nothing needs to: the
    // declared length alone is the whole attack, and refusing by it is what makes it free.
    const received: number[] = []
    chSentinel.listen((n) => received.push(n))
    const oversize = new Blob([encodeSseRequestMetadata({ connId: victimId }), encodeU32(WIRE_MAX_RAW_FRAME_BYTES + 1)])
    await handleSseChannelRequest(new Request('http://test.local/_telefunc', { method: 'POST', body: oversize }))

    // Real termination: the mux no longer holds the connection at all.
    expect(getChannelMux().getConnectionByConnId(victimId)).toBeUndefined()

    // The live sentinel: a different SSE connection still routes application traffic.
    const sentinelPost = new Blob([
      encodeSseRequestMetadata({ connId: sentinelId }),
      lengthPrefixed(encode.text(0, JSON.stringify(4_242), 1)),
    ])
    await handleSseChannelRequest(new Request('http://test.local/_telefunc', { method: 'POST', body: sentinelPost }))
    await settle()

    expect(getChannelMux().getConnectionByConnId(sentinelId)).toBeDefined()
    expect(received).toEqual([4_242])
  })

  test('control: an ordinary frame on the same path is delivered and the connection survives', async () => {
    const connId = crypto.randomUUID()
    const channel = registerChannel(`C-${connId}`)
    await openDownstream(connId, channel.id)

    const received: number[] = []
    channel.listen((n) => received.push(n))
    const body = new Blob([encodeSseRequestMetadata({ connId }), lengthPrefixed(encode.text(0, JSON.stringify(11), 1))])
    await handleSseChannelRequest(new Request('http://test.local/_telefunc', { method: 'POST', body }))
    await settle()

    expect(getChannelMux().getConnectionByConnId(connId)).toBeDefined()
    expect(received).toEqual([11])
  })
})

// ── Shipped values, and the bridges proving production reads them ────────────────────────────

describe('shipped default values', () => {
  test('the inbound wire caps are the agreed defaults', () => {
    expect(WIRE_MAX_RAW_FRAME_BYTES).toBe(64 * 1024 * 1024)
    expect(WIRE_MAX_RECV_BACKLOG_BYTES).toBe(64 * 1024 * 1024)
    expect(WIRE_MAX_RECV_BACKLOG_FRAMES).toBe(50_000)
  })

  // The bridge for the raw-frame ceiling is the real-SSE row above: it declares
  // `WIRE_MAX_RAW_FRAME_BYTES + 1` against a default-constructed (module-global) mux, so a
  // production path that stopped reading the constant would not terminate that connection.

  test('a default-constructed mux binds each limit field to ITS OWN constant', () => {
    // The row that closes the blind spot BETWEEN the other two kinds. Injecting a small limit proves
    // enforcement dereferences `limits.<field>`; pinning a constant proves that constant holds its
    // agreed number. Neither can see a `DEFAULT_MUX_LIMITS` that assigned the RIGHT constants to the
    // WRONG fields — `maxRecvBacklogFrames: WIRE_MAX_RECV_BACKLOG_BYTES` silently ships a 67,108,864
    // frame ceiling with every other test still green. Read off the resolved limits object itself,
    // so this compares the values enforcement compares rather than a restatement of them.
    //
    // All nine fields, not just T7's three: the defect is a property of the object literal, and the
    // six upgrade fields are exposed to it identically.
    const limits = (harness = createMuxHarness()).mux._getResourceLimits()

    expect(limits).toEqual({
      maxFrameBytes: UPGRADE_MAX_FRAME_BYTES,
      maxOpenEntries: UPGRADE_MAX_OPEN_ENTRIES,
      maxIdBytes: UPGRADE_MAX_ID_BYTES,
      maxStagedRecords: UPGRADE_MAX_STAGED_RECORDS,
      maxStagedBytes: UPGRADE_MAX_STAGED_BYTES,
      stageTtlMs: UPGRADE_STAGE_TTL_MS,
      maxRawFrameBytes: WIRE_MAX_RAW_FRAME_BYTES,
      maxRecvBacklogBytes: WIRE_MAX_RECV_BACKLOG_BYTES,
      maxRecvBacklogFrames: WIRE_MAX_RECV_BACKLOG_FRAMES,
    })
  })

  test('a default-constructed mux charges the backlog fields it enforces on', async () => {
    // The bridge for the backlog pair. A 64 MiB / 50k overflow is not reachable in a unit test, so
    // what is asserted instead is that the DEFAULT mux — the one production builds — runs the same
    // accounting, at the same fields, with a real park holding the charge open. A mux rewired to
    // skip charging unless a limit was injected fails here.
    const h = (harness = createMuxHarness())
    const parked = h.createChannel('P')

    void h.ws.deliver(reconcileFrame({ open: [{ id: 'P', ix: 0, lastSeq: 0, initial: true }] }))
    void h.ws.deliver(textFrame(0, 1, 11))
    await settle()

    expect(h.ws.backlog()).toMatchObject({ frames: 2 })
    expect(h.ws.backlog()!.bytes).toBeGreaterThan(0)

    h.register(parked)
    await settle()
    expect(h.ws.backlog()).toMatchObject({ frames: 0, bytes: 0 })
  })
})
