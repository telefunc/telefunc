// Every resource bound the upgrade introduces, in one place: the server's PREPARE/barrier admission
// caps, the per-connection inbound wire caps, and the client's shared handoff-window budget.
//
// ── Two kinds of test, and neither alone is a gate ───────────────────────────────────────────
//   MECHANISM — a small limit is injected into the REAL `ChannelMux`, so the real enforcement path
//     runs it. An accountant standing beside the code stays green when production stops consulting
//     the limit, which is the exact failure these guard against.
//   BINDING — the resolved limits object is compared FIELD BY FIELD to the named constants. This is
//     the seam between the other two kinds: injecting a small limit proves enforcement dereferences
//     `limits.<field>`, and a constant holding its agreed number proves nothing about which field
//     reads it. `maxRecvBacklogFrames: WIRE_MAX_RECV_BACKLOG_BYTES` silently ships a 67,108,864 frame
//     ceiling with every mechanism test still green.
//
// ── The oracle every over-limit row uses ─────────────────────────────────────────────────────
// Three things, always: the offending wire is really terminated, the frame that overran was NOT
// delivered, and a live sentinel on ANOTHER connection still routes. The third is the independent
// one — "rejected" is otherwise satisfied by an implementation that rejected by tearing everything
// down, which is precisely what a cap must not do. Caps protect the server from a probe; they must
// never cost an established client its session.
//
// ── The park, and why it is the only honest one ──────────────────────────────────────────────
// Backlog only exists while a turn is unfinished, so a backlog test needs something genuinely
// awaited. The ONLY such seam reachable from `handleFrame` is `attach`'s
// `waitForChannelRegistration`. Async listeners park NOTHING (`_dispatchFrame` is `void`), so a
// slow-listener test could not fail. Each backlog row therefore opens with an instrument check that
// the park is real — the queued frames are provably still charged at assertion time.

import { afterEach, describe, expect, test, vi } from 'vitest'
import { stringify } from '@brillout/json-serializer/stringify'

import {
  createMuxHarness,
  disposeGlobalChannels,
  globalRegisterChannel,
  lengthPrefixed,
  openSseDownstream,
  pingFrame,
  prepareFrame,
  reconcileFrame,
  settle,
  textFrame,
} from './upgrade-mux-harness.js'
import { createUpgradeHarness, waitUntil, type UpgradeHarness } from './upgrade-client-harness.js'
import { getChannelMux } from './server/mux.js'
import { OversizeFrameError, StreamReader } from './server/request/StreamReader.js'
import { handleSseChannelRequest } from './server/sse.js'
import { encodeSseRequestMetadata } from './sse-request.js'
import { encodeU32 } from './frame.js'
import { TAG, encode } from './shared-ws.js'
import {
  UPGRADE_HANDOFF_BUFFER_BYTES,
  UPGRADE_HANDOFF_BUFFER_FRAMES,
  UPGRADE_MAX_FRAME_BYTES,
  UPGRADE_MAX_ID_BYTES,
  UPGRADE_MAX_OPEN_ENTRIES,
  UPGRADE_MAX_STAGED_BYTES,
  UPGRADE_MAX_STAGED_RECORDS,
  UPGRADE_STAGE_TTL_MS,
  SSE_METADATA_MAX_BYTES,
  WIRE_MAX_RAW_FRAME_BYTES,
  WIRE_MAX_RECV_BACKLOG_BYTES,
  WIRE_MAX_RECV_BACKLOG_FRAMES,
} from './constants.js'

type Harness = ReturnType<typeof createMuxHarness>

let harness: Harness | null = null
let clientHarness: UpgradeHarness | null = null
afterEach(() => {
  harness?.dispose()
  harness = null
  clientHarness?.dispose()
  clientHarness = null
  disposeGlobalChannels()
})

const EMPTY_STAGE = { records: 0, reverseRecords: 0, bytes: 0 }

async function connectSse(h: Harness) {
  const chA = h.registerChannel('A')
  await h.sse.deliver(reconcileFrame({ open: [{ id: 'A', ix: 0, lastSeq: 0, initial: true }] }))
  const s0 = h.sse.sessionId()
  expect(s0).toBeTypeOf('string')
  await h.sse.deliver(textFrame(0, 1, 111))
  expect(chA.received).toEqual([111])
  return { chA, s0: s0! }
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

/** The full over-limit oracle for an admission row. */
async function expectRejectedProbe(
  h: Harness,
  probe: { sent: { tag: number }[]; terminated: () => boolean },
  chA: { received: number[] },
) {
  expect(probe.terminated()).toBe(true)
  expect(probe.sent.filter((f) => f.tag === TAG.READY)).toHaveLength(0)
  expect(h.mux._getUpgradeResourceSnapshot()).toEqual(EMPTY_STAGE)
  await expectSentinelAlive(h, chA)
}

const openEntries = (count: number, idLength = 1) =>
  Array.from({ length: count }, (_, ix) => ({ id: 'c'.repeat(idLength) + ix, ix, lastSeq: 0 }))

type Probe = { sent: { tag: number }[]; terminated: () => boolean; sessionId: () => string | undefined }

/** Stage a probe, then hand the old wire the barrier under test. Every cap that bounds the decoded
 *  object graph is exercised on THIS route: PREPARE carries identity only, so the barrier is the one
 *  upgrade frame with membership to bound — and it was always the larger of the two. */
async function stageThenBarrier(
  h: Harness,
  probe: Probe & { deliver: (f: Uint8Array<ArrayBuffer>) => Promise<void> },
  s0: string,
  upgradeId: string,
  open: { id: string; ix: number; lastSeq: number }[],
) {
  await probe.deliver(prepareFrame({ upgradeId, sessionId: s0 }))
  expect(probe.sent.filter((f) => f.tag === TAG.READY)).toHaveLength(1)
  await h.sse.deliver(reconcileFrame({ sessionId: s0, barrier: true, upgradeId, open }))
}

/** A refused barrier kills the PROBE, not the wire that merely carried the oversized frame — a
 *  pre-commit rejection must leave the old session usable. */
async function expectRefusedBarrier(h: Harness, probe: Probe, chA: { received: number[] }, s0: string) {
  expect(probe.terminated()).toBe(true)
  expect(probe.sent.filter((f) => f.tag === TAG.RECONCILED)).toHaveLength(0) // nothing committed
  expect(probe.sessionId()).toBeUndefined()
  expect(h.mux._getUpgradeResourceSnapshot()).toEqual(EMPTY_STAGE)
  expect(h.sse.sessionId()).toBe(s0)
  await expectSentinelAlive(h, chA)
}

// ═════════════════════════════════════════════════════════════════════════════════════════════
// Server admission caps — PREPARE and barrier only
// ═════════════════════════════════════════════════════════════════════════════════════════════

describe('per-frame admission caps', () => {
  test('a PREPARE over the byte cap is refused, and one under it is accepted', async () => {
    // The pairing is the point: "over the cap is refused" alone is satisfied by a cap of zero, which
    // would refuse every upgrade forever and still look like working enforcement.
    const tight = (harness = createMuxHarness({ maxFrameBytes: 64 }))
    const { chA, s0 } = await connectSse(tight)
    await tight.ws.deliver(prepareFrame({ upgradeId: 'upg-1', sessionId: s0 }))
    await expectRejectedProbe(tight, tight.ws, chA)
    tight.dispose()

    const roomy = (harness = createMuxHarness({ maxFrameBytes: 4_096 }))
    const { s0: s1 } = await connectSse(roomy)
    await roomy.ws.deliver(prepareFrame({ upgradeId: 'upg-1', sessionId: s1 }))
    expect(roomy.ws.sent.filter((f) => f.tag === TAG.READY)).toHaveLength(1)
    expect(roomy.mux._getUpgradeResourceSnapshot().records).toBe(1)
  })

  test('the open-entry cap is pinned from BOTH sides — one over refused, exactly AT accepted', async () => {
    // Bounds the decoded object graph, which the byte cap alone does not: these frames are small.
    // Pinning from both directions is what stops a cap that merely moved from looking correct.
    const over = (harness = createMuxHarness({ maxOpenEntries: 2 }))
    const { chA, s0 } = await connectSse(over)
    await stageThenBarrier(over, over.ws, s0, 'upg-1', openEntries(3))
    await expectRefusedBarrier(over, over.ws, chA, s0)
    over.dispose()

    const at = (harness = createMuxHarness({ maxOpenEntries: 2 }))
    const { s0: s1 } = await connectSse(at)
    await stageThenBarrier(at, at.ws, s1, 'upg-1', [
      { id: 'A', ix: 0, lastSeq: 0 },
      { id: 'B', ix: 1, lastSeq: 0 },
    ])
    expect(at.ws.sent.filter((f) => f.tag === TAG.RECONCILED)).toHaveLength(1)
  })

  test('the id cap counts UTF-8 BYTES, not UTF-16 code units', async () => {
    // The cap exists to bound what the decoder allocated. A 3-character id of 3-byte code points is
    // 9 bytes on the wire; measuring `.length` would let it through and under-count by 3x. This row
    // is therefore strictly stronger than an ASCII over-long id, which the default-mux bridge below
    // covers anyway.
    const h = (harness = createMuxHarness({ maxIdBytes: 4 }))
    const { chA, s0 } = await connectSse(h)

    await stageThenBarrier(h, h.ws, s0, 'upg-1', [{ id: '一二三', ix: 0, lastSeq: 0 }])

    await expectRefusedBarrier(h, h.ws, chA, s0)
  })

  test('an ORDINARY reconcile is NOT capped — its contract is unchanged', async () => {
    // Load-bearing scope limit. Narrowing an established path would be a behaviour change to
    // certified code; these are admission control for the upgrade, not a new protocol rule.
    const h = (harness = createMuxHarness({ maxOpenEntries: 1, maxIdBytes: 2, maxFrameBytes: 64 }))
    const { s0 } = await connectSse(h)

    await h.sse.deliver(
      reconcileFrame({
        sessionId: s0,
        open: [
          { id: 'A', ix: 0, lastSeq: 1 },
          { id: 'a-very-long-channel-id-well-past-the-cap', ix: 1, lastSeq: 0 },
        ],
      }),
    )

    expect(h.sse.terminated()).toBe(false)
    expect(h.sse.sent.filter((f) => f.tag === TAG.RECONCILED)).toHaveLength(2)
  })

  test('the PREPARE byte cap runs BEFORE decode — the payload is never parsed', async () => {
    // The cap is documented as bounding decoder ALLOCATION, and a check running after `decode` has
    // already materialized the JSON does not do that. PREPARE's tag is byte 0, so the length can be
    // rejected with no decode at all. Behaviour cannot distinguish the two orderings — both refuse
    // and terminate — so the oracle is the allocation itself: whether `JSON.parse` ran.
    const h = (harness = createMuxHarness({ maxFrameBytes: 200 }))
    const { chA, s0 } = await connectSse(h)
    const parse = vi.spyOn(JSON, 'parse')

    try {
      await h.ws.deliver(prepareFrame({ upgradeId: 'u'.repeat(400), sessionId: s0 }))
      expect(parse).not.toHaveBeenCalled()
      // The spy CAN disagree: an admissible PREPARE on the same path is parsed. Without this, "never
      // parsed" would be satisfied by a spy watching the wrong function.
      const roomy = createMuxHarness({ maxFrameBytes: 4_096 })
      const { s0: s1 } = await connectSse(roomy)
      await roomy.ws.deliver(prepareFrame({ upgradeId: 'upg-1', sessionId: s1 }))
      expect(parse).toHaveBeenCalled()
      expect(roomy.ws.sent.filter((f) => f.tag === TAG.READY)).toHaveLength(1)
      roomy.dispose()
    } finally {
      parse.mockRestore()
    }
    await expectRejectedProbe(h, h.ws, chA)
  })
})

describe('global staged budget', () => {
  /** A second, fully independent SSE session, so a second stage is legal but for the budget. */
  async function connectSecondSse(h: Harness) {
    const sse2 = h.makeWire('sse-conn-2')
    h.registerChannel('B')
    await sse2.deliver(reconcileFrame({ open: [{ id: 'B', ix: 0, lastSeq: 0, initial: true }] }))
    const s = sse2.sessionId()
    expect(s).toBeTypeOf('string')
    return { sse2, s: s! }
  }

  test('the record cap rejects the NEW probe and leaves the incumbent stage intact', async () => {
    const h = (harness = createMuxHarness({ maxStagedRecords: 1 }))
    const { chA, s0 } = await connectSse(h)
    const { s: s2 } = await connectSecondSse(h)
    await h.ws.deliver(prepareFrame({ upgradeId: 'upg-1', sessionId: s0 }))

    const probe2 = h.makeWire(null)
    await probe2.deliver(prepareFrame({ upgradeId: 'upg-2', sessionId: s2 }))

    expect(probe2.terminated()).toBe(true)
    expect(probe2.sent.filter((f) => f.tag === TAG.READY)).toHaveLength(0)
    // At capacity the server sheds the newcomer, never the incumbent.
    expect(h.ws.terminated()).toBe(false)
    expect(h.mux._getUpgradeResourceSnapshot()).toMatchObject({ records: 1, reverseRecords: 1 })
    await expectSentinelAlive(h, chA)
  })

  test('the byte cap rejects the probe that would exceed it', async () => {
    const sizer = createMuxHarness()
    const oneFrameBytes = prepareFrame({ upgradeId: 'upg-1', sessionId: 'x'.repeat(36) }).byteLength
    sizer.dispose()

    // Room for exactly one stage of that size, not two.
    const h = (harness = createMuxHarness({ maxStagedBytes: oneFrameBytes + 4 }))
    const { chA, s0 } = await connectSse(h)
    const { s: s2 } = await connectSecondSse(h)
    await h.ws.deliver(prepareFrame({ upgradeId: 'upg-1', sessionId: s0 }))
    expect(h.mux._getUpgradeResourceSnapshot().records).toBe(1)

    const probe2 = h.makeWire(null)
    await probe2.deliver(prepareFrame({ upgradeId: 'upg-2', sessionId: s2 }))

    expect(probe2.terminated()).toBe(true)
    expect(h.mux._getUpgradeResourceSnapshot().records).toBe(1)
    await expectSentinelAlive(h, chA)
  })

  test('the budget is REFUNDED on cleanup — a later stage is accepted where the last one was refused', async () => {
    // The refund limb, expressed behaviourally. Asserting an empty snapshot after cleanup proves the
    // bookkeeping was zeroed, not that the budget is usable again — and a decrement that zeroes the
    // map while ratcheting the byte total passes that assertion while permanently refusing every
    // subsequent upgrade on a long-lived server. The oracle here is the thing a user would notice:
    // the NEXT stage is admitted.
    // Driven through the BYTE limb specifically: it is the one a wrong refund hides in. The record
    // limb is a map size, so a failure to decrement it is visible everywhere at once; the byte total
    // is an accumulator, and one that ratchets still reports an empty map after cleanup.
    const sizer = createMuxHarness()
    const oneFrameBytes = prepareFrame({ upgradeId: 'upg-1', sessionId: 'x'.repeat(36) }).byteLength
    sizer.dispose()

    const h = (harness = createMuxHarness({ maxStagedBytes: oneFrameBytes + 4 }))
    const { s0 } = await connectSse(h)
    const { sse2, s: s2 } = await connectSecondSse(h)
    await h.ws.deliver(prepareFrame({ upgradeId: 'upg-1', sessionId: s0 }))

    // At capacity: refused, exactly as the row above establishes.
    const probe2 = h.makeWire(null)
    await probe2.deliver(prepareFrame({ upgradeId: 'upg-2', sessionId: s2 }))
    expect(probe2.terminated()).toBe(true)

    // The incumbent goes away — its probe closes, which is the ordinary abandoned-attempt ending.
    h.ws.close()
    await settle()

    // Same wire, same limit, and now it fits. Nothing about the request changed; only the cleanup ran.
    const probe3 = h.makeWire(null)
    await probe3.deliver(prepareFrame({ upgradeId: 'upg-3', sessionId: s2 }))

    expect(probe3.terminated()).toBe(false)
    expect(probe3.sent.filter((f) => f.tag === TAG.READY)).toHaveLength(1)
    // And it is a real stage, not merely an un-refused one: it commits.
    await sse2.deliver(
      reconcileFrame({ sessionId: s2, barrier: true, upgradeId: 'upg-3', open: [{ id: 'B', ix: 0, lastSeq: 0 }] }),
    )
    expect(probe3.sent.filter((f) => f.tag === TAG.RECONCILED)).toHaveLength(1)
  })
})

describe('raw frame ceiling', () => {
  test('a frame over the ceiling terminates its wire and is never dispatched, one AT it is delivered', async () => {
    const h = (harness = createMuxHarness({ maxRawFrameBytes: 512 }))
    const chA = h.registerChannel('A')
    await h.sse.deliver(reconcileFrame({ open: [{ id: 'A', ix: 0, lastSeq: 0, initial: true }] }))
    const chW = h.registerChannel('W')
    await h.ws.deliver(reconcileFrame({ open: [{ id: 'W', ix: 0, lastSeq: 0, initial: true }] }))

    // The control first, on the same wire: a frame AT the ceiling flows. Without it, "over the
    // ceiling is refused" is satisfied by a ceiling of zero.
    const small = encode.text(0, JSON.stringify(42), 1)
    expect(small.byteLength).toBeLessThanOrEqual(512)
    await h.ws.deliver(small)
    expect(chW.received).toEqual([42])
    expect(h.ws.terminated()).toBe(false)

    await h.ws.deliver(encode.text(0, JSON.stringify('x'.repeat(2_000)), 2))

    expect(h.ws.terminated()).toBe(true)
    // Refused BEFORE decode and before dispatch — not decoded and then discarded.
    expect(chW.received).toEqual([42])
    await expectSentinelAlive(h, chA)
  })
})

describe('per-connection recv-chain backlog', () => {
  test('overflowing the queued-frame count terminates the offending wire only, and refunds on drain', async () => {
    const h = (harness = createMuxHarness({ maxRecvBacklogFrames: 3 }))
    const chA = h.registerChannel('A')
    await h.sse.deliver(reconcileFrame({ open: [{ id: 'A', ix: 0, lastSeq: 0, initial: true }] }))
    const parked = h.createChannel('P')

    void h.ws.deliver(reconcileFrame({ open: [{ id: 'P', ix: 0, lastSeq: 0, initial: true }] }))
    void h.ws.deliver(textFrame(0, 1, 11))
    void h.ws.deliver(textFrame(0, 2, 22))
    await settle()

    // INSTRUMENT CHECK: the park is real. Three frames are admitted and still charged, and the
    // enforcement fields say so — without this the overflow below could be an artifact of frames that
    // already drained.
    expect(h.ws.backlog()).toMatchObject({ frames: 3 })
    expect(h.ws.terminated()).toBe(false)

    void h.ws.deliver(textFrame(0, 3, 33)) // the fourth cannot be admitted
    await settle()

    expect(h.ws.terminated()).toBe(true)
    // Rejected, not queued: the charge is unchanged by the frame that overran. Asserted BEFORE the
    // park is released, so the queued frames cannot have terminated the wire for some other reason.
    expect(h.ws.backlog()).toMatchObject({ frames: 3 })
    await expectSentinelAlive(h, chA)

    // Releasing the park drains the chain and REFUNDS every charge, observed on the field enforcement
    // reads rather than on a tally beside it.
    h.register(parked)
    await settle()
    expect(h.ws.backlog()).toMatchObject({ frames: 0, bytes: 0 })
  })

  test('a PING is answered while the recv chain is PARKED — the bypass reads the raw byte', async () => {
    // The bypass predicate's own row, and it gates the thing the synchronous-refund row cannot: that
    // PING is separated from ordinary traffic BEFORE the chain, by `rawFrame[0]`, rather than after
    // decode. Liveness must not be serialized behind the slowest awaitable on the connection —
    // otherwise a wire parked on a channel registration stops ponging and the client drops a
    // connection that is perfectly healthy.
    //
    // A refactor broke exactly this predicate once and only the refund half had a row.
    const h = (harness = createMuxHarness())
    const chA = h.registerChannel('A')
    await h.sse.deliver(reconcileFrame({ open: [{ id: 'A', ix: 0, lastSeq: 0, initial: true }] }))
    const parked = h.createChannel('P')

    // Park the chain on a registration that has not happened, then queue ordinary traffic behind it.
    void h.ws.deliver(reconcileFrame({ open: [{ id: 'P', ix: 0, lastSeq: 0, initial: true }] }))
    void h.ws.deliver(textFrame(0, 1, 11))
    await settle()
    // INSTRUMENT CHECK: the park is real, so "the PONG came back" cannot be an artifact of an idle
    // chain. The queued data frame is the discriminator — it must still be waiting.
    expect(h.ws.backlog()).toMatchObject({ frames: 2 })
    expect(h.ws.sent.filter((f) => f.tag === TAG.PONG)).toHaveLength(0)

    await h.ws.deliver(pingFrame())

    // Answered while everything ahead of it is still parked.
    expect(h.ws.sent.filter((f) => f.tag === TAG.PONG)).toHaveLength(1)
    expect(h.ws.backlog()).toMatchObject({ frames: 2 })

    // Control: releasing the park lets the queued traffic through, proving it really was blocked and
    // that the PING did not somehow drain the chain on its way past.
    h.register(parked)
    await settle()
    expect(chA.received).toEqual([])
    expect(h.ws.backlog()).toMatchObject({ frames: 0 })
  })

  test('frames far past the cap flow freely when the chain is never parked', async () => {
    // The decrement's own row. With the refund skipped the counter ratchets and this wire dies
    // partway through — even though it never has more than one frame in flight at a time.
    const h = (harness = createMuxHarness({ maxRecvBacklogFrames: 3 }))
    const chW = h.registerChannel('W')
    await h.ws.deliver(reconcileFrame({ open: [{ id: 'W', ix: 0, lastSeq: 0, initial: true }] }))

    for (let i = 1; i <= 12; i++) await h.ws.deliver(textFrame(0, i, i))

    expect(h.ws.terminated()).toBe(false)
    expect(chW.received).toEqual([1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12])
    expect(h.ws.backlog()).toMatchObject({ frames: 0, bytes: 0 })
  })

  test('a PING refunds SYNCHRONOUSLY, so same-stack PINGs never accumulate as backlog', () => {
    // A PING bypasses the recv chain and is handled without ever awaiting, so its refund must land
    // before `dispatchInbound` returns. Wrap that turn in one more `async` layer and the refund slips
    // by a microtask — at which point a transport adapter that delivers PING callbacks in a single
    // turn kills its own healthy wire. At the shipped ceiling that takes 50,001 callbacks; with the
    // cap at 1 the second PING is enough to see it.
    const h = (harness = createMuxHarness({ maxRecvBacklogFrames: 1 }))

    void h.ws.deliver(pingFrame())
    // INSTRUMENT CHECK: the refund really was synchronous. Nothing has awaited yet, so a refund
    // deferred to a microtask still reads `frames: 1` here and this row fails on its own terms —
    // before the second PING has had any chance to be the thing that overran.
    expect(h.ws.backlog()).toMatchObject({ frames: 0, bytes: 0 })

    void h.ws.deliver(pingFrame())

    expect(h.ws.terminated()).toBe(false)
    expect(h.ws.backlog()).toMatchObject({ frames: 0, bytes: 0 })
  })

  test('overflowing the queued BYTE budget terminates the offending wire only', async () => {
    // Sized from the real frames, so this proves the byte unit specifically: the FRAME cap is at its
    // 50k default throughout and only three frames are ever in flight.
    const park = reconcileFrame({ open: [{ id: 'P', ix: 0, lastSeq: 0, initial: true }] })
    const big = encode.text(0, JSON.stringify('x'.repeat(4_000)), 1)
    const h = (harness = createMuxHarness({ maxRecvBacklogBytes: park.byteLength + big.byteLength }))
    const chA = h.registerChannel('A')
    await h.sse.deliver(reconcileFrame({ open: [{ id: 'A', ix: 0, lastSeq: 0, initial: true }] }))
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

describe('SSE framing — the DECLARED length, before the body is pulled', () => {
  /** A source reporting how many chunks the reader actually pulled. The pull count is the whole
   *  point: a cap enforced AFTER the read bounds what is RETAINED while still letting a hostile u32
   *  drive the allocation, and only this counter can tell the two apart. */
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

  test('the metadata header is capped by its DECLARED length too [B8]', async () => {
    // The metadata read is the FIRST read on every POST shape and runs ahead of every validation —
    // the earliest allocation an unauthenticated request can drive, through `readExact`'s repeated
    // `concat`. The pull counter is what separates a real pre-read refusal from one that bounds only
    // what is retained; without the cap the reader keeps asking for the announced bytes.
    const over = countingSource([encodeU32(SSE_METADATA_MAX_BYTES + 1) as Uint8Array<ArrayBuffer>])
    await expect(new StreamReader(over.source).readMetadata(SSE_METADATA_MAX_BYTES)).rejects.toBeInstanceOf(
      OversizeFrameError,
    )
    expect(over.pulls()).toBe(1) // reading the declared body would need a second

    // AT the boundary it is read normally, so the refusal is the length check and not a reader that
    // simply fails on this shape.
    const json = new TextEncoder().encode('{"connId":"x"}') as Uint8Array<ArrayBuffer>
    const at = countingSource([encodeU32(json.byteLength) as Uint8Array<ArrayBuffer>, json])
    expect(await new StreamReader(at.source).readMetadata(json.byteLength)).toBe('{"connId":"x"}')
    expect(at.pulls()).toBe(2)
  })

  test('at the shipped default, a hostile metadata length is a 400 through the real entry point [B8]', async () => {
    // THE BRIDGE, and honestly scoped: it gates the WIRING — that `handleRequest` reaches
    // `readMetadata` with a ceiling and that an `OversizeFrameError` lands in the 400 sink rather
    // than the internal-error log. It does NOT discriminate the cap's value: a body truncated after
    // the prefix produces the same 400 by a different route. The row above is what gates the cap
    // itself, by counting pulls.
    const four = encodeU32(SSE_METADATA_MAX_BYTES + 1) as Uint8Array<ArrayBuffer>
    const response = await handleSseChannelRequest(
      new Request('http://test.local/_telefunc', { method: 'POST', body: new Blob([four]) }),
    )
    expect(response?.statusCode).toBe(400)
  })

  test('a declared length over the cap is refused without pulling one body byte', async () => {
    // The prefix alone is delivered. No body follows it, and none is asked for.
    const over = countingSource([encodeU32(64) as Uint8Array<ArrayBuffer>])
    await expect(new StreamReader(over.source).readLengthPrefixedBytesOrNull(63)).rejects.toBeInstanceOf(
      OversizeFrameError,
    )
    expect(over.pulls()).toBe(1) // reading the declared body would need a second

    // AT the cap it is read normally — so the refusal is the length check, not a reader that fails.
    const body = new Uint8Array(64) as Uint8Array<ArrayBuffer>
    body[0] = 7
    const at = countingSource([encodeU32(64) as Uint8Array<ArrayBuffer>, body])
    const frame = await new StreamReader(at.source).readLengthPrefixedBytesOrNull(64)
    expect(frame?.byteLength).toBe(64)
    expect(frame?.[0]).toBe(7)
    expect(at.pulls()).toBe(2)
  })

  // THE BRIDGE for the raw-frame ceiling: this drives the module-global mux at its SHIPPED default,
  // so a production path that stopped reading the constant would not terminate that connection.
  test('at the shipped default, a body declaring more than the ceiling kills that connection and no other', async () => {
    const victimId = crypto.randomUUID()
    const sentinelId = crypto.randomUUID()
    const chVictim = globalRegisterChannel(`V-${victimId}`)
    const chSentinel = globalRegisterChannel(`S-${sentinelId}`)
    await openSseDownstream(victimId, chVictim.id)
    await openSseDownstream(sentinelId, chSentinel.id)

    // FOUR BYTES on the wire announce 64 MiB + 1. Nothing follows, and nothing needs to: the declared
    // length alone is the whole attack, and refusing by it is what makes refusing free.
    const received: number[] = []
    chSentinel.listen((n) => received.push(n))
    const oversize = new Blob([encodeSseRequestMetadata({ connId: victimId }), encodeU32(WIRE_MAX_RAW_FRAME_BYTES + 1)])
    await handleSseChannelRequest(new Request('http://test.local/_telefunc', { method: 'POST', body: oversize }))

    // Real termination: the mux no longer holds the connection at all.
    expect(getChannelMux().getConnectionByConnId(victimId)).toBeUndefined()

    // The live sentinel: a DIFFERENT SSE connection still routes application traffic on the same path.
    const sentinelPost = new Blob([
      encodeSseRequestMetadata({ connId: sentinelId }),
      lengthPrefixed(encode.text(0, JSON.stringify(4_242), 1)),
    ])
    await handleSseChannelRequest(new Request('http://test.local/_telefunc', { method: 'POST', body: sentinelPost }))
    await settle()

    expect(getChannelMux().getConnectionByConnId(sentinelId)).toBeDefined()
    expect(received).toEqual([4_242])
  })
})

// ═════════════════════════════════════════════════════════════════════════════════════════════
// The client's shared handoff-window budget
// ═════════════════════════════════════════════════════════════════════════════════════════════

describe('the handoff buffer budget is ONE budget shared across both wires', () => {
  const handoffHarness = async () => (clientHarness = await createUpgradeHarness(['A'], { barrier: 'refuse' }))

  /** Budget for the overflow rows, and it is load-bearing rather than a convenience.
   *
   *  Measured trap: with the cap mutated to `Infinity` these still went green, because a flood that
   *  receives neither FIN nor COMMITTED ALSO trips the 2 s join deadline and falls back — same
   *  observable, entirely different guard. The two are separable only in time: the cap trips
   *  synchronously during the flood (fallback ≈ the 500 ms reconnect delay), the deadline cannot trip
   *  before 2 s. This budget sits between them. */
  const OVERFLOW_BUDGET_MS = 1_500
  const waitForFallback = (h: UpgradeHarness, budgetMs: number) =>
    waitUntil(() => h.sseConnects() === 2, 'upgrade fell back to a fresh SSE connection', budgetMs)

  const wsFrame = (seq: number) => encode.text(0, stringify(`ws-${seq}`), seq)

  test('a new-wire flood past the FRAME budget falls back and discards the new buffer WHOLE', async () => {
    const h = await handoffHarness()

    // Exactly one frame past the budget. Emitting more after the trip would be unfaithful — the real
    // WS is disposed by the fallback and delivers nothing further.
    for (let seq = 1; seq <= UPGRADE_HANDOFF_BUFFER_FRAMES + 1; seq++) h.ws.pushFrame(wsFrame(seq))

    await waitForFallback(h, OVERFLOW_BUDGET_MS)

    // Not one frame of the new buffer is dispatched — not even the prefix that fit. Dispatching a
    // prefix would advance `trackSeq` past old-wire frames that never arrived, converting a
    // recoverable abort into permanent loss.
    expect(h.channels[0]!.received).toEqual([])
  })

  test('control: a new-wire burst that stays within the frame budget completes normally', async () => {
    // The instrument that can disagree, at the exact boundary: one frame BELOW the budget completes
    // and delivers everything. So it is the limit that trips the fallback, not the flood.
    const h = await handoffHarness()

    for (let seq = 1; seq <= UPGRADE_HANDOFF_BUFFER_FRAMES; seq++) h.ws.pushFrame(wsFrame(seq))
    h.ws.pushFrame(h.committedFrame())
    h.sse.pushFrame(encode.fin())
    await waitUntil(() => h.handoffDrained(), 'handoff completed')

    expect(h.sseConnects()).toBe(1)
    expect(h.channels[0]!.received).toHaveLength(UPGRADE_HANDOFF_BUFFER_FRAMES)
  })

  test('a flood split across BOTH wires trips the budget neither partition would trip alone', async () => {
    // Every other row floods ONE wire, so an implementation with independent per-source limits would
    // pass them all while permitting almost twice the intended total. This is the case that
    // discriminates — each partition stays comfortably under and only their SUM exceeds. It also
    // exercises the fallback's asymmetry (old prefix delivered in full, new buffer discarded whole),
    // which single-source rows cannot show because there the old partition is empty. And it drives
    // the BYTE limb: 17 frames is nowhere near the 4096 frame budget.
    const h = await handoffHarness()

    const CHUNK = 512 * 1024
    const OLD_FRAMES = 9 // 4.5 MiB
    const NEW_FRAMES = 8 // 4.0 MiB — sum 8.5 MiB, over the 8 MiB budget; neither half is close
    const chunk = (seq: number) => encode.binary(0, new Uint8Array(CHUNK), seq)

    for (let seq = 1; seq <= OLD_FRAMES; seq++) h.sse.pushFrame(chunk(seq))
    // The SSE frames travel a real ReadableStream, so wait for the accounting to show them rather
    // than sleeping — this is also the assertion that the old partition alone is well under.
    await waitUntil(() => (h.handoffBuffered()?.frames ?? 0) === OLD_FRAMES, 'old-wire frames buffered')
    expect(h.handoffBuffered()!.bytes).toBeLessThan(UPGRADE_HANDOFF_BUFFER_BYTES)

    for (let seq = OLD_FRAMES + 1; seq <= OLD_FRAMES + NEW_FRAMES; seq++) {
      if (h.handoffBuffered() === null) break // stop at the trip
      h.ws.pushFrame(chunk(seq))
    }
    expect((NEW_FRAMES * CHUNK) / UPGRADE_HANDOFF_BUFFER_BYTES).toBeLessThan(1)
    expect(NEW_FRAMES + OLD_FRAMES).toBeLessThan(UPGRADE_HANDOFF_BUFFER_FRAMES)

    await waitForFallback(h, OVERFLOW_BUDGET_MS)

    expect(h.channels[0]!.received).toHaveLength(OLD_FRAMES)
  })

  test('a probe that is not yet committing DROPS what it receives instead of buffering it', async () => {
    // `prepare: 'withhold'` holds the client in `staging`. The probe socket is live but it is nobody's
    // wire and carries no session, so no compliant server can address a frame to it: between PREPARE
    // and the commit the mux answers a staged probe with nothing but READY. Anything arriving here is
    // hostile or buggy, and the bounded answer is to DROP it — buffering would pin memory against an
    // unauthenticated socket the attempt may never adopt.
    //
    // Dropping is only safe because it is SILENT: the loss that matters is not "a frame went
    // missing", it is `trackSeq` advancing past a frame that was never dispatched. Once it has, the
    // client reports the HIGHER cursor and the server's replay is dup-suppressed forever. So the
    // oracle is whether a frame at the ORIGINAL seq is still deliverable — not a buffer count.
    const h = (clientHarness = await createUpgradeHarness(['A'], { prepare: 'withhold', barrier: 'refuse' }))
    const channel = h.channels[0]!
    expect(h.upgradeTag()).toBe('staging')

    for (let seq = 1; seq <= UPGRADE_HANDOFF_BUFFER_FRAMES + 2; seq++) {
      h.ws.pushFrame(encode.text(0, `"dropped-${seq}"`, seq))
    }
    // Nothing dispatched and nothing accounted: there is no record to charge them against yet.
    expect(channel.received).toHaveLength(0)
    expect(h.handoffBuffered()).toBeNull()

    // The attempt is untouched by the flood — it commits exactly as it would have without it.
    h.sendReady()
    await waitUntil(() => h.flipped(), 'the attempt committed and flipped')
    h.ws.pushFrame(h.committedFrame())
    h.sse.pushFrame(encode.fin())
    await waitUntil(() => h.handoffDrained(), 'the join settled', 6_000)

    expect(channel.received).toHaveLength(0)
    expect(channel.isClosed).toBe(false)
    expect(h.sseConnects()).toBe(1)

    // THE oracle: `trackSeq` must not have moved, so a frame at seq 1 is still deliverable. Had any
    // dropped frame been dispatched, the cursor would sit past 4096 and this would be a silent dup.
    h.ws.pushFrame(encode.text(0, '"replayed-1"', 1))
    await waitUntil(() => channel.received.length === 1, 'the dropped prefix is still replayable', 6_000)
    expect(channel.received).toEqual([{ kind: 'text', value: 'replayed-1' }])
  }, 20_000)

  test('frames held between the barrier and the flip are charged against that same budget', async () => {
    // The real pre-flip hold window, and the only one there is: from the barrier's emission the
    // commit record exists, the COMMITTED may legitimately arrive on the probe, and it must WAIT for
    // the flip. `hangBarrierPost` holds the client exactly there. Uncharged, this window is unbounded
    // memory the documented cap never sees — the flood arrives while the emission's own await is
    // still parked.
    const h = (clientHarness = await createUpgradeHarness(['A'], {
      batchMode: true,
      barrier: 'refuse',
      hangBarrierPost: true,
    }))
    const channel = h.channels[0]!
    expect(h.upgradeTag()).toBe('committing')
    expect(h.flipped()).toBe(false)

    for (let seq = 1; seq <= UPGRADE_HANDOFF_BUFFER_FRAMES + 1; seq++) {
      if (h.handoffBuffered() === null) break // stop at the trip
      h.ws.pushFrame(encode.text(0, `"new-${seq}"`, seq))
    }

    // Pre-flip a budget trip only ABORTS the attempt — `commitBarrier`'s verdict routing owns the one
    // recovery, and it takes the sticky both-wires fallback because the barrier may already be read.
    await waitForFallback(h, OVERFLOW_BUDGET_MS)

    // Discarded as a SET — keeping the prefix that fit would keep exactly the frames whose seq is
    // highest, which is what poisons the cursor.
    expect(channel.received).toEqual([])
    expect(channel.isClosed).toBe(false)
  }, 20_000)

  test('a held frame is charged ONCE — the flip refunds before it re-ingests', async () => {
    // The measured scar this design dissolves: the pre-flip queue and the post-flip buffer used to be
    // two accumulators that never saw each other's totals, so a frame was charged at capture AND
    // again at replay. True peak was 2× the documented budget, with different overflow policies at
    // each half. One record with one budget makes charge-once the only expressible shape, and this
    // row is what pins it: a held set just over HALF the budget completes normally, and would trip a
    // client that charged it twice. Nothing else discriminates — every other budget row floods far
    // enough past the cap that doubling changes nothing.
    const HELD = Math.ceil(UPGRADE_HANDOFF_BUFFER_FRAMES / 2) + 1
    const h = (clientHarness = await createUpgradeHarness(['A'], {
      batchMode: true,
      barrier: 'refuse',
      hangBarrierPost: true,
    }))
    const channel = h.channels[0]!

    for (let seq = 1; seq <= HELD; seq++) h.ws.pushFrame(encode.text(0, `"held-${seq}"`, seq))
    expect(h.handoffBuffered()?.frames).toBe(HELD)
    expect(HELD * 2).toBeGreaterThan(UPGRADE_HANDOFF_BUFFER_FRAMES)

    h.releaseHungPosts()
    await waitUntil(() => h.flipped(), 'the emission settled and the flip ran')
    // Charged once, so the flip leaves the accounting exactly where it found it.
    expect(h.handoffBuffered()?.frames).toBe(HELD)

    h.ws.pushFrame(h.committedFrame([{ ix: 0, lastSeq: 0 }]))
    h.sse.pushFrame(encode.fin())
    await waitUntil(() => h.handoffDrained(), 'the join settled', 6_000)

    // No fallback: the budget was never exceeded, so every held frame is delivered.
    expect(h.sseConnects()).toBe(1)
    expect(channel.received).toHaveLength(HELD)
  }, 20_000)

  test('control: a held burst UNDER the cap is delivered in full, in order, on the NEW wire', async () => {
    // Without this, "zero delivered" above is satisfied just as well by a client that drops every
    // held frame — a different bug with the same assertion.
    //
    // It also gates the flip's ORDERING, which nothing else does: the adoption must happen BEFORE the
    // held frames are re-ingested. Re-ingesting first routes the COMMITTED among them through a
    // connection whose transport is still the OLD wire, so the frames its settlement releases go out
    // on the wire the barrier just retired — hence the assertion on which wire carried them.
    const h = (clientHarness = await createUpgradeHarness(['A'], {
      batchMode: true,
      barrier: 'refuse',
      hangBarrierPost: true,
    }))
    const channel = h.channels[0]!

    for (let seq = 1; seq <= 3; seq++) h.ws.pushFrame(encode.text(0, `"new-${seq}"`, seq))
    // The COMMITTED rides in the SAME held set, behind the data — the ordinary ordering whenever the
    // server commits before the client's emission await has resumed.
    h.ws.pushFrame(h.committedFrame([{ ix: 0, lastSeq: 0 }]))
    expect(h.handoffBuffered()?.frames).toBe(4)
    const upstreamBeforeFlip = h.sse.upstream.length

    h.releaseHungPosts()
    await waitUntil(() => h.flipped(), 'the emission settled and the flip ran')
    h.sse.pushFrame(encode.fin())
    await waitUntil(() => h.handoffDrained(), 'the join settled', 6_000)

    expect(channel.received).toEqual([
      { kind: 'text', value: 'new-1' },
      { kind: 'text', value: 'new-2' },
      { kind: 'text', value: 'new-3' },
    ])
    // The retired wire carried nothing after the barrier.
    expect(h.sse.upstream).toHaveLength(upstreamBeforeFlip)
    expect(h.sseConnects()).toBe(1)
  }, 20_000)
})

describe('a default-constructed mux reads the shipped constants', () => {
  test('the DEFAULT mux enforces the shipped id and open-entry caps', async () => {
    // The bridge for the admission pair: without it, both other kinds of test could be true of a mux
    // rewired to ignore the constants entirely.
    const h = (harness = createMuxHarness())
    const { chA, s0 } = await connectSse(h)

    // One past the entry cap, with short ids so the BYTE cap is nowhere near — refused for its entry
    // count or not at all.
    const over = openEntries(UPGRADE_MAX_OPEN_ENTRIES + 1)
    expect(reconcileFrame({ sessionId: s0, barrier: true, upgradeId: 'upg-1', open: over }).byteLength).toBeLessThan(
      UPGRADE_MAX_FRAME_BYTES,
    )
    await stageThenBarrier(h, h.ws, s0, 'upg-1', over)
    await expectRefusedBarrier(h, h.ws, chA, s0)

    const probe2 = h.makeWire(null)
    await stageThenBarrier(h, probe2, s0, 'upg-2', [{ id: 'x'.repeat(UPGRADE_MAX_ID_BYTES + 1), ix: 0, lastSeq: 0 }])
    expect(probe2.terminated()).toBe(true)
    expect(probe2.sent.filter((f) => f.tag === TAG.RECONCILED)).toHaveLength(0)

    // ...and one AT the entry cap is accepted, so the default is pinned from both sides.
    const probe3 = h.makeWire(null)
    await stageThenBarrier(h, probe3, s0, 'upg-3', openEntries(UPGRADE_MAX_OPEN_ENTRIES))
    expect(probe3.sent.filter((f) => f.tag === TAG.RECONCILED)).toHaveLength(1)
  })

  test('the DEFAULT mux charges the backlog fields it enforces on', async () => {
    // The bridge for the backlog pair. A 64 MiB / 50k overflow is not reachable in a unit test, so
    // what is asserted instead is that the DEFAULT mux — the one production builds — runs the same
    // accounting, at the same fields, with a real park holding the charge open. A mux rewired to skip
    // charging unless a limit was injected fails here.
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
