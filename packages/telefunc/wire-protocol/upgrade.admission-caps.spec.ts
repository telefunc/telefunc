// T4 — ADMISSION CAPS on the barrier upgrade.
//
// Two kinds of test per limit, deliberately separated:
//
//   MECHANISM — a small limit is injected into the REAL `ChannelMux` and the real `handlePrepare`
//     enforces it. Not a pure accountant standing beside the code: an accountant test stays green
//     when the production path stops consulting it, which is the whole failure this guards against.
//   VALUE — the shipped default is pinned by name. A mechanism test at limit 2 passes just as
//     happily if someone changes the production default to 2, or to a billion.
//
// Neither alone is a gate. The mechanism test proves the check runs; the value test proves it runs
// at the number that was agreed.
//
// ── The oracle every over-limit row uses ─────────────────────────────────────────────────────
// Three assertions, always: the offending wire is really terminated, NO stage exists (read from the
// same fields admission enforces on), and the old SSE session still routes a live sentinel. The
// third is the independent one — without it, "rejected" is satisfied by an implementation that
// rejected by tearing down everything, which is precisely what a cap must not do. Caps exist to
// protect the server from a probe, not to cost an established client its session.

import { afterEach, describe, expect, test } from 'vitest'

import { createMuxHarness, prepareFrame, reconcileFrame, settle, textFrame } from './upgrade-mux-harness.js'
import {
  UPGRADE_MAX_FRAME_BYTES,
  UPGRADE_MAX_ID_BYTES,
  UPGRADE_MAX_OPEN_ENTRIES,
  UPGRADE_MAX_STAGED_BYTES,
  UPGRADE_MAX_STAGED_RECORDS,
  UPGRADE_STAGE_TTL_MS,
} from './constants.js'
import { TAG } from './shared-ws.js'

let harness: ReturnType<typeof createMuxHarness> | null = null
afterEach(() => {
  harness?.dispose()
  harness = null
})

const EMPTY_STAGE = { records: 0, reverseRecords: 0, bytes: 0 }

async function connectSse(h: ReturnType<typeof createMuxHarness>) {
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

/** The full over-limit oracle. Every mechanism test below ends here. */
async function expectRejectedProbe(
  h: ReturnType<typeof createMuxHarness>,
  probe: { sent: { tag: number }[]; terminated: () => boolean },
  chA: { received: number[] },
) {
  expect(probe.terminated()).toBe(true)
  expect(probe.sent.filter((f) => f.tag === TAG.READY)).toHaveLength(0)
  expect(h.mux._getUpgradeResourceSnapshot()).toEqual(EMPTY_STAGE)
  expect(h.sse.terminated()).toBe(false)
  // The independent oracle: the established session is untouched and still delivering.
  const value = ++sentinelValue
  await h.sse.deliver(textFrame(0, ++sentinelSeq, value))
  expect(chA.received.at(-1)).toBe(value)
}

const openEntries = (count: number, idLength = 1) =>
  Array.from({ length: count }, (_, ix) => ({ id: 'c'.repeat(idLength) + ix, ix }))

describe('per-frame caps — mechanism', () => {
  test('a PREPARE over the byte cap is refused', async () => {
    const h = (harness = createMuxHarness({ maxFrameBytes: 64 }))
    const { chA, s0 } = await connectSse(h)

    // Comfortably over 64 bytes once the header and JSON are counted.
    await h.ws.deliver(prepareFrame({ upgradeId: 'upg-1', sessionId: s0, open: openEntries(8) }))

    await expectRejectedProbe(h, h.ws, chA)
  })

  test('a PREPARE under the byte cap is accepted — the control for the row above', async () => {
    // Without this pairing, "over the cap is refused" is satisfied by a cap of zero, which would
    // refuse every upgrade forever and still look like working enforcement.
    const h = (harness = createMuxHarness({ maxFrameBytes: 4_096 }))
    const { s0 } = await connectSse(h)

    await h.ws.deliver(prepareFrame({ upgradeId: 'upg-1', sessionId: s0, open: openEntries(8) }))

    expect(h.ws.sent.filter((f) => f.tag === TAG.READY)).toHaveLength(1)
    expect(h.mux._getUpgradeResourceSnapshot().records).toBe(1)
  })

  test('a PREPARE over the open-entry cap is refused', async () => {
    // Bounds the decoded object graph, which the byte cap alone does not: this frame is small.
    const h = (harness = createMuxHarness({ maxOpenEntries: 2 }))
    const { chA, s0 } = await connectSse(h)

    await h.ws.deliver(prepareFrame({ upgradeId: 'upg-1', sessionId: s0, open: openEntries(3) }))

    await expectRejectedProbe(h, h.ws, chA)
  })

  test('a PREPARE exactly AT the open-entry cap is accepted', async () => {
    const h = (harness = createMuxHarness({ maxOpenEntries: 2 }))
    const { s0 } = await connectSse(h)

    await h.ws.deliver(prepareFrame({ upgradeId: 'upg-1', sessionId: s0, open: openEntries(2) }))

    expect(h.ws.sent.filter((f) => f.tag === TAG.READY)).toHaveLength(1)
  })

  test('a PREPARE with an over-long channel id is refused', async () => {
    const h = (harness = createMuxHarness({ maxIdBytes: 4 }))
    const { chA, s0 } = await connectSse(h)

    await h.ws.deliver(prepareFrame({ upgradeId: 'upg-1', sessionId: s0, open: [{ id: 'AAAAAAAA', ix: 0 }] }))

    await expectRejectedProbe(h, h.ws, chA)
  })

  test('the id cap counts UTF-8 bytes, not UTF-16 code units', async () => {
    // The cap exists to bound what the decoder allocated. A 3-character id of 3-byte code points is
    // 9 bytes on the wire; measuring `.length` would let it through and under-count by 3x.
    const h = (harness = createMuxHarness({ maxIdBytes: 4 }))
    const { chA, s0 } = await connectSse(h)

    await h.ws.deliver(prepareFrame({ upgradeId: 'upg-1', sessionId: s0, open: [{ id: '一二三', ix: 0 }] }))

    await expectRejectedProbe(h, h.ws, chA)
  })

  test('a BARRIER is capped on the same terms as a PREPARE', async () => {
    // The caps apply to `barrier: true` reconciles too — the barrier carries the authoritative
    // membership, so it is the larger of the two frames and the one worth bounding.
    const h = (harness = createMuxHarness({ maxOpenEntries: 2 }))
    const { chA, s0 } = await connectSse(h)
    await h.ws.deliver(prepareFrame({ upgradeId: 'upg-1', sessionId: s0, open: [{ id: 'A', ix: 0 }] }))

    await h.sse.deliver(
      reconcileFrame({
        sessionId: s0,
        upgrade: true,
        barrier: true,
        upgradeId: 'upg-1',
        open: openEntries(3).map(({ id, ix }) => ({ id, ix, lastSeq: 0 })),
      }),
    )

    // The probe dies, not the wire that carried the oversized frame — a pre-commit rejection must
    // leave the old session usable. (The shared oracle does not fit here: this probe legitimately
    // received its READY before the barrier arrived, so "no READY" would be the wrong assertion.)
    expect(h.ws.terminated()).toBe(true)
    expect(h.ws.sent.filter((f) => f.tag === TAG.RECONCILED)).toHaveLength(0) // nothing committed
    expect(h.ws.sessionId()).toBeUndefined()
    expect(h.mux._getUpgradeResourceSnapshot()).toEqual(EMPTY_STAGE)
    expect(h.sse.terminated()).toBe(false)
    expect(h.sse.sessionId()).toBe(s0)
    const value = ++sentinelValue
    await h.sse.deliver(textFrame(0, ++sentinelSeq, value))
    expect(chA.received.at(-1)).toBe(value)
  })

  test('an ORDINARY reconcile is NOT capped — its contract is unchanged', async () => {
    // Load-bearing scope limit. Narrowing an established path would be a behaviour change to
    // certified code; the caps are admission control for the upgrade, not a new protocol rule.
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
})

describe('global staged budget — mechanism', () => {
  /** A second, fully independent SSE session, so a second stage is legal but for the budget. */
  async function connectSecondSse(h: ReturnType<typeof createMuxHarness>) {
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
    await h.ws.deliver(prepareFrame({ upgradeId: 'upg-1', sessionId: s0, open: [{ id: 'A', ix: 0 }] }))

    const probe2 = h.makeWire(null)
    await probe2.deliver(prepareFrame({ upgradeId: 'upg-2', sessionId: s2, open: [{ id: 'B', ix: 0 }] }))

    expect(probe2.terminated()).toBe(true)
    expect(probe2.sent.filter((f) => f.tag === TAG.READY)).toHaveLength(0)
    // At capacity the server sheds the newcomer, never the incumbent.
    expect(h.ws.terminated()).toBe(false)
    expect(h.mux._getUpgradeResourceSnapshot()).toMatchObject({ records: 1, reverseRecords: 1 })
    expect(h.sse.terminated()).toBe(false)
    const value = ++sentinelValue
    await h.sse.deliver(textFrame(0, ++sentinelSeq, value))
    expect(chA.received.at(-1)).toBe(value)
  })

  test('the byte cap rejects the probe that would exceed it', async () => {
    const h0 = createMuxHarness()
    const oneFrameBytes = prepareFrame({
      upgradeId: 'upg-1',
      sessionId: 'x'.repeat(36),
      open: [{ id: 'A', ix: 0 }],
    }).byteLength
    h0.dispose()

    // Room for exactly one stage of that size, not two.
    const h = (harness = createMuxHarness({ maxStagedBytes: oneFrameBytes + 4 }))
    const { chA, s0 } = await connectSse(h)
    const { s: s2 } = await connectSecondSse(h)
    await h.ws.deliver(prepareFrame({ upgradeId: 'upg-1', sessionId: s0, open: [{ id: 'A', ix: 0 }] }))
    expect(h.mux._getUpgradeResourceSnapshot().records).toBe(1)

    const probe2 = h.makeWire(null)
    await probe2.deliver(prepareFrame({ upgradeId: 'upg-2', sessionId: s2, open: [{ id: 'B', ix: 0 }] }))

    expect(probe2.terminated()).toBe(true)
    expect(h.mux._getUpgradeResourceSnapshot().records).toBe(1)
    expect(h.sse.terminated()).toBe(false)
    const value = ++sentinelValue
    await h.sse.deliver(textFrame(0, ++sentinelSeq, value))
    expect(chA.received.at(-1)).toBe(value)
  })

  test('a released stage refunds its byte charge, so the budget is not a one-way ratchet', async () => {
    // Without the refund the server would stop accepting upgrades after enough churn, with no
    // stages actually held — a leak that looks exactly like correct enforcement from outside.
    const h = (harness = createMuxHarness({ maxStagedBytes: 512 }))
    const { s0 } = await connectSse(h)

    for (let i = 0; i < 20; i++) {
      const probe = h.makeWire(null)
      await probe.deliver(prepareFrame({ upgradeId: `upg-${i}`, sessionId: s0, open: [{ id: 'A', ix: 0 }] }))
      expect(probe.sent.filter((f) => f.tag === TAG.READY)).toHaveLength(1)
      probe.close()
      expect(h.mux._getUpgradeResourceSnapshot()).toEqual(EMPTY_STAGE)
    }
  })
})

describe('injected stage TTL — mechanism', () => {
  test('the deadline is the injected value, driven through the real timer', async () => {
    const h = (harness = createMuxHarness({ stageTtlMs: 25 }))
    const { s0 } = await connectSse(h)
    await h.ws.deliver(prepareFrame({ upgradeId: 'upg-1', sessionId: s0, open: [{ id: 'A', ix: 0 }] }))
    expect(h.mux._getUpgradeResourceSnapshot().records).toBe(1)

    await new Promise((resolve) => setTimeout(resolve, 60))
    await settle()

    expect(h.mux._getUpgradeResourceSnapshot()).toEqual(EMPTY_STAGE)
  })
})

describe('shipped default values', () => {
  // Pins the numbers the design agreed on. A mechanism test run at limit 2 cannot tell you whether
  // production ships 1024 or 2.
  test('the admission caps are the agreed defaults', () => {
    expect(UPGRADE_MAX_FRAME_BYTES).toBe(256 * 1024)
    expect(UPGRADE_MAX_OPEN_ENTRIES).toBe(1024)
    expect(UPGRADE_MAX_ID_BYTES).toBe(256)
  })

  test('the global staged budget is the agreed default', () => {
    expect(UPGRADE_MAX_STAGED_RECORDS).toBe(1024)
    expect(UPGRADE_MAX_STAGED_BYTES).toBe(64 * 1024 * 1024)
  })

  test('the stage deadline is 10s, matching the reconcile timeout', () => {
    expect(UPGRADE_STAGE_TTL_MS).toBe(10_000)
  })

  // ── The bridge between the two kinds of test ── Without these, BOTH kinds could be true of a mux
  // that had been rewired to ignore the constants entirely: the constants would still hold their
  // agreed values, and the injected-limit tests would still pass at their injected values. One
  // bridge per constant that a default-constructed mux is supposed to read.
  test('a default-constructed mux enforces the shipped id cap', async () => {
    const h = (harness = createMuxHarness())
    const { chA, s0 } = await connectSse(h)
    const overLongId = 'x'.repeat(UPGRADE_MAX_ID_BYTES + 1)

    await h.ws.deliver(prepareFrame({ upgradeId: 'upg-1', sessionId: s0, open: [{ id: overLongId, ix: 0 }] }))

    await expectRejectedProbe(h, h.ws, chA)
  })

  test('a default-constructed mux enforces the shipped open-entry cap', async () => {
    const h = (harness = createMuxHarness())
    const { chA, s0 } = await connectSse(h)

    // One past the cap, with short ids so the BYTE cap is nowhere near — this frame is refused for
    // its entry count or not at all.
    const frame = prepareFrame({ upgradeId: 'upg-1', sessionId: s0, open: openEntries(UPGRADE_MAX_OPEN_ENTRIES + 1) })
    expect(frame.byteLength).toBeLessThan(UPGRADE_MAX_FRAME_BYTES)
    await h.ws.deliver(frame)

    await expectRejectedProbe(h, h.ws, chA)
  })

  test('a default-constructed mux accepts a frame AT the shipped open-entry cap', async () => {
    const h = (harness = createMuxHarness())
    const { s0 } = await connectSse(h)

    await h.ws.deliver(prepareFrame({ upgradeId: 'upg-1', sessionId: s0, open: openEntries(UPGRADE_MAX_OPEN_ENTRIES) }))

    expect(h.ws.sent.filter((f) => f.tag === TAG.READY)).toHaveLength(1)
  })
})
