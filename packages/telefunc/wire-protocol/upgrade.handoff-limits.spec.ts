// R2 — the two bounds on the upgrade handoff window, and the fallback they share.
//
// Both wires are live during the handoff and frames from both are buffered until FIN (old wire)
// and RECONCILED (new wire) have BOTH landed. Two things could previously make that window
// unbounded:
//
//   1. NO DEADLINE ON A MISSING FIN. `finTimer` was armed only once FIN had already arrived, so it
//      bounded the RECONCILED limb and nothing bounded the FIN limb. With RECONCILED settled there
//      is no `reconcileTimer` either — so a FIN that never comes wedged the handoff for the
//      connection's lifetime, buffering every server→client frame and never delivering one.
//   2. NO CAP ON THE BUFFERS. `state.upgrade.buffer` was a plain array with no limit and no
//      eviction, so the window's memory cost was whatever the server chose to send.
//
// R2 arms ONE symmetric join deadline at `enterUpgradeHandoff` and charges ONE shared budget across
// both buffers. Tripping either runs the same fallback: stop arrivals, dispatch the OLD wire's
// received FIFO prefix while channel membership is intact, discard the NEW buffer whole, then one
// fresh reconcile with `upgradeDisabled` sticky.
//
// ── Oracle ───────────────────────────────────────────────────────────────────────────────────
// `sseConnects()`. A successful handoff opens exactly one SSE downstream and then lives on WS; the
// fallback installs a fresh SSE transport and reconnects, making it two. `handoffDrained()` is NOT
// usable here — the old wire's fetch is aborted on the success path too, so it cannot discriminate.
// The first test is the instrument check that this signal is not stuck on.
//
// ── Why the time budgets are what they are ───────────────────────────────────────────────────
// Every deadline test runs under a budget below `RECONCILE_TIMEOUT_MS` (10 s). That is the whole
// point: if the join deadline were removed, the missing-RECONCILED limb would still eventually
// abort via the 10 s reconcile watchdog, and a generous budget would let that incidental rescue
// keep this file green while the guard under test was gone. The budget is what makes it a gate.
//
// ── Declared coverage gap ────────────────────────────────────────────────────────────────────
// The `upgradeDisabled` stickiness after a fallback is NOT asserted here. The harness does not
// answer a reconnect's RECONCILE, so the client never re-reaches `maybeStartUpgrade` and an
// "it did not probe again" assertion would pass whether or not the flag were sticky — a gate that
// cannot fail. Left uncovered and declared rather than faked.

import { afterEach, describe, expect, test } from 'vitest'
import { stringify } from '@brillout/json-serializer/stringify'

import { UPGRADE_HANDOFF_BUFFER_BYTES, UPGRADE_HANDOFF_BUFFER_FRAMES } from './constants.js'
import { encode } from './shared-ws.js'
import { createUpgradeHarness, reconciledPayload, waitUntil, type UpgradeHarness } from './upgrade-client-harness.js'

let harness: UpgradeHarness | null = null
afterEach(() => {
  harness?.dispose()
  harness = null
})

const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms))

/** A server→client TEXT frame on channel ix 0. */
const wsFrame = (seq: number) => encode.text(0, stringify(`ws-${seq}`), seq)
/** ~1 MiB of binary payload, for driving the BYTE limb of the shared budget. */
const bigWsFrame = (seq: number) => encode.binary(0, new Uint8Array(1024 * 1024), seq)

/** The fallback reconnects on SSE. `CHANNEL_RECONNECT_INITIAL_DELAY_MS` is 500 ms, so allow for the
 *  2 s join deadline plus that delay plus slack — while staying well under the 10 s watchdog. */
const FALLBACK_BUDGET_MS = 5_000

/** Budget for the BUDGET-OVERFLOW tests, and it is load-bearing rather than a convenience.
 *
 *  Measured trap: with the cap mutated to `Infinity` the overflow tests still went green, because
 *  a flood that never receives FIN or RECONCILED also trips the 2 s JOIN DEADLINE and falls back —
 *  same observable, entirely different guard. The two paths are only distinguishable in time: the
 *  cap trips synchronously during the flood (fallback ≈ the 500 ms reconnect delay), the deadline
 *  cannot trip before 2 s (fallback ≈ 2.5 s). This budget sits between them, so an overflow test
 *  can only pass if the cap — not the deadline — is what tripped. */
const OVERFLOW_BUDGET_MS = 1_500

const waitForFallback = (h: UpgradeHarness, budgetMs = FALLBACK_BUDGET_MS) =>
  waitUntil(() => h.sseConnects() === 2, 'upgrade fell back to a fresh SSE connection', budgetMs)

describe('R2 — handoff join deadline and shared buffer budget', () => {
  // INSTRUMENT CHECK. A handoff that joins normally must NOT reconnect. Without this, every
  // `sseConnects() === 2` assertion below would pass just as happily against a client that fell
  // back unconditionally.
  test('control: a handoff that receives both FIN and RECONCILED never falls back', async () => {
    const h = (harness = await createUpgradeHarness(['A']))

    h.ws.pushFrame(encode.reconciled(reconciledPayload([{ ix: 0, lastSeq: 0 }])))
    h.sse.pushFrame(encode.fin())
    await waitUntil(() => h.handoffDrained(), 'handoff completed')
    // Past the join deadline: a completed handoff must not be re-aborted by a stale timer.
    await sleep(2_500)

    expect(h.sseConnects()).toBe(1)
    expect(h.channels[0]!.isClosed).toBe(false)
  })

  // ── Join deadline: the FIN limb ────────────────────────────────────────────────────────────
  // The limb that had NO watchdog at all before R2. RECONCILED settles, so `reconciling` is false
  // and `reconcileTimer` is cleared — with the deadline unarmed nothing bounds this at any
  // horizon, and the mutation hangs until the test budget expires.
  test('RECONCILED arrives but FIN never does → falls back within the join deadline', async () => {
    const h = (harness = await createUpgradeHarness(['A']))

    // An old-wire frame that must survive the fallback: it is in the old buffer, and the old
    // prefix is dispatched while channel membership is still intact.
    h.sse.pushFrame(encode.text(0, stringify('old-wire-1'), 1))
    await sleep(200) // well inside the 2 s deadline — the SSE stream needs a turn to yield it
    h.ws.pushFrame(encode.reconciled(reconciledPayload([{ ix: 0, lastSeq: 0 }])))

    await waitForFallback(h)

    expect(h.channels[0]!.received).toEqual([{ kind: 'text', value: 'old-wire-1' }])
  })

  // ── Join deadline: the RECONCILED limb ─────────────────────────────────────────────────────
  // This one WOULD eventually abort via `RECONCILE_TIMEOUT_MS`, so the budget is the gate.
  test('FIN arrives but RECONCILED never does → falls back within the join deadline', async () => {
    const h = (harness = await createUpgradeHarness(['A']))

    h.sse.pushFrame(encode.fin())

    await waitForFallback(h)
  })

  // ── Shared budget: the frame limb ──────────────────────────────────────────────────────────
  test('a new-wire flood past the frame budget falls back and discards the NEW buffer whole', async () => {
    const h = (harness = await createUpgradeHarness(['A']))

    // Exactly one frame past the budget. Emitting further frames after the trip would be
    // unfaithful — the real WS is disposed by the fallback and delivers nothing more.
    for (let seq = 1; seq <= UPGRADE_HANDOFF_BUFFER_FRAMES + 1; seq++) h.ws.pushFrame(wsFrame(seq))

    await waitForFallback(h, OVERFLOW_BUDGET_MS)

    // Not one frame of the new buffer is dispatched — not even the prefix that fit. Dispatching a
    // prefix would advance `trackSeq` past old-wire frames that never arrived.
    expect(h.channels[0]!.received).toEqual([])
  })

  // The instrument that can disagree, at the exact boundary: one frame BELOW the budget completes
  // normally and delivers everything. So it is the limit that trips the fallback, not the flood.
  test('control: a new-wire burst that stays within the frame budget completes normally', async () => {
    const h = (harness = await createUpgradeHarness(['A']))

    for (let seq = 1; seq <= UPGRADE_HANDOFF_BUFFER_FRAMES; seq++) h.ws.pushFrame(wsFrame(seq))
    h.ws.pushFrame(encode.reconciled(reconciledPayload([{ ix: 0, lastSeq: 0 }])))
    h.sse.pushFrame(encode.fin())
    await waitUntil(() => h.handoffDrained(), 'handoff completed')

    expect(h.sseConnects()).toBe(1)
    expect(h.channels[0]!.received).toHaveLength(UPGRADE_HANDOFF_BUFFER_FRAMES)
  })

  // ── Shared budget: the byte limb ───────────────────────────────────────────────────────────
  // Far below the frame limit (9 frames), so only the byte accounting can trip this.
  test('a new-wire flood past the byte budget falls back', async () => {
    const h = (harness = await createUpgradeHarness(['A']))

    const frames = Math.ceil(UPGRADE_HANDOFF_BUFFER_BYTES / (1024 * 1024)) + 1
    expect(frames).toBeLessThan(UPGRADE_HANDOFF_BUFFER_FRAMES)
    for (let seq = 1; seq <= frames; seq++) h.ws.pushFrame(bigWsFrame(seq))

    await waitForFallback(h, OVERFLOW_BUDGET_MS)

    expect(h.channels[0]!.received).toEqual([])
  })
})
