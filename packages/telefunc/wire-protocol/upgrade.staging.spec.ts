// T4 — server-side STAGING: `PREPARE` → `READY`, the pre-COMMITTED probe-wire policy, and all six
// cleanup paths. The commit half lives in `upgrade.barrier.spec.ts`; the admission caps in
// `upgrade.admission-caps.spec.ts`.
//
// ── The oracle discipline this file follows ──────────────────────────────────────────────────
// Every rejection assertion below is paired with a LIVE-SSE SENTINEL: after the probe wire is
// terminated, the old wire must still route a fresh application payload to its listener. Without
// that, "the WS was terminated" is satisfied just as happily by an implementation that tore down
// both wires, which is the exact failure the design forbids (pre-barrier failures leave the SSE
// session untouched). The sentinel is the independent oracle; `terminated()` alone is not.
//
// ── What is NOT credited as coverage here ────────────────────────────────────────────────────
// A channel data frame on a staged probe wire is rejected by INHERITED code — `handleFrame`'s
// session-less guard has always thrown for it, and `dispatchInbound`'s catch has always terminated.
// It is characterized below so the behaviour is pinned, but it gates nothing new: the test passes
// identically with every line T4 added deleted. The rows that ARE new (plain RECONCILE, barrier on
// the probe wire, second PREPARE, PREPARE on a sessioned wire, PREPARE for an already-staged
// session) assert a SPECIFIC consequence that only the new guard produces — no rotation, no second
// READY, no stage — rather than the generic "not dispatched".

import { afterEach, describe, expect, test, vi } from 'vitest'

import { createMuxHarness, pingFrame, prepareFrame, reconcileFrame, settle, textFrame } from './upgrade-mux-harness.js'
import { UPGRADE_STAGE_TTL_MS } from './constants.js'
import { TAG } from './shared-ws.js'

let harness: ReturnType<typeof createMuxHarness> | null = null
afterEach(() => {
  harness?.dispose()
  harness = null
  vi.useRealTimers()
})

/** Brings the SSE wire up with channel A at ix 0 and proves it delivers, so every later
 *  "still alive" assertion has an instrument that could have disagreed. */
async function connectSse(h: ReturnType<typeof createMuxHarness>) {
  const chA = h.registerChannel('A')
  await h.sse.deliver(reconcileFrame({ open: [{ id: 'A', ix: 0, lastSeq: 0, initial: true }] }))
  const s0 = h.sse.sessionId()
  expect(s0).toBeTypeOf('string')
  await h.sse.deliver(textFrame(0, 1, 111))
  expect(chA.received).toEqual([111])
  return { chA, s0: s0! }
}

const prepare = (sessionId: string, upgradeId = 'upg-1') =>
  prepareFrame({ upgradeId, sessionId, open: [{ id: 'A', ix: 0 }] })

const readyFrames = (h: ReturnType<typeof createMuxHarness>, wire: 'ws' | 'sse' = 'ws') =>
  h[wire].sent.filter((f) => f.tag === TAG.READY)

/** The live-SSE sentinel. Its seq must keep climbing across a file, hence the counter. */
let sentinelSeq = 1
let sentinelValue = 900
async function expectSseStillDelivers(h: ReturnType<typeof createMuxHarness>, chA: { received: number[] }) {
  const value = ++sentinelValue
  await h.sse.deliver(textFrame(0, ++sentinelSeq, value))
  expect(chA.received.at(-1)).toBe(value)
}
afterEach(() => {
  sentinelSeq = 1
  sentinelValue = 900
})

describe('PREPARE stages an upgrade and answers READY', () => {
  test('a valid PREPARE stages exactly one record and answers READY echoing the upgradeId', async () => {
    const h = (harness = createMuxHarness())
    const { s0 } = await connectSse(h)

    await h.ws.deliver(prepare(s0, 'upg-7'))

    const ready = readyFrames(h)
    expect(ready).toHaveLength(1)
    expect(ready[0]?.tag === TAG.READY && ready[0].payload.upgradeId).toBe('upg-7')
    expect(h.mux._getUpgradeResourceSnapshot()).toMatchObject({ records: 1, reverseRecords: 1 })
    expect(h.mux._getUpgradeResourceSnapshot().bytes).toBeGreaterThan(0)
  })

  test('staging has NO side effect on the old session: no rotation, no attach, no reconciled', async () => {
    // The whole failure story ("pre-barrier failures leave the SSE untouched") rests on PREPARE
    // being inert. If staging quietly rotated or re-attached anything, every abandoned attempt
    // would damage a healthy connection.
    const h = (harness = createMuxHarness())
    const { chA, s0 } = await connectSse(h)
    const sseSentBefore = h.sse.sent.length

    await h.ws.deliver(prepare(s0))

    expect(h.sse.sessionId()).toBe(s0)
    expect(h.ws.sessionId()).toBeUndefined()
    expect(h.sse.sent).toHaveLength(sseSentBefore)
    expect(h.ws.sent.filter((f) => f.tag === TAG.RECONCILED)).toHaveLength(0)
    await expectSseStillDelivers(h, chA)
  })

  test('the PREPARE turn resolves with READY already sent and nothing left pending', async () => {
    // Seat 3's no-unresolved-WS-wait oracle, in its observable form: the dispatch promise settles
    // WITHOUT a barrier ever arriving. A `handlePrepare` that awaited the barrier would leave this
    // promise pending forever — and would retain every later raw frame in a chain closure.
    const h = (harness = createMuxHarness())
    const { s0 } = await connectSse(h)

    let settledWith: 'resolved' | 'pending' = 'pending'
    const dispatch = h.ws.deliver(prepare(s0)).then(() => {
      settledWith = 'resolved'
    })
    await dispatch
    await settle()

    expect(settledWith).toBe('resolved')
    expect(readyFrames(h)).toHaveLength(1)
    // And the wire is immediately usable for the only other thing it may send.
    await h.ws.deliver(pingFrame())
    expect(h.ws.sent.filter((f) => f.tag === TAG.PONG)).toHaveLength(1)
    expect(h.ws.terminated()).toBe(false)
  })
})

describe('pre-COMMITTED probe-wire policy', () => {
  test('PING is permitted and leaves the stage intact', async () => {
    // The one exception. PING bypasses the recv chain and only resets liveness, so allowing it
    // costs nothing — and the client drives the probe wire with a heartbeat throughout staging.
    const h = (harness = createMuxHarness())
    const { s0 } = await connectSse(h)
    await h.ws.deliver(prepare(s0))

    await h.ws.deliver(pingFrame())

    expect(h.ws.sent.filter((f) => f.tag === TAG.PONG)).toHaveLength(1)
    expect(h.ws.terminated()).toBe(false)
    expect(h.mux._getUpgradeResourceSnapshot().records).toBe(1)
  })

  // ── NEW ROW ── The dispatch hole. RECONCILE is handled ABOVE the session-less guard, so without
  // the explicit staged-wire check this frame runs the destructive rotation and steps around the
  // stage completely. The specific consequence asserted is exactly the one the guard prevents: the
  // probe wire must NOT acquire a session, and the old session must survive to route a sentinel.
  // With the guard removed, `ws.sessionId()` becomes a fresh id, S0 is removed from the registry,
  // and the sentinel is silently dropped.
  test('a plain RECONCILE on a staged probe wire is a violation — it never rotates', async () => {
    const h = (harness = createMuxHarness())
    const { chA, s0 } = await connectSse(h)
    await h.ws.deliver(prepare(s0))

    await h.ws.deliver(reconcileFrame({ sessionId: s0, open: [{ id: 'A', ix: 0, lastSeq: 1 }] }))

    // Scoped to the rotation the guard exists to prevent. The stage cleanup that ALSO happens on a
    // violation is asserted by cleanup path 5 below, on its own, so a regression in either is
    // reported by the test named for it.
    expect(h.ws.terminated()).toBe(true)
    expect(h.ws.sessionId()).toBeUndefined() // ← the rotation the guard exists to prevent
    expect(h.ws.sent.filter((f) => f.tag === TAG.RECONCILED)).toHaveLength(0)
    expect(h.sse.terminated()).toBe(false)
    await expectSseStillDelivers(h, chA)
  })

  // ── NEW ROW ── Same guard, barrier-flagged. The barrier belongs on the OLD wire; arriving here it
  // must be named as wrong rather than left to the live-session check to reject as a side effect.
  test('a barrier RECONCILE on the staged probe wire is a violation', async () => {
    const h = (harness = createMuxHarness())
    const { chA, s0 } = await connectSse(h)
    await h.ws.deliver(prepare(s0))

    await h.ws.deliver(
      reconcileFrame({
        sessionId: s0,
        upgrade: true,
        barrier: true,
        upgradeId: 'upg-1',
        open: [{ id: 'A', ix: 0, lastSeq: 1 }],
      }),
    )

    expect(h.ws.terminated()).toBe(true)
    expect(h.ws.sessionId()).toBeUndefined()
    expect(h.mux._getUpgradeResourceSnapshot().records).toBe(0)
    expect(h.sse.terminated()).toBe(false)
    await expectSseStillDelivers(h, chA)
  })

  // ── NEW ROW ── Second PREPARE. The specific consequence: exactly ONE READY was ever emitted.
  // Without the guard the stage is silently re-installed (leaking the first timer) and a second
  // READY goes out, so the client could act on an attempt the server has re-keyed underneath it.
  test('a second PREPARE on the same probe wire is a violation — never a re-stage', async () => {
    const h = (harness = createMuxHarness())
    const { chA, s0 } = await connectSse(h)
    await h.ws.deliver(prepare(s0, 'upg-1'))

    await h.ws.deliver(prepare(s0, 'upg-2'))

    expect(readyFrames(h)).toHaveLength(1)
    expect(readyFrames(h)[0]?.tag === TAG.READY && readyFrames(h)[0]!.payload.upgradeId).toBe('upg-1')
    expect(h.ws.terminated()).toBe(true)
    expect(h.mux._getUpgradeResourceSnapshot()).toMatchObject({ records: 0, reverseRecords: 0, bytes: 0 })
    await expectSseStillDelivers(h, chA)
  })

  // ── NEW ROW ── Newly reachable BECAUSE the PREPARE branch sits above the session-less guard: on
  // an already-sessioned wire nothing else would have rejected it.
  test('a PREPARE on an already-sessioned wire is a violation', async () => {
    const h = (harness = createMuxHarness())
    const { s0 } = await connectSse(h)

    await h.sse.deliver(prepare(s0))

    expect(readyFrames(h, 'sse')).toHaveLength(0)
    expect(h.sse.terminated()).toBe(true)
    expect(h.mux._getUpgradeResourceSnapshot().records).toBe(0)
  })

  // ── NEW ROW ── One stage per OLD SESSION, not merely per probe wire. Two probes racing for the
  // same session must not both stage, or the second would overwrite the reverse index and strand
  // the first's record — a leak the snapshot's `reverseRecords` is there to catch.
  test('a second probe wire PREPARing for the same old session is a violation', async () => {
    const h = (harness = createMuxHarness())
    const { chA, s0 } = await connectSse(h)
    await h.ws.deliver(prepare(s0, 'upg-1'))
    const ws2 = h.makeWire(null)

    await ws2.deliver(prepare(s0, 'upg-2'))

    expect(ws2.sent.filter((f) => f.tag === TAG.READY)).toHaveLength(0)
    expect(ws2.terminated()).toBe(true)
    // The FIRST stage is untouched — rejecting the newcomer must not evict the incumbent.
    expect(h.ws.terminated()).toBe(false)
    expect(h.mux._getUpgradeResourceSnapshot()).toMatchObject({ records: 1, reverseRecords: 1 })
    await expectSseStillDelivers(h, chA)
  })

  // ── CHARACTERIZATION ONLY, NOT CREDITED AS COVERAGE ──
  // This passes with every line T4 added removed: `handleFrame`'s session-less guard has always
  // thrown for a channel frame on a wire with no session, and `dispatchInbound`'s catch has always
  // terminated. Recorded so the inherited behaviour is pinned, and so the grid does not mistake it
  // for a gate on the new policy.
  test('characterization (inherited): a channel data frame on a staged probe wire terminates it', async () => {
    const h = (harness = createMuxHarness())
    const { chA, s0 } = await connectSse(h)
    await h.ws.deliver(prepare(s0))

    await h.ws.deliver(textFrame(0, 1, 555))

    expect(h.ws.terminated()).toBe(true)
    expect(chA.received).not.toContain(555)
    await expectSseStillDelivers(h, chA)
  })
})

describe('staged-state cleanup — all six paths', () => {
  const empty = { records: 0, reverseRecords: 0, bytes: 0 }

  // Path 1 (success) is asserted in `upgrade.barrier.spec.ts`, where the commit lives.

  test('path 2 — the TTL expires the stage without disturbing the old session', async () => {
    // Confounder control: BOTH wires are PINGed across the window. The mux's own ping deadline
    // (2 x pingInterval) is shorter than the stage TTL, so without the heartbeat the wires would
    // die of unrelated liveness and these assertions would be measuring the wrong teardown —
    // measured: the SSE terminates mid-window. The stage must also still be alive at T-1s, or a
    // stage that expired immediately would satisfy the final assertion just as well.
    vi.useFakeTimers()
    const h = (harness = createMuxHarness())
    const { s0 } = await connectSse(h)
    await h.ws.deliver(prepare(s0))

    for (let elapsed = 0; elapsed < UPGRADE_STAGE_TTL_MS - 1_000; elapsed += 1_000) {
      await vi.advanceTimersByTimeAsync(1_000)
      await h.ws.deliver(pingFrame())
      await h.sse.deliver(pingFrame())
    }
    expect(h.mux._getUpgradeResourceSnapshot().records).toBe(1) // still staged at T-1s

    await vi.advanceTimersByTimeAsync(1_001)

    expect(h.mux._getUpgradeResourceSnapshot()).toEqual(empty)
    expect(h.sse.sessionId()).toBe(s0) // the old session is untouched — the client just falls back
    expect(h.sse.terminated()).toBe(false)
    // ── F4a ── Expiry must also TERMINATE the probe, per the timeout table. Clearing the record
    // alone leaves a session-less WS that PING keeps alive indefinitely: the stage it was opened for
    // is gone, nothing it can now send is legal, and the server has no reason to hold the socket.
    expect(h.ws.terminated()).toBe(true)
  })

  test('path 2b — PINGing does NOT refresh the deadline', async () => {
    // Non-refreshing is the entire point: PING resets liveness, and a client that pings forever
    // while withholding its barrier would otherwise hold a stage open for the socket's lifetime.
    vi.useFakeTimers()
    const h = (harness = createMuxHarness())
    const { s0 } = await connectSse(h)
    await h.ws.deliver(prepare(s0))

    for (let i = 0; i < 10; i++) {
      await vi.advanceTimersByTimeAsync(UPGRADE_STAGE_TTL_MS / 10)
      await h.ws.deliver(pingFrame())
      await h.sse.deliver(pingFrame())
    }

    expect(h.mux._getUpgradeResourceSnapshot()).toEqual(empty)
  })

  test('path 3 — the probe wire closing clears the stage (above the session-less early return)', async () => {
    // The placement trap. A staged probe wire has NO session id, so cleanup below
    // `onConnectionClosed`'s `if (!sessionId) return` would never run, and every abandoned attempt
    // would leak two map entries plus a live timer keyed by a dead object, permanently.
    const h = (harness = createMuxHarness())
    const { chA, s0 } = await connectSse(h)
    await h.ws.deliver(prepare(s0))
    expect(h.ws.sessionId()).toBeUndefined() // ← precondition of the trap, asserted not assumed

    h.ws.close()

    expect(h.mux._getUpgradeResourceSnapshot()).toEqual(empty)
    await expectSseStillDelivers(h, chA)
  })

  test('path 4 — the OLD wire closing clears the stage keyed by its session', async () => {
    const h = (harness = createMuxHarness())
    const { s0 } = await connectSse(h)
    await h.ws.deliver(prepare(s0))

    h.sse.close()

    expect(h.mux._getUpgradeResourceSnapshot()).toEqual(empty)
  })

  test('path 5 — a violation on the staged wire clears the stage as it terminates', async () => {
    const h = (harness = createMuxHarness())
    const { s0 } = await connectSse(h)
    await h.ws.deliver(prepare(s0, 'upg-1'))

    await h.ws.deliver(prepare(s0, 'upg-2')) // duplicate PREPARE = violation

    expect(h.mux._getUpgradeResourceSnapshot()).toEqual(empty)
  })

  // ── F3 ── The reverse index is keyed by the session the stage was opened against. Once an
  // ordinary reconcile rotates the old wire S0 → S1, that key no longer matches anything the wire
  // reports, so old-wire-close cleanup (which looks up S1) walks straight past the record — and the
  // stage survives, with its bytes and its timer, until the 10 s TTL. The stage is already dead at
  // that point: live-session equality would refuse its barrier. So it is released the moment the
  // rotation that kills it is dispatched, rather than left for the TTL to reap.
  test('path 4b — a stage is released when its old wire rotates away from the staged session', async () => {
    const h = (harness = createMuxHarness())
    const { s0 } = await connectSse(h)
    await h.ws.deliver(prepare(s0))

    await h.sse.deliver(reconcileFrame({ sessionId: s0, open: [{ id: 'A', ix: 0, lastSeq: 1 }] }))

    expect(h.sse.sessionId()).not.toBe(s0) // the rotation really happened
    expect(h.mux._getUpgradeResourceSnapshot()).toEqual(empty)
    expect(h.ws.terminated()).toBe(true) // the attempt can never commit, so the probe is released too
  })

  test('path 4b — and the record does not survive a rotate-then-close either', async () => {
    // The leak as the reviewer described it: close cleanup looks up the CURRENT session, so without
    // the eager release above nothing on the close path can find a record still keyed by the old one.
    const h = (harness = createMuxHarness())
    const { s0 } = await connectSse(h)
    await h.ws.deliver(prepare(s0))
    await h.sse.deliver(reconcileFrame({ sessionId: s0, open: [{ id: 'A', ix: 0, lastSeq: 1 }] }))

    h.sse.close()

    expect(h.mux._getUpgradeResourceSnapshot()).toEqual(empty)
  })

  test('path 6 — dispose() clears staged state', async () => {
    const h = (harness = createMuxHarness())
    const { s0 } = await connectSse(h)
    await h.ws.deliver(prepare(s0))
    expect(h.mux._getUpgradeResourceSnapshot().records).toBe(1)

    h.mux.dispose()

    expect(h.mux._getUpgradeResourceSnapshot()).toEqual(empty)
  })

  test('repeated stage/abandon cycles return the accounting to exactly zero', async () => {
    // The leak detector proper. A cleanup path that dropped only ONE of the two map entries, or
    // forgot to refund the byte charge, shows up here as drift even though each individual cycle
    // looks healthy — `reverseRecords` and `bytes` are what make that visible.
    const h = (harness = createMuxHarness())
    const { s0 } = await connectSse(h)

    for (let i = 0; i < 5; i++) {
      const probe = h.makeWire(null)
      await probe.deliver(prepare(s0, `upg-${i}`))
      expect(h.mux._getUpgradeResourceSnapshot().records).toBe(1)
      probe.close()
      expect(h.mux._getUpgradeResourceSnapshot()).toEqual(empty)
    }
  })
})

describe('I8 (server rows) — failure interleavings leave no staged residue', () => {
  const empty = { records: 0, reverseRecords: 0, bytes: 0 }

  test('row 1 — probe wire dies before READY could be acted on: SSE traffic continues', async () => {
    const h = (harness = createMuxHarness())
    const { chA, s0 } = await connectSse(h)
    await h.ws.deliver(prepare(s0))

    h.ws.close()

    expect(h.mux._getUpgradeResourceSnapshot()).toEqual(empty)
    expect(h.sse.sessionId()).toBe(s0)
    await expectSseStillDelivers(h, chA)
  })

  // Row 2 (probe dies after READY, barrier then finds nothing) needs the commit path and lives in
  // `upgrade.barrier.spec.ts` alongside the other post-barrier rows.

  test('row 7 — both wires die while staged: single teardown, no residue', async () => {
    const h = (harness = createMuxHarness())
    const { s0 } = await connectSse(h)
    await h.ws.deliver(prepare(s0))

    h.ws.close()
    h.sse.close()
    await settle()

    expect(h.mux._getUpgradeResourceSnapshot()).toEqual(empty)
  })

  test('the old wire closing first, then the probe, is equally clean (order-independent)', async () => {
    const h = (harness = createMuxHarness())
    const { s0 } = await connectSse(h)
    await h.ws.deliver(prepare(s0))

    h.sse.close()
    h.ws.close()
    await settle()

    expect(h.mux._getUpgradeResourceSnapshot()).toEqual(empty)
  })
})
