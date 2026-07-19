// T4 — the COMMIT half: a `barrier: true` RECONCILE arriving on the OLD wire commits the staged
// probe wire. Staging itself is `upgrade.staging.spec.ts`; the caps `upgrade.admission-caps.spec.ts`.
//
// ── Why the commit needs no new sequencing primitive ─────────────────────────────────────────
// The barrier executes as an ordinary turn on the old connection's `recvChain`, so it is ordered
// after every earlier old-wire frame BY CONSTRUCTION. Nothing on the server awaits the barrier;
// nothing bridges the two wires' chains. The commit is then the EXISTING, unmodified `reconcile`
// invoked with the staged connection's entry — which is what binds the send closure, `setSessionId`
// and the new finalizer to the probe wire, while FIN still binds to the old one because
// `buildUpgradeFinalizer` reads the PREVIOUS session's finalizer.
//
// ── The dominant false-green risk, and what is done about it ─────────────────────────────────
// In a fast in-memory harness the recv chain is empty at barrier time, so a barrier test with
// nothing in flight passes IDENTICALLY with and without the ordering edge. Two mechanisms are used
// against that, and they are deliberately different in kind:
//
//   I1a parks the chain on the only genuinely awaited seam reachable from `handleFrame` — a prior
//       `initial: true` reconcile held in `waitForChannelRegistration`. While parked, a commit is
//       structurally impossible; a barrier routed outside `chainRecv` commits immediately and the
//       test goes red on its first assertion. This one is DETERMINISTIC, not timing-shaped.
//   I1b drives a burst of genuinely queued in-flight frames through a COMMITTING barrier, which is
//       what the park cannot do (see the note below) and is the realistic production shape.
//
// ── Note on the park and live-session equality (measured) ────────────────────────────────────
// A parked reconcile ALWAYS re-sessions the old wire when it releases — `reconcile` ends by calling
// `setSessionId` on its own connection, and the only non-rotating exit is the closed-during-await
// throw. So a barrier queued behind a park necessarily names a session that is no longer live by
// the time it runs, and live-session equality correctly refuses it. That is not a limitation of the
// test: it is the concurrent-rotation defect the equality check exists to catch, observed. It does
// mean the park proves ORDERING (nothing commits early, the earlier frame is dispatched first) and
// cannot also prove a successful commit — which is why I1b exists and is a separate test.

import { afterEach, describe, expect, test } from 'vitest'

import { createMuxHarness, prepareFrame, reconcileFrame, settle, textFrame } from './upgrade-mux-harness.js'
import { TAG, type DecodedFrame } from './shared-ws.js'

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

const prepare = (sessionId: string, upgradeId = 'upg-1') =>
  prepareFrame({ upgradeId, sessionId, open: [{ id: 'A', ix: 0 }] })

/** The old wire's FINAL frame: authoritative membership plus barrier-fresh cursors. */
const barrier = (sessionId: string, upgradeId = 'upg-1', lastSeq = 1) =>
  reconcileFrame({ sessionId, upgrade: true, barrier: true, upgradeId, open: [{ id: 'A', ix: 0, lastSeq }] })

/** Typed as `DecodedFrame[]` rather than `{ tag }[]` so callers can narrow into `.payload`. */
const reconciledOn = (wire: { sent: readonly DecodedFrame[] }) => wire.sent.filter((f) => f.tag === TAG.RECONCILED)
const finsOn = (wire: { sent: readonly DecodedFrame[] }) => wire.sent.filter((f) => f.tag === TAG.FIN)

/** Seqs well clear of every in-test frame, so a sentinel can never be dropped as a duplicate by
 *  the channel's `_lastClientSeq` dedup and read as a routing failure. */
let sentinelSeq = 5_000
let sentinelValue = 9_000
afterEach(() => {
  sentinelSeq = 5_000
  sentinelValue = 9_000
})
async function expectWireDelivers(
  wire: { deliver: (f: Uint8Array<ArrayBuffer>) => Promise<void> },
  chA: { received: number[] },
) {
  const value = ++sentinelValue
  await wire.deliver(textFrame(0, ++sentinelSeq, value))
  expect(chA.received.at(-1)).toBe(value)
}

describe('the barrier commits the staged wire', () => {
  // Scoped to `deliverTo` + the upgradeId echo ONLY. The FIN and the stage cleanup each get their
  // own test below, so that a regression in one cannot be reported by the other — a test that
  // asserts four invariants tells you nothing about which of them broke.
  test('COMMITTED lands on the probe wire, carrying the upgradeId', async () => {
    const h = (harness = createMuxHarness())
    const { s0 } = await connectSse(h)
    await h.ws.deliver(prepare(s0, 'upg-7'))

    await h.sse.deliver(barrier(s0, 'upg-7'))

    // COMMITTED *is* a RECONCILED — reusing the payload is what keeps the client's settlement path
    // unforked — discriminated from an ordinary one solely by the echoed upgradeId.
    const committed = reconciledOn(h.ws)
    expect(committed).toHaveLength(1)
    expect(committed[0]?.tag === TAG.RECONCILED && committed[0].payload.upgradeId).toBe('upg-7')
    // `deliverTo` in action: the turn ran on the SSE chain, but its answer went to the WS. The SSE
    // still shows only its own original reconciled, with no upgradeId on it.
    expect(reconciledOn(h.sse)).toHaveLength(1)
    expect(reconciledOn(h.sse)[0]?.tag === TAG.RECONCILED && reconciledOn(h.sse)[0]!.payload.upgradeId).toBeUndefined()
  })

  // ── CLEANUP PATH 1 (success) ── Its own test, so the leak it guards is reported by name.
  test('a successful commit releases the stage', async () => {
    const h = (harness = createMuxHarness())
    const { s0 } = await connectSse(h)
    await h.ws.deliver(prepare(s0))

    await h.sse.deliver(barrier(s0))

    expect(h.mux._getUpgradeResourceSnapshot()).toEqual(EMPTY_STAGE)
  })

  test('the committed channel is attached to the probe wire and routes there', async () => {
    const h = (harness = createMuxHarness())
    const { chA, s0 } = await connectSse(h)
    await h.ws.deliver(prepare(s0))

    await h.sse.deliver(barrier(s0))

    expect(h.ws.sessionId()).toBeTypeOf('string')
    expect(h.ws.sessionId()).not.toBe(s0)
    await expectWireDelivers(h.ws, chA)
  })

  // ── INVARIANT: exactly-once rotation ── Distinct from finalizer liveness below, and stranded by a
  // completely different route, so the two get separate tests rather than one over-determined one.
  test('a commit mints exactly ONE new session', async () => {
    const h = (harness = createMuxHarness())
    const { s0 } = await connectSse(h)
    await h.ws.deliver(prepare(s0))

    await h.sse.deliver(barrier(s0))
    await settle()

    const minted = [...h.sse.sent, ...h.ws.sent]
      .filter((f) => f.tag === TAG.RECONCILED)
      .map((f) => (f.tag === TAG.RECONCILED ? f.payload.sessionId : ''))
    // S0 from the initial connect, S1 from the commit. A second rotation would show up as a third.
    expect(minted).toHaveLength(2)
    expect(new Set(minted).size).toBe(2)
    expect(minted[0]).toBe(s0)
    expect(minted[1]).toBe(h.ws.sessionId())
  })

  // ── INVARIANT: finalizer liveness ── The FIN is the old wire's completeness proof, not cleanup
  // decoration: it is what tells the client its old FIFO is fully drained.
  test('FIN is delivered on the old wire and never on the new one', async () => {
    const h = (harness = createMuxHarness())
    const { s0 } = await connectSse(h)
    await h.ws.deliver(prepare(s0))

    await h.sse.deliver(barrier(s0))

    // Exactly one, on the old wire only. The count is the sharp edge: firing the finalizer early
    // (before the commit's own `finalizeUpgrade`) shows up here as a SECOND FIN, and a client
    // joining FIN with COMMITTED would settle against the wrong one.
    expect(finsOn(h.sse)).toHaveLength(1)
    expect(finsOn(h.ws)).toHaveLength(0)
  })

  test('an ordinary (non-barrier) upgrade reconcile is completely unaffected', async () => {
    // The control that keeps every assertion above honest: the legacy flow must still work, on the
    // wire it arrived on, with no upgradeId and no staging involved.
    const h = (harness = createMuxHarness())
    const { s0 } = await connectSse(h)

    await h.ws.deliver(reconcileFrame({ sessionId: s0, upgrade: true, open: [{ id: 'A', ix: 0, lastSeq: 1 }] }))

    const answered = reconciledOn(h.ws)
    expect(answered).toHaveLength(1)
    expect(answered[0]?.tag === TAG.RECONCILED && answered[0].payload.upgradeId).toBeUndefined()
    expect(finsOn(h.sse)).toHaveLength(1)
    expect(h.mux._getUpgradeResourceSnapshot()).toEqual(EMPTY_STAGE)
  })
})

describe('I1 — the barrier is ordered behind everything already on the old wire', () => {
  /** Parks the old wire's chain on `waitForChannelRegistration`, then queues a data frame and the
   *  barrier behind it. Returns the release handle. */
  async function parkOldWireWithBarrierQueued(h: ReturnType<typeof createMuxHarness>, s0: string) {
    const chB = h.createChannel('B') // built but NOT registered → the next reconcile parks on it
    const parked = h.sse.deliver(
      reconcileFrame({
        sessionId: s0,
        open: [
          { id: 'A', ix: 0, lastSeq: 1 },
          { id: 'B', ix: 1, lastSeq: 0, initial: true },
        ],
      }),
    )
    await settle()
    const inflight = h.sse.deliver(textFrame(0, 2, 222))
    const barrierTurn = h.sse.deliver(barrier(s0, 'upg-1', 2))
    await settle()
    return {
      release: async () => {
        h.register(chB)
        await Promise.all([parked, inflight, barrierTurn])
        await settle()
      },
    }
  }

  // I1a. Deterministic, and scoped to the ORDERING EDGE ALONE — it asserts only what is true while
  // the chain is parked, and never reaches validation. A barrier routed outside `chainRecv` commits
  // immediately and reddens this; nothing else does, which is what makes it a gate on the edge
  // rather than on the checks that happen to sit downstream of it.
  test('nothing commits while an earlier old-wire turn is still unfinished', async () => {
    const h = (harness = createMuxHarness())
    const { chA, s0 } = await connectSse(h)
    await h.ws.deliver(prepare(s0))

    await parkOldWireWithBarrierQueued(h, s0)

    // The park is real, and the barrier sits behind BOTH earlier items.
    expect(reconciledOn(h.sse)).toHaveLength(1) // the parked reconcile has not answered
    expect(chA.received).toEqual([111]) // 222 is IN the chain, neither before nor after it
    expect(reconciledOn(h.ws)).toHaveLength(0) // ← nothing committed
    expect(finsOn(h.sse)).toHaveLength(0) // ← and the old wire has not been told it is finished
    expect(h.ws.sessionId()).toBeUndefined() // ← and nothing rotated
    expect(h.mux._getUpgradeResourceSnapshot().records).toBe(1) // the stage is untouched, not consumed
  })

  test('the parked frame is dispatched before the barrier gets its turn', async () => {
    const h = (harness = createMuxHarness())
    const { chA, s0 } = await connectSse(h)
    await h.ws.deliver(prepare(s0))
    const parked = await parkOldWireWithBarrierQueued(h, s0)

    await parked.release()

    // The earlier frame got its turn before the barrier got its own.
    expect(chA.received).toEqual([111, 222])
    // And the barrier — whose session the parked reconcile rotated away underneath it — is refused
    // rather than committed against a dead session. See the file header: this is the concurrent-
    // rotation case, observed, not a shortcoming of the park.
    expect(h.ws.terminated()).toBe(true)
    expect(h.sse.terminated()).toBe(false)
    expect(h.mux._getUpgradeResourceSnapshot()).toEqual(EMPTY_STAGE)
  })

  // I1b. The commit-side half: a genuine backlog of queued in-flight frames, all delivered before
  // the commit rotates the session. This is the production shape — the old wire's upload body can
  // hold many frames when the barrier is appended to it.
  test('a burst of in-flight old-wire frames is fully delivered before the commit', async () => {
    const h = (harness = createMuxHarness())
    const { chA, s0 } = await connectSse(h)
    await h.ws.deliver(prepare(s0))

    const BURST = 32
    const inflight = Array.from({ length: BURST }, (_, i) => h.sse.deliver(textFrame(0, 2 + i, 200 + i)))
    const barrierTurn = h.sse.deliver(barrier(s0, 'upg-1', 2 + BURST))
    await Promise.all([...inflight, barrierTurn])
    await settle()

    // Not one frame lost to the rotation — the defect PC1 records, closed.
    expect(chA.received).toEqual([111, ...Array.from({ length: BURST }, (_, i) => 200 + i)])
    expect(reconciledOn(h.ws)).toHaveLength(1)
    expect(finsOn(h.sse)).toHaveLength(1)
  })
})

describe('I2 — the retired old wire rejects post-barrier frames loudly', () => {
  test('a data frame after the commit is a violation, not a silent drop', async () => {
    // The `?.` at the routing site swallows this for free: the old connection still reports the
    // rotated-away session id, so the lookup misses for EVERY ix — no throw, no counter, no log.
    // That silence is what made the defect invisible; the disposition here is "loud".
    const h = (harness = createMuxHarness())
    const { chA, s0 } = await connectSse(h)
    await h.ws.deliver(prepare(s0))
    await h.sse.deliver(barrier(s0))

    await h.sse.deliver(textFrame(0, 400, 777))

    expect(chA.received).not.toContain(777)
    expect(h.sse.terminated()).toBe(true) // ← loud
    // Counter-sentinel: the channel, its listener and its dedup are all fine — the SAME payload
    // arrives when it comes in on the wire that now owns the session. So the rejection is about the
    // retired wire, not a dead channel.
    expect(h.ws.terminated()).toBe(false)
    await expectWireDelivers(h.ws, chA)
  })
})

describe('validation — two checks for two distinct defects', () => {
  // ── upgradeId: closes STALE SETTLEMENT ── Scoped to the rejection itself; which wire dies is a
  // separate invariant with its own test at the end of this block.
  test('a barrier whose upgradeId does not match the stage is refused', async () => {
    const h = (harness = createMuxHarness())
    const { s0 } = await connectSse(h)
    await h.ws.deliver(prepare(s0, 'upg-1'))

    await h.sse.deliver(barrier(s0, 'upg-OTHER'))

    expect(reconciledOn(h.ws)).toHaveLength(0)
    expect(h.ws.sessionId()).toBeUndefined()
    expect(h.sse.sessionId()).toBe(s0) // refused before any rotation
  })

  // ── live-session equality: closes CONCURRENT ROTATION ── Neither check substitutes for the other:
  // here the upgradeId matches perfectly and the frame is still poison.
  test('a barrier is refused when the old wire has rotated out from under the stage', async () => {
    const h = (harness = createMuxHarness())
    const { chA, s0 } = await connectSse(h)
    await h.ws.deliver(prepare(s0, 'upg-1'))

    // An ordinary register-reconcile lands on the old wire and rotates S0 → S1.
    await h.sse.deliver(reconcileFrame({ sessionId: s0, open: [{ id: 'A', ix: 0, lastSeq: 1 }] }))
    const s1 = h.sse.sessionId()
    expect(s1).not.toBe(s0)
    // The client, still believing S0, emits its barrier. The id check passes; only equality catches it.
    await h.sse.deliver(barrier(s0, 'upg-1'))

    expect(reconciledOn(h.ws)).toHaveLength(0)
    expect(h.ws.sessionId()).toBeUndefined()
    // Committing against dead S0 would send NO FIN at all (its finalizer was already deleted by the
    // rotation) and leave S1 holding handles to channels re-attached to the probe wire — which later
    // transient-detach fires `_onPeerDisconnect` against live, healthy channels.
    expect(finsOn(h.sse)).toHaveLength(0)
    expect(h.sse.sessionId()).toBe(s1)
    await expectWireDelivers(h.sse, chA) // S1 is intact and routing
  })

  // ── Risk 6: which wire dies ── A distinct invariant from either check above, so it gets its own
  // test — and the barrier here fails BOTH checks at once, deliberately. That is what makes this
  // test insensitive to either check individually and sensitive ONLY to the choice of victim: with
  // the retarget removed, the failure kills the connection whose chain merely HOSTED the turn, and
  // an established client loses its session to a probe's misbehaviour.
  test('a rejected commit terminates the probe wire, never the wire that hosted the turn', async () => {
    const h = (harness = createMuxHarness())
    const { chA, s0 } = await connectSse(h)
    await h.ws.deliver(prepare(s0, 'upg-1'))
    await h.sse.deliver(reconcileFrame({ sessionId: s0, open: [{ id: 'A', ix: 0, lastSeq: 1 }] })) // rotates
    const s1 = h.sse.sessionId()

    await h.sse.deliver(barrier(s0, 'upg-WRONG')) // wrong id AND a stale session

    expect(h.ws.terminated()).toBe(true)
    expect(h.sse.terminated()).toBe(false)
    expect(h.sse.sessionId()).toBe(s1)
    await expectWireDelivers(h.sse, chA)
  })

  test('a barrier naming no staged upgrade is refused on the wire it arrived on', async () => {
    const h = (harness = createMuxHarness())
    const { s0 } = await connectSse(h)

    await h.sse.deliver(barrier(s0)) // no PREPARE ever happened

    expect(reconciledOn(h.sse)).toHaveLength(1) // only the original connect
    expect(h.sse.terminated()).toBe(true)
    expect(h.sse.sessionId()).toBe(s0) // refused BEFORE any rotation
  })

  test('a duplicate barrier can never rotate twice', async () => {
    // The stage is single-use: it is cleared the moment the commit is committed to, so the second
    // copy resolves nothing. Without that, a replayed barrier would rotate a second time and delete
    // the finalizer the first one had just installed.
    const h = (harness = createMuxHarness())
    const { s0 } = await connectSse(h)
    await h.ws.deliver(prepare(s0))
    await h.sse.deliver(barrier(s0))
    const committedSession = h.ws.sessionId()

    await h.sse.deliver(barrier(s0))

    // Counted across BOTH wires, so the assertion does not quietly depend on `deliverTo` routing.
    const minted = [...h.sse.sent, ...h.ws.sent].filter((f) => f.tag === TAG.RECONCILED)
    expect(minted).toHaveLength(2) // the initial connect, and the one commit
    expect(h.ws.sessionId()).toBe(committedSession)
  })
})

describe('I8 (post-barrier rows) — failures collapse to ordinary recovery', () => {
  test('row 2 — the probe wire dies after READY: the barrier then finds nothing to commit', async () => {
    const h = (harness = createMuxHarness())
    const { chA, s0 } = await connectSse(h)
    await h.ws.deliver(prepare(s0))
    h.ws.close()

    await h.sse.deliver(barrier(s0))

    expect(h.mux._getUpgradeResourceSnapshot()).toEqual(EMPTY_STAGE)
    expect(reconciledOn(h.ws)).toHaveLength(0)
    // The channel is untouched — never recovery-failed — so the client's fresh reconcile reattaches it.
    expect(chA.channel._didShutdown).toBe(false)
  })

  test('row 4 — the probe wire dies between the barrier and its commit', async () => {
    // Modelled at the only point the server can observe it: the entry is gone when the barrier
    // resolves it, so the commit is refused outright rather than half-applied.
    const h = (harness = createMuxHarness())
    const { chA, s0 } = await connectSse(h)
    await h.ws.deliver(prepare(s0))
    h.ws.close()

    await h.sse.deliver(barrier(s0))
    await settle()

    expect(h.ws.sessionId()).toBeUndefined()
    expect(h.mux._getUpgradeResourceSnapshot()).toEqual(EMPTY_STAGE)
    expect(chA.channel._didShutdown).toBe(false)
  })

  test('the old wire dying after a successful commit leaves the new session alive', async () => {
    // The reverse of the pre-barrier rule: once the commit landed, the old wire is expendable and
    // its close must NOT drag down the session that now lives on the probe wire.
    const h = (harness = createMuxHarness())
    const { chA, s0 } = await connectSse(h)
    await h.ws.deliver(prepare(s0))
    await h.sse.deliver(barrier(s0))
    const committedSession = h.ws.sessionId()

    h.sse.close()
    await settle()

    expect(h.ws.sessionId()).toBe(committedSession)
    expect(h.ws.terminated()).toBe(false)
    await expectWireDelivers(h.ws, chA)
  })

  test('both wires dying after a commit is an ordinary teardown with no residue', async () => {
    const h = (harness = createMuxHarness())
    const { s0 } = await connectSse(h)
    await h.ws.deliver(prepare(s0))
    await h.sse.deliver(barrier(s0))

    h.sse.close()
    h.ws.close()
    await settle()

    expect(h.mux._getUpgradeResourceSnapshot()).toEqual(EMPTY_STAGE)
  })
})
