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

import { createMuxHarness, pingFrame, prepareFrame, reconcileFrame, settle, textFrame } from './upgrade-mux-harness.js'
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

describe('the commit is single-use and stays policy-authoritative while it runs', () => {
  // ── F1a ── `attach` is the ONE awaitable inside `reconcile`, and it waits only for an
  // `initial: true` entry naming an unregistered channel. Letting a barrier carry one would park
  // the commit mid-flight, holding the probe in a state where it has no session yet and — before
  // this guard — no stage either. Established channels only: the barrier's membership is
  // authoritative, so a channel it names that the server does not have is simply omitted, never
  // waited for. (Seat 1 O1: the staged contract must exclude `initial: true` waits.)
  test('a barrier carrying an initial:true entry is refused rather than parking the commit', async () => {
    const h = (harness = createMuxHarness())
    const { chA, s0 } = await connectSse(h)
    await h.ws.deliver(prepare(s0))
    h.createChannel('B') // built but NOT registered — an `initial` entry naming it would park

    await h.sse.deliver(
      reconcileFrame({
        sessionId: s0,
        upgrade: true,
        barrier: true,
        upgradeId: 'upg-1',
        open: [
          { id: 'A', ix: 0, lastSeq: 1 },
          { id: 'B', ix: 1, lastSeq: 0, initial: true },
        ],
      }),
    )

    // The sharp assertion is the SNAPSHOT: if the entry were accepted, `attach` would still be
    // parked on channel B right now and the record would sit there in `committing`. An empty
    // snapshot means the commit never started, which is the property this guard exists for.
    // (Deliberately no `terminated()` assertions here — those belong to the Risk-6 test, and
    // asserting them too would make this test report that finding's regressions as well as its own.)
    expect(h.mux._getUpgradeResourceSnapshot()).toEqual(EMPTY_STAGE)
    expect(reconciledOn(h.ws)).toHaveLength(0)
    expect(h.ws.sessionId()).toBeUndefined()
    // Pre-commit refusal: the old session is untouched and still routing.
    expect(h.sse.sessionId()).toBe(s0)
    await expectWireDelivers(h.sse, chA)
  })

  // ── F1b ── The stage is the probe wire's phase marker: while it exists, that wire may send only
  // PING and its first PREPARE. `reconcile` is async, so releasing the marker at the START of the
  // commit opens a window in which the probe is neither staged nor sessioned and a plain RECONCILE
  // runs a SECOND destructive rotation concurrently with the one committing. The record must
  // therefore survive as COMMITTING until the commit settles.
  test('a plain RECONCILE on the probe cannot slip in while the commit is in flight', async () => {
    const h = (harness = createMuxHarness())
    const { s0 } = await connectSse(h)
    await h.ws.deliver(prepare(s0))

    // Delivered in the same burst: the barrier's turn starts first, the intruder lands while the
    // commit is still resolving.
    const commit = h.sse.deliver(barrier(s0))
    const intruder = h.ws.deliver(reconcileFrame({ sessionId: s0, open: [{ id: 'A', ix: 0, lastSeq: 1 }] }))
    await Promise.all([commit, intruder])
    await settle()

    // Exactly one rotation, and it is the commit's. Counted across BOTH wires so the assertion does
    // not also depend on `deliverTo` routing, and stated as a count rather than as "the intruder was
    // terminated" so it does not double as a report on which wire a violation kills.
    const minted = [...h.sse.sent, ...h.ws.sent].filter((f) => f.tag === TAG.RECONCILED)
    expect(minted).toHaveLength(2) // the initial connect, and the commit — a served intruder makes 3
  })

  // The same invariant read through a DIFFERENT check, so that losing the record mid-commit is
  // reported by something other than the reconcile guard: `handlePrepare`'s one-stage-per-probe rule
  // consults the record's mere presence. If the record is released when the commit starts, a second
  // PREPARE walks straight in and the server answers READY for an attempt it is already committing.
  test('a second PREPARE on the probe is still refused while the commit is in flight', async () => {
    const h = (harness = createMuxHarness())
    const { s0 } = await connectSse(h)
    await h.ws.deliver(prepare(s0, 'upg-1'))

    const commit = h.sse.deliver(barrier(s0))
    const second = h.ws.deliver(prepare(s0, 'upg-2'))
    await Promise.all([commit, second])
    await settle()

    expect(h.ws.sent.filter((f) => f.tag === TAG.READY)).toHaveLength(1)
  })
})

describe('I1 — the barrier is ordered behind everything already on the old wire', () => {
  /** Parks the old wire's chain on `waitForChannelRegistration`, stages an upgrade while it is
   *  parked, then queues a data frame and the barrier behind the park. Returns the release handle.
   *
   *  ORDER MATTERS: the park is an ordinary reconcile, and an ordinary reconcile now releases any
   *  stage keyed to the session it is about to rotate away. Staging AFTER the park is established is
   *  therefore both necessary and the more faithful shape — a client PREPAREs on the probe while its
   *  old wire is still busy, which is exactly the interleaving being modelled. */
  async function parkOldWireThenStageAndQueueBarrier(h: ReturnType<typeof createMuxHarness>, s0: string) {
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
    await h.ws.deliver(prepare(s0))
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
    await parkOldWireThenStageAndQueueBarrier(h, s0)

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
    const parked = await parkOldWireThenStageAndQueueBarrier(h, s0)

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

  // ── F2 ── RECONCILE is dispatched ABOVE the retired check, so a late plain reconcile on the
  // committed-away wire would run the certified rotation there: minting a fresh session on a wire
  // the client has already abandoned and REATTACHING the channels away from the probe that just
  // won them. The barrier is the old wire's final SEMANTIC frame, so a reconcile after it is a
  // violation on exactly the same terms as a data frame — the data-frame test alone misses this
  // because the two take different dispatch paths.
  test('a plain RECONCILE after the commit is a violation, and never re-mints on the retired wire', async () => {
    const h = (harness = createMuxHarness())
    const { chA, s0 } = await connectSse(h)
    await h.ws.deliver(prepare(s0))
    await h.sse.deliver(barrier(s0))
    const committedSession = h.ws.sessionId()

    await h.sse.deliver(reconcileFrame({ sessionId: s0, open: [{ id: 'A', ix: 0, lastSeq: 1 }] }))

    expect(h.sse.terminated()).toBe(true)
    expect(h.sse.sessionId()).toBe(s0) // ← no new session minted on a wire that is already spent
    expect(reconciledOn(h.sse)).toHaveLength(1) // only its own original connect
    // And the channel is still the committed wire's — not stolen back by the retired one.
    expect(h.ws.sessionId()).toBe(committedSession)
    await expectWireDelivers(h.ws, chA)
  })

  test('PING is still answered on the retired wire — it is spent, not dead', async () => {
    // The discriminator for the two tests above: `retiredByBarrier` must reject SEMANTIC frames
    // only. The wire stays open until the client sees FIN and drops it, and the client keeps
    // pinging until then; rejecting PING would tear down a wire that is still doing its job.
    const h = (harness = createMuxHarness())
    const { s0 } = await connectSse(h)
    await h.ws.deliver(prepare(s0))
    await h.sse.deliver(barrier(s0))

    await h.sse.deliver(pingFrame())

    expect(h.sse.terminated()).toBe(false)
    expect(h.sse.sent.filter((f) => f.tag === TAG.PONG)).toHaveLength(1)
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

  /** A second, fully independent SSE connection with its own session. */
  async function connectSecondSse(h: ReturnType<typeof createMuxHarness>) {
    const sse2 = h.makeWire('sse-conn-2')
    h.registerChannel('B')
    await sse2.deliver(reconcileFrame({ open: [{ id: 'B', ix: 0, lastSeq: 0, initial: true }] }))
    expect(sse2.sessionId()).toBeTypeOf('string')
    return sse2
  }

  // ── live-session equality: closes CONCURRENT ROTATION ── Neither check substitutes for the other:
  // here the upgradeId matches perfectly and the frame is still poison.
  //
  // NOTE ON THE SCENARIO. This used to rotate the old wire out from under its own stage and then
  // barrier from it. Releasing a stage eagerly when its session is rotated away (the leak fix) makes
  // that path terminate at the stage lookup instead, which would have left this check with no
  // reachable case and no killer. The equality is really "the barrier must arrive on the wire that
  // still OWNS the staged session", and THAT is what is exercised here — a copy of a perfectly valid
  // barrier landing on a different, live connection. Committing it would rotate a session the naming
  // wire does not hold: no FIN would be sent, and the real old session would survive holding handles
  // to channels re-attached to the probe, later transient-detaching live healthy channels.
  test('a barrier is refused when it arrives on a wire that does not own the staged session', async () => {
    const h = (harness = createMuxHarness())
    const { chA, s0 } = await connectSse(h)
    await h.ws.deliver(prepare(s0, 'upg-1'))
    const sse2 = await connectSecondSse(h)

    // Correct upgradeId, correct staged session named — wrong wire. Only equality catches this.
    await sse2.deliver(barrier(s0, 'upg-1'))

    expect(reconciledOn(h.ws)).toHaveLength(0)
    expect(h.ws.sessionId()).toBeUndefined()
    expect(finsOn(h.sse)).toHaveLength(0)
    // Both real sessions survive untouched. (Which wire a rejection TERMINATES is the Risk-6 test's
    // invariant, not this one's, so it is deliberately not asserted here.)
    expect(h.sse.sessionId()).toBe(s0)
    expect(sse2.sessionId()).toBeTypeOf('string')
    await expectWireDelivers(h.sse, chA)
  })

  // ── Risk 6: which wire dies ── A distinct invariant from either check above, so it gets its own
  // test — and the barrier here fails BOTH checks at once, deliberately. That is what makes it
  // insensitive to either check individually and sensitive ONLY to the choice of victim: with the
  // retarget removed, the failure kills the connection whose chain merely HOSTED the turn, and a
  // bystander loses its session to a probe's misbehaviour.
  test('a rejected commit terminates the probe wire, never the wire that hosted the turn', async () => {
    const h = (harness = createMuxHarness())
    const { chA, s0 } = await connectSse(h)
    await h.ws.deliver(prepare(s0, 'upg-1'))
    const sse2 = await connectSecondSse(h)

    await sse2.deliver(barrier(s0, 'upg-WRONG')) // wrong id AND the wrong wire

    expect(h.ws.terminated()).toBe(true)
    expect(sse2.terminated()).toBe(false)
    expect(h.sse.terminated()).toBe(false)
    await expectWireDelivers(h.sse, chA)
  })

  // ── F4b ── A barrier whose stage is GONE (expired, or its probe closed) is refused without
  // terminating anything. Two wrong alternatives were considered and rejected: terminating the old
  // wire costs an established client its healthy session because a probe timed out, and there is no
  // longer any record of which probe to terminate instead. So the commit is refused, the rotation
  // never happens, both wires are left exactly as they were, and the client's own 10 s attempt
  // deadline aborts the upgrade. This is a refusal, not a silent drop: no state changes and nothing
  // the client sent is consumed — the distinction that matters against the loss PC1 records.
  test('a barrier naming no staged upgrade is refused, and terminates neither wire', async () => {
    const h = (harness = createMuxHarness())
    const { chA, s0 } = await connectSse(h)

    await h.sse.deliver(barrier(s0)) // no PREPARE ever happened

    expect(reconciledOn(h.sse)).toHaveLength(1) // only the original connect
    expect(h.sse.sessionId()).toBe(s0) // refused BEFORE any rotation
    expect(h.sse.terminated()).toBe(false) // ← the established session does not pay for this
    await expectWireDelivers(h.sse, chA)
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
