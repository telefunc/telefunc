// T5 — the CLIENT half of the barrier upgrade: the capability gate, the `PREPARE`/`READY`
// exchange, the barrier as the old wire's final frame, and the flip that must NOT reconcile.
// Failure paths and deadlines live in `upgrade.client-barrier-failures.spec.ts`.
//
// ── What makes these assertions non-vacuous ──────────────────────────────────────────────────
// Half of this file's subject matter is a frame that must NOT appear (no `PREPARE` against a server
// without the capability; no `RECONCILE` on the new wire, ever). An absence assertion passes just
// as happily when the flow never ran at all, so every one of them is PAIRED with positive proof
// that the upgrade did happen and the channel still works. Where the pairing is the whole point it
// is called out on the test.
//
// ── Timing discipline ────────────────────────────────────────────────────────────────────────
// Real timers, condition-polling with a deadline (`waitUntil`), never a bare settle: these specs
// drive the real `sse.ts` stream machinery, whose wakeups are promise-driven rather than
// timer-driven, and a single-turn settle passes alone and fails intermittently under suite load.

import { afterEach, describe, expect, test } from 'vitest'

import { createUpgradeHarness, reconciledPayload, waitUntil, type UpgradeHarness } from './upgrade-client-harness.js'
import { TAG, encode, type DecodedFrame } from './shared-ws.js'

type ReconcileFrame = DecodedFrame & { tag: typeof TAG.RECONCILE }

let harness: UpgradeHarness | null = null
afterEach(() => {
  harness?.dispose()
  harness = null
})

const barrierHarness = async (channelIds: string[] = ['A'], options = {}) => {
  harness = await createUpgradeHarness(channelIds, { barrierUpgrade: true, ...options })
  return harness
}

const reconcilesOn = (frames: readonly DecodedFrame[]): ReconcileFrame[] =>
  frames.filter((f): f is ReconcileFrame => f.tag === TAG.RECONCILE)

/** Drive the commit by hand. Pairs with `barrier: 'refuse'`, which makes the mock server say
 *  nothing so a test owns the exact interleaving of FIN and COMMITTED. */
function commit(h: UpgradeHarness, parts: { fin?: boolean; committed?: boolean } = {}): void {
  const { fin = true, committed = true } = parts
  const barrier = h.barriers.at(-1)
  expect(barrier).toBeDefined()
  if (fin) h.sse.pushFrame(encode.fin())
  if (committed) {
    h.ws.pushFrame(
      encode.reconciled({
        ...reconciledPayload(
          barrier!.open.map((e) => ({ ix: e.ix, lastSeq: e.lastSeq })),
          undefined,
          true,
        ),
        upgradeId: barrier!.upgradeId,
      }),
    )
  }
}

describe('I5 — the capability gate decides the flow, in both polarities', () => {
  test('capability ABSENT: the legacy flow runs and not one new tag reaches the wire', async () => {
    // PAIRED on purpose. "No PREPARE was sent" is satisfied just as well by an upgrade that never
    // started, so the legacy handoff RECONCILE — which only the legacy flow emits — is asserted
    // alongside it as proof that the client really did upgrade, the old way.
    const h = (harness = await createUpgradeHarness(['A']))

    expect(h.prepares).toHaveLength(0)
    expect(h.barriers).toHaveLength(0)
    const legacyReconcile = reconcilesOn(h.ws.sent).find((f) => f.payload.upgrade)
    expect(legacyReconcile).toBeDefined()
    expect(legacyReconcile!.payload.barrier).toBeUndefined()
    // The definitive check: nothing on either wire carries a tag an older server cannot decode.
    for (const frame of [...h.ws.sent, ...h.sse.upstream]) {
      expect(frame.tag).not.toBe(TAG.PREPARE)
    }
  })

  test('capability PRESENT: a PREPARE goes out carrying identity only', async () => {
    const h = await barrierHarness(['A'])

    expect(h.prepares).toHaveLength(1)
    const prepare = h.prepares[0]!
    expect(prepare.upgradeId).toBeTypeOf('string')
    expect(prepare.sessionId).toBeTypeOf('string')
    expect(prepare.open).toEqual([{ id: 'A', ix: 0 }])
    // No cursors. A watermark captured at staging time is stale by the time the barrier commits,
    // and replaying from it would burst past the peer's flow-control credit.
    for (const entry of prepare.open) expect(entry).not.toHaveProperty('lastSeq')
  })

  test('capability PRESENT: the barrier is the old wire, and the new wire never reconciles', async () => {
    const h = await barrierHarness(['A'])

    // The barrier went out on the OLD wire...
    const barrier = h.barriers[0]!
    expect(barrier.barrier).toBe(true)
    expect(barrier.upgrade).toBe(true)
    expect(barrier.upgradeId).toBe(h.prepares[0]!.upgradeId)
    expect(barrier.sessionId).toBeTypeOf('string')
    expect(barrier.open).toEqual([{ id: 'A', ix: 0, lastSeq: 0 }])

    // ...and the NEW wire carries no reconcile at all. This is the double-RECONCILE trap: firing
    // the transport's own `sendReconcileOnOpen` on the flip would rotate the just-committed session
    // a second time and drop the FIN finalizer the barrier installed — while still completing the
    // upgrade, so a happy-path assertion alone would never notice.
    // ...and the NEW wire carries no reconcile at all — right through to a completed handoff, so
    // the absence is not merely "not yet".
    await waitUntil(() => h.handoffDrained(), 'the barrier upgrade completed')
    expect(reconcilesOn(h.ws.sent)).toHaveLength(0)
    expect(h.upgradeTag()).toBe('none')
    // One SSE connect for the whole story: the upgrade succeeded rather than falling back.
    expect(h.sseConnects()).toBe(1)
  })

  test('a new tag reaching an OLD server terminates that wire cleanly, losing no data', async () => {
    // The gate is what makes this unreachable, but the cost of getting it wrong is what makes the
    // gate load-bearing rather than an optimization — so the cost is measured. The server here
    // does what an old one does with tag 0x07: kills the wire.
    const h = await barrierHarness(['A'], { prepare: 'terminate' })

    await waitUntil(() => h.upgradeTag() === 'none', 'the attempt released after the probe died')
    // The old session is untouched: pre-barrier failures never cost an established client its wire.
    expect(h.sseConnects()).toBe(1)
    expect(h.channels[0]!.isClosed).toBe(false)

    // ...and it still delivers. The instrument could disagree: a client that tore down both wires
    // would have no route for this frame.
    h.sse.pushFrame(encode.text(0, 'null', 1))
    await waitUntil(() => h.channels[0]!.received.length === 1, 'the old wire still delivers')
  })
})

describe('I4 — settlement happens on COMMITTED, and exactly once', () => {
  test('sendBuffer is released only after COMMITTED, and the channel opens once with batched:false', async () => {
    const h = await barrierHarness(['A'], { barrier: 'refuse' })
    const channel = h.channels[0]!
    const opensBefore = channel.opens.length

    // Gated behind the in-flight reconcile, so it queues rather than racing the commit.
    h.send(0, '"queued"')
    expect(h.bufferedSendCount()).toBe(1)

    // FIN alone must not settle anything — the join needs both limbs. Waiting for the client to
    // have CONSUMED the FIN is what makes this assertion able to fail: pushing a frame onto the SSE
    // downstream only queues it, so asserting straight afterwards would be asserting against a
    // client that has not seen it yet, and would pass whatever the client did with it.
    commit(h, { committed: false })
    await waitUntil(() => h.handoffFinReceived(), 'the client consumed the FIN')
    expect(h.bufferedSendCount()).toBe(1)
    expect(channel.opens.length).toBe(opensBefore)

    commit(h, { fin: false })
    await waitUntil(() => h.bufferedSendCount() === 0, 'sendBuffer released after COMMITTED')

    // Exactly one open, and it reports the WS as unbatched. Two would mean the flip reconciled.
    expect(channel.opens.length).toBe(opensBefore + 1)
    expect(channel.opens.at(-1)).toBe(false)
    expect(reconcilesOn(h.ws.sent)).toHaveLength(0)
    await waitUntil(() => h.handoffDrained(), 'handoff completed and the old wire was torn down')
  })

  test('a RECONCILED that does not echo this attempt is NOT consumed as the commit', async () => {
    // The stale-settlement defect: an ordinary reconciled still in flight from the old wire would
    // otherwise complete a handoff the server never performed, retiring a live SSE session against
    // a WS that holds no session at all.
    const h = await barrierHarness(['A'], { barrier: 'refuse' })

    h.sse.pushFrame(encode.fin())
    // Same shape as the real commit in every respect EXCEPT the upgradeId.
    h.ws.pushFrame(encode.reconciled({ ...reconciledPayload([{ ix: 0, lastSeq: 0 }], undefined, true) }))
    h.ws.pushFrame(
      encode.reconciled({ ...reconciledPayload([{ ix: 0, lastSeq: 0 }], undefined, true), upgradeId: 'someone-else' }),
    )
    // The WS imposters are delivered synchronously by the stub; the FIN is not, so this is what
    // proves the client has processed everything pushed above before the assertions run.
    await waitUntil(() => h.handoffFinReceived(), 'the client consumed the FIN and both imposters')

    expect(h.inHandoff()).toBe(true)
    expect(h.handoffDrained()).toBe(false)

    // The real one still settles it — so the guard rejects imposters rather than everything.
    commit(h, { fin: false })
    await waitUntil(() => h.handoffDrained(), 'the real COMMITTED settled the handoff')
  })
})

describe('the barrier payload is read at emission, not at staging', () => {
  test('cursors advanced during the staging window are in the barrier', async () => {
    const h = await barrierHarness(['A'], { prepare: 'withhold', barrier: 'refuse' })
    const channel = h.channels[0]!

    // Staged, awaiting READY — and deliberately UNGATED, so the old wire keeps flowing.
    expect(h.upgradeTag()).toBe('preparing')
    h.sse.pushFrame(encode.text(0, '"one"', 1))
    h.sse.pushFrame(encode.text(0, '"two"', 2))
    await waitUntil(() => channel.received.length === 2, 'both frames delivered during staging')

    h.sendReady()
    await waitUntil(() => h.barriers.length > 0, 'barrier emitted')

    // Built at emission: a payload snapshotted at PREPARE time would still report lastSeq 0, and
    // the server would replay two frames the client already has.
    expect(h.barriers[0]!.open).toEqual([{ id: 'A', ix: 0, lastSeq: 2 }])
  })
})

describe('I11 — registrations stay deferred across staging', () => {
  test('a channel opened mid-staging is not in the barrier and lands on the follow-up reconcile', async () => {
    const h = await barrierHarness(['A'], { prepare: 'withhold', barrier: 'refuse' })
    expect(h.upgradeTag()).toBe('preparing')
    const upstreamBefore = h.sse.upstream.length

    const late = h.register('B')

    // Deferred, not sent: `flushPendingRegisterReconcile` keeps the obligation while an upgrade is
    // in flight. Relaxing that is what the rotation race made unsafe — a register-reconcile landing
    // mid-stage rotates the session out from under the barrier.
    //
    // `scheduleRegisterReconcile` arms a 0 ms timer, so a real interval has to elapse before "no
    // RECONCILE was sent" means anything at all. The post-commit follow-up asserted at the end of
    // this test is the control: the same registration DOES reach the wire once the upgrade is over.
    await new Promise((resolve) => setTimeout(resolve, 50))
    expect(reconcilesOn(h.sse.upstream.slice(upstreamBefore))).toHaveLength(0)
    expect(late.opens).toHaveLength(0)

    h.sendReady()
    await waitUntil(() => h.barriers.length > 0, 'barrier emitted')

    // Established channels only. The server REFUSES a barrier carrying `initial: true`, because
    // `attach` would park the commit mid-flight waiting for a channel to appear.
    expect(h.barriers[0]!.open).toEqual([{ id: 'A', ix: 0, lastSeq: 0 }])
    for (const entry of h.barriers[0]!.open) expect(entry).not.toHaveProperty('initial')

    commit(h)
    // Left out of the barrier's `reconcileIxes` too, so settlement reads it as registered-after-the-
    // fact and stages the ordinary follow-up — rather than as server-omitted, which would RELEASE a
    // channel the user just opened.
    await waitUntil(() => reconcilesOn(h.ws.sent).length === 1, 'follow-up reconcile for the late channel')
    expect(late.isClosed).toBe(false)

    const followUp = reconcilesOn(h.ws.sent)[0]!
    const lateEntry = followUp.payload.open.find((e) => e.id === 'B')
    expect(lateEntry).toMatchObject({ id: 'B', initial: true })
    expect(followUp.payload.barrier).toBeUndefined()

    h.ws.pushFrame(
      encode.reconciled(
        reconciledPayload(
          followUp.payload.open.map((e) => ({ ix: e.ix, lastSeq: 0 })),
          undefined,
          true,
        ),
      ),
    )
    await waitUntil(() => late.opens.length === 1, 'the late channel opened on the new wire')
  })
})

describe('PC1 (client-side) — a frame in flight at barrier time is delivered, not dropped', () => {
  test('an old-wire frame racing the commit survives the handoff', async () => {
    // The client-side inversion of PC1. The old wire's frame is pushed AFTER the barrier has left
    // and BEFORE the commit settles — precisely the window in which a client that merged both
    // wires into one arrival-ordered buffer, or that drained the new wire first, loses it.
    const h = await barrierHarness(['A'], { barrier: 'refuse' })
    const channel = h.channels[0]!
    expect(h.inHandoff()).toBe(true)

    h.sse.pushFrame(encode.text(0, '"in-flight"', 1))
    // Buffered rather than dispatched — the handoff is still open, which is what makes the
    // ordering rule observable at all.
    await waitUntil(() => (h.handoffBuffered()?.frames ?? 0) === 1, 'the in-flight frame is held')
    expect(channel.received).toHaveLength(0)

    commit(h)
    await waitUntil(() => h.handoffDrained(), 'handoff settled')

    expect(channel.received).toEqual([{ kind: 'text', value: 'in-flight' }])
    expect(channel.isClosed).toBe(false)
  })

  test('control: with no frame in flight the same handoff settles empty', async () => {
    // The counter-sentinel. Without it, a harness that fabricated deliveries would pass the test
    // above for the wrong reason.
    const h = await barrierHarness(['A'], { barrier: 'refuse' })
    commit(h)
    await waitUntil(() => h.handoffDrained(), 'handoff settled')
    expect(h.channels[0]!.received).toHaveLength(0)
  })
})
