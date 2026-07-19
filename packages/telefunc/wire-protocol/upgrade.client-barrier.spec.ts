// The CLIENT half of the barrier upgrade: the `PREPARE`/`READY` exchange, the barrier as the old
// wire's final frame, the flip that must NOT reconcile, settlement on COMMITTED, and every way an
// attempt can end badly.
//
// ── The single rule the failure rows pin ─────────────────────────────────────────────────────
// PRE-barrier, nothing the client sent can rotate the server's session, so a failure costs the
// attempt and nothing else: the old SSE wire keeps its session and keeps delivering. ONCE THE
// BARRIER MAY HAVE LEFT, no wire can be trusted to hold the session, so both are abandoned for one
// fresh reconcile and upgrades are disabled for good. `sseConnects()` is the discriminator
// (1 = the old wire survived, 2 = it fell back).
//
// ── What makes the absence assertions non-vacuous ────────────────────────────────────────────
// Much of this file's subject matter is a frame that must NOT appear (no RECONCILE on the new wire,
// ever; no barrier after a failed stage). An absence assertion passes just as happily when the flow
// never ran at all, so each is PAIRED with positive proof that the upgrade did happen and the
// channel still works.
//
// ── Timing discipline ────────────────────────────────────────────────────────────────────────
// Real timers, condition-polling with a deadline, never a bare settle: these specs drive the real
// `sse.ts` stream machinery, whose wakeups are promise-driven rather than timer-driven, and a
// single-turn settle passes alone and fails intermittently under suite load. The deadline rows fake
// `setTimeout` but NOT `Date.now`, with `shouldAdvanceTime`, so the stream machinery and the pollers
// keep running on real time — the only way to reach a 10 s deadline without a 10 s test.

import { afterEach, describe, expect, test, vi } from 'vitest'
import { stringify } from '@brillout/json-serializer/stringify'

import { createUpgradeHarness, waitUntil, type UpgradeHarness } from './upgrade-client-harness.js'
import { UPGRADE_ATTEMPT_TIMEOUT_MS, UPGRADE_HANDOFF_JOIN_TIMEOUT_MS } from './constants.js'
import { TAG, encode, type DecodedFrame } from './shared-ws.js'

type ReconcileFrame = DecodedFrame & { tag: typeof TAG.RECONCILE }
const reconcilesOn = (frames: readonly DecodedFrame[]): ReconcileFrame[] =>
  frames.filter((f): f is ReconcileFrame => f.tag === TAG.RECONCILE)

/** Text payloads the mock server RECEIVED, in arrival order.
 *
 *  Deliberately `sse.upstream` and not `batchPosts`: `batchPosts` records only out-of-band POSTs and
 *  NOT the connect POST — and the connect POST's initial batch is exactly where a frame that jumps
 *  ahead of replay would ride. An oracle blind to it cannot see that regression at all. */
const textsUpstream = (h: UpgradeHarness): string[] =>
  h.sse.upstream.filter((f): f is DecodedFrame & { tag: typeof TAG.TEXT } => f.tag === TAG.TEXT).map((f) => f.text)

/** `WebSocket.CLOSED`. The harness exports the class as a type only, so the wire value is used. */
const WS_CLOSED = 3

/** The join deadline is 2 s and the next-slowest bound that could produce the same fallback is
 *  `RECONCILE_TIMEOUT_MS` at 10 s, so a budget in between is what separates them. Falling back
 *  inside it is the ONLY observable the two limbs have: `abortUpgradeAndReconnectSse` reconnects
 *  rather than closing channels, so a limb-naming error never reaches any test surface. */
const JOIN_BUDGET_MS = UPGRADE_HANDOFF_JOIN_TIMEOUT_MS + 3_000

let harness: UpgradeHarness | null = null
afterEach(() => {
  harness?.dispose()
  harness = null
  vi.useRealTimers()
})

const upgradeHarness = async (options: Record<string, unknown> = {}, channelIds = ['A']) =>
  (harness = await createUpgradeHarness(channelIds, options))

describe('the PREPARE/barrier exchange', () => {
  test('the PREPARE carries identity only — no cursors', async () => {
    const h = await upgradeHarness()

    expect(h.prepares).toHaveLength(1)
    const prepare = h.prepares[0]!
    expect(prepare.upgradeId).toBeTypeOf('string')
    expect(prepare.sessionId).toBeTypeOf('string')
    expect(prepare.open).toEqual([{ id: 'A', ix: 0 }])
    // A watermark captured at staging time is stale by the time the barrier commits, and replaying
    // from it would burst past the peer's flow-control credit.
    for (const entry of prepare.open) expect(entry).not.toHaveProperty('lastSeq')
  })

  test('the barrier is the old wire, the new wire never reconciles, and one socket does it all', async () => {
    const h = await upgradeHarness()

    const barrier = h.barriers[0]!
    expect(barrier.barrier).toBe(true)
    expect(barrier.upgradeId).toBe(h.prepares[0]!.upgradeId)
    expect(barrier.sessionId).toBeTypeOf('string')
    expect(barrier.open).toEqual([{ id: 'A', ix: 0, lastSeq: 0 }])

    // THE DOUBLE-RECONCILE TRAP: firing the transport's own `sendReconcileOnOpen` on the flip would
    // rotate the just-committed session a second time and drop the FIN finalizer the barrier
    // installed — while still completing the upgrade, so a happy-path assertion alone would never
    // notice. Asserted right through to a completed handoff, so the absence is not merely "not yet".
    await waitUntil(() => h.handoffDrained(), 'the barrier upgrade completed')
    expect(reconcilesOn(h.ws.sent)).toHaveLength(0)
    expect(h.upgradeTag()).toBe('none')
    // Exactly one socket: the flip ADOPTS the probed socket. Were it to connect a fresh one instead,
    // that socket would (correctly) send its own RECONCILE and both counts would change.
    expect(h.sockets).toHaveLength(1)
    expect(h.ws.sent.filter((f) => f.tag === TAG.PREPARE)).toHaveLength(1)
    // One SSE connect for the whole story: the upgrade succeeded rather than falling back...
    expect(h.sseConnects()).toBe(1)
    // ...and the committed wire is the live transport.
    h.send(0, '"on-the-new-wire"')
    await waitUntil(() => h.ws.sent.some((f) => f.tag === TAG.TEXT), 'the committed WS carries user sends')
  })

  test('the barrier payload is read at EMISSION, not at staging', async () => {
    const h = await upgradeHarness({ prepare: 'withhold', barrier: 'refuse' })
    const channel = h.channels[0]!

    // Staged, awaiting READY — and deliberately UNGATED, so the old wire keeps flowing.
    expect(h.upgradeTag()).toBe('preparing')
    h.sse.pushFrame(encode.text(0, '"one"', 1))
    h.sse.pushFrame(encode.text(0, '"two"', 2))
    await waitUntil(() => channel.received.length === 2, 'both frames delivered during staging')

    h.sendReady()
    await waitUntil(() => h.barriers.length > 0, 'barrier emitted')

    // A payload snapshotted at PREPARE time would still report lastSeq 0, and the server would
    // replay two frames the client already has.
    expect(h.barriers[0]!.open).toEqual([{ id: 'A', ix: 0, lastSeq: 2 }])
  })

  test('a channel opened mid-staging is not in the barrier and lands on the follow-up reconcile', async () => {
    const h = await upgradeHarness({ prepare: 'withhold', barrier: 'refuse' })
    expect(h.upgradeTag()).toBe('preparing')
    const upstreamBefore = h.sse.upstream.length

    const late = h.register('B')

    // Deferred, not sent: `flushPendingRegisterReconcile` keeps the obligation while an upgrade is in
    // flight, because a register-reconcile landing mid-stage rotates the session out from under the
    // barrier. `scheduleRegisterReconcile` arms a 0 ms timer, so a real interval has to elapse before
    // "no RECONCILE was sent" means anything; the post-commit follow-up below is the control that the
    // same registration DOES reach the wire once the upgrade is over.
    await new Promise((resolve) => setTimeout(resolve, 50))
    expect(reconcilesOn(h.sse.upstream.slice(upstreamBefore))).toHaveLength(0)
    expect(late.opens).toHaveLength(0)

    h.sendReady()
    await waitUntil(() => h.barriers.length > 0, 'barrier emitted')

    // Established channels only — the server REFUSES a barrier carrying `initial: true`.
    expect(h.barriers[0]!.open).toEqual([{ id: 'A', ix: 0, lastSeq: 0 }])
    for (const entry of h.barriers[0]!.open) expect(entry).not.toHaveProperty('initial')

    h.sse.pushFrame(encode.fin())
    h.ws.pushFrame(h.committedFrame())
    // Left out of the barrier's `reconcileIxes` too, so settlement reads it as registered-after-the-
    // fact and stages the ordinary follow-up — rather than as server-omitted, which would RELEASE a
    // channel the user just opened.
    await waitUntil(() => reconcilesOn(h.ws.sent).length === 1, 'follow-up reconcile for the late channel')
    expect(late.isClosed).toBe(false)

    const followUp = reconcilesOn(h.ws.sent)[0]!
    expect(followUp.payload.open.find((e) => e.id === 'B')).toMatchObject({ id: 'B', initial: true })
    expect(followUp.payload.barrier).toBeUndefined()
  })
})

describe('settlement happens on COMMITTED, and exactly once', () => {
  test('sendBuffer is released only after COMMITTED, and the channel opens once with batched:false', async () => {
    const h = await upgradeHarness({ barrier: 'refuse' })
    const channel = h.channels[0]!
    const opensBefore = channel.opens.length

    // Gated behind the in-flight reconcile, so it queues rather than racing the commit.
    h.send(0, '"queued"')
    expect(h.bufferedSendCount()).toBe(1)

    // FIN alone must not settle anything — the join needs both limbs. Waiting for the client to have
    // CONSUMED the FIN is what makes this able to fail: pushing a frame onto the SSE downstream only
    // queues it, so asserting straight afterwards would pass whatever the client did with it.
    h.sse.pushFrame(encode.fin())
    await waitUntil(() => h.handoffFinReceived(), 'the client consumed the FIN')
    expect(h.bufferedSendCount()).toBe(1)
    expect(channel.opens.length).toBe(opensBefore)

    h.ws.pushFrame(h.committedFrame())
    await waitUntil(() => h.bufferedSendCount() === 0, 'sendBuffer released after COMMITTED')

    // Exactly one open, reporting the WS as unbatched. Two would mean the flip reconciled.
    expect(channel.opens.length).toBe(opensBefore + 1)
    expect(channel.opens.at(-1)).toBe(false)
    expect(reconcilesOn(h.ws.sent)).toHaveLength(0)
    await waitUntil(() => h.handoffDrained(), 'handoff completed and the old wire was torn down')
  })

  test('a RECONCILED that does not echo this attempt is NOT consumed as the commit', async () => {
    // The stale-settlement defect: an ordinary reconciled still in flight from the old wire would
    // otherwise complete a handoff the server never performed, retiring a live SSE session against a
    // WS that holds no session at all.
    const h = await upgradeHarness({ barrier: 'refuse' })

    h.sse.pushFrame(encode.fin())
    // Same shape as the real commit in every respect EXCEPT the upgradeId.
    h.ws.pushFrame(h.committedFrame([{ ix: 0, lastSeq: 0 }], { upgradeId: undefined }))
    h.ws.pushFrame(h.committedFrame([{ ix: 0, lastSeq: 0 }], { upgradeId: 'someone-else' }))
    // The WS imposters are delivered synchronously by the stub; the FIN is not, so this is what
    // proves the client processed everything pushed above before the assertions run.
    await waitUntil(() => h.handoffFinReceived(), 'the client consumed the FIN and both imposters')

    expect(h.inHandoff()).toBe(true)
    expect(h.handoffDrained()).toBe(false)

    // The real one still settles it — so the guard rejects imposters rather than everything.
    h.ws.pushFrame(h.committedFrame())
    await waitUntil(() => h.handoffDrained(), 'the real COMMITTED settled the handoff')
  })

  test('an old-wire frame racing the commit survives the handoff', async () => {
    // PC1's client-side inversion. The old wire's frame is pushed AFTER the barrier has left and
    // BEFORE the commit settles — precisely the window in which a client that merged both wires into
    // one arrival-ordered buffer, or drained the new wire first, loses it.
    const h = await upgradeHarness({ barrier: 'refuse' })
    const channel = h.channels[0]!
    expect(h.inHandoff()).toBe(true)

    h.sse.pushFrame(encode.text(0, '"in-flight"', 1))
    // Buffered rather than dispatched — the handoff is still open, which is what makes the ordering
    // rule observable at all.
    await waitUntil(() => (h.handoffBuffered()?.frames ?? 0) === 1, 'the in-flight frame is held')
    expect(channel.received).toHaveLength(0)

    h.sse.pushFrame(encode.fin())
    h.ws.pushFrame(h.committedFrame())
    await waitUntil(() => h.handoffDrained(), 'handoff settled')

    expect(channel.received).toEqual([{ kind: 'text', value: 'in-flight' }])
    expect(channel.isClosed).toBe(false)
  })
})

describe('settlement must not skip the secondary cleanup it moved past', () => {
  // Before the split, a channel the settling RECONCILED omitted was released INSIDE
  // `applyReconciled` — ahead of two things that depend on it: `handleReconciled`'s
  // `startTtlIfIdle()`, and `applyReconciled`'s own `drainBufferedFrames` compaction. Deferring the
  // release to `tryCompleteUpgradeHandoff` moved it AFTER both, and nothing downstream re-runs
  // either step. FIN-first is the ordering that still works (the handoff completes from inside
  // `handleReconciled`), so each finding is paired with the ordering twin that proves it is about
  // the ORDERING and not about the harness, the idle timeout or the send path.
  const IDLE_TIMEOUT_MS = 300
  /** `WebSocket.CLOSED` on the probe socket is the outside-visible proof that the idle TTL fired and
   *  tore the connection down, rather than leaving it cached with a live wire. */
  const waitForDispose = (h: UpgradeHarness) =>
    waitUntil(() => h.ws.socket.readyState === WS_CLOSED, 'connection went idle and disposed', 2_000)

  test('RECONCILED-before-FIN omitting the last channel still lets the connection go idle', async () => {
    const h = await upgradeHarness({ barrier: 'refuse' })

    h.ws.pushFrame(h.committedFrame([], { idleTimeout: IDLE_TIMEOUT_MS }))
    h.sse.pushFrame(encode.fin())
    await waitUntil(() => h.handoffDrained(), 'handoff completed')
    expect(h.channels[0]!.isClosed).toBe(true)

    // Zero channels remain. The connection must not stay cached with a live wire.
    await waitForDispose(h)
  })

  test('control: a settlement that KEEPS the channel does not dispose', async () => {
    const h = await upgradeHarness({ barrier: 'refuse' })

    h.ws.pushFrame(h.committedFrame([{ ix: 0, lastSeq: 0 }], { idleTimeout: IDLE_TIMEOUT_MS }))
    h.sse.pushFrame(encode.fin())
    await waitUntil(() => h.handoffDrained(), 'handoff completed')

    await new Promise((resolve) => setTimeout(resolve, IDLE_TIMEOUT_MS * 3))
    expect(h.ws.socket.readyState).not.toBe(WS_CLOSED)
    expect(h.channels[0]!.isClosed).toBe(false)
  })

  test('a send queued for an omitted channel is not left pinned in the send buffer', async () => {
    const h = await upgradeHarness({ barrier: 'refuse' })

    h.send(0, stringify('queued-during-handoff'))
    expect(h.bufferedSendCount()).toBe(1) // precondition: really buffered, not sent

    h.ws.pushFrame(h.committedFrame([]))
    h.sse.pushFrame(encode.fin())
    await waitUntil(() => h.handoffDrained(), 'handoff completed')

    expect(h.channels[0]!.isClosed).toBe(true)
    expect(h.bufferedSendCount()).toBe(0)
  })

  test('control: a send queued for an ACKNOWLEDGED channel is released onto the new wire', async () => {
    // So `bufferedSendCount()` can reach 0 on this path, and the row above is not passing on a
    // counter that is always 0.
    const h = await upgradeHarness({ barrier: 'refuse' })

    h.send(0, stringify('queued-during-handoff'))
    expect(h.bufferedSendCount()).toBe(1)

    h.ws.pushFrame(h.committedFrame([{ ix: 0, lastSeq: 0 }]))
    h.sse.pushFrame(encode.fin())
    await waitUntil(() => h.handoffDrained(), 'handoff completed')

    expect(h.bufferedSendCount()).toBe(0)
    expect(h.channels[0]!.isClosed).toBe(false)
    // ...and it actually went out on the new wire, rather than merely being discarded.
    expect(JSON.stringify(h.ws.sent)).toContain('queued-during-handoff')
  })
})

describe('the attempt deadline is the only abort path when the server says nothing', () => {
  test('READY never arrives: the deadline ends the attempt and the old wire is untouched', async () => {
    vi.useFakeTimers({ shouldAdvanceTime: true, toFake: ['setTimeout', 'clearTimeout', 'setInterval'] })
    const h = await upgradeHarness({ prepare: 'withhold' })

    expect(h.upgradeTag()).toBe('preparing')
    // Ungated by design: `preparing` is not `draining`, so the old wire keeps carrying user sends for
    // the whole staging window.
    h.sse.pushFrame(encode.text(0, '"during-staging"', 1))
    await waitUntil(() => h.channels[0]!.received.length === 1, 'the old wire still delivers while staged')

    vi.advanceTimersByTime(UPGRADE_ATTEMPT_TIMEOUT_MS + 100)
    await waitUntil(() => h.upgradeTag() === 'none', 'the attempt deadline released the attempt')

    // Pre-barrier: no fallback, no fresh SSE, nothing closed.
    expect(h.sseConnects()).toBe(1)
    expect(h.channels[0]!.isClosed).toBe(false)
    expect(h.barriers).toHaveLength(0)
    // The probe is let go with the attempt — otherwise a socket the server has staged and will never
    // commit stays alive on PINGs alone.
    expect(h.sockets[0]!.readyState).toBe(WS_CLOSED)

    // And the old wire is genuinely still live, not merely un-torn-down.
    h.sse.pushFrame(encode.text(0, '"after-deadline"', 2))
    await waitUntil(() => h.channels[0]!.received.length === 2, 'the old wire delivers after the deadline')
  })

  test('a READY naming another attempt ends this one rather than licensing a barrier', async () => {
    // An impostor proves nothing about whether OUR stage was installed, so emitting the old wire's
    // final frame on its word would be a barrier against a stage that may not exist.
    const h = await upgradeHarness({ prepare: 'withhold' })
    expect(h.upgradeTag()).toBe('preparing')

    h.sendReady('not-our-attempt')
    await waitUntil(() => h.upgradeTag() !== 'preparing', 'the impostor READY ended the attempt')

    expect(h.barriers).toHaveLength(0)
    expect(h.upgradeTag()).toBe('none')
    expect(h.sseConnects()).toBe(1)
    expect(h.sockets[0]!.readyState).toBe(WS_CLOSED)

    h.sse.pushFrame(encode.text(0, '"still-here"', 1))
    await waitUntil(() => h.channels[0]!.received.length === 1, 'the old wire is untouched')
  })

  test('a new tag reaching an OLD server terminates that wire cleanly, losing no data', async () => {
    // The version-skew row: an old server decode-asserts on tag 0x07 and kills the probe. Nothing is
    // done about that beyond making sure it costs the established client nothing.
    const h = await upgradeHarness({ prepare: 'terminate' })

    await waitUntil(() => h.upgradeTag() === 'none', 'the attempt released after the probe died')
    expect(h.sseConnects()).toBe(1)
    expect(h.channels[0]!.isClosed).toBe(false)

    // ...and it still delivers. The instrument could disagree: a client that tore down both wires
    // would have no route for this frame.
    h.sse.pushFrame(encode.text(0, 'null', 1))
    await waitUntil(() => h.channels[0]!.received.length === 1, 'the old wire still delivers')
  })
})

describe('once the barrier may have left, both wires are abandoned', () => {
  test('a silently refused barrier still ends: neither limb of the join ever arrives', async () => {
    // The exact server behaviour a stale barrier gets — no COMMITTED, no FIN, no termination, nothing
    // rotated. On the client that is indistinguishable from a server that simply stopped, which is
    // why only a client-side deadline can end it.
    const h = await upgradeHarness({ barrier: 'refuse' })

    await waitUntil(() => h.sseConnects() === 2, 'the refused barrier eventually fell back', JOIN_BUDGET_MS)
    expect(h.channels[0]!.isClosed).toBe(false)
  }, 10_000)

  test('COMMITTED never arrives: the join deadline forces the fallback', async () => {
    const h = await upgradeHarness({ barrier: 'fin-only' })

    await waitUntil(() => h.sseConnects() === 2, 'the join deadline forced the fallback', JOIN_BUDGET_MS)
    expect(h.channels[0]!.isClosed).toBe(false)
  }, 10_000)

  test('FIN never arrives: the OTHER limb of the same join is bounded too', async () => {
    // Arming the deadline at handoff ENTRY rather than on FIN arrival is what makes this limb bounded
    // at all: with the reconciled already settled there is no `reconcileTimer` either, so nothing
    // else in the system is watching.
    const h = await upgradeHarness({ barrier: 'committed-only' })

    await waitUntil(() => h.sseConnects() === 2, 'the join deadline forced the fallback', JOIN_BUDGET_MS)
    expect(h.channels[0]!.isClosed).toBe(false)
  }, 10_000)

  test('control: a handoff that receives BOTH limbs never falls back', async () => {
    // Without this, every `sseConnects() === 2` above is satisfied just as well by a client that
    // falls back on every upgrade regardless of what arrives.
    const h = await upgradeHarness()
    await waitUntil(() => h.handoffDrained(), 'the upgrade committed')
    await new Promise((resolve) => setTimeout(resolve, UPGRADE_HANDOFF_JOIN_TIMEOUT_MS + 500))
    expect(h.sseConnects()).toBe(1)
  }, 10_000)

  test('the SSE dies after the barrier: one fresh reconcile, and the in-flight frame survives', async () => {
    const h = await upgradeHarness({ barrier: 'refuse' })
    expect(h.inHandoff()).toBe(true)

    // Received on the old wire but not yet dispatched — terminal old-wire frames carry no seq and no
    // replay can reproduce them, so the fallback must drain what it already has.
    h.sse.pushFrame(encode.text(0, '"pre-death"', 1))
    await waitUntil(() => (h.handoffBuffered()?.frames ?? 0) === 1, 'the frame is held in the handoff buffer')

    h.sse.close()
    await waitUntil(() => h.sseConnects() === 2, 'client fell back to a fresh SSE')

    expect(h.channels[0]!.received).toEqual([{ kind: 'text', value: 'pre-death' }])
    expect(h.channels[0]!.isClosed).toBe(false)
  })

  test('upgradeDisabled is sticky: the post-fallback reconnect settles and starts NO second upgrade', async () => {
    // Without the harness answering the reconnect's RECONCILE, `maybeStartUpgrade` is never reached
    // on the fresh wire, so "no second attempt" would hold for the wrong reason — a gate that cannot
    // fail. `autoReconcile` is what gives it teeth.
    const h = await upgradeHarness({ barrier: 'fin-only', autoReconcile: true })

    // The instrument is proven live by this very assertion: the FIRST settled RECONCILED, of exactly
    // the shape `autoReconcile` replays, did produce a probe and a PREPARE. So "no second socket"
    // below is a claim about stickiness, not about a reply that never arms anything.
    expect(h.sockets).toHaveLength(1)
    expect(h.prepares).toHaveLength(1)

    await waitUntil(() => h.sseConnects() === 2, 'client fell back to a fresh SSE', JOIN_BUDGET_MS)
    await waitUntil(
      () => reconcilesOn(h.sse.upstream).length >= 2,
      'the reconnect sent its own RECONCILE',
      JOIN_BUDGET_MS,
    )
    h.sse.pushFrame(encode.text(0, '"post-fallback"', 1))
    await waitUntil(() => h.channels[0]!.received.length > 0, 'the fresh wire settled and delivers', JOIN_BUDGET_MS)

    // ...and reaching `maybeStartUpgrade` on it changed nothing, for the connection's whole life.
    expect(h.sockets).toHaveLength(1)
    expect(h.prepares).toHaveLength(1)
  }, 15_000)
})

describe('batch-mode emission: the deadline must be able to interrupt a quiesce', () => {
  test('a prior POST hangs before the barrier: C2S traffic actually RESUMES on the wire', async () => {
    // ── The observer here is the whole point ─────────────────────────────────────────────────
    // The obvious assertion — that `sendBuffer` drained — reads the queue ABOVE the transport and
    // proves only that frames changed hands. The hung POST still owns `SseTransport.flushing`, so
    // released frames land in the outbox and every later `flushOutbox` returns at that guard: the
    // connection reports itself healthy while the wire is permanently mute. So the oracle is BELOW
    // the transport — a POST body the mock server actually received.
    vi.useFakeTimers({ shouldAdvanceTime: true, toFake: ['setTimeout', 'clearTimeout', 'setInterval'] })
    const h = await upgradeHarness({ batchMode: true, prepare: 'withhold', barrier: 'refuse', autoReconcile: true })
    expect(h.upgradeTag()).toBe('preparing')

    // A batch POST the server reads and then never answers. `flushing` stays true, so the barrier can
    // never be emitted: sending it alongside would put two POSTs on the wire with no defined
    // server-side dispatch order, which is the one thing emission must never do.
    h.setBatchPostsHang(true)
    h.send(0, '"stuck"')
    await waitUntil(() => h.batchPosts.length > 0, 'the batch POST is in flight', 6_000)
    const postsWhileWedged = h.batchPosts.length
    // Captured HERE, while still wedged — anything after this index went out because of the recovery.
    // Reading the WHOLE history is a false green: the original wedged POST already holds `"stuck"` at
    // index 0, so an order check over it holds trivially, including when the replayed copy never
    // arrives at all.
    const textsWhileWedged = textsUpstream(h).length
    expect(textsWhileWedged).toBeGreaterThan(0)

    h.sendReady()
    await waitUntil(() => h.upgradeTag() === 'draining', 'READY gated the wire for emission', 6_000)
    h.send(0, '"gated"')
    expect(h.bufferedSendCount()).toBeGreaterThan(0)

    // The wire answers again from here, so nothing but the wedge itself is under test.
    h.setBatchPostsHang(false)
    await vi.advanceTimersByTimeAsync(UPGRADE_ATTEMPT_TIMEOUT_MS + 500)

    // A wedged flush means that wire is de facto dead, so recovery is the ordinary transport-loss
    // path. The transport is REPLACED rather than reset: the stalled POST owns that instance's flush
    // gate until it settles, and replacing it also makes its eventual settlement harmless.
    await waitUntil(() => h.sseConnects() === 2, 'the stalled wire was recovered by a reconnect', 8_000)

    // Not one barrier byte was written, so the classification stays PRE-barrier. The oracle for "not
    // sticky" is behavioural rather than a flag read: the settled RECONCILED on the fresh wire is
    // allowed to start ANOTHER attempt, which a sticky `upgradeDisabled` would have forbidden.
    expect(h.barriers).toHaveLength(0)
    await waitUntil(() => h.prepares.length === 2, 'upgrades are still enabled after the recovery', 8_000)

    expect(h.batchPosts.length).toBeGreaterThan(postsWhileWedged)
    h.send(0, '"after-recovery"')
    await waitUntil(
      () => textsUpstream(h).slice(textsWhileWedged).includes('"after-recovery"'),
      'a frame sent AFTER the deadline reached the server',
      8_000,
    )
    expect(h.channels[0]!.isClosed).toBe(false)

    // Order across the recovery, judged only on what went out AFTER it. `"stuck"` has to appear a
    // SECOND time — the wedged POST swallowed the first copy and the certified replay path re-sent it
    // from the server's reported `lastSeq`. `"gated"` must FOLLOW it rather than jump ahead: gated
    // frames are released by the post-RECONCILED drain instead of riding the reconnect's initial
    // batch, because that batch would carry them ahead of the replay and the server would dup-drop
    // the older ones. Disabling that deferral inverts exactly these two.
    const texts = textsUpstream(h).slice(textsWhileWedged)
    expect(texts).toContain('"stuck"')
    expect(texts.indexOf('"stuck"')).toBeLessThan(texts.indexOf('"gated"'))
    expect(texts.indexOf('"gated"')).toBeLessThan(texts.indexOf('"after-recovery"'))

    // EXACTLY one recovery reconnect. Disposing the wedged transport aborts the stalled POST, which
    // then rejects and reports failure — against a transport that is no longer `this.transport`. A
    // straggler that tore down the SUCCESSOR wire would show up here as a third connect.
    expect(h.sseConnects()).toBe(2)
  }, 30_000)

  test('a POST that settles OK around the abort leaves the wire usable — no recovery', async () => {
    // The other side of the wedge, and the only branch separating "nothing written, wire fine" from
    // "nothing written, wire stuck". Both reach `emitBarrier`; both abort pre-barrier. What differs is
    // whether the flush ever let go.
    //
    // Sequence: a POST is in flight, so emission parks in the quiesce. The attempt is aborted there —
    // the probe wire dying does it, no clock needed — and only THEN does the POST answer 200. By the
    // time the quiesce returns the wire is perfectly healthy, so treating this as wedged would spend a
    // reconnect and a full replay to fix nothing. The idle-wire control below cannot gate this: it
    // aborts before `emitBarrier` is ever entered.
    const h = await upgradeHarness({ batchMode: true, prepare: 'withhold', barrier: 'refuse' })
    const channel = h.channels[0]!

    h.setBatchPostsHang(true)
    h.send(0, '"in-flight"')
    await waitUntil(() => h.batchPosts.length > 0, 'the batch POST is in flight', 6_000)
    const postsBefore = h.batchPosts.length

    h.sendReady()
    await waitUntil(() => h.upgradeTag() === 'draining', 'emission is parked in the quiesce', 6_000)

    h.setBatchPostsHang(false)
    h.ws.socket.close() // closing the probe is what aborts the attempt
    h.releaseHungPosts()

    await waitUntil(() => h.upgradeTag() === 'none', 'the attempt released without recovering', 6_000)

    expect(h.barriers).toHaveLength(0)
    expect(h.sseConnects()).toBe(1)
    expect(channel.isClosed).toBe(false)

    // And it is genuinely still carrying traffic, in both directions, on that same connection.
    h.send(0, '"after-abort"')
    await waitUntil(
      () =>
        h.batchPosts
          .slice(postsBefore)
          .flat()
          .some((f) => f.tag === TAG.TEXT && f.text === '"after-abort"'),
      'the untouched wire carried a frame sent after the abort',
      8_000,
    )
    h.sse.pushFrame(encode.text(0, '"downstream"', 1))
    await waitUntil(() => channel.received.length === 1, 'and still delivers downstream')
    expect(h.sseConnects()).toBe(1)
  }, 30_000)

  test('control: a pre-barrier abort on an IDLE wire does not force a reconnect', async () => {
    // The discriminator from the opposite side: an attempt that ends with nothing in flight must
    // leave the connection exactly where it was, or every ordinary pre-barrier timeout would cost a
    // needless reconnect and replay.
    vi.useFakeTimers({ shouldAdvanceTime: true, toFake: ['setTimeout', 'clearTimeout', 'setInterval'] })
    const h = await upgradeHarness({ batchMode: true, prepare: 'withhold', barrier: 'refuse' })
    expect(h.upgradeTag()).toBe('preparing')

    await vi.advanceTimersByTimeAsync(UPGRADE_ATTEMPT_TIMEOUT_MS + 500)
    await waitUntil(() => h.upgradeTag() === 'none', 'the attempt deadline released the attempt', 6_000)

    expect(h.barriers).toHaveLength(0)
    expect(h.sseConnects()).toBe(1)
    expect(h.channels[0]!.isClosed).toBe(false)

    const before = h.batchPosts.length
    h.send(0, '"idle-abort"')
    await waitUntil(() => h.batchPosts.length > before, 'the untouched wire still carries sends', 6_000)
    h.sse.pushFrame(encode.text(0, '"downstream"', 1))
    await waitUntil(() => h.channels[0]!.received.length === 1, 'and still delivers downstream')
  }, 30_000)

  test('the barrier POST itself hangs: sticky post-barrier fallback, not a generic reconnect', async () => {
    vi.useFakeTimers({ shouldAdvanceTime: true, toFake: ['setTimeout', 'clearTimeout', 'setInterval'] })
    const h = await upgradeHarness({ batchMode: true, prepare: 'withhold', barrier: 'refuse', autoReconcile: true })

    // Nothing in flight, so emission reaches its own POST — which the server reads (the barrier IS
    // recorded) and then never answers.
    h.setBatchPostsHang(true)
    h.sendReady()
    await waitUntil(() => h.barriers.length > 0, 'the barrier reached the server and the POST hung', 6_000)

    // The barrier MAY have committed, so no wire can be trusted. The attempt deadline must reach this
    // before the reconcile timer armed at barrier-build time, or the generic reconnect takes over and
    // upgrades are never disabled. It wins by construction — both are 10 s, but the attempt deadline
    // is armed at PREPARE, strictly earlier — and this row is what keeps that honest.
    await vi.advanceTimersByTimeAsync(UPGRADE_ATTEMPT_TIMEOUT_MS + 500)
    await waitUntil(() => h.sseConnects() === 2, 'the post-barrier fallback reconnected', 6_000)

    // Sticky: the fresh wire settles (autoReconcile answers it) and starts NO second attempt.
    await waitUntil(() => reconcilesOn(h.sse.upstream).length >= 2, 'the reconnect sent its RECONCILE', 6_000)
    h.sse.pushFrame(encode.text(0, '"post-fallback"', 1))
    await waitUntil(() => h.channels[0]!.received.length > 0, 'the fresh wire delivers', 6_000)
    expect(h.sockets).toHaveLength(1)
    expect(h.prepares).toHaveLength(1)
    expect(h.channels[0]!.isClosed).toBe(false)
  }, 30_000)

  test('control: batch-mode emission commits normally when the wire answers', async () => {
    // Proves the rows above fail for the reason claimed — batch mode itself is a working path, and the
    // barrier really is emitted through a quiesce-then-POST sequence.
    const h = await upgradeHarness({ batchMode: true })
    await waitUntil(() => h.handoffDrained(), 'the batch-mode barrier upgrade committed', 10_000)

    expect(h.barriers).toHaveLength(1)
    expect(h.sseConnects()).toBe(1)
    expect(reconcilesOn(h.ws.sent)).toHaveLength(0)
  }, 20_000)
})
