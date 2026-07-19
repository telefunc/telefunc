// T5 fixup — the two seams the review found, both of which only appear when something goes wrong
// LATE in the attempt:
//
//   F1: frames the server sends on the probe wire before the transport flip are HELD, and that
//       queue is charged against the same shared budget the handoff buffer enforces. Over budget,
//       the whole set is discarded and the attempt ends — a prefix must never survive.
//   F2: in batch mode the barrier cannot be emitted until the wire quiesces, and that wait has to
//       be interruptible by the attempt deadline, with an EXACT verdict afterwards on whether the
//       barrier may already have left. The two answers take opposite recovery paths.
//
// ── Why the F1 shape needs a burst rather than an assertion about counters ───────────────────
// The loss is not "a frame was buffered wrongly", it is `trackSeq` advancing past frames that were
// dropped: once it has, the client reports the HIGHER cursor and the server's replay of the
// dropped prefix is dup-suppressed forever. So the oracle is whether a frame at the ORIGINAL seq
// is still deliverable afterwards — not the buffer state, and not a frame count.
//
// ── Timing discipline ────────────────────────────────────────────────────────────────────────
// The deadline rows fake `setTimeout` but NOT `Date.now`, with `shouldAdvanceTime`, so the real
// stream machinery and the condition-pollers keep running on real time. Budgets are chosen to sit
// between the competing timeouts they have to discriminate.

import { afterEach, describe, expect, test, vi } from 'vitest'

import { createUpgradeHarness, reconciledPayload, waitUntil, type UpgradeHarness } from './upgrade-client-harness.js'
import { UPGRADE_ATTEMPT_TIMEOUT_MS, UPGRADE_HANDOFF_BUFFER_FRAMES } from './constants.js'
import { TAG, encode, type DecodedFrame } from './shared-ws.js'

type ReconcileFrame = DecodedFrame & { tag: typeof TAG.RECONCILE }
const reconcilesOn = (frames: readonly DecodedFrame[]): ReconcileFrame[] =>
  frames.filter((f): f is ReconcileFrame => f.tag === TAG.RECONCILE)

let harness: UpgradeHarness | null = null
afterEach(() => {
  harness?.dispose()
  harness = null
  vi.useRealTimers()
})

const barrierHarness = async (options: Record<string, unknown> = {}, channelIds = ['A']) => {
  harness = await createUpgradeHarness(channelIds, { barrierUpgrade: true, ...options })
  return harness
}

describe('F1 — held pre-flip frames obey the handoff cap and its fallback', () => {
  test('a held burst past the budget loses NOTHING: zero dispatched, cursor unmoved', async () => {
    // The reviewer's shape. `prepare: 'withhold'` holds the client in `preparing`, which is the
    // window in which `probe.onFrame` is live but the transport has not flipped — so everything
    // pushed on the WS below is HELD rather than delivered.
    const h = await barrierHarness({ prepare: 'withhold', barrier: 'refuse' })
    const channel = h.channels[0]!
    expect(h.upgradeTag()).toBe('preparing')

    const upgradeId = h.prepares[0]!.upgradeId
    // COMMITTED first, so nothing about this burst is a malformed-server story: it is the exact
    // ordering a fast committed server produces when FIN is delayed.
    h.ws.pushFrame(encode.reconciled({ ...reconciledPayload([{ ix: 0, lastSeq: 0 }], undefined, true), upgradeId }))
    // Past the frame budget. Without accounting these all survive to replay, where the cap trips
    // mid-loop and the frames after the trip escape into `dispatchFrame`.
    const total = UPGRADE_HANDOFF_BUFFER_FRAMES + 2
    for (let seq = 1; seq <= total; seq++) h.ws.pushFrame(encode.text(0, `"new-${seq}"`, seq))
    expect(channel.received).toHaveLength(0)

    await waitUntil(() => h.upgradeTag() === 'none', 'the over-budget attempt released itself', 6_000)

    // Not one new-wire frame may have been delivered. They are discarded as a SET — keeping any
    // suffix would be keeping exactly the frames whose seq is highest.
    expect(channel.received).toHaveLength(0)
    expect(channel.isClosed).toBe(false)
    // Nothing was emitted, so the old wire keeps its session: a pre-barrier failure.
    expect(h.barriers).toHaveLength(0)
    expect(h.sseConnects()).toBe(1)

    // THE oracle for permanent loss, and it needs no reconnect: `trackSeq` must not have moved, so
    // a frame at seq 1 is still deliverable. Had any held frame escaped, the cursor would sit at
    // 4098 and the server's replay of the whole discarded prefix would be dup-suppressed forever.
    h.sse.pushFrame(encode.text(0, '"replayed-1"', 1))
    await waitUntil(() => channel.received.length === 1, 'the discarded prefix is still replayable', 6_000)
    expect(channel.received).toEqual([{ kind: 'text', value: 'replayed-1' }])
  }, 20_000)

  test('control: a held burst UNDER the cap is delivered in full, in order', async () => {
    // Without this, "zero delivered" above is satisfied just as well by a client that drops every
    // held frame — which would be a different bug with the same assertion.
    const h = await barrierHarness({ prepare: 'withhold', barrier: 'refuse' })
    const channel = h.channels[0]!
    const upgradeId = h.prepares[0]!.upgradeId

    for (let seq = 1; seq <= 3; seq++) h.ws.pushFrame(encode.text(0, `"new-${seq}"`, seq))
    h.ws.pushFrame(encode.reconciled({ ...reconciledPayload([{ ix: 0, lastSeq: 0 }], undefined, true), upgradeId }))

    h.sendReady()
    await waitUntil(() => h.barriers.length > 0, 'barrier emitted')
    h.sse.pushFrame(encode.fin())
    await waitUntil(() => h.handoffDrained(), 'handoff settled', 6_000)

    expect(channel.received).toEqual([
      { kind: 'text', value: 'new-1' },
      { kind: 'text', value: 'new-2' },
      { kind: 'text', value: 'new-3' },
    ])
    expect(h.sseConnects()).toBe(1)
  }, 20_000)

  test('the bound applies to BYTES too, not only to a frame count', async () => {
    // The shared budget has two units and either alone is trivially evadable: a handful of large
    // frames never approaches 4096, and 4096 tiny ones never approach 8 MiB.
    const h = await barrierHarness({ prepare: 'withhold', barrier: 'refuse' })
    const big = `"${'x'.repeat(1_000_000)}"`
    for (let seq = 1; seq <= 12; seq++) h.ws.pushFrame(encode.text(0, big, seq))

    await waitUntil(() => h.upgradeTag() === 'none', 'the byte budget released the attempt', 6_000)
    expect(h.channels[0]!.received).toHaveLength(0)
    expect(h.barriers).toHaveLength(0)

    h.sse.pushFrame(encode.text(0, '"still-here"', 1))
    await waitUntil(() => h.channels[0]!.received.length === 1, 'the old wire is untouched')
  }, 20_000)

  test('control: a held burst just UNDER the frame budget still commits', async () => {
    // Pins the bound from the other side. Without it, "over budget aborts" is satisfied just as
    // well by a client that aborts on any held frame at all.
    const h = await barrierHarness({ prepare: 'withhold', barrier: 'refuse' })
    const channel = h.channels[0]!
    const upgradeId = h.prepares[0]!.upgradeId

    for (let seq = 1; seq <= UPGRADE_HANDOFF_BUFFER_FRAMES - 1; seq++) {
      h.ws.pushFrame(encode.text(0, `"new-${seq}"`, seq))
    }
    h.ws.pushFrame(encode.reconciled({ ...reconciledPayload([{ ix: 0, lastSeq: 0 }], undefined, true), upgradeId }))

    h.sendReady()
    await waitUntil(() => h.barriers.length > 0, 'barrier emitted', 6_000)
    h.sse.pushFrame(encode.fin())
    await waitUntil(() => h.handoffDrained(), 'handoff settled', 6_000)

    expect(channel.received).toHaveLength(UPGRADE_HANDOFF_BUFFER_FRAMES - 1)
    expect(channel.received.at(-1)).toEqual({ kind: 'text', value: `new-${UPGRADE_HANDOFF_BUFFER_FRAMES - 1}` })
    expect(h.sseConnects()).toBe(1)
  }, 20_000)
})

describe('F2 — the attempt deadline can interrupt batch-mode emission', () => {
  test('a prior POST hangs before the barrier: C2S traffic actually RESUMES on the wire', async () => {
    // ── The observer here is the whole point of this test ────────────────────────────────────
    // The obvious assertion — that `sendBuffer` drained — reads the queue ABOVE the transport and
    // proves only that frames changed hands. The hung POST still owns `SseTransport.flushing`, so
    // released frames land in the outbox and every later `flushOutbox` returns at that guard: the
    // connection reports itself healthy while the wire is permanently mute. So the oracle is
    // BELOW the transport — a POST body the mock server actually received.
    vi.useFakeTimers({ shouldAdvanceTime: true, toFake: ['setTimeout', 'clearTimeout', 'setInterval'] })
    const h = await barrierHarness({ batchMode: true, prepare: 'withhold', barrier: 'refuse', autoReconcile: true })
    expect(h.upgradeTag()).toBe('preparing')

    // A batch POST the server reads and then never answers. `flushing` stays true, so the barrier
    // can never be emitted: sending it alongside would put two POSTs on the wire with no defined
    // server-side dispatch order, which is the one thing emission must never do.
    h.setBatchPostsHang(true)
    h.send(0, '"stuck"')
    await waitUntil(() => h.batchPosts.length > 0, 'the batch POST is in flight', 6_000)
    const postsWhileWedged = h.batchPosts.length

    h.sendReady()
    await waitUntil(() => h.upgradeTag() === 'draining', 'READY gated the wire for emission', 6_000)
    h.send(0, '"gated"')
    expect(h.bufferedSendCount()).toBeGreaterThan(0)

    // The wire is answering again from here, so nothing but the wedge itself is under test.
    h.setBatchPostsHang(false)
    await vi.advanceTimersByTimeAsync(UPGRADE_ATTEMPT_TIMEOUT_MS + 500)

    // A wedged flush means that wire is de facto dead, so recovery is the ordinary transport-loss
    // path — the same one a dead wire gets, certified since T2.
    await waitUntil(() => h.sseConnects() === 2, 'the stalled wire was recovered by a reconnect', 8_000)

    // Not one barrier byte was written, so the classification stays PRE-barrier. The oracle for
    // "not sticky" is behavioural rather than a flag read: the settled RECONCILED on the fresh wire
    // is allowed to start ANOTHER attempt, which a sticky `upgradeDisabled` would have forbidden.
    expect(h.barriers).toHaveLength(0)
    await waitUntil(() => h.prepares.length === 2, 'upgrades are still enabled after the recovery', 8_000)

    // THE assertion this test exists for. Above the transport everything already looked fine before
    // the fix, so the oracle has to be a specific payload the mock server RECEIVED — not a POST
    // count, which the recovery's own reconnect POSTs would satisfy on their own.
    const textsOnWire = () =>
      h.batchPosts
        .flat()
        .filter((f): f is DecodedFrame & { tag: typeof TAG.TEXT } => f.tag === TAG.TEXT)
        .map((f) => f.text)
    expect(h.batchPosts.length).toBeGreaterThan(postsWhileWedged)
    h.send(0, '"after-recovery"')
    await waitUntil(
      () => textsOnWire().includes('"after-recovery"'),
      'a frame sent AFTER the deadline reached the server',
      8_000,
    )
    expect(h.channels[0]!.isClosed).toBe(false)

    // Order is preserved across the recovery. `"stuck"` reappears because the wedged POST swallowed
    // it and the certified replay path re-sent it from the server's reported `lastSeq`; `"gated"`
    // follows it rather than jumping the queue, because gated frames are released by the
    // post-RECONCILED drain instead of being carried in the reconnect's initial batch.
    const texts = textsOnWire()
    expect(texts).toContain('"stuck"')
    expect(texts.indexOf('"stuck"')).toBeLessThan(texts.indexOf('"gated"'))
    expect(texts.indexOf('"gated"')).toBeLessThan(texts.indexOf('"after-recovery"'))
  }, 30_000)

  test('control: a pre-barrier abort on an IDLE wire does not force a reconnect', async () => {
    // The discriminator for the recovery above. A wedged flush is what makes the wire unusable;
    // an attempt that ends with nothing in flight must leave the connection exactly where it was,
    // or every ordinary pre-barrier timeout would cost a needless reconnect and replay.
    vi.useFakeTimers({ shouldAdvanceTime: true, toFake: ['setTimeout', 'clearTimeout', 'setInterval'] })
    const h = await barrierHarness({ batchMode: true, prepare: 'withhold', barrier: 'refuse' })
    expect(h.upgradeTag()).toBe('preparing')

    // No POST in flight, and READY never arrives — so the deadline fires while the emitter has not
    // even been reached.
    await vi.advanceTimersByTimeAsync(UPGRADE_ATTEMPT_TIMEOUT_MS + 500)
    await waitUntil(() => h.upgradeTag() === 'none', 'the attempt deadline released the attempt', 6_000)

    expect(h.barriers).toHaveLength(0)
    expect(h.sseConnects()).toBe(1)
    expect(h.channels[0]!.isClosed).toBe(false)

    // Still the same live wire, still carrying traffic in both directions.
    const before = h.batchPosts.length
    h.send(0, '"idle-abort"')
    await waitUntil(() => h.batchPosts.length > before, 'the untouched wire still carries sends', 6_000)
    h.sse.pushFrame(encode.text(0, '"downstream"', 1))
    await waitUntil(() => h.channels[0]!.received.length === 1, 'and still delivers downstream')
  }, 30_000)

  test('the barrier POST itself hangs: sticky post-barrier fallback, not a generic reconnect', async () => {
    vi.useFakeTimers({ shouldAdvanceTime: true, toFake: ['setTimeout', 'clearTimeout', 'setInterval'] })
    const h = await barrierHarness({ batchMode: true, prepare: 'withhold', barrier: 'refuse', autoReconcile: true })
    expect(h.upgradeTag()).toBe('preparing')

    // Nothing in flight, so emission reaches its own POST — which the server reads (the barrier IS
    // recorded) and then never answers.
    h.setBatchPostsHang(true)
    h.sendReady()
    await waitUntil(() => h.barriers.length > 0, 'the barrier reached the server and the POST hung', 6_000)

    // The barrier MAY have committed, so no wire can be trusted to hold the session. The deadline
    // must reach this before the reconcile timer armed at barrier-build time, or the generic
    // reconnect takes over and upgrades are never disabled.
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
    // Proves the two rows above fail for the reason claimed — batch mode itself is a working path,
    // and the barrier really is emitted through a quiesce-then-POST sequence.
    const h = await barrierHarness({ batchMode: true })
    await waitUntil(() => h.handoffDrained(), 'the batch-mode barrier upgrade committed', 10_000)

    expect(h.barriers).toHaveLength(1)
    expect(h.sseConnects()).toBe(1)
    expect(reconcilesOn(h.ws.sent)).toHaveLength(0)
  }, 20_000)
})
