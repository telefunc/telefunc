// The three positive controls, each now FLIPPED to assert its fix. Each originally asserted a real
// defect; the original assertions are preserved verbatim in comments, because they are the only
// artifact proving the design was necessary rather than merely plausible.
//
// Every one is paired with an instrument that CAN disagree — same path, same frames, one variable
// changed — so a "not lost" assertion can never pass against a harness that delivers nothing.

import { afterEach, describe, expect, test } from 'vitest'
import { stringify } from '@brillout/json-serializer/stringify'

import { isAbort } from '../shared/Abort.js'
import { ReplayBuffer } from './replay-buffer.js'
import { CHANNEL_SERVER_REPLAY_BUFFER_BYTES } from './constants.js'
import { TAG, decode, encode } from './shared-ws.js'
import { createMuxHarness, prepareFrame, reconcileFrame, settle, textFrame } from './upgrade-mux-harness.js'
import { createUpgradeHarness, waitUntil, type UpgradeHarness } from './upgrade-client-harness.js'

// PC1 — a C2S frame in flight on the old wire, lost once the upgrade rotated the session:
// `setSessionId` wrote the new id onto the NEW connection only, so the old one kept reporting the
// dead id and the routing lookup short-circuited on `?.` for EVERY ix — no throw, no counter, no
// log. The drop site still exists; the barrier is a frame on the OLD WIRE'S OWN recv chain, so
// nothing legitimate reaches it any more. Oracle: the real `ServerChannel.listen()` payload log —
// `_lastClientSeq` is the same bookkeeping the replay decision uses, so it would be circular.

let muxHarness: ReturnType<typeof createMuxHarness> | null = null
afterEach(() => {
  muxHarness?.dispose()
  muxHarness = null
})

async function connectSse(h: ReturnType<typeof createMuxHarness>) {
  const chA = h.registerChannel('A')
  await h.sse.deliver(reconcileFrame({ open: [{ id: 'A', ix: 0, lastSeq: 0, initial: true }] }))
  const s0 = h.sse.sessionId()
  expect(s0).toBeTypeOf('string')
  // Sentinel: this wire delivers. Without it every "not received" assertion below would pass just as
  // happily against a listener that was never wired up.
  await h.sse.deliver(textFrame(0, 1, 111))
  expect(chA.received).toEqual([111])
  return { chA, s0: s0! }
}

describe('PC1 — an old-wire C2S frame in flight at commit time', () => {
  // The instrument that can disagree, at the exact assertion site the flip below uses. Same wire,
  // same ix, same seq, same code path — the ONLY difference is that nothing rotated the session.
  test('control: with no upgrade the same old-wire frame IS delivered', async () => {
    const h = (muxHarness = createMuxHarness())
    const { chA } = await connectSse(h)

    await h.sse.deliver(textFrame(0, 2, 222))

    expect(chA.received).toEqual([111, 222])
  })

  test('FIXED: the frame is delivered, not dropped', async () => {
    const h = (muxHarness = createMuxHarness())
    const { chA, s0 } = await connectSse(h)

    // Stage on the probe wire. Deliberately inert: no rotation, no attach, nothing on the old wire.
    await h.ws.deliver(prepareFrame({ upgradeId: 'upg-1', sessionId: s0 }))
    expect(h.sse.sessionId()).toBe(s0)
    expect(h.ws.sessionId()).toBeUndefined()

    // The in-flight old-wire frame, then the barrier behind it — the exact ordering the client
    // produces, since the barrier is appended to the same upload body.
    const inflight = h.sse.deliver(textFrame(0, 2, 222))
    const commit = h.sse.deliver(
      reconcileFrame({ sessionId: s0, barrier: true, upgradeId: 'upg-1', open: [{ id: 'A', ix: 0, lastSeq: 2 }] }),
    )
    await Promise.all([inflight, commit])
    await settle()

    expect(chA.received).toEqual([111, 222]) // ← THE FIX: 222 survives the rotation
    //
    // ── PRESERVED, from before the flip. Under the pre-barrier upgrade reconcile these all held:
    //
    //     await h.ws.deliver(reconcileFrame({ sessionId: s0, open: [{ id: 'A', ix: 0, lastSeq: 1 }] }))
    //     expect(h.sse.sessionId()).toBe(s0)      // the old wire keeps reporting the dead id
    //     await h.sse.deliver(textFrame(0, 2, 222))
    //     expect(chA.received).toEqual([111])     // ← THE DEFECT: 222 is gone
    //     expect(h.sse.terminated()).toBe(false)  // ...and gone silently

    // The commit really did happen — otherwise "222 was delivered" would be trivially true of a
    // server that simply never upgraded.
    expect(h.ws.sessionId()).toBeTypeOf('string')
    expect(h.ws.sessionId()).not.toBe(s0)
    expect(h.ws.sent.filter((f) => f.tag === TAG.RECONCILED)).toHaveLength(1)
    expect(h.sse.sent.filter((f) => f.tag === TAG.FIN)).toHaveLength(1)

    // Counter-sentinel: the channel routes on the wire that now owns the session, so the assertion
    // above is about ordering and not about a channel that happens to accept everything.
    await h.ws.deliver(textFrame(0, 3, 333))
    expect(chA.received).toEqual([111, 222, 333])
  })
})

// PC2 — a S2C frame in flight on the OLD wire, lost when a NEWER frame arrived first on the new
// one, because the handoff kept ONE arrival-ordered buffer for both sources. The barrier does not
// fix this (it is a C2S cut); the buffer is partitioned by source wire and drains old-before-new.
// Oracle: the channel's `_dispatchFrame`, BELOW the `trackSeq` dedup that does the dropping.

let clientHarness: UpgradeHarness | null = null
afterEach(() => {
  clientHarness?.dispose()
  clientHarness = null
})

/** `barrier: 'refuse'` makes the mock server say nothing, so the test owns the exact interleaving
 *  of the two join limbs — which is the whole subject here. */
const handoffHarness = async (channelIds = ['A']) =>
  (clientHarness = await createUpgradeHarness(channelIds, { barrier: 'refuse' }))

/** Frame N: a TEXT frame on the old wire. Oversized on the SERVER's replay buffer, which is what
 *  gaps its lane — its size is irrelevant to the client, which never sees that buffer. */
const frameN = (seq: number) => encode.text(0, stringify(`old-wire-${seq}`), seq)
/** Frame N+1: a BINARY frame, i.e. the OTHER lane — the reason it replays past the gap. */
const frameNPlus1 = (seq: number) => encode.binary(0, new Uint8Array([1, 2, 3]), seq)

/** Finish the handoff. FIN goes LAST on the SSE stream, so every old-wire frame pushed before it is
 *  necessarily already buffered when the drain runs (same-wire FIFO). */
async function completeHandoff(h: UpgradeHarness) {
  h.ws.pushFrame(h.committedFrame())
  h.sse.pushFrame(encode.fin())
  await waitUntil(() => h.handoffDrained(), 'handoff completed and buffer drained')
}

describe('PC2 — S2C loss across the upgrade handoff', () => {
  // Reachability, on the REAL server-side buffer. Without this the client-side flip would be
  // demonstrating a loss for an arrival order the server could never actually produce.
  test('reachability: a gapped text lane still lets the binary lane replay past it', () => {
    const replay = new ReplayBuffer(CHANNEL_SERVER_REPLAY_BUFFER_BYTES, 60_000, 2 * 1024 * 1024)

    // Frame 1: TEXT, oversized ⇒ stored as a gap marker, not as a frame.
    const oversized = encode.text(0, stringify('x'.repeat(CHANNEL_SERVER_REPLAY_BUFFER_BYTES + 1)), 1)
    expect(replay.push(1, oversized, false)).toBe(false)
    expect(replay.push(2, encode.binary(0, new Uint8Array([1, 2, 3]), 2), true)).toBe(true)

    // Control: within a single lane the gap DOES hold replay back, so the lane merge is what leaks.
    const textOnly = new ReplayBuffer(CHANNEL_SERVER_REPLAY_BUFFER_BYTES, 60_000, 2 * 1024 * 1024)
    textOnly.push(1, oversized, false)
    textOnly.push(2, encode.text(0, stringify('two'), 2), false)
    expect(textOnly.getAfter(0)).toHaveLength(0)

    // The defect: seq 2 replays while seq 1 is a hole the client is never told about.
    const replayed = replay.getAfter(0).map((frame) => decode(frame))
    expect(replayed).toHaveLength(1)
    expect(replayed[0]).toMatchObject({ tag: TAG.BINARY, seq: 2 })
  })

  // The instrument that names the fix: it is the ORDER that kills frame N, not the dedup itself.
  // Both frames on the same wire arrive in seq order and both survive — exactly the state R1's
  // "drain the entire old buffer first" restores.
  test('control: N before N+1 in the buffer delivers both', async () => {
    const h = await handoffHarness()

    h.sse.pushFrame(frameN(1))
    h.sse.pushFrame(frameNPlus1(2))
    await completeHandoff(h)

    expect(h.channels[0]!.received).toEqual([
      { kind: 'text', value: 'old-wire-1' },
      { kind: 'binary', bytes: new Uint8Array([1, 2, 3]) },
    ])
  })

  test('FIXED: an old-wire frame in flight survives a newer new-wire frame arriving first', async () => {
    const h = await handoffHarness()

    // The replayed N+1 lands on the new WS wire. `socket.emit` is synchronous through
    // `_onTransportFrame`, so it is in the buffer before anything the SSE stream yields.
    h.ws.pushFrame(frameNPlus1(2))
    // N is still in flight on the old SSE wire and arrives second.
    h.sse.pushFrame(frameN(1))
    await completeHandoff(h)

    // ── PRESERVED, from before R1: this asserted the LOSS —
    //     expect(h.channels[0]!.received).toEqual([{ kind: 'binary', bytes: new Uint8Array([1, 2, 3]) }])
    // i.e. N+1 arrived and N did not, with no error, no counter, nothing to alert on.
    //
    // Arrival order was N+1 then N; DELIVERY order is N then N+1, because the old buffer drains in
    // full first. So `trackSeq` sees 1 before 2 and neither is dup-dropped.
    expect(h.channels[0]!.received).toEqual([
      { kind: 'text', value: 'old-wire-1' },
      { kind: 'binary', bytes: new Uint8Array([1, 2, 3]) },
    ])
    expect(h.channels[0]!.closeErrors).toEqual([])
    expect(h.channels[0]!.isClosed).toBe(false)
  })
})

// PC3 — the settling RECONCILED omitted a channel whose ABORT was still in the old wire's handoff
// buffer, so `applyReconciled` released it with a manufactured reconnect error and the server's real
// abort VALUE died in `closeRemoteChannel`'s first-line no-op. This loss class is special: an abort
// value is TERMINAL and NON-REPLAYABLE, where every other buffered frame class is replayable (data),
// reconstructible (CLOSE) or intentionally ephemeral (WINDOW). The fix DEFERS omitted channels while
// a handoff settles. Oracle: the error object handed to `_onTransportClose` — what the application's
// `onClose` actually receives, not connection-internal release bookkeeping.

/** The value only the server knows. If this never reaches the application, it is gone. */
const ABORT_VALUE = { reason: 'quota-exhausted', retryAfter: 42 }

/** `acknowledged` selects the ONE difference between the control and the defect: whether the
 *  settling RECONCILED still lists ix 0. ABORT then FIN on the old wire mirrors the real order. */
async function runHandoffWithBufferedAbort(h: UpgradeHarness, acknowledged: boolean) {
  h.sse.pushFrame(encode.abort(0, stringify(ABORT_VALUE)))
  h.ws.pushFrame(h.committedFrame(acknowledged ? [{ ix: 0, lastSeq: 0 }] : []))
  h.sse.pushFrame(encode.fin())
  await waitUntil(() => h.handoffDrained(), 'handoff completed and buffer drained')
}

describe('PC3 — an abort value swallowed by a reconcile that omits the channel', () => {
  // The instrument that can disagree. Identical path, identical buffered ABORT — the only difference
  // is that the RECONCILED still acknowledges ix 0, so the release does not race the drain.
  test('control: when the reconcile still lists the channel, the true abort value is delivered', async () => {
    const h = await handoffHarness()

    await runHandoffWithBufferedAbort(h, /* acknowledged */ true)

    const errors = h.channels[0]!.closeErrors
    expect(errors).toHaveLength(1)
    expect(isAbort(errors[0])).toBe(true)
    expect((errors[0] as { abortValue?: unknown }).abortValue).toEqual(ABORT_VALUE)
  })

  test('FIXED: when the reconcile omits the channel, the buffered abort still wins with its true value', async () => {
    const h = await handoffHarness()

    await runHandoffWithBufferedAbort(h, /* acknowledged */ false)

    // ── PRESERVED, from before R4: this asserted the SWALLOW —
    //     expect(isAbort(errors[0])).toBe(false)
    //     expect(errors[0]?.message).toBe('Channel not acknowledged by server after reconnect')
    //     expect((errors[0] as { abortValue?: unknown }).abortValue).toBeUndefined()
    const errors = h.channels[0]!.closeErrors
    // Still exactly one close — the deferred generic release finds the entry already gone and skips
    // it, so the application is not closed twice.
    expect(errors).toHaveLength(1)
    expect(isAbort(errors[0])).toBe(true)
    expect((errors[0] as { abortValue?: unknown }).abortValue).toEqual(ABORT_VALUE)
  })

  // The OTHER half of the split settlement, and not optional: deferring a release is only safe if
  // something eventually performs it. Without this, "never release at all" passes both rows above.
  test('an omitted channel with no buffered terminal frame still gets the generic release', async () => {
    const h = await handoffHarness()

    h.ws.pushFrame(h.committedFrame([]))
    h.sse.pushFrame(encode.fin())
    await waitUntil(() => h.handoffDrained(), 'handoff completed and buffer drained')

    const errors = h.channels[0]!.closeErrors
    expect(errors).toHaveLength(1)
    expect(isAbort(errors[0])).toBe(false)
    expect(errors[0]?.message).toBe('Channel not acknowledged by server after reconnect')
    expect(h.channels[0]!.isClosed).toBe(true)
  })
})
