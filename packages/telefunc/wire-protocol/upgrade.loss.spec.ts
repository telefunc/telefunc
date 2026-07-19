// PC1 — POSITIVE CONTROL. Asserts the CURRENT (defective) behaviour: a client→server frame the
// client emitted on the old SSE wire before the upgrade is silently dropped once the WS upgrade
// reconcile has rotated the session.
//
// Mechanism (`mux.ts`): `reconcileSession` removes the previous session (`:305`) and
// `transport.setSessionId` (`:283`) writes the NEW id onto the NEW connection only. The old SSE
// connection therefore keeps reporting the dead id at `:245`, so the routing lookup at `:251`
//
//     this.sessions.get(sessionId, index)?.channel._dispatchFrame(...)
//
// short-circuits on `?.` for EVERY ix — no throw, no counter, no log. That `?.` exists for a real
// case (client closed a channel, server reconciled it out, a frame was still in flight) and it
// swallows this one for free.
//
// Oracle discipline: delivery is read ONLY from the real `ServerChannel.listen()` payload log.
// `_lastClientSeq` and the RECONCILED `open[].lastSeq` are the same bookkeeping the replay
// decision uses, so asserting on them would be circular.
//
// ── On the parking mechanism, and why it is NOT what expresses this loss ──────────────────────
// The plan proposed parking the in-flight frame behind a prior `initial: true` reconcile held in
// `waitForChannelRegistration`. That park is real and is exercised below — but measurement shows
// it cannot produce THIS loss, for a structural reason: `reconcile` is the only awaitable
// `handleFrame` can return (`mux.ts:239`; every other branch returns `null`), and every
// non-throwing `reconcile` ends by calling `setSessionId` on its own connection (`:283`). So a
// parked SSE chain always re-establishes an SSE session before the frame behind it gets its turn,
// and the frame is delivered. Measured: park + WS rotation, no close ⇒ received `[111, 222]`.
//
// Making the parked variant "lose" the frame requires additionally killing the SSE wire, which
// diverts `reconcile` into the closed-during-await branch (`:274-279`) so `setSessionId` never
// runs. That interleaving DOES lose the frame — but it loses it with the WS rotation removed too
// (measured: `[111]`), because `reconcileSession` already dropped the old session at `:305`. It is
// therefore a different latent defect wearing PC1's clothes, and asserting it here would be a
// textbook wrong-reason green. It is reported separately rather than smuggled in as coverage.
//
// What this file establishes instead is the precondition that actually matters: at the moment the
// old-wire frame is routed, the rotation has already happened and the old connection is orphaned.
// That is asserted, not assumed — and the paired no-rotation control below is the instrument that
// can disagree.
//
// Disposition: T2 rewrites the loss assertions in place to assert delivery. Do not delete — they
// are the only artifact proving the fix was necessary.

import { afterEach, describe, expect, test } from 'vitest'

import { createMuxHarness, reconcileFrame, settle, textFrame } from './upgrade-mux-harness.js'
import { TAG } from './shared-ws.js'

let harness: ReturnType<typeof createMuxHarness> | null = null
afterEach(() => {
  harness?.dispose()
  harness = null
})

/** First reconcile on the SSE wire: attaches channel A at ix 0 and mints session S0. */
async function connectSse(h: ReturnType<typeof createMuxHarness>) {
  const chA = h.registerChannel('A')
  await h.sse.deliver(reconcileFrame({ open: [{ id: 'A', ix: 0, lastSeq: 0, initial: true }] }))
  const s0 = h.sse.sessionId()
  expect(s0).toBeTypeOf('string')
  // Sentinel: this wire delivers. Without it every "not received" assertion below would pass just
  // as happily against a listener that was never wired up.
  await h.sse.deliver(textFrame(0, 1, 111))
  expect(chA.received).toEqual([111])
  return { chA, s0: s0! }
}

const upgradeReconcile = (s0: string) =>
  reconcileFrame({ sessionId: s0, upgrade: true, open: [{ id: 'A', ix: 0, lastSeq: 1 }] })

describe('PC1 — old-wire C2S frame after upgrade session rotation', () => {
  // Harness capability gate, required before any interleaving assertion is worth anything: the
  // driver must be able to hold a frame IN the recv chain and observe that it has not been
  // dispatched. Without a genuine deferral the chain drains in a microtask and every ordering test
  // degenerates into "frames arrive in the order I sent them".
  //
  // The only genuinely awaited seam reachable from `handleFrame` is `attach`'s
  // `waitForChannelRegistration` (`mux.ts:318-327`). Slow `listen()` callbacks park NOTHING:
  // `_dispatchFrame` is `void` and `_onPeerMessage` fire-and-forgets listener promises
  // (`channel.ts:453-455`), so the chain turn resolves immediately regardless.
  test('harness: a frame behind a parked reconcile is genuinely undispatched at assertion time', async () => {
    const h = (harness = createMuxHarness())
    const { chA, s0 } = await connectSse(h)

    const chB = h.createChannel('B') // built but NOT registered → the next reconcile parks
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
    // The park is real: the reconcile has not answered yet (only the first RECONCILED is on the wire).
    expect(h.sse.sent.filter((f) => f.tag === TAG.RECONCILED)).toHaveLength(1)

    const inflight = h.sse.deliver(textFrame(0, 2, 222))
    await settle()
    expect(chA.received).toEqual([111]) // 222 is sitting IN the chain — not before it, not after it

    h.register(chB) // fires the registration waiters synchronously
    await Promise.all([parked, inflight])
    await settle()
    expect(chA.received).toEqual([111, 222]) // and it drains once the park releases
  })

  // The instrument that can disagree, at the exact assertion site the control below uses. Same
  // wire, same ix, same seq, same code path — the ONLY difference is that no upgrade reconcile
  // rotated the session. If this ever goes red, the control below is proving nothing.
  test('control: with no upgrade rotation the same old-wire frame IS delivered', async () => {
    const h = (harness = createMuxHarness())
    const { chA } = await connectSse(h)

    await h.sse.deliver(textFrame(0, 2, 222))

    expect(chA.received).toEqual([111, 222])
  })

  // THE POSITIVE CONTROL. The frame is one the client legitimately emitted on the old wire before
  // the upgrade; the server simply had not routed it yet when the WS reconcile landed. Nothing in
  // today's code orders the two wires' recv chains against each other, so this interleaving is
  // reachable — that absent happens-before edge is exactly what the barrier design exists to create.
  test('CURRENT: an old-wire frame is silently dropped after the upgrade rotation', async () => {
    const h = (harness = createMuxHarness())
    const { chA, s0 } = await connectSse(h)

    // The client's WS upgrade reconcile. Rotates S0 → S1 against the WS connection.
    await h.ws.deliver(upgradeReconcile(s0))
    const s1 = h.ws.sessionId()
    expect(s1).toBeTypeOf('string')
    expect(s1).not.toBe(s0)
    // THE PRECONDITION, asserted rather than assumed: at routing time the old connection still
    // reports the rotated-away id. Read from the same accessor `handleFrame` reads at `mux.ts:245`.
    expect(h.sse.sessionId()).toBe(s0)

    // The in-flight old-wire frame finally reaches the mux.
    await h.sse.deliver(textFrame(0, 2, 222))

    expect(chA.received).toEqual([111]) // ← THE DEFECT: 222 is gone
    // ...and gone silently: nothing terminated, nothing reported, nothing to alert on.
    expect(h.sse.terminated()).toBe(false)
    expect(h.ws.terminated()).toBe(false)

    // Counter-sentinel. The channel is alive and its listener still fires — the SAME payload on the
    // SAME ix arrives when it comes in on the wire that owns the live session. So the loss is caused
    // by the rotation orphaning the old connection, not by a dead channel, a dead listener, or dedup.
    await h.ws.deliver(textFrame(0, 3, 333))
    expect(chA.received).toEqual([111, 333])
  })
})
