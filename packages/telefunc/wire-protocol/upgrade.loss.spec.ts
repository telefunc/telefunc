// PC1 — POSITIVE CONTROL, NOW INVERTED. This file first asserted the defect; it now asserts the fix.
//
// ── Disposition ──────────────────────────────────────────────────────────────────────────────
// Flipped in place by `feat(wire-protocol): commit staged upgrades on the old wire's barrier`
// (T4). The original loss assertions are preserved verbatim as comments inside the final test
// below — they are the only artifact proving the design was necessary, so they are commented, not
// deleted. Everything above them describes the code as it was when the loss was real.
//
// What changed: the upgrade's session rotation is no longer driven by a frame on the NEW wire,
// racing whatever is still queued on the old one. It is driven by the BARRIER — a frame on the OLD
// wire's own recv chain — so every earlier old-wire frame is ordered before the rotation by
// construction. The drop site is unchanged and still reachable; nothing legitimate reaches it.
//
// ── The original defect, for the record ──────────────────────────────────────────────────────
// A client→server frame the client emitted on the old SSE wire before the upgrade was silently
// dropped once the WS upgrade reconcile had rotated the session.
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
// That analysis is what shaped the tests below. The park is retained as a HARNESS CAPABILITY GATE
// (first test) rather than as the expression of the loss, and the flip is asserted on the ordering
// the barrier actually creates: an in-flight old-wire frame ahead of the commit on the same chain.

import { afterEach, describe, expect, test } from 'vitest'

import { createMuxHarness, prepareFrame, reconcileFrame, settle, textFrame } from './upgrade-mux-harness.js'
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

/** The barrier flow's two frames: stage on the probe wire, then commit from the OLD wire. */
const prepareFor = (s0: string) => prepareFrame({ upgradeId: 'upg-1', sessionId: s0, open: [{ id: 'A', ix: 0 }] })
const barrierFor = (s0: string) =>
  reconcileFrame({
    sessionId: s0,
    upgrade: true,
    barrier: true,
    upgradeId: 'upg-1',
    open: [{ id: 'A', ix: 0, lastSeq: 2 }],
  })

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

  // THE INVERTED CONTROL. Same frame, same ix, same seq, same drop site — the ONLY thing that
  // changed is which wire drives the rotation. Under the barrier flow the rotation is caused by a
  // frame ON THE OLD WIRE'S OWN CHAIN, so an earlier old-wire frame cannot be overtaken by it.
  test('FIXED: an old-wire frame in flight at commit time is delivered, not dropped', async () => {
    const h = (harness = createMuxHarness())
    const { chA, s0 } = await connectSse(h)

    // Stage on the probe wire. Deliberately inert: no rotation, no attach, nothing on the old wire.
    await h.ws.deliver(prepareFor(s0))
    expect(h.sse.sessionId()).toBe(s0)
    expect(h.ws.sessionId()).toBeUndefined()

    // The in-flight old-wire frame, and then the barrier behind it — the exact ordering the client
    // produces, since the barrier is appended to the same upload body.
    const inflight = h.sse.deliver(textFrame(0, 2, 222))
    const commit = h.sse.deliver(barrierFor(s0))
    await Promise.all([inflight, commit])
    await settle()

    expect(chA.received).toEqual([111, 222]) // ← THE FIX: 222 survives the rotation
    //
    // ── PRESERVED, from before the flip. These are the assertions that proved the defect was real
    // ── on the pre-barrier code, and they are kept verbatim rather than deleted because they are
    // ── the only artifact showing the design was necessary rather than merely plausible. Under the
    // ── legacy (non-barrier) upgrade reconcile they all held:
    //
    //     await h.ws.deliver(upgradeReconcile(s0))
    //     expect(h.sse.sessionId()).toBe(s0)      // the old wire keeps reporting the dead id
    //     await h.sse.deliver(textFrame(0, 2, 222))
    //     expect(chA.received).toEqual([111])     // ← THE DEFECT: 222 is gone
    //     expect(h.sse.terminated()).toBe(false)  // ...and gone silently
    //     expect(h.ws.terminated()).toBe(false)
    //
    // The legacy path is untouched by T4 and still behaves exactly that way; `upgradeReconcile` is
    // still exercised by the control below. What T4 adds is a path that does not have to.

    // The commit really did happen — otherwise "222 was delivered" would be trivially true of a
    // server that simply never upgraded.
    expect(h.ws.sessionId()).toBeTypeOf('string')
    expect(h.ws.sessionId()).not.toBe(s0)
    expect(h.ws.sent.filter((f) => f.tag === TAG.RECONCILED)).toHaveLength(1)
    expect(h.sse.sent.filter((f) => f.tag === TAG.FIN)).toHaveLength(1)

    // Counter-sentinel, unchanged in purpose: the channel routes on the wire that now owns the
    // session, so the assertion above is about ordering and not about a channel that happens to
    // accept everything.
    await h.ws.deliver(textFrame(0, 3, 333))
    expect(chA.received).toEqual([111, 222, 333])
  })

  // The legacy flow, still reachable and still lossy, asserted so the flip above cannot be mistaken
  // for "the drop site was removed". It was not: it is still there, and the barrier is what keeps
  // legitimate traffic away from it.
  test('the legacy upgrade reconcile still loses the frame — the drop site is unchanged', async () => {
    const h = (harness = createMuxHarness())
    const { chA, s0 } = await connectSse(h)

    await h.ws.deliver(upgradeReconcile(s0))
    expect(h.sse.sessionId()).toBe(s0)

    await h.sse.deliver(textFrame(0, 2, 222))

    expect(chA.received).toEqual([111])
    expect(h.sse.terminated()).toBe(false)
  })
})
