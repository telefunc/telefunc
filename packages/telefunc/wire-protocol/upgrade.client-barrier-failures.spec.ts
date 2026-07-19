// T5 — the CLIENT's failure rows (I8) for the barrier upgrade, plus the `upgradeDisabled`
// stickiness assertion T2 declared and deferred here.
//
// ── The single rule these tests are pinning ──────────────────────────────────────────────────
// PRE-barrier, nothing the client sent can rotate the server's session, so a failure costs the
// attempt and nothing else: the old SSE wire keeps its session and keeps delivering. ONCE THE
// BARRIER MAY HAVE LEFT, no wire can be trusted to hold the session, so both are abandoned for one
// fresh reconcile and upgrades are disabled for good. Every test below asserts which side of that
// line it is on — `sseConnects()` is the discriminator (1 = old wire survived, 2 = fell back).
//
// ── Why the deadline row needs a clock and the others do not ─────────────────────────────────
// A stale or refused barrier is refused SILENTLY server-side: no COMMITTED, no FIN, no
// termination, nothing rotates. There is no wire event to wait for, which is exactly why the
// client-side attempt deadline is the only thing that can end such an attempt — and why the test
// for it has to move a clock rather than push a frame. Timers are faked but `Date.now` is NOT, so
// the stream machinery and the condition-polling below keep running on real time.

import { afterEach, describe, expect, test, vi } from 'vitest'

import { createUpgradeHarness, waitUntil, type UpgradeHarness } from './upgrade-client-harness.js'
import { UPGRADE_ATTEMPT_TIMEOUT_MS, UPGRADE_HANDOFF_JOIN_TIMEOUT_MS } from './constants.js'
import { TAG, encode } from './shared-ws.js'

/** `WebSocket.CLOSED`. The stub carries it as a static, but the harness exports the class as a
 *  type only, so the wire value is used directly. */
const WS_CLOSED = 3

/** The join deadline is 2 s and the next-slowest bound that could produce the same fallback is
 *  `RECONCILE_TIMEOUT_MS` at 10 s, so a budget in between is what separates them. Falling back
 *  inside it is the ONLY observable the two limbs have: `abortUpgradeAndReconnectSse` reconnects
 *  rather than closing channels, so the limb-naming error never reaches any test surface. */
const JOIN_BUDGET_MS = UPGRADE_HANDOFF_JOIN_TIMEOUT_MS + 3_000

let harness: UpgradeHarness | null = null
afterEach(() => {
  harness?.dispose()
  harness = null
  vi.useRealTimers()
})

const barrierHarness = async (options: Record<string, unknown> = {}) => {
  harness = await createUpgradeHarness(['A'], { barrierUpgrade: true, ...options })
  return harness
}

describe('I8 — the attempt deadline is the only abort path when the server says nothing', () => {
  test('READY never arrives: the deadline ends the attempt and the old wire is untouched', async () => {
    // `shouldAdvanceTime` keeps the SSE stream and the pollers running on real time; the jump below
    // is what reaches the deadline without the test taking ten seconds.
    vi.useFakeTimers({ shouldAdvanceTime: true, toFake: ['setTimeout', 'clearTimeout', 'setInterval'] })
    const h = await barrierHarness({ prepare: 'withhold' })

    expect(h.upgradeTag()).toBe('preparing')
    // Ungated by design: `preparing` is not `draining`, so the old wire keeps carrying user sends
    // for the whole staging window.
    h.sse.pushFrame(encode.text(0, '"during-staging"', 1))
    await waitUntil(() => h.channels[0]!.received.length === 1, 'the old wire still delivers while staged')

    vi.advanceTimersByTime(UPGRADE_ATTEMPT_TIMEOUT_MS + 100)
    await waitUntil(() => h.upgradeTag() === 'none', 'the attempt deadline released the attempt')

    // Pre-barrier: no fallback, no fresh SSE, nothing closed.
    expect(h.sseConnects()).toBe(1)
    expect(h.channels[0]!.isClosed).toBe(false)
    expect(h.barriers).toHaveLength(0)
    // The probe is let go with the attempt — otherwise a socket the server has staged and will
    // never commit stays alive on PINGs alone.
    expect(h.sockets[0]!.readyState).toBe(WS_CLOSED)

    // And the old wire is genuinely still live, not merely un-torn-down.
    h.sse.pushFrame(encode.text(0, '"after-deadline"', 2))
    await waitUntil(() => h.channels[0]!.received.length === 2, 'the old wire delivers after the deadline')
  })

  test('control: the same attempt commits when READY DOES arrive', async () => {
    // Without this, the test above passes just as well against a client that never gets past
    // `preparing` for some entirely different reason.
    const h = await barrierHarness({ prepare: 'withhold' })
    expect(h.upgradeTag()).toBe('preparing')
    h.sendReady()
    await waitUntil(() => h.barriers.length > 0, 'READY licensed the barrier')
  })
})

describe('I8 — once the barrier may have left, both wires are abandoned', () => {
  test('the SSE dies after the barrier: one fresh reconcile, and the in-flight frame survives', async () => {
    const h = await barrierHarness({ barrier: 'refuse' })
    expect(h.inHandoff()).toBe(true)

    // Received on the old wire but not yet dispatched — terminal old-wire frames carry no seq and
    // no replay can reproduce them, so the fallback must drain what it already has.
    h.sse.pushFrame(encode.text(0, '"pre-death"', 1))
    await waitUntil(() => (h.handoffBuffered()?.frames ?? 0) === 1, 'the frame is held in the handoff buffer')

    h.sse.close()
    await waitUntil(() => h.sseConnects() === 2, 'client fell back to a fresh SSE')

    expect(h.channels[0]!.received).toEqual([{ kind: 'text', value: 'pre-death' }])
    expect(h.channels[0]!.isClosed).toBe(false)
  })

  test('COMMITTED never arrives: the join deadline forces the fallback', async () => {
    const h = await barrierHarness({ barrier: 'fin-only' })

    await waitUntil(() => h.sseConnects() === 2, 'the join deadline forced the fallback', JOIN_BUDGET_MS)
    expect(h.channels[0]!.isClosed).toBe(false)
  }, 10_000)

  test('FIN never arrives: the OTHER limb of the same join is bounded too', async () => {
    // Arming the deadline at handoff ENTRY rather than on FIN arrival is what makes this limb
    // bounded at all: with the reconciled already settled there is no `reconcileTimer` either, so
    // nothing else in the system is watching.
    const h = await barrierHarness({ barrier: 'committed-only' })

    await waitUntil(() => h.sseConnects() === 2, 'the join deadline forced the fallback', JOIN_BUDGET_MS)
    expect(h.channels[0]!.isClosed).toBe(false)
  }, 10_000)

  test('a silently refused barrier still ends: neither limb ever arrives', async () => {
    // The exact server behaviour a stale barrier gets — no COMMITTED, no FIN, no termination,
    // nothing rotated. On the client that is indistinguishable from a server that simply stopped.
    const h = await barrierHarness({ barrier: 'refuse' })

    await waitUntil(() => h.sseConnects() === 2, 'the refused barrier eventually fell back', JOIN_BUDGET_MS)
    expect(h.channels[0]!.isClosed).toBe(false)
  }, 10_000)

  test('control: a handoff that receives BOTH limbs never falls back', async () => {
    // Without this, every `sseConnects() === 2` above is satisfied just as well by a client that
    // falls back on every upgrade regardless of what arrives.
    const h = await barrierHarness()
    await waitUntil(() => h.handoffDrained(), 'the upgrade committed')
    await new Promise((resolve) => setTimeout(resolve, UPGRADE_HANDOFF_JOIN_TIMEOUT_MS + 500))
    expect(h.sseConnects()).toBe(1)
  }, 10_000)
})

describe('upgradeDisabled is sticky after a fallback (T2 gap, closed here)', () => {
  test('the post-fallback reconnect settles and does NOT start a second upgrade', async () => {
    // This is the assertion T2 could not make: without the harness answering the reconnect's
    // RECONCILE, `maybeStartUpgrade` is never reached on the fresh wire, so "no second attempt"
    // holds for the wrong reason — a gate that cannot fail. `autoReconcile` is what gives it teeth.
    const h = await barrierHarness({ barrier: 'fin-only', autoReconcile: true })

    // The instrument is proven live by this very assertion: the FIRST settled RECONCILED, of
    // exactly the shape `autoReconcile` replays, did produce a probe and a PREPARE. So "no second
    // socket" below is a claim about stickiness, not about a reply that never arms anything.
    expect(h.sockets).toHaveLength(1)
    expect(h.prepares).toHaveLength(1)

    await waitUntil(() => h.sseConnects() === 2, 'client fell back to a fresh SSE', JOIN_BUDGET_MS)

    // The fresh wire really did reconcile and settle...
    await waitUntil(
      () => h.sse.upstream.filter((f) => f.tag === TAG.RECONCILE).length >= 2,
      'the reconnect sent its own RECONCILE',
      JOIN_BUDGET_MS,
    )
    h.sse.pushFrame(encode.text(0, '"post-fallback"', 1))
    await waitUntil(() => h.channels[0]!.received.length > 0, 'the fresh wire settled and delivers', JOIN_BUDGET_MS)

    // ...and reaching `maybeStartUpgrade` on it changed nothing: still one socket, still one
    // PREPARE, for the connection's whole life.
    expect(h.sockets).toHaveLength(1)
    expect(h.prepares).toHaveLength(1)
  }, 15_000)
})

describe('a READY for another attempt is not this attempt’s READY', () => {
  test('a mismatched upgradeId leaves the client staged, not draining', async () => {
    const h = await barrierHarness({ prepare: 'withhold' })
    expect(h.upgradeTag()).toBe('preparing')

    h.sendReady('not-our-attempt')
    await waitUntil(() => h.upgradeTag() !== 'preparing', 'the impostor READY ended the attempt')

    // Ended the attempt rather than licensing a barrier: the impostor proves nothing about whether
    // OUR stage was installed, so emitting the old wire's final frame on its word would be a
    // barrier against a stage that may not exist.
    expect(h.barriers).toHaveLength(0)
    expect(h.upgradeTag()).toBe('none')
    expect(h.sseConnects()).toBe(1)
    expect(h.sockets[0]!.readyState).toBe(WS_CLOSED)

    h.sse.pushFrame(encode.text(0, '"still-here"', 1))
    await waitUntil(() => h.channels[0]!.received.length === 1, 'the old wire is untouched')
  })
})

describe('the flip adopts the probed socket rather than opening a new one', () => {
  test('a committed upgrade uses exactly one WS socket', async () => {
    // Guards the suppression window: it spans only the SYNCHRONOUS adoption of the probed socket,
    // so if the flip ever started connecting a fresh socket instead, that socket would (correctly)
    // send its own RECONCILE and this count would change.
    const h = await barrierHarness()
    await waitUntil(() => h.handoffDrained(), 'the upgrade committed')

    expect(h.sockets).toHaveLength(1)
    expect(h.ws.sent.filter((f) => f.tag === TAG.RECONCILE)).toHaveLength(0)
    expect(h.ws.sent.filter((f) => f.tag === TAG.PREPARE)).toHaveLength(1)

    // The committed wire is the live transport: a send now goes out on it.
    h.send(0, '"on-the-new-wire"')
    await waitUntil(() => h.ws.sent.some((f) => f.tag === TAG.TEXT), 'the committed WS carries user sends')
  })
})
