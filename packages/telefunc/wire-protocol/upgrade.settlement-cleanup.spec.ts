// R4 follow-up — the SECONDARY cleanup that split settlement moved out from under the existing
// machinery.
//
// Before R4, a channel the settling RECONCILED omitted was released INSIDE `applyReconciled`,
// which put that release ahead of two things that happen to depend on it:
//
//   1. `handleReconciled`'s `startTtlIfIdle()` — released first, so a connection whose last
//      channel had just gone saw `channels.size === 0` and armed its idle TTL.
//   2. `applyReconciled`'s own `drainBufferedFrames(serverMap, this.channels)` pass — released
//      first, so the channel was absent from BOTH sets and its queued sends were compacted away.
//
// R4 moved that release to AFTER both, into `tryCompleteUpgradeHandoff`. Nothing downstream
// re-runs either step, so on the RECONCILED-before-FIN ordering the connection is left holding
// zero channels with no TTL armed (wire + heartbeat alive indefinitely), and the omitted channel's
// queued sends stay pinned in `sendBuffer` with nothing that will ever collect them.
//
// Both are settlement-ordering bugs with the same root cause and the same seam, so they are
// covered together here.
//
// ── Why the FIN-first controls matter ────────────────────────────────────────────────────────
// FIN-first is the ordering that still works: `tryCompleteUpgradeHandoff` completes from inside
// `handleReconciled`, so the deferred release lands BEFORE `startTtlIfIdle()` runs. Pairing each
// test with its FIN-first twin is what proves these assertions are about the ORDERING and not
// about the harness, the idle timeout, or the send path.

import { afterEach, describe, expect, test } from 'vitest'
import { stringify } from '@brillout/json-serializer/stringify'

import { encode } from './shared-ws.js'
import { createUpgradeHarness, reconciledPayload, waitUntil, type UpgradeHarness } from './upgrade-client-harness.js'

let harness: UpgradeHarness | null = null
afterEach(() => {
  harness?.dispose()
  harness = null
})

/** `WebSocket.CLOSED`. The connection's `dispose()` runs `transport.dispose()`, which closes the
 *  socket — so this is the outside-visible proof that the idle TTL fired and tore the connection
 *  down, rather than leaving it cached with a live wire. */
const WS_CLOSED = 3

/** Short enough to observe inside a test budget; `applyReconciled` adopts `ctrl.idleTimeout`. */
const IDLE_TIMEOUT_MS = 300

/** A settling RECONCILED that OMITS every channel, carrying a short idle timeout. */
const omittingReconciled = () => encode.reconciled({ ...reconciledPayload([]), idleTimeout: IDLE_TIMEOUT_MS })
/** The same settlement, but still acknowledging ix 0. */
const acknowledgingReconciled = () =>
  encode.reconciled({ ...reconciledPayload([{ ix: 0, lastSeq: 0 }]), idleTimeout: IDLE_TIMEOUT_MS })

const waitForDispose = (h: UpgradeHarness) =>
  waitUntil(() => h.ws.socket.readyState === WS_CLOSED, 'connection went idle and disposed', 2_000)

describe('R4 follow-up — settlement ordering must not skip secondary cleanup', () => {
  describe('idle teardown after a deferred release', () => {
    // THE FINDING. RECONCILED omits the last channel and arrives BEFORE FIN, so the deferred
    // release happens in `tryCompleteUpgradeHandoff`, long after `startTtlIfIdle()` already ran
    // and found the channel still present.
    test('RECONCILED-before-FIN omitting the last channel still lets the connection go idle', async () => {
      const h = (harness = await createUpgradeHarness(['A']))

      h.ws.pushFrame(omittingReconciled())
      h.sse.pushFrame(encode.fin())
      await waitUntil(() => h.handoffDrained(), 'handoff completed')
      expect(h.channels[0]!.isClosed).toBe(true)

      // Zero channels remain. The connection must not stay cached with a live wire.
      await waitForDispose(h)
    })

    // INSTRUMENT CHECK #1 — the ordering that already worked. `tryCompleteUpgradeHandoff` runs
    // from inside `handleReconciled` here, so the deferred release lands BEFORE `startTtlIfIdle()`
    // and the TTL is armed. If this ever goes red, the test above is proving something about the
    // harness, not about the ordering.
    //
    // The wait between the two pushes is load-bearing, not slack: the WS stub delivers
    // synchronously through `_onTransportFrame`, while the SSE frame has to travel a real
    // `ReadableStream`. Pushing FIN first and RECONCILED second therefore still delivers
    // RECONCILED first — which is how this control was originally written, and it made the control
    // a duplicate of the finding above (measured: both red for the same reason).
    test('control: FIN-before-RECONCILED omitting the last channel goes idle', async () => {
      const h = (harness = await createUpgradeHarness(['A']))

      h.sse.pushFrame(encode.fin())
      await new Promise((resolve) => setTimeout(resolve, 100))
      h.ws.pushFrame(omittingReconciled())
      await waitUntil(() => h.handoffDrained(), 'handoff completed')

      await waitForDispose(h)
    })

    // INSTRUMENT CHECK #2 — a real buffered ABORT closes the channel during the old-buffer drain,
    // and `dispatchFrame`'s ABORT branch calls `startTtlIfIdle()` itself. So this route to an
    // empty connection was never affected, which locates the gap precisely at the generic release.
    test('control: a buffered ABORT closing the last channel goes idle', async () => {
      const h = (harness = await createUpgradeHarness(['A']))

      h.sse.pushFrame(encode.abort(0, stringify('gone')))
      h.ws.pushFrame(omittingReconciled())
      h.sse.pushFrame(encode.fin())
      await waitUntil(() => h.handoffDrained(), 'handoff completed')

      await waitForDispose(h)
    })

    // The connection must NOT tear itself down while it still owns a channel.
    test('control: a settlement that keeps the channel does not dispose', async () => {
      const h = (harness = await createUpgradeHarness(['A']))

      h.ws.pushFrame(acknowledgingReconciled())
      h.sse.pushFrame(encode.fin())
      await waitUntil(() => h.handoffDrained(), 'handoff completed')

      await new Promise((resolve) => setTimeout(resolve, IDLE_TIMEOUT_MS * 3))
      expect(h.ws.socket.readyState).not.toBe(WS_CLOSED)
      expect(h.channels[0]!.isClosed).toBe(false)
    })
  })

  // CHARACTERIZATION, not a gate. The R2 fallback releases deferred channels too, so it was worth
  // checking whether it shares the gap above — it does not, and for a reason that predates R4:
  // `abortUpgradeAndReconnectSse` ends in `handleTransportLoss`, which either disposes outright
  // (no channels left — the case here) or reconnects, and the reconnect's `_onTransportOpen` runs
  // its own `drainBufferedFrames` compaction. Both assertions below therefore hold with OR without
  // the fix; they are recorded so a future change to that path cannot silently remove the coverage
  // it currently gets for free.
  test('characterization: the fallback path disposes an emptied connection rather than lingering', async () => {
    const h = (harness = await createUpgradeHarness(['A']))

    h.send(0, stringify('queued-during-handoff'))
    // RECONCILED omits the only channel (so it is deferred), FIN never arrives → the 2s join
    // deadline trips the fallback.
    h.ws.pushFrame(omittingReconciled())

    await waitUntil(() => h.channels[0]!.isClosed, 'deferred channel released by the fallback', 5_000)

    // No channels remain, so `handleTransportLoss` disposes instead of opening a fresh SSE wire.
    expect(h.sseConnects()).toBe(1)
    expect(h.bufferedSendCount()).toBe(0)
  })

  describe('queued sends for a deferred-released channel', () => {
    // THE FINDING. The send is queued while the handoff RECONCILE is pending. `applyReconciled`'s
    // drain pass retains it, because the deferred channel is still in `this.channels` — and by the
    // time the channel is actually released, that pass is over and nothing runs it again.
    test('a send queued for an omitted channel is not left pinned in the send buffer', async () => {
      const h = (harness = await createUpgradeHarness(['A']))

      h.send(0, stringify('queued-during-handoff'))
      expect(h.bufferedSendCount()).toBe(1) // precondition: it really is buffered, not sent

      h.ws.pushFrame(omittingReconciled())
      h.sse.pushFrame(encode.fin())
      await waitUntil(() => h.handoffDrained(), 'handoff completed')

      expect(h.channels[0]!.isClosed).toBe(true)
      expect(h.bufferedSendCount()).toBe(0)
    })

    // Same shape, but a real buffered ABORT is what removes the channel — also after the drain
    // pass, so it retains the entry by the same mechanism.
    test('a send queued for a channel closed by a buffered ABORT is not left pinned', async () => {
      const h = (harness = await createUpgradeHarness(['A']))

      h.send(0, stringify('queued-during-handoff'))
      expect(h.bufferedSendCount()).toBe(1)

      h.sse.pushFrame(encode.abort(0, stringify('gone')))
      h.ws.pushFrame(omittingReconciled())
      h.sse.pushFrame(encode.fin())
      await waitUntil(() => h.handoffDrained(), 'handoff completed')

      expect(h.bufferedSendCount()).toBe(0)
    })

    // INSTRUMENT CHECK — when the settlement KEEPS the channel, the queued send is released onto
    // the new wire and leaves the buffer by the ordinary route. So `bufferedSendCount()` can reach
    // 0 on this path, and the assertions above are not passing on a counter that is always 0.
    test('control: a send queued for an acknowledged channel is released, not pinned', async () => {
      const h = (harness = await createUpgradeHarness(['A']))

      h.send(0, stringify('queued-during-handoff'))
      expect(h.bufferedSendCount()).toBe(1)

      h.ws.pushFrame(acknowledgingReconciled())
      h.sse.pushFrame(encode.fin())
      await waitUntil(() => h.handoffDrained(), 'handoff completed')

      expect(h.bufferedSendCount()).toBe(0)
      expect(h.channels[0]!.isClosed).toBe(false)
      // ...and it actually went out on the new wire, rather than merely being discarded.
      expect(JSON.stringify(h.ws.sent)).toContain('queued-during-handoff')
    })
  })
})
