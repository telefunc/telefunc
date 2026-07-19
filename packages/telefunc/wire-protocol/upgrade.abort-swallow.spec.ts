// PC3 — POSITIVE CONTROL, now FLIPPED to assert the fix (R4). It originally asserted the defect:
// when the RECONCILED that settles the upgrade omitted a channel whose ABORT was still sitting in
// the old wire's handoff buffer, the channel was released with a generic reconnect error and the
// server's real abort VALUE was swallowed.
//
// The mechanism of the defect (`connection.ts`):
//   1. `applyReconciled` walked the channels it reconciled. A channel the server did not
//      acknowledge was released IMMEDIATELY with a manufactured error:
//        `new NetworkError('Channel not acknowledged by server after reconnect', true)`
//   2. Only afterwards did `tryCompleteUpgradeHandoff` drain the handoff buffer, where the real
//      `ABORT` finally reached `closeRemoteChannel`.
//   3. By then the entry was gone, so `closeRemoteChannel` no-opped on its first line and the
//      abort value died there.
//
// Fix (R4): `applyReconciled` takes a collector param — null on every ordinary reconcile, so that
// path is unchanged — and while a handoff settles it DEFERS omitted channels instead of releasing
// them. The old buffer drains first, then whatever is still deferred gets the generic release. A
// real ABORT/ERROR therefore wins with its true value, and a channel that genuinely vanished
// server-side still ends up closed exactly as before.
//
// Why this class of loss is special: an abort value is TERMINAL and NON-REPLAYABLE. A fresh
// reconnect cannot reproduce it — the channel is gone server-side. Every other frame class in the
// handoff buffer is either replayable (data) or reconstructible (CLOSE) or intentionally ephemeral
// (WINDOW / MSG_WINDOW). So this is the one where "released generically" is not merely a worse
// message, it is information that no later round-trip can recover.
//
// Only the omitted-channel release is deferred. The rest of `applyReconciled` is NOT: deferring the
// whole apply would regress the C2S ungate, the flow-control reset and `installHeartbeat`.
//
// Oracle discipline: the error object handed to `_onTransportClose` — what the application's
// `onClose` callback actually receives. Not connection-internal release bookkeeping.
//
// Mutation control: remove the guarded early-`continue` from `applyReconciled` (release omitted
// channels immediately again). Only the flipped test below goes red.

import { afterEach, describe, expect, test } from 'vitest'
import { stringify } from '@brillout/json-serializer/stringify'

import { isAbort } from '../shared/Abort.js'
import { encode } from './shared-ws.js'
import { createUpgradeHarness, reconciledPayload, waitUntil, type UpgradeHarness } from './upgrade-client-harness.js'

let harness: UpgradeHarness | null = null
afterEach(() => {
  harness?.dispose()
  harness = null
})

/** The value only the server knows. If this string never reaches the application, it is gone. */
const ABORT_VALUE = { reason: 'quota-exhausted', retryAfter: 42 }

/**
 * Run the handoff to completion with an ABORT for ix 0 buffered on the dying old wire.
 *
 * `acknowledged` selects the only difference between the control and the defect: whether the
 * settling RECONCILED still lists ix 0.
 *
 * Frame order mirrors the real one — ABORT then FIN on the old wire (same-wire FIFO guarantees the
 * ABORT is buffered before the drain can run), RECONCILED whenever on the new one.
 */
async function runHandoffWithBufferedAbort(h: UpgradeHarness, acknowledged: boolean) {
  h.sse.pushFrame(encode.abort(0, stringify(ABORT_VALUE)))
  h.ws.pushFrame(encode.reconciled(reconciledPayload(acknowledged ? [{ ix: 0, lastSeq: 0 }] : [])))
  h.sse.pushFrame(encode.fin())
  await waitUntil(() => h.handoffDrained(), 'handoff completed and buffer drained')
}

describe('PC3 — abort value swallowed by a reconcile that omits the channel', () => {
  // The instrument that can disagree. Identical path, identical buffered ABORT — the ONLY
  // difference is that the RECONCILED still acknowledges ix 0, so the release does not race the
  // drain. The real abort value arrives. If this ever goes red, the control below proves nothing.
  test('control: when the reconcile still lists the channel, the true abort value is delivered', async () => {
    const h = (harness = await createUpgradeHarness(['A']))

    await runHandoffWithBufferedAbort(h, /* acknowledged */ true)

    const errors = h.channels[0]!.closeErrors
    expect(errors).toHaveLength(1)
    expect(isAbort(errors[0])).toBe(true)
    expect((errors[0] as { abortValue?: unknown }).abortValue).toEqual(ABORT_VALUE)
  })

  // THE POSITIVE CONTROL, FLIPPED BY R4.
  //
  // Before R4 (commit `fix(wire-protocol): settle omitted channels after the old wire drains`)
  // this test asserted the SWALLOW:
  //
  //     expect(errors).toHaveLength(1)
  //     expect(isAbort(errors[0])).toBe(false)
  //     expect(errors[0]?.message).toBe('Channel not acknowledged by server after reconnect')
  //     expect((errors[0] as { abortValue?: unknown }).abortValue).toBeUndefined()
  //     expect(JSON.stringify(h.channels[0]!.received)).not.toContain('quota-exhausted')
  //     expect(JSON.stringify(h.channels[0]!.ctrl)).not.toContain('quota-exhausted')
  //
  // i.e. a manufactured reconnect error reached the application and the server's real abort value
  // existed nowhere. Kept verbatim because it is the only artifact proving the rider was necessary.
  test('when the reconcile omits the channel, the buffered abort still wins with its true value', async () => {
    const h = (harness = await createUpgradeHarness(['A']))

    await runHandoffWithBufferedAbort(h, /* acknowledged */ false)

    const errors = h.channels[0]!.closeErrors
    // Still exactly one close — the deferred generic release finds the entry already gone and
    // skips it, so the application is not closed twice.
    expect(errors).toHaveLength(1)
    expect(isAbort(errors[0])).toBe(true)
    expect((errors[0] as { abortValue?: unknown }).abortValue).toEqual(ABORT_VALUE)
  })

  // The OTHER half of the split settlement, and it is not optional: deferring the release is only
  // safe if something eventually performs it. A channel the server omitted with NO terminal frame
  // in flight must still end up closed, with today's generic error. Without this test, "never
  // release at all" would pass the control and the flipped case above.
  test('an omitted channel with no buffered terminal frame still gets the generic release', async () => {
    const h = (harness = await createUpgradeHarness(['A']))

    h.ws.pushFrame(encode.reconciled(reconciledPayload([])))
    h.sse.pushFrame(encode.fin())
    await waitUntil(() => h.handoffDrained(), 'handoff completed and buffer drained')

    const errors = h.channels[0]!.closeErrors
    expect(errors).toHaveLength(1)
    expect(isAbort(errors[0])).toBe(false)
    expect(errors[0]?.message).toBe('Channel not acknowledged by server after reconnect')
    expect(h.channels[0]!.isClosed).toBe(true)
  })
})
