// PC3 — POSITIVE CONTROL. Asserts the CURRENT (defective) behaviour: when the RECONCILED that
// settles the upgrade omits a channel whose ABORT is still sitting in the old wire's handoff
// buffer, the channel is released with a generic reconnect error and the server's real abort
// VALUE is swallowed.
//
// Mechanism (`connection.ts`):
//   1. `applyReconciled` walks the channels it reconciled. A channel the server did not
//      acknowledge is released immediately with a manufactured error (`:1090-1094`):
//        `new NetworkError('Channel not acknowledged by server after reconnect', true)`
//   2. Only afterwards does `tryCompleteUpgradeHandoff` drain the handoff buffer (`:812` → `:779`),
//      where the real `ABORT` finally reaches `closeRemoteChannel`.
//   3. By then the entry is gone, so `closeRemoteChannel` no-ops on its first line (`:1116-1117`)
//      and the abort value dies there.
//
// Why this class of loss is special: an abort value is TERMINAL and NON-REPLAYABLE. A fresh
// reconnect cannot reproduce it — the channel is gone server-side. Every other frame class in the
// handoff buffer is either replayable (data) or reconstructible (CLOSE) or intentionally ephemeral
// (WINDOW / MSG_WINDOW). So this is the one where "released generically" is not merely a worse
// message, it is information that no later round-trip can recover.
//
// Fix (R4): defer omitted channels instead of releasing them, drain the old prefix, and only then
// generically release whatever is still deferred — so a real ABORT wins with its true value. The
// whole `applyReconciled` must NOT be deferred: that would regress the C2S ungate (`:1103`), the
// flow-control reset (`:811`) and `installHeartbeat` (`:808`).
//
// Oracle discipline: the error object handed to `_onTransportClose` — what the application's
// `onClose` callback actually receives. Not connection-internal release bookkeeping.
//
// Disposition: T2 rewrites the swallow assertion in place to assert the abort value survives.
// Do not delete.

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

  // THE POSITIVE CONTROL.
  test('CURRENT: when the reconcile omits the channel, the buffered abort value is swallowed', async () => {
    const h = (harness = await createUpgradeHarness(['A']))

    await runHandoffWithBufferedAbort(h, /* acknowledged */ false)

    const errors = h.channels[0]!.closeErrors
    // The channel closes exactly once — the later ABORT no-ops rather than closing it again.
    expect(errors).toHaveLength(1)
    // ← THE DEFECT: a manufactured reconnect error, not the server's abort.
    expect(isAbort(errors[0])).toBe(false)
    expect(errors[0]?.message).toBe('Channel not acknowledged by server after reconnect')
    // And the value itself is nowhere: not on the error, not on the channel, not on any wire.
    expect((errors[0] as { abortValue?: unknown }).abortValue).toBeUndefined()
    expect(JSON.stringify(h.channels[0]!.received)).not.toContain('quota-exhausted')
    expect(JSON.stringify(h.channels[0]!.ctrl)).not.toContain('quota-exhausted')
  })
})
