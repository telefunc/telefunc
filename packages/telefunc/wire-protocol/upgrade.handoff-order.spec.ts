// PC2 — POSITIVE CONTROL. Asserts the CURRENT (defective) behaviour: a server→client frame still
// in flight on the OLD SSE wire is lost forever when a NEWER frame arrives first on the new WS
// wire, because the upgrade handoff keeps ONE arrival-ordered buffer for both sources.
//
// The barrier design does NOT fix this — the barrier is a C2S cut; this is S2C loss, reachable on
// the current tree today. Two independent halves have to hold for it to be reachable, and both are
// asserted here rather than assumed:
//
//  1. THE SERVER REALLY DOES REPLAY N+1 WITHOUT N. `ReplayBuffer` keeps text and binary in
//     separate lanes. An oversized text frame N is stored as a `null` gap marker
//     (`replay-buffer.ts:190-201`) and `Lane.getAfter` stops at it (`:216-220`) — but
//     `ReplayBuffer.getAfter` merges the two lanes INDEPENDENTLY: `if (t.frames.length === 0)
//     return b.frames` (`:68-71`). So a gapped text lane does not hold back the binary lane, and
//     attach (`mux.ts:335`) replays N+1 alone.
//  2. THE CLIENT CANNOT RECOVER FROM THAT ORDER. Frames from both wires land in the single
//     arrival-ordered `state.upgrade.buffer` (`connection.ts:691-702`). On handoff completion the
//     buffer is drained in arrival order (`:779`), so WS-(N+1) advances `trackSeq`
//     (`:1132-1136`) past the in-flight SSE-N, which is then dropped as `'dup'` — and the gap is
//     baked into the next RECONCILE's reported `lastSeq` (`:1021`), so it is never replayed again.
//
// Fix (R1): partition the handoff buffer by source transport and drain the ENTIRE old buffer
// before the new one. `trackSeq` / `drainBufferedFrames` / `dispatchFrame` stay untouched.
//
// Oracle discipline: delivery is read at the channel's `_dispatchFrame`, which sits BELOW the
// `trackSeq` dedup that does the dropping. Reported `lastSeq` is the same bookkeeping the drop
// decision uses and would be circular.
//
// Disposition: T2 rewrites the loss assertion in place to assert delivery. Do not delete.

import { afterEach, describe, expect, test } from 'vitest'
import { stringify } from '@brillout/json-serializer/stringify'

import { ReplayBuffer } from './replay-buffer.js'
import { TAG, decode, encode } from './shared-ws.js'
import { CHANNEL_SERVER_REPLAY_BUFFER_BYTES } from './constants.js'
import { createUpgradeHarness, reconciledPayload, waitUntil, type UpgradeHarness } from './upgrade-client-harness.js'

let harness: UpgradeHarness | null = null
afterEach(() => {
  harness?.dispose()
  harness = null
})

/** Frame N: a TEXT frame the server sent on the old wire. Oversized on the server's replay buffer,
 *  which is what gaps its lane — its size is irrelevant to the client, which never sees the buffer. */
const frameN = (seq: number) => encode.text(0, stringify(`old-wire-${seq}`), seq)
/** Frame N+1: a BINARY frame, i.e. the OTHER lane — the reason it replays past the gap. */
const frameNPlus1 = (seq: number) => encode.binary(0, new Uint8Array([1, 2, 3]), seq)

/** Finish the handoff. RECONCILED settles the new wire; FIN goes LAST on the SSE stream, so every
 *  old-wire frame pushed before it is necessarily already buffered when the drain runs (same-wire
 *  FIFO). The handoff commits only once both have landed (`connection.ts:771-773`). */
async function completeHandoff(h: UpgradeHarness) {
  h.ws.pushFrame(encode.reconciled(reconciledPayload([{ ix: 0, lastSeq: 0 }])))
  h.sse.pushFrame(encode.fin())
  await waitUntil(() => h.handoffDrained(), 'handoff completed and buffer drained')
}

describe('PC2 — S2C loss across the upgrade handoff', () => {
  // Half 1, on the REAL server-side buffer. Without this the client-side control below would be
  // demonstrating a loss for an arrival order the server could never actually produce.
  test('reachability: a gapped text lane still lets the binary lane replay past it', () => {
    const replay = new ReplayBuffer(CHANNEL_SERVER_REPLAY_BUFFER_BYTES, 60_000, 2 * 1024 * 1024)

    // Frame 1: TEXT, oversized ⇒ stored as a gap marker, not as a frame.
    const oversized = encode.text(0, stringify('x'.repeat(CHANNEL_SERVER_REPLAY_BUFFER_BYTES + 1)), 1)
    expect(replay.push(1, oversized, false)).toBe(false)
    // Frame 2: BINARY, stored normally in the other lane.
    expect(replay.push(2, encode.binary(0, new Uint8Array([1, 2, 3]), 2), true)).toBe(true)

    // Control: within a single lane the gap DOES hold replay back, so the merge is what leaks.
    const textOnly = new ReplayBuffer(CHANNEL_SERVER_REPLAY_BUFFER_BYTES, 60_000, 2 * 1024 * 1024)
    textOnly.push(1, oversized, false)
    textOnly.push(2, encode.text(0, stringify('two'), 2), false)
    expect(textOnly.getAfter(0)).toHaveLength(0)

    // The defect: seq 2 replays while seq 1 is a hole the client is never told about.
    const replayed = replay.getAfter(0).map((frame) => decode(frame))
    expect(replayed).toHaveLength(1)
    expect(replayed[0]).toMatchObject({ tag: TAG.BINARY, seq: 2 })
  })

  // Instrument check: this exact path CAN deliver frame N. Every "not received" assertion below
  // would pass just as happily against a harness that never delivered anything.
  test('control: an old-wire frame with no competing new-wire frame is delivered', async () => {
    const h = (harness = await createUpgradeHarness(['A']))

    h.sse.pushFrame(frameN(1))
    await completeHandoff(h)

    expect(h.channels[0]!.received).toEqual([{ kind: 'text', value: 'old-wire-1' }])
  })

  // Second instrument check, and the one that names the fix: it is the ORDER that kills frame N,
  // not the dedup itself. Both frames on the same wire arrive in seq order and both survive — which
  // is exactly the state R1's "drain the entire old buffer first" restores.
  test('control: N before N+1 in the buffer delivers both', async () => {
    const h = (harness = await createUpgradeHarness(['A']))

    h.sse.pushFrame(frameN(1))
    h.sse.pushFrame(frameNPlus1(2))
    await completeHandoff(h)

    expect(h.channels[0]!.received).toEqual([
      { kind: 'text', value: 'old-wire-1' },
      { kind: 'binary', bytes: new Uint8Array([1, 2, 3]) },
    ])
  })

  // THE POSITIVE CONTROL. Same two frames, same channel, same seqs — only the wire each arrives on
  // differs, which is precisely what the recipe above makes reachable.
  test('CURRENT: a new-wire frame arriving first silently loses the in-flight old-wire frame', async () => {
    const h = (harness = await createUpgradeHarness(['A']))

    // The replayed N+1 lands on the new WS wire. `socket.emit` is synchronous through
    // `_onTransportFrame`, so it is in the buffer before anything the SSE stream yields.
    h.ws.pushFrame(frameNPlus1(2))
    // N is still in flight on the old SSE wire and arrives second.
    h.sse.pushFrame(frameN(1))
    await completeHandoff(h)

    // ← THE DEFECT: N+1 arrived, N did not. No error, no counter, nothing to alert on.
    expect(h.channels[0]!.received).toEqual([{ kind: 'binary', bytes: new Uint8Array([1, 2, 3]) }])
    expect(h.channels[0]!.closeErrors).toEqual([])
    expect(h.channels[0]!.isClosed).toBe(false)
  })
})
