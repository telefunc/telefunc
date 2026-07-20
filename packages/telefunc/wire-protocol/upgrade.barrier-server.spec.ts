// The SERVER half of the barrier upgrade: `PREPARE` stages a probe wire, a `barrier: true` RECONCILE
// on the OLD wire commits it, and every abandoned attempt leaves no residue.
//
// The barrier runs as an ordinary turn on the old connection's `recvChain`, so it is ordered after
// every earlier old-wire frame BY CONSTRUCTION; the commit is then the existing, unmodified
// `reconcile` invoked with the STAGED entry, which binds the session to the probe while FIN still
// binds to the old wire (`buildUpgradeFinalizer` reads the PREVIOUS session's finalizer).
//
// TWO FALSE-GREEN DEFENCES, deliberately different in kind. In a fast in-memory harness the recv
// chain is empty at barrier time, so a barrier test with nothing in flight passes identically with
// and without the ordering edge. The PARK gates the edge alone (deterministic: while parked a commit
// is structurally impossible); the BURST drives genuinely queued frames through a committing barrier.
// They cannot be one test — a parked reconcile ALWAYS re-sessions the old wire on release, so a
// barrier behind a park necessarily names a dead session and equality correctly refuses it.
//
// EVERY REJECTION ROW ENDS IN A LIVE-SSE SENTINEL: after the probe is terminated the old wire must
// still route a fresh payload to its real listener. Without it, "the probe was terminated" is
// satisfied just as happily by an implementation that tore down BOTH wires — precisely what the
// design forbids, since a pre-barrier failure must cost an established client nothing.

import { afterEach, describe, expect, test, vi } from 'vitest'

import {
  createMuxHarness,
  disposeGlobalChannels,
  globalRegisterChannel,
  lengthPrefixed,
  openGlobalProbeWire,
  openSseDownstream,
  pingFrame,
  prepareFrame,
  reconcileFrame,
  settle,
  textFrame,
} from './upgrade-mux-harness.js'
import { handleSseChannelRequest } from './server/sse.js'
import { encodeSseRequestMetadata } from './sse-request.js'
import { UPGRADE_STAGE_TTL_MS } from './constants.js'
import { TAG, encode, type DecodedFrame } from './shared-ws.js'

let harness: ReturnType<typeof createMuxHarness> | null = null
let sentinelSeq = 5_000
let sentinelValue = 9_000
afterEach(() => {
  harness?.dispose()
  harness = null
  disposeGlobalChannels()
  vi.useRealTimers()
  sentinelSeq = 5_000
  sentinelValue = 9_000
})

const EMPTY_STAGE = { records: 0, reverseRecords: 0, bytes: 0 }

async function connectSse(h: ReturnType<typeof createMuxHarness>) {
  const chA = h.registerChannel('A')
  await h.sse.deliver(reconcileFrame({ open: [{ id: 'A', ix: 0, lastSeq: 0, initial: true }] }))
  const s0 = h.sse.sessionId()
  expect(s0).toBeTypeOf('string')
  await h.sse.deliver(textFrame(0, 1, 111))
  expect(chA.received).toEqual([111]) // sentinel: this wire delivers
  return { chA, s0: s0! }
}

/** A second, fully independent SSE connection with its own session. */
async function connectSecondSse(h: ReturnType<typeof createMuxHarness>) {
  const sse2 = h.makeWire('sse-conn-2')
  h.registerChannel('B')
  await sse2.deliver(reconcileFrame({ open: [{ id: 'B', ix: 0, lastSeq: 0, initial: true }] }))
  expect(sse2.sessionId()).toBeTypeOf('string')
  return sse2
}

const prepare = (sessionId: string, upgradeId = 'upg-1') =>
  prepareFrame({ upgradeId, sessionId, open: [{ id: 'A', ix: 0 }] })

/** The old wire's FINAL frame: authoritative membership plus barrier-fresh cursors. */
const barrier = (sessionId: string, upgradeId = 'upg-1', lastSeq = 1) =>
  reconcileFrame({ sessionId, barrier: true, upgradeId, open: [{ id: 'A', ix: 0, lastSeq }] })

const reconciledOn = (wire: { sent: readonly DecodedFrame[] }) => wire.sent.filter((f) => f.tag === TAG.RECONCILED)
const finsOn = (wire: { sent: readonly DecodedFrame[] }) => wire.sent.filter((f) => f.tag === TAG.FIN)
const readysOn = (wire: { sent: readonly DecodedFrame[] }) => wire.sent.filter((f) => f.tag === TAG.READY)

/** Seqs well clear of every in-test frame, so a sentinel can never be dropped as a duplicate by the
 *  channel's `_lastClientSeq` dedup and misread as a routing failure. */
async function expectWireDelivers(
  wire: { deliver: (f: Uint8Array<ArrayBuffer>) => Promise<void> },
  chA: { received: number[] },
) {
  const value = ++sentinelValue
  await wire.deliver(textFrame(0, ++sentinelSeq, value))
  expect(chA.received.at(-1)).toBe(value)
}

describe('PREPARE stages, and staging alone is inert', () => {
  test('a valid PREPARE answers READY echoing the upgradeId, with no side effect on the old session', async () => {
    // The whole failure story ("pre-barrier failures leave the SSE untouched") rests on PREPARE being
    // inert: if staging quietly rotated or re-attached anything, every abandoned attempt would damage
    // a healthy connection.
    const h = (harness = createMuxHarness())
    const { chA, s0 } = await connectSse(h)
    const sseSentBefore = h.sse.sent.length

    await h.ws.deliver(prepare(s0, 'upg-7'))

    expect(readysOn(h.ws)).toHaveLength(1)
    expect(readysOn(h.ws)[0]?.tag === TAG.READY && readysOn(h.ws)[0]!.payload.upgradeId).toBe('upg-7')
    expect(h.mux._getUpgradeResourceSnapshot()).toMatchObject({ records: 1, reverseRecords: 1 })
    expect(h.mux._getUpgradeResourceSnapshot().bytes).toBeGreaterThan(0)
    expect(h.sse.sessionId()).toBe(s0)
    expect(h.ws.sessionId()).toBeUndefined()
    expect(h.sse.sent).toHaveLength(sseSentBefore)
    await expectWireDelivers(h.sse, chA)
  })

  test('a plain RECONCILE on a staged probe wire is a violation — it never rotates', async () => {
    // THE DISPATCH HOLE: RECONCILE is handled ABOVE the session-less guard, so without the explicit
    // staged-wire check this runs the destructive rotation and steps around the stage entirely —
    // `ws.sessionId()` becomes a fresh id, S0 leaves the registry, the sentinel is silently dropped.
    const h = (harness = createMuxHarness())
    const { chA, s0 } = await connectSse(h)
    await h.ws.deliver(prepare(s0))

    await h.ws.deliver(reconcileFrame({ sessionId: s0, open: [{ id: 'A', ix: 0, lastSeq: 1 }] }))

    expect(h.ws.terminated()).toBe(true)
    expect(h.ws.sessionId()).toBeUndefined() // ← the rotation the guard exists to prevent
    expect(reconciledOn(h.ws)).toHaveLength(0)
    await expectWireDelivers(h.sse, chA)
  })

  test('a second PREPARE on the same probe wire is a violation — never a re-stage', async () => {
    // Without the guard the stage is silently re-installed (leaking the first timer) and a SECOND
    // READY goes out, so the client could act on an attempt the server re-keyed underneath it.
    const h = (harness = createMuxHarness())
    const { chA, s0 } = await connectSse(h)
    await h.ws.deliver(prepare(s0, 'upg-1'))

    await h.ws.deliver(prepare(s0, 'upg-2'))

    expect(readysOn(h.ws)).toHaveLength(1)
    expect(readysOn(h.ws)[0]?.tag === TAG.READY && readysOn(h.ws)[0]!.payload.upgradeId).toBe('upg-1')
    expect(h.ws.terminated()).toBe(true)
    expect(h.mux._getUpgradeResourceSnapshot()).toEqual(EMPTY_STAGE)
    await expectWireDelivers(h.sse, chA)
  })
})

describe('the barrier commits the staged wire, exactly once', () => {
  test('COMMITTED lands on the probe wire with the upgradeId, and the channel routes there', async () => {
    const h = (harness = createMuxHarness())
    const { chA, s0 } = await connectSse(h)
    await h.ws.deliver(prepare(s0, 'upg-7'))

    await h.sse.deliver(barrier(s0, 'upg-7'))

    // COMMITTED *is* a RECONCILED — reusing the payload keeps the client's settlement path unforked —
    // discriminated from an ordinary one solely by the echoed upgradeId.
    const committed = reconciledOn(h.ws)
    expect(committed).toHaveLength(1)
    expect(committed[0]?.tag === TAG.RECONCILED && committed[0].payload.upgradeId).toBe('upg-7')
    // `deliverTo` in action: the turn ran on the SSE chain, its answer went to the WS.
    expect(reconciledOn(h.sse)).toHaveLength(1)
    expect(reconciledOn(h.sse)[0]?.tag === TAG.RECONCILED && reconciledOn(h.sse)[0]!.payload.upgradeId).toBeUndefined()
    expect(h.ws.sessionId()).toBeTypeOf('string')
    expect(h.ws.sessionId()).not.toBe(s0)
    expect(h.mux._getUpgradeResourceSnapshot()).toEqual(EMPTY_STAGE)
    await expectWireDelivers(h.ws, chA)
  })

  test('a commit mints exactly ONE new session', async () => {
    const h = (harness = createMuxHarness())
    const { s0 } = await connectSse(h)
    await h.ws.deliver(prepare(s0))

    await h.sse.deliver(barrier(s0))
    await settle()

    const minted = [...h.sse.sent, ...h.ws.sent]
      .filter((f) => f.tag === TAG.RECONCILED)
      .map((f) => (f.tag === TAG.RECONCILED ? f.payload.sessionId : ''))
    // S0 from the initial connect, S1 from the commit. A second rotation shows up as a third.
    expect(minted).toHaveLength(2)
    expect(new Set(minted).size).toBe(2)
    expect(minted[0]).toBe(s0)
    expect(minted[1]).toBe(h.ws.sessionId())
  })

  test('FIN is delivered on the old wire and never on the new one', async () => {
    // The FIN is the old wire's completeness proof, not cleanup decoration: it is what tells the
    // client its old FIFO is fully drained. The COUNT is the sharp edge — firing the finalizer early
    // shows up as a SECOND FIN, and a client joining FIN with COMMITTED settles against the wrong one.
    const h = (harness = createMuxHarness())
    const { s0 } = await connectSse(h)
    await h.ws.deliver(prepare(s0))

    await h.sse.deliver(barrier(s0))

    expect(finsOn(h.sse)).toHaveLength(1)
    expect(finsOn(h.ws)).toHaveLength(0)
  })

  test('a barrier carrying an initial:true entry is refused rather than parking the commit', async () => {
    // `attach` is the ONE awaitable inside `reconcile`, and it waits only for an `initial: true` entry
    // naming an unregistered channel. Letting a barrier carry one would park the commit mid-flight,
    // holding the probe in a state where it has neither a session nor a stage.
    const h = (harness = createMuxHarness())
    const { chA, s0 } = await connectSse(h)
    await h.ws.deliver(prepare(s0))
    h.createChannel('B') // built but NOT registered — an `initial` entry naming it would park

    await h.sse.deliver(
      reconcileFrame({
        sessionId: s0,
        barrier: true,
        upgradeId: 'upg-1',
        open: [
          { id: 'A', ix: 0, lastSeq: 1 },
          { id: 'B', ix: 1, lastSeq: 0, initial: true },
        ],
      }),
    )

    // The SNAPSHOT is the sharp assertion: were the entry accepted, `attach` would still be parked on
    // channel B right now and the record would sit there in `committing`.
    expect(h.mux._getUpgradeResourceSnapshot()).toEqual(EMPTY_STAGE)
    expect(reconciledOn(h.ws)).toHaveLength(0)
    expect(h.ws.sessionId()).toBeUndefined()
    expect(h.sse.sessionId()).toBe(s0)
    await expectWireDelivers(h.sse, chA)
  })

  test('a plain RECONCILE on the probe cannot slip in while the commit is in flight', async () => {
    // The stage is the probe's phase marker. `reconcile` is async, so releasing the marker at the
    // START of the commit opens a window in which the probe is neither staged nor sessioned and a
    // plain RECONCILE runs a SECOND destructive rotation concurrently. The record survives as
    // COMMITTING until the commit settles.
    const h = (harness = createMuxHarness())
    const { s0 } = await connectSse(h)
    await h.ws.deliver(prepare(s0))

    // Same burst: the barrier's turn starts first, the intruder lands while the commit resolves.
    const commit = h.sse.deliver(barrier(s0))
    const intruder = h.ws.deliver(reconcileFrame({ sessionId: s0, open: [{ id: 'A', ix: 0, lastSeq: 1 }] }))
    await Promise.all([commit, intruder])
    await settle()

    // Counted across BOTH wires so this does not also depend on `deliverTo` routing, and stated as a
    // count rather than "the intruder was terminated" so it does not double as a report on victims.
    const minted = [...h.sse.sent, ...h.ws.sent].filter((f) => f.tag === TAG.RECONCILED)
    expect(minted).toHaveLength(2) // the connect and the commit — a served intruder makes 3
  })

  test('a duplicate barrier can never rotate twice', async () => {
    // The stage is single-use: cleared the moment the commit is committed to. Without that, a replayed
    // barrier rotates again and deletes the finalizer the first one just installed.
    const h = (harness = createMuxHarness())
    const { s0 } = await connectSse(h)
    await h.ws.deliver(prepare(s0))
    await h.sse.deliver(barrier(s0))
    const committedSession = h.ws.sessionId()

    await h.sse.deliver(barrier(s0))

    expect([...h.sse.sent, ...h.ws.sent].filter((f) => f.tag === TAG.RECONCILED)).toHaveLength(2)
    expect(h.ws.sessionId()).toBe(committedSession)
  })
})

describe('happens-before: the barrier is ordered behind everything on the old wire', () => {
  /** Parks the old wire's chain on `waitForChannelRegistration`, stages while parked, then queues a
   *  data frame and the barrier behind the park. ORDER MATTERS: the park is an ordinary reconcile, and
   *  an ordinary reconcile releases any stage keyed to the session it rotates away — so staging AFTER
   *  the park is both necessary and the faithful shape (a client sends PREPARE while its old wire is busy). */
  async function parkThenStageAndQueueBarrier(h: ReturnType<typeof createMuxHarness>, s0: string) {
    const chB = h.createChannel('B') // built but NOT registered → the next reconcile parks on it
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
    await h.ws.deliver(prepare(s0))
    const inflight = h.sse.deliver(textFrame(0, 2, 222))
    const barrierTurn = h.sse.deliver(barrier(s0, 'upg-1', 2))
    await settle()
    return {
      release: async () => {
        h.register(chB)
        await Promise.all([parked, inflight, barrierTurn])
        await settle()
      },
    }
  }

  test('nothing commits while an earlier old-wire turn is still unfinished', async () => {
    // Scoped to the ORDERING EDGE ALONE — asserts only what is true while parked, never reaching
    // validation. A barrier routed outside `chainRecv` commits immediately and reddens this.
    const h = (harness = createMuxHarness())
    const { chA, s0 } = await connectSse(h)
    await parkThenStageAndQueueBarrier(h, s0)

    expect(reconciledOn(h.sse)).toHaveLength(1) // the parked reconcile has not answered
    expect(chA.received).toEqual([111]) // 222 is IN the chain, neither before nor after it
    expect(reconciledOn(h.ws)).toHaveLength(0) // ← nothing committed
    expect(finsOn(h.sse)).toHaveLength(0) // ← the old wire has not been told it is finished
    expect(h.ws.sessionId()).toBeUndefined() // ← nothing rotated
    expect(h.mux._getUpgradeResourceSnapshot().records).toBe(1) // the stage is untouched
  })

  test('the parked frame is dispatched before the barrier gets its turn', async () => {
    const h = (harness = createMuxHarness())
    const { chA, s0 } = await connectSse(h)
    const parked = await parkThenStageAndQueueBarrier(h, s0)

    await parked.release()

    expect(chA.received).toEqual([111, 222])
    // And the barrier — whose session the parked reconcile rotated away underneath it — is refused
    // rather than committed against a dead session: concurrent rotation, observed.
    expect(h.ws.terminated()).toBe(true)
    expect(h.sse.terminated()).toBe(false)
    expect(h.mux._getUpgradeResourceSnapshot()).toEqual(EMPTY_STAGE)
  })

  test('a burst of in-flight old-wire frames is fully delivered before the commit', async () => {
    // The commit-side half, and the production shape: the old wire's upload body can hold many frames
    // when the barrier is appended to it.
    const h = (harness = createMuxHarness())
    const { chA, s0 } = await connectSse(h)
    await h.ws.deliver(prepare(s0))

    const BURST = 32
    const inflight = Array.from({ length: BURST }, (_, i) => h.sse.deliver(textFrame(0, 2 + i, 200 + i)))
    const barrierTurn = h.sse.deliver(barrier(s0, 'upg-1', 2 + BURST))
    await Promise.all([...inflight, barrierTurn])
    await settle()

    expect(chA.received).toEqual([111, ...Array.from({ length: BURST }, (_, i) => 200 + i)])
    expect(reconciledOn(h.ws)).toHaveLength(1)
    expect(finsOn(h.sse)).toHaveLength(1)
  })
})

describe('the retired old wire rejects post-barrier frames loudly', () => {
  test('a data frame after the commit is a violation, not a silent drop', async () => {
    // The `?.` at the routing site swallows this for free: the old connection still reports the
    // rotated-away session id, so the lookup misses for EVERY ix. That silence is what made PC1's
    // defect invisible; the disposition here is "loud".
    const h = (harness = createMuxHarness())
    const { chA, s0 } = await connectSse(h)
    await h.ws.deliver(prepare(s0))
    await h.sse.deliver(barrier(s0))

    // PING first, and it is the DISCRIMINATOR for everything below: `retiredByBarrier` must reject
    // SEMANTIC frames only. The wire stays open until the client sees FIN and drops it, and the
    // client keeps pinging until then — rejecting PING would tear down a wire still doing its job.
    await h.sse.deliver(pingFrame())
    expect(h.sse.terminated()).toBe(false)
    expect(h.sse.sent.filter((f) => f.tag === TAG.PONG)).toHaveLength(1)

    await h.sse.deliver(textFrame(0, 400, 777))

    expect(chA.received).not.toContain(777)
    expect(h.sse.terminated()).toBe(true) // ← loud
    // Counter-sentinel: channel, listener and dedup are all fine — the SAME payload arrives on the
    // wire that now owns the session.
    expect(h.ws.terminated()).toBe(false)
    await expectWireDelivers(h.ws, chA)
  })

  test('a plain RECONCILE after the commit never re-mints on the retired wire', async () => {
    // RECONCILE is dispatched ABOVE the retired check, so a late plain reconcile on the committed-away
    // wire would run the certified rotation THERE: minting a session on a wire the client abandoned
    // and REATTACHING the channels away from the probe that just won them. The data-frame row misses
    // this because the two take different dispatch paths.
    const h = (harness = createMuxHarness())
    const { chA, s0 } = await connectSse(h)
    await h.ws.deliver(prepare(s0))
    await h.sse.deliver(barrier(s0))
    const committedSession = h.ws.sessionId()

    await h.sse.deliver(reconcileFrame({ sessionId: s0, open: [{ id: 'A', ix: 0, lastSeq: 1 }] }))

    expect(h.sse.terminated()).toBe(true)
    expect(h.sse.sessionId()).toBe(s0) // ← no new session on a wire that is already spent
    expect(reconciledOn(h.sse)).toHaveLength(1) // only its own original connect
    expect(h.ws.sessionId()).toBe(committedSession)
    await expectWireDelivers(h.ws, chA)
  })
})

describe('refusal — two checks for two distinct defects, and neither substitutes', () => {
  test('a barrier whose upgradeId does not match the stage is refused', async () => {
    // upgradeId closes STALE SETTLEMENT: a delayed ordinary reconciled consumed as this commit.
    const h = (harness = createMuxHarness())
    const { s0 } = await connectSse(h)
    await h.ws.deliver(prepare(s0, 'upg-1'))

    await h.sse.deliver(barrier(s0, 'upg-OTHER'))

    expect(reconciledOn(h.ws)).toHaveLength(0)
    expect(h.ws.sessionId()).toBeUndefined()
    expect(h.sse.sessionId()).toBe(s0) // refused before any rotation
  })

  test('a barrier is refused when it arrives on a wire that does not own the staged session', async () => {
    // Live-session equality closes CONCURRENT ROTATION: here the upgradeId matches perfectly and the
    // frame is still poison. Committing a copy landing elsewhere would rotate a session the naming
    // wire does not hold — no FIN sent, and the real old session left holding handles to channels
    // re-attached to the probe.
    const h = (harness = createMuxHarness())
    const { chA, s0 } = await connectSse(h)
    await h.ws.deliver(prepare(s0, 'upg-1'))
    const sse2 = await connectSecondSse(h)

    await sse2.deliver(barrier(s0, 'upg-1')) // correct id, correct session named — wrong wire

    expect(reconciledOn(h.ws)).toHaveLength(0)
    expect(h.ws.sessionId()).toBeUndefined()
    expect(finsOn(h.sse)).toHaveLength(0)
    expect(h.sse.sessionId()).toBe(s0)
    expect(sse2.sessionId()).toBeTypeOf('string')
    await expectWireDelivers(h.sse, chA)
  })

  test('a rejected commit terminates the probe wire, never the wire that hosted the turn', async () => {
    // WHICH WIRE DIES is a distinct invariant, and this barrier fails BOTH checks at once
    // deliberately: that makes it insensitive to either check individually and sensitive ONLY to the
    // choice of victim. With the retarget removed the failure kills the connection whose chain merely
    // HOSTED the turn, and a bystander loses its session to a probe's misbehaviour.
    const h = (harness = createMuxHarness())
    const { chA, s0 } = await connectSse(h)
    await h.ws.deliver(prepare(s0, 'upg-1'))
    const sse2 = await connectSecondSse(h)

    await sse2.deliver(barrier(s0, 'upg-WRONG')) // wrong id AND the wrong wire

    expect(h.ws.terminated()).toBe(true)
    expect(sse2.terminated()).toBe(false)
    expect(h.sse.terminated()).toBe(false)
    await expectWireDelivers(h.sse, chA)
  })

  test('a barrier naming no staged upgrade is refused, and terminates neither wire', async () => {
    // A barrier whose stage is GONE is refused without terminating anything. Both alternatives were
    // rejected: killing the old wire costs an established client a healthy session because a probe
    // timed out, and there is no record left of which probe to kill instead. This is a REFUSAL, not a
    // silent drop — no state changes and nothing the client sent is consumed, which is the
    // distinction that matters against the loss PC1 records.
    const h = (harness = createMuxHarness())
    const { chA, s0 } = await connectSse(h)

    await h.sse.deliver(barrier(s0)) // no PREPARE ever happened

    expect(reconciledOn(h.sse)).toHaveLength(1) // only the original connect
    expect(h.sse.sessionId()).toBe(s0) // refused BEFORE any rotation
    expect(h.sse.terminated()).toBe(false) // ← the established session does not pay for this
    await expectWireDelivers(h.sse, chA)
  })
})

// `decode` only CASTS the JSON it parsed, so `ReconcilePayload`'s union — either both barrier legs
// absent, or `barrier === true` with a string `upgradeId` — buys nothing at runtime unless the
// dispatch seam enforces it. Truthiness-testing `barrier` left two openings, and they are opposite
// in kind: a truthy non-`true` value COMMITS an upgrade, while a falsy-but-present one falls through
// to the ordinary path, where `releaseStagesForReconcile` kills the stage and `reconcile` performs a
// destructive session rotation. A malformed barrier-shaped frame must reach neither.
//
// Every row asserts the same three things, because a check that produced only one of them would be
// half a fix: the sender's own wire dies, the old session is NOT rotated, and the stage survives
// intact for the legitimate attempt still in flight.
describe('the barrier discriminant is a shape, not a truthiness test', () => {
  /** A RECONCILE carrying whatever the caller says. The typed helper cannot express these shapes —
   *  which is precisely why nothing but a runtime check can stop them. */
  const hostileReconcile = (payload: Record<string, unknown>) =>
    reconcileFrame(payload as unknown as Parameters<typeof reconcileFrame>[0])

  const open = [{ id: 'A', ix: 0, lastSeq: 1 }]

  async function stagedConnection() {
    const h = (harness = createMuxHarness())
    const { chA, s0 } = await connectSse(h)
    await h.ws.deliver(prepare(s0, 'upg-1'))
    expect(h.mux._getUpgradeResourceSnapshot()).toMatchObject({ records: 1 })
    return { h, chA, s0 }
  }

  function expectRefused(h: ReturnType<typeof createMuxHarness>, s0: string) {
    expect(h.sse.terminated()).toBe(true) // the malformed frame costs its SENDER the wire
    expect(h.sse.sessionId()).toBe(s0) // ← no rotation: it never reached `reconcile`
    expect(reconciledOn(h.ws)).toHaveLength(0) // ← and no commit
    expect(finsOn(h.sse)).toHaveLength(0)
    expect(h.mux._getUpgradeResourceSnapshot()).toMatchObject({ records: 1 }) // the stage survives
  }

  test('barrier:false with an otherwise valid upgradeId does not fall through to ordinary rotation', async () => {
    const { h, s0 } = await stagedConnection()

    await h.sse.deliver(hostileReconcile({ sessionId: s0, barrier: false, upgradeId: 'upg-1', open }))

    expectRefused(h, s0)
  })

  test('barrier:"yes" with a matching upgradeId does not commit', async () => {
    const { h, s0 } = await stagedConnection()

    await h.sse.deliver(hostileReconcile({ sessionId: s0, barrier: 'yes', upgradeId: 'upg-1', open }))

    expectRefused(h, s0)
  })

  test('an orphaned upgradeId with no barrier leg is not ordinary traffic', async () => {
    const { h, s0 } = await stagedConnection()

    await h.sse.deliver(hostileReconcile({ sessionId: s0, upgradeId: 'upg-1', open }))

    expectRefused(h, s0)
  })

  test('barrier:true without a string upgradeId dies on the SENDER, not on the probe', async () => {
    // The one row whose outcome MOVES rather than flips. Today it reaches `handleBarrier`, fails the
    // id equality, and retargets the kill to the probe — the right victim for a well-formed barrier
    // the server refuses, and the wrong one for a frame that never satisfied the wire contract at
    // all. Screened at the seam, the sender pays and the probe is left for its own deadline.
    const { h, s0 } = await stagedConnection()

    await h.sse.deliver(hostileReconcile({ sessionId: s0, barrier: true, open }))

    expectRefused(h, s0)
    expect(h.ws.terminated()).toBe(false)
  })

  test('control: the well-formed shapes on either side of the check still take their own path', async () => {
    // Without this the four rows above are satisfied just as well by a seam that rejects EVERY
    // reconcile. Both legal shapes run here, in sequence, on the same connection: an ordinary
    // reconcile rotates, and a real barrier commits.
    const { h, s0 } = await stagedConnection()

    await h.sse.deliver(reconcileFrame({ sessionId: s0, open })) // ordinary: rotates, releases the stage
    const s1 = h.sse.sessionId()
    expect(s1).not.toBe(s0)
    expect(h.mux._getUpgradeResourceSnapshot()).toEqual(EMPTY_STAGE)

    await h.ws.deliver(prepare(s1!, 'upg-2'))
    await h.sse.deliver(barrier(s1!, 'upg-2'))

    expect(reconciledOn(h.ws)).toHaveLength(1) // ← COMMITTED on the probe
    expect(finsOn(h.sse)).toHaveLength(1)
    expect(h.sse.terminated()).toBe(false)
  })
})

// `staged → committing` is a LINEARIZATION POINT: past it the commit continuation owns the stage's
// fate exclusively, and every other actor may only mark intent. Without that rule the six stage
// destroyers all reach a stage whose commit is mid-flight — `settleBarrierCommit` awaits `reconcile`,
// and any turn on another connection's chain runs inside that await. The observable damage is a
// COMMITTED delivered to a probe the server just terminated: the client flips onto a corpse and can
// only recover at its join deadline.
//
// The commit never parks, so the continuation completes in a bounded microtask chain — a barrier
// refuses `initial: true`, which is the one thing that could make `reconcile` await a registration.
// That is what makes owning the stage across the await safe rather than unbounded.
describe('only the commit continuation may release a committing stage', () => {
  const ordinaryClaim = (sessionId: string) => reconcileFrame({ sessionId, open: [{ id: 'A', ix: 0, lastSeq: 1 }] })

  test('a concurrent claim on the old session lands mid-commit and does not strand it', async () => {
    // SOL's reproduced merge blocker. The claim is delivered on a THIRD wire while the barrier's
    // continuation is suspended, so `releaseStagesForReconcile` reaches a `committing` stage.
    const h = (harness = createMuxHarness())
    const { s0 } = await connectSse(h)
    await h.ws.deliver(prepare(s0))

    const commit = h.sse.deliver(barrier(s0)) // deliberately un-awaited: the continuation is in flight
    const sse2 = h.makeWire('sse-conn-2')
    const claim = sse2.deliver(ordinaryClaim(s0))
    await Promise.all([commit, claim])

    expect(h.ws.terminated()).toBe(false) // ← the probe survives the claim
    const committed = reconciledOn(h.ws)
    expect(committed).toHaveLength(1) // ← COMMITTED reached a LIVE probe
    expect(h.ws.sessionId()).toBeTypeOf('string')
    // The continuation is the sole releaser, and it did release: no stage leaks past the commit.
    expect(h.mux._getUpgradeResourceSnapshot()).toEqual(EMPTY_STAGE)
    // The claim is not collateral either — it is ordinary traffic and must complete on its own wire.
    expect(reconciledOn(sse2)).toHaveLength(1)
  })

  test('the stage TTL handler refuses a committing stage', async () => {
    // The same rule reached through a timer rather than a peer wire — `abandonStage` is the TTL's
    // handler, so both interleavings share one guard.
    //
    // ⚠️ Honest scope: this interleaving is NOT reachable through the real event loop today. The
    // commit is a bounded microtask chain (a barrier refuses `initial: true`, which is the only
    // thing that could make `reconcile` park), and timer callbacks cannot land between microtasks.
    // The SYNCHRONOUS `advanceTimersByTime` below is what places the callback inside the suspended
    // continuation; `advanceTimersByTimeAsync` would drain the commit first and the row would assert
    // nothing. It is therefore a structural guard on the handler, not a reproduction — and what it
    // protects is the day someone introduces a park into the commit path. The reachable interleaving
    // is the concurrent claim above.
    vi.useFakeTimers()
    const h = (harness = createMuxHarness({ stageTtlMs: 50 }))
    const { s0 } = await connectSse(h)
    await h.ws.deliver(prepare(s0))

    const commit = h.sse.deliver(barrier(s0))
    await Promise.resolve() // the continuation is now suspended inside `reconcileSession`
    vi.advanceTimersByTime(51) // ← sync: the TTL callback runs while it is suspended
    await commit

    expect(h.ws.terminated()).toBe(false)
    expect(reconciledOn(h.ws)).toHaveLength(1)
    expect(h.ws.sessionId()).toBeTypeOf('string')
    expect(h.mux._getUpgradeResourceSnapshot()).toEqual(EMPTY_STAGE)
  })

  test('control: the same claim BEFORE the barrier still abandons the stage', async () => {
    // The discriminator. The guard must refuse only `committing` stages — a `staged` one is exactly
    // what the claim is supposed to destroy, and a guard that refused both would silently convert
    // every abandoned attempt into a leak.
    const h = (harness = createMuxHarness())
    const { s0 } = await connectSse(h)
    await h.ws.deliver(prepare(s0))

    const sse2 = h.makeWire('sse-conn-2')
    await sse2.deliver(ordinaryClaim(s0))

    expect(h.ws.terminated()).toBe(true)
    expect(h.mux._getUpgradeResourceSnapshot()).toEqual(EMPTY_STAGE)
  })
})

describe('a refused commit leaves the old session fully intact', () => {
  test('a commit that fails after the phase flip unwinds the retired flag', async () => {
    // B1b. `retiredByBarrier` is written BEFORE the commit is known to succeed, because the barrier
    // is the old wire's final frame only if the commit happens. When it does not, the flag is a
    // wire that refuses every semantic frame it is sent — the client's own recovery is an ordinary
    // reconcile, and that is precisely what the stale flag rejects. The old wire is then unusable
    // until the join deadline, for a commit the server never performed.
    const h = (harness = createMuxHarness())
    const { chA, s0 } = await connectSse(h)
    await h.ws.deliver(prepare(s0))

    // The probe dies INSIDE the commit window — after the phase flip, while `reconcileSession` is
    // suspended — which is the only way to reach a post-flip refusal: `reconcile`'s `state.closed`
    // check is the one throw downstream of `retiredByBarrier = true`. The single microtask hop is
    // what places the close there rather than before the barrier's turn (where it would merely clear
    // a `staged` record and the barrier would find nothing to commit).
    // Transient, which is what a probe socket dropping actually is: the channels keep their
    // `_onPeerDisconnect` grace, so the old wire's recovery reconcile can re-attach them. A
    // permanent close would detach them outright and the session would be gone for a reason that
    // has nothing to do with the flag under test.
    const commit = h.sse.deliver(barrier(s0))
    await Promise.resolve()
    h.ws.close(false)
    await commit

    // The client's recovery: an ordinary reconcile on the wire it still holds. This is the assertion
    // the stale flag breaks — the wire answers with a violation instead of a fresh session.
    await h.sse.deliver(reconcileFrame({ sessionId: s0, open: [{ id: 'A', ix: 0, lastSeq: 1 }] }))

    expect(h.sse.terminated()).toBe(false)
    expect(h.sse.sessionId()).not.toBe(s0) // ← the rotation succeeded
    await expectWireDelivers(h.sse, chA)
  })
})

describe('a stage never outlives what it depends on', () => {
  test('the TTL expires the stage and releases the probe, without disturbing the old session', async () => {
    // Confounder control: BOTH wires are PINGed across the window, because the mux's own ping deadline
    // is shorter than the stage TTL and the wires would otherwise die of unrelated liveness. PINGing
    // must NOT refresh the deadline — a client that pings forever while withholding its barrier would
    // hold a stage open for the socket's lifetime. The T-1s check is what makes the final assertion
    // mean something: a stage that expired immediately would satisfy it just as well.
    vi.useFakeTimers()
    const h = (harness = createMuxHarness())
    const { s0 } = await connectSse(h)
    await h.ws.deliver(prepare(s0))

    for (let elapsed = 0; elapsed < UPGRADE_STAGE_TTL_MS - 1_000; elapsed += 1_000) {
      await vi.advanceTimersByTimeAsync(1_000)
      await h.ws.deliver(pingFrame())
      await h.sse.deliver(pingFrame())
    }
    expect(h.mux._getUpgradeResourceSnapshot().records).toBe(1) // still staged at T-1s

    await vi.advanceTimersByTimeAsync(1_001)

    expect(h.mux._getUpgradeResourceSnapshot()).toEqual(EMPTY_STAGE)
    expect(h.sse.sessionId()).toBe(s0) // the old session is untouched — the client just falls back
    expect(h.sse.terminated()).toBe(false)
    // Expiry must also TERMINATE the probe: clearing the record alone leaves a session-less WS that
    // PING keeps alive indefinitely, waiting on a commit the server will never perform.
    expect(h.ws.terminated()).toBe(true)
  })

  test('the probe wire closing clears the stage — above the session-less early return', async () => {
    // The placement trap, and the leak detector for the whole cleanup family: a staged probe has NO
    // session id, so cleanup below `onConnectionClosed`'s `if (!sessionId) return` would never run
    // and every abandoned attempt would leak two map entries plus a live timer keyed by a dead
    // object, permanently. Repeated because a path that dropped only ONE of the two map entries, or
    // forgot the byte refund, shows up as drift across cycles even when one looks healthy.
    const h = (harness = createMuxHarness())
    const { chA, s0 } = await connectSse(h)

    for (let i = 0; i < 5; i++) {
      const probe = h.makeWire(null)
      await probe.deliver(prepare(s0, `upg-${i}`))
      expect(probe.sessionId()).toBeUndefined() // ← precondition of the trap, asserted not assumed
      expect(h.mux._getUpgradeResourceSnapshot().records).toBe(1)
      probe.close()
      expect(h.mux._getUpgradeResourceSnapshot()).toEqual(EMPTY_STAGE)
    }
    await expectWireDelivers(h.sse, chA)
  })

  test('the OLD wire closing releases the stage AND the probe that can no longer commit', async () => {
    // The missing sibling of the TTL / probe-close / rotation rows above, and the one cleanup path
    // that used to clear the RECORD without letting go of the PROBE. Two things go wrong when it
    // does. The probe is session-less, so nothing but PING keeps it alive — and PING it will,
    // forever, waiting on a commit that can never come. Worse, the record it just lost is also the
    // phase marker `handleFrame` reads: with it gone the `stagedUpgrades.has(connection)` guard no
    // longer fires, so the wire is not merely alive but ELIGIBLE, free to establish itself as an
    // ordinary WS session through a plain RECONCILE.
    const h = (harness = createMuxHarness())
    const { s0 } = await connectSse(h)
    await h.ws.deliver(prepare(s0))
    expect(h.mux._getUpgradeResourceSnapshot()).toMatchObject({ records: 1, reverseRecords: 1 })

    h.sse.close(true)

    expect(h.mux._getUpgradeResourceSnapshot()).toEqual(EMPTY_STAGE)
    expect(h.ws.terminated()).toBe(true)
    // Permanent, so the probe gets no reconnect grace: there is nothing left for it to come back to.
    expect(h.mux.consumePermanentTermination(h.ws.conn)).toBe(true)
  })

  test('a stage is released when its old wire rotates away from the staged session', async () => {
    // The reverse index is keyed by the session the stage was opened against, so once an ordinary
    // reconcile rotates the old wire S0 → S1 that key matches nothing the wire reports and close
    // cleanup (which looks up S1) walks straight past the record. The stage is already dead at that
    // point — equality would refuse its barrier — so it is released when the rotation is dispatched
    // rather than left for the TTL to reap.
    const h = (harness = createMuxHarness())
    const { s0 } = await connectSse(h)
    await h.ws.deliver(prepare(s0))

    await h.sse.deliver(reconcileFrame({ sessionId: s0, open: [{ id: 'A', ix: 0, lastSeq: 1 }] }))

    expect(h.sse.sessionId()).not.toBe(s0) // the rotation really happened
    expect(h.mux._getUpgradeResourceSnapshot()).toEqual(EMPTY_STAGE)
    expect(h.ws.terminated()).toBe(true) // the attempt can never commit, so the probe goes too
  })

  test('another connection CLAIMING the staged session releases it, and the original cannot commit', async () => {
    // The eager release above keys off the reconciling WIRE's session, but the destructive rotation is
    // driven by `ctrl.sessionId` — and those differ on a reconnect race. A replacement connection can
    // CLAIM the staged session while holding none of its own; the stage then survives something that
    // already consumed everything it depends on, and the original wire (which still reports S0, since
    // nothing wrote to its transport) sails through every equality leg to commit a SECOND rotation
    // with no FIN available.
    const h = (harness = createMuxHarness())
    const { s0 } = await connectSse(h)
    await h.ws.deliver(prepare(s0))

    const sse2 = h.makeWire('sse-conn-2')
    await sse2.deliver(reconcileFrame({ sessionId: s0, open: [{ id: 'A', ix: 0, lastSeq: 1 }] }))

    expect(h.mux._getUpgradeResourceSnapshot()).toEqual(EMPTY_STAGE)
    expect(h.ws.terminated()).toBe(true)

    expect(h.sse.sessionId()).toBe(s0) // STRING-equal on every leg it is checked against
    await h.sse.deliver(barrier(s0))

    expect(reconciledOn(h.ws)).toHaveLength(0) // ← zero COMMITTED
    expect(h.ws.sessionId()).toBeUndefined() // ← and no second rotation
    expect(finsOn(h.sse)).toHaveLength(0)
  })

  test('control: a session-less reconcile from another connection leaves the stage alone', async () => {
    // THE DISCRIMINATOR, and what stops the release from over-firing. A reconcile naming NO session
    // destroys nothing — `reconcileSession` skips the whole previous-session block, so S0 keeps its
    // registry entry AND its finalizer and a later commit is still coherent. Clearing the stage here
    // would abort a healthy upgrade every time an unrelated connection attached by channel id.
    const h = (harness = createMuxHarness())
    const { s0 } = await connectSse(h)
    await h.ws.deliver(prepare(s0))

    const sse2 = h.makeWire('sse-conn-2')
    await sse2.deliver(reconcileFrame({ open: [{ id: 'A', ix: 0, lastSeq: 1, initial: true }] }))

    expect(h.mux._getUpgradeResourceSnapshot()).toMatchObject({ records: 1, reverseRecords: 1 })
    expect(h.ws.terminated()).toBe(false)

    await h.sse.deliver(barrier(s0)) // and the upgrade still commits, coherently

    expect(reconciledOn(h.ws)).toHaveLength(1)
    expect(finsOn(h.sse)).toHaveLength(1)
  })

  // delivers straight to `onConnectionRawMessage` and never reaches them.
})

describe('through the real sse.ts', () => {
  test('COMMITTED still reaches the probe wire when the old connection closes mid-batch', async () => {
    // Both batch entry points suppress their reconciled when the connection that carried the frames
    // has closed. Right for an ordinary reconcile — the answer is bound for that very wire. WRONG for
    // a barrier commit: its answer is bound for the STAGED probe, and the old connection closing is
    // not an error but the expected end of the flow. Suppressing there leaves the server rotated and
    // the client never told.
    const connId = 'batch-barrier-1'
    const channelId = 'batch-barrier-ch-1'
    globalRegisterChannel(channelId)
    const s0 = await openSseDownstream(connId, channelId)

    const probe = openGlobalProbeWire()
    await probe.deliver(encode.prepare({ upgradeId: 'upg-1', sessionId: s0, open: [{ id: channelId, ix: 0 }] }))
    expect(probe.sent.filter((f) => f.tag === TAG.READY)).toHaveLength(1)

    // A batch POST whose body is held open, so the close below lands mid-drain.
    let pushBody!: (chunk: Uint8Array<ArrayBuffer>) => void
    let endBody!: () => void
    const stream = new ReadableStream<Uint8Array<ArrayBuffer>>({
      start(controller) {
        pushBody = (chunk) => controller.enqueue(chunk)
        endBody = () => controller.close()
      },
    })
    const post = handleSseChannelRequest(
      new Request('http://test.local/_telefunc', { method: 'POST', body: stream, duplex: 'half' } as RequestInit),
    )
    pushBody(encodeSseRequestMetadata({ connId }))
    pushBody(
      lengthPrefixed(
        encode.reconcile({
          sessionId: s0,
          barrier: true,
          upgradeId: 'upg-1',
          open: [{ id: channelId, ix: 0, lastSeq: 0 }],
        }),
      ),
    )
    // The probe now holds the new session, but its COMMITTED is not sent until the POST decides to.
    for (let i = 0; i < 1_000 && typeof probe.sessionId() !== 'string'; i++) await settle()
    expect(probe.sessionId()).toBeTypeOf('string')
    expect(probe.sessionId()).not.toBe(s0)
    expect(probe.sent.filter((f) => f.tag === TAG.RECONCILED)).toHaveLength(0) // ...not yet sent

    // The old connection dies before the POST resolves: a client reconnect on the same connId, which
    // is exactly what closes an existing connection. Real and racing, not simulated.
    await openSseDownstream(connId, channelId)
    endBody()

    expect((await post)?.statusCode).toBe(200)
    const committed = probe.sent.filter((f) => f.tag === TAG.RECONCILED)
    expect(committed).toHaveLength(1)
    expect(committed[0]?.tag === TAG.RECONCILED && committed[0].payload.upgradeId).toBe('upg-1')
  })

  test('control: an ORDINARY reconcile in a batch POST is never routed to the probe wire', async () => {
    // Without this the row above is satisfied by a batch path that sent EVERY reconciled to whatever
    // probe happened to exist — `deliverTo` being set is what routes a commit, and nothing else may.
    const connId = 'batch-barrier-2'
    const channelId = 'batch-barrier-ch-2'
    globalRegisterChannel(channelId)
    const s0 = await openSseDownstream(connId, channelId)
    const probe = openGlobalProbeWire()
    await probe.deliver(encode.prepare({ upgradeId: 'upg-2', sessionId: s0, open: [{ id: channelId, ix: 0 }] }))

    const body = new Blob([
      encodeSseRequestMetadata({ connId }),
      // Same wire, same channel, same session — everything the barrier had EXCEPT the flag.
      lengthPrefixed(encode.reconcile({ sessionId: s0, open: [{ id: channelId, ix: 0, lastSeq: 0 }] })),
    ])
    const response = await handleSseChannelRequest(new Request('http://test.local/_telefunc', { method: 'POST', body }))

    expect(response?.statusCode).toBe(200)
    expect(probe.sent.filter((f) => f.tag === TAG.RECONCILED)).toHaveLength(0)
    expect(probe.sessionId()).toBeUndefined() // the ordinary reconcile rotated ITS OWN wire
  })

  test('the streamRequest upload POST does not resolve while a frame it carried is still parked', async () => {
    // The loop deliberately does not await each dispatch (a slow turn must never stall the body), but
    // `void`ing the promises made body-end and processing-end unrelated events: the POST could resolve
    // 200 with a frame still in `recvChain`. Worthless unless something is genuinely parked at EOF —
    // with an empty chain the response resolves in a microtask either way and the unfixed code passes.
    const connId = crypto.randomUUID()
    const chA = globalRegisterChannel(`A-${connId}`)
    await openSseDownstream(connId, chA.id)

    let controller!: ReadableStreamDefaultController<Uint8Array<ArrayBuffer>>
    const body = new ReadableStream<Uint8Array<ArrayBuffer>>({ start: (c) => (controller = c) })
    controller.enqueue(encodeSseRequestMetadata({ connId, streamRequest: true }))
    let resolved = false
    const response = handleSseChannelRequest(
      new Request('http://test.local/_telefunc', { method: 'POST', body, duplex: 'half' } as RequestInit),
    ).then((r) => {
      resolved = true
      return r
    })

    const parkedId = `B-${connId}`
    controller.enqueue(
      lengthPrefixed(
        encode.reconcile({
          open: [
            { id: chA.id, ix: 0, lastSeq: 0 },
            { id: parkedId, ix: 1, lastSeq: 0, initial: true },
          ],
        }),
      ),
    )
    controller.close()

    // INSTRUMENT CHECK: the body is finished, the event loop has turned, the response is still
    // pending. This is what makes the assertion after the release meaningful.
    await settle()
    await settle()
    expect(resolved).toBe(false)

    globalRegisterChannel(parkedId) // registration waiters fire synchronously

    expect((await response)?.statusCode).toBe(200)
  })
})
