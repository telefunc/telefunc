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

type Harness = ReturnType<typeof createMuxHarness>
type HarnessWire = Harness['sse']

let harness: Harness | null = null
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

const prepare = (sessionId: string, upgradeId = 'upg-1') => prepareFrame({ upgradeId, sessionId })

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

  // ── Staging admission: one rule per case, one oracle for all of them ──────────────────────────
  //
  // Every case ends the same way — the SENDER loses its wire, no READY was issued to it, the stage
  // accounting is what the case says it is, and a bystander connection still routes to its real
  // listener. That last part is the independent one: without it "the probe was terminated" is
  // satisfied by an implementation that tore down both wires, which is exactly what a pre-barrier
  // refusal must never do.
  type StagingCase = {
    name: string
    /** Delivers the offending frame; returns the wire that sent it. */
    offend: (h: Harness, s0: string) => Promise<HarnessWire>
    /** Stage accounting AFTER the refusal. */
    snapshot: typeof EMPTY_STAGE | { records: number; reverseRecords: number; bytes: unknown }
    /** READYs on the offending wire. Zero unless an EARLIER, accepted PREPARE legitimately earned
     *  one — the invariant is that the REFUSED frame produced none, not that the wire never got any. */
    readys?: number
    /** Anything true of this case alone. */
    extra?: (h: Harness, s0: string) => Promise<void>
  }

  const STAGING_CASES: StagingCase[] = [
    {
      // THE DISPATCH HOLE: RECONCILE is handled ABOVE the session-less guard, so without the explicit
      // staged-wire check this runs the destructive rotation and steps around the stage entirely.
      name: 'a plain RECONCILE on a staged probe wire — it never rotates',
      offend: async (h, s0) => {
        await h.ws.deliver(prepare(s0))
        await h.ws.deliver(reconcileFrame({ sessionId: s0, open: [{ id: 'A', ix: 0, lastSeq: 1 }] }))
        return h.ws
      },
      snapshot: EMPTY_STAGE,
      readys: 1, // from the accepted PREPARE that staged it
      extra: async (h) => {
        expect(h.ws.sessionId()).toBeUndefined() // ← the rotation the guard exists to prevent
        expect(reconciledOn(h.ws)).toHaveLength(0)
      },
    },
    {
      // Staging is otherwise willing to pin memory and a socket against any string a client invents:
      // the session id is only ever compared for EQUALITY, at barrier time, so a junk one produces a
      // record that can never commit and is reaped only by its TTL.
      name: 'a PREPARE naming a session the server does not hold [B4]',
      offend: async (h) => {
        await h.ws.deliver(prepare('no-such-session'))
        return h.ws
      },
      snapshot: EMPTY_STAGE,
    },
    {
      // A stage pairs a SESSIONLESS probe with the old session it may commit against. On a wire that
      // already owns a session the record would name that wire as both sides, and the commit would
      // rotate a session onto the wire it was rotating away from.
      name: 'a PREPARE on a wire that already holds a session [T4-M13]',
      offend: async (h, s0) => {
        await h.sse.deliver(prepare(s0)) // ← the OLD wire, which holds s0
        return h.sse
      },
      snapshot: EMPTY_STAGE,
    },
    {
      // One stage per old session, enforced separately from one-stage-per-probe-wire: this arrives on
      // a DIFFERENT, perfectly valid probe. Without it two probes race to commit the same session and
      // the loser's stage survives to its TTL holding a socket the client has forgotten.
      name: 'a second stage for the SAME old session [T4-M18]',
      offend: async (h, s0) => {
        await h.ws.deliver(prepare(s0))
        expect(readysOn(h.ws)).toHaveLength(1)
        const ws2 = h.makeWire('ws-probe-2')
        await ws2.deliver(prepare(s0, 'upg-2'))
        return ws2
      },
      snapshot: { records: 1, reverseRecords: 1, bytes: expect.any(Number) },
      extra: async (h, s0) => {
        // The INCUMBENT is untouched — refusing the newcomer must never evict the stage in progress —
        // and it can still commit, which is the property an eviction bug would have destroyed.
        expect(h.ws.terminated()).toBe(false)
        await h.sse.deliver(barrier(s0))
        expect(reconciledOn(h.ws)).toHaveLength(1)
      },
    },
    {
      // Without the guard the stage is silently re-installed (leaking the first timer) and a SECOND
      // READY goes out, so the client could act on an attempt the server re-keyed underneath it.
      name: 'a second PREPARE on the same probe wire — never a re-stage',
      offend: async (h, s0) => {
        await h.ws.deliver(prepare(s0, 'upg-1'))
        await h.ws.deliver(prepare(s0, 'upg-2'))
        return h.ws
      },
      snapshot: EMPTY_STAGE,
      readys: 1, // exactly one: the SECOND PREPARE must not have produced its own
      extra: async (h) => {
        // The FIRST attempt's READY stands; the refusal did not re-key it.
        expect(readysOn(h.ws)[0]?.tag === TAG.READY && readysOn(h.ws)[0]!.payload.upgradeId).toBe('upg-1')
      },
    },
  ]

  for (const stagingCase of STAGING_CASES) {
    test(`refused at admission: ${stagingCase.name}`, async () => {
      const h = (harness = createMuxHarness())
      const { chA, s0 } = await connectSse(h)
      // A bystander opened BEFORE the violation, so it is a witness rather than a retry.
      const sse2 = h.makeWire('sse-conn-2')
      const chB = h.registerChannel('B')
      await sse2.deliver(reconcileFrame({ open: [{ id: 'B', ix: 0, lastSeq: 0, initial: true }] }))

      const offender = await stagingCase.offend(h, s0)

      expect(offender.terminated()).toBe(true)
      expect(readysOn(offender)).toHaveLength(stagingCase.readys ?? 0)
      expect(h.mux._getUpgradeResourceSnapshot()).toEqual(stagingCase.snapshot)
      await stagingCase.extra?.(h, s0)
      // The live sentinel, on a connection that was never party to any of this.
      expect(sse2.terminated()).toBe(false)
      await sse2.deliver(textFrame(0, 1, 4_242))
      expect(chB.received).toContain(4_242)
      void chA
    })
  }
})

describe('the barrier commits the staged wire, exactly once', () => {
  test('one integrated commit: COMMITTED to the probe, ONE new session, FIN on the old wire only', async () => {
    // The happy path in one place, because its four claims are four readings of a SINGLE commit and
    // splitting them let each one be true of a different run.
    //
    //   1. routing   — `deliverTo`: the turn ran on the SSE chain, the answer went to the WS.
    //   2. identity  — COMMITTED *is* a RECONCILED (reusing the payload keeps the client's settlement
    //                  path unforked), discriminated ONLY by the echoed upgradeId.
    //   3. rotation  — exactly ONE new session, counted across BOTH wires so a second rotation
    //                  anywhere shows up as a third minted id.
    //   4. FIN       — count 1 on the old wire, 0 on the new. The count is the sharp edge: firing the
    //                  finalizer early shows up as a SECOND FIN, and a client joining FIN with
    //                  COMMITTED then settles against the wrong one.
    const h = (harness = createMuxHarness())
    const { chA, s0 } = await connectSse(h)
    await h.ws.deliver(prepare(s0, 'upg-7'))

    await h.sse.deliver(barrier(s0, 'upg-7'))
    await settle()

    // 1 + 2
    const committed = reconciledOn(h.ws)
    expect(committed).toHaveLength(1)
    expect(committed[0]?.tag === TAG.RECONCILED && committed[0].payload.upgradeId).toBe('upg-7')
    expect(reconciledOn(h.sse)).toHaveLength(1)
    expect(reconciledOn(h.sse)[0]?.tag === TAG.RECONCILED && reconciledOn(h.sse)[0]!.payload.upgradeId).toBeUndefined()

    // 3
    const minted = [...h.sse.sent, ...h.ws.sent]
      .filter((f) => f.tag === TAG.RECONCILED)
      .map((f) => (f.tag === TAG.RECONCILED ? f.payload.sessionId : ''))
    expect(minted).toHaveLength(2) // S0 from the connect, S1 from the commit
    expect(new Set(minted).size).toBe(2)
    expect(minted[0]).toBe(s0)
    expect(minted[1]).toBe(h.ws.sessionId())
    expect(h.ws.sessionId()).not.toBe(s0)

    // 4
    expect(finsOn(h.sse)).toHaveLength(1)
    expect(finsOn(h.ws)).toHaveLength(0)

    // The stage is spent, and the channel now really routes to the wire that won it.
    expect(h.mux._getUpgradeResourceSnapshot()).toEqual(EMPTY_STAGE)
    await expectWireDelivers(h.ws, chA)
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
  // Two checks, two distinct defects, and a third case pinning WHICH WIRE dies. Every case refuses
  // before any rotation, so the shared oracle is: no COMMITTED, the probe never gained a session, the
  // old wire kept s0, and the old wire still delivers.
  const REFUSAL_CASES: {
    name: string
    /** Delivers the poison barrier. */
    poison: (h: Harness, s0: string) => Promise<void>
    extra?: (h: Harness) => void
  }[] = [
    {
      // upgradeId closes STALE SETTLEMENT: a delayed ordinary reconciled consumed as this commit.
      name: 'an upgradeId that does not match the stage',
      poison: async (h, s0) => {
        await h.sse.deliver(barrier(s0, 'upg-OTHER'))
      },
    },
    {
      // Live-session equality closes CONCURRENT ROTATION: here the upgradeId matches perfectly and
      // the frame is still poison. Committing a copy landing elsewhere would rotate a session the
      // naming wire does not hold — no FIN sent, and the real old session left holding handles to
      // channels re-attached to the probe.
      name: 'a correct upgradeId arriving on a wire that does not own the staged session',
      poison: async (h, s0) => {
        const sse2 = await connectSecondSse(h)
        await sse2.deliver(barrier(s0, 'upg-1'))
      },
      extra: (h) => {
        expect(finsOn(h.sse)).toHaveLength(0)
      },
    },
    {
      // WHICH WIRE DIES is a distinct invariant, and this barrier fails BOTH checks at once
      // deliberately: that makes it insensitive to either check individually and sensitive ONLY to
      // the choice of victim. With the retarget removed the failure kills the connection whose chain
      // merely HOSTED the turn, and a bystander loses its session to a probe's misbehaviour.
      name: 'a barrier failing BOTH checks kills the PROBE, never the wire that hosted the turn',
      poison: async (h, s0) => {
        const sse2 = await connectSecondSse(h)
        await sse2.deliver(barrier(s0, 'upg-WRONG'))
        expect(sse2.terminated()).toBe(false) // ← the victim choice, asserted where the wire is in scope
      },
      extra: (h) => {
        expect(h.ws.terminated()).toBe(true)
        expect(h.sse.terminated()).toBe(false)
      },
    },
  ]

  for (const refusal of REFUSAL_CASES) {
    test(`refused before any rotation: ${refusal.name}`, async () => {
      const h = (harness = createMuxHarness())
      const { chA, s0 } = await connectSse(h)
      await h.ws.deliver(prepare(s0, 'upg-1'))

      await refusal.poison(h, s0)

      expect(reconciledOn(h.ws)).toHaveLength(0)
      expect(h.ws.sessionId()).toBeUndefined()
      expect(h.sse.sessionId()).toBe(s0)
      refusal.extra?.(h)
      await expectWireDelivers(h.sse, chA)
    })
  }

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

// The hostile-SHAPE table lives at the decode seam (`upgrade.wire-codec.spec.ts`), where the refusal
// now happens. What has to be gated HERE is the end-to-end consequence the shape table cannot see:
// that a refused frame really kills a wire, that it kills the RIGHT one, and that nothing it touched
// on the way is left rotated or released.
describe('a malformed reconcile dies end-to-end, and takes only its sender with it', () => {
  test('a barrier-shaped frame that fails the seam costs its sender the wire and nothing else', async () => {
    const h = (harness = createMuxHarness())
    const { chA, s0 } = await connectSse(h)
    await h.ws.deliver(prepare(s0, 'upg-1'))
    expect(h.mux._getUpgradeResourceSnapshot()).toMatchObject({ records: 1 })

    // `barrier: false` with a valid upgradeId: the shape that used to fall through to the ordinary
    // path and rotate the session destructively. One representative is enough — the seam refuses the
    // whole family identically, and the codec table is what enumerates it.
    await h.sse.deliver(
      reconcileFrame({
        sessionId: s0,
        barrier: false,
        upgradeId: 'upg-1',
        open: [{ id: 'A', ix: 0, lastSeq: 1 }],
      } as unknown as Parameters<typeof reconcileFrame>[0]),
    )

    expect(h.sse.terminated()).toBe(true) // the malformed frame costs its SENDER the wire
    expect(h.sse.sessionId()).toBe(s0) // ← no rotation: it never reached `reconcile`
    expect(reconciledOn(h.ws)).toHaveLength(0) // ← and no commit
    expect(finsOn(h.sse)).toHaveLength(0)
    expect(h.ws.terminated()).toBe(false) // ← the staged probe is not collateral
    expect(h.mux._getUpgradeResourceSnapshot()).toMatchObject({ records: 1 }) // the stage survives
    void chA
  })

  test('control: the well-formed shapes on either side of the check still take their own path', async () => {
    // Without this the row above is satisfied just as well by a seam that rejects EVERY reconcile.
    // Both legal shapes run here, in sequence, on the same connection: an ordinary reconcile
    // rotates, and a real barrier commits.
    const h = (harness = createMuxHarness())
    const { s0 } = await connectSse(h)
    await h.ws.deliver(prepare(s0, 'upg-1'))

    await h.sse.deliver(reconcileFrame({ sessionId: s0, open: [{ id: 'A', ix: 0, lastSeq: 1 }] }))
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

  test('the probe dying INSIDE the commit window leaves no stage behind [T4-I8r4]', async () => {
    // The third destroyer of the same window, and the one the guard deliberately does NOT cover:
    // probe-close accounting clears the record raw. That is safe only because the close is recorded
    // before the entry is deleted, so the suspended continuation still unwinds into its `finally`.
    // What this pins is the OUTCOME of that reasoning — no leak, no throw, no half-released stage —
    // rather than the reasoning itself.
    //
    // Restored: no row closed a wire inside the commit's async window at all.
    const h = (harness = createMuxHarness())
    const { chA, s0 } = await connectSse(h)
    await h.ws.deliver(prepare(s0))

    const commit = h.sse.deliver(barrier(s0)) // un-awaited: the continuation is in flight
    h.ws.close() // ← the probe dies mid-commit
    await commit

    // The accounting is the assertion: a stage that survives its own probe is a leak the TTL would
    // eventually reap while holding a dead socket's record until then.
    expect(h.mux._getUpgradeResourceSnapshot()).toEqual(EMPTY_STAGE)
    // The old wire was retired by the barrier either way, so the client's recovery is a fresh
    // reconcile — but nothing here may take the whole mux down with it.
    const sse2 = h.makeWire('sse-conn-2')
    await sse2.deliver(reconcileFrame({ sessionId: s0, open: [{ id: 'A', ix: 0, lastSeq: 1 }] }))
    expect(sse2.terminated()).toBe(false)
    await expectWireDelivers(sse2, chA)
  })

  test('the old wire dying AFTER a successful commit leaves the new session alive [T4-I8r6]', async () => {
    // The mirror of the old-wire-close row in the lifecycle block, on the other side of the commit.
    // Before the commit, an old-wire close must release the stage and the probe; after it, the same
    // close must touch nothing — the probe now owns the session, and tearing it down here would
    // undo the upgrade at the exact moment it succeeded. One handler, two opposite duties, and only
    // the pre-commit half had a row.
    const h = (harness = createMuxHarness())
    const { chA, s0 } = await connectSse(h)
    await h.ws.deliver(prepare(s0))
    await h.sse.deliver(barrier(s0))
    const upgradedSession = h.ws.sessionId()
    expect(upgradedSession).toBeTypeOf('string')
    expect(upgradedSession).not.toBe(s0)

    h.sse.close() // ← the retired wire goes away, as it must once the client sees FIN
    await settle()

    // The new session survives its predecessor's death, and the channel still routes to it.
    expect(h.ws.terminated()).toBe(false)
    expect(h.ws.sessionId()).toBe(upgradedSession)
    await expectWireDelivers(h.ws, chA)
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
    expect(h.mux.readPermanentTermination(h.ws.conn)).toBe(true)
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
    await probe.deliver(encode.prepare({ upgradeId: 'upg-1', sessionId: s0 }))
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
    await probe.deliver(encode.prepare({ upgradeId: 'upg-2', sessionId: s0 }))

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

  test('a TRUNCATED streamRequest body drains its parked frames before the POST resolves', async () => {
    // The exit that matters most: a client whose upload body dies mid-frame is the one about to
    // reconnect, so a turn left half-applied here is what the reconnect then reconciles against.
    // Draining only on clean EOF made the contract hold exactly where it was least needed. The
    // control for this row is the clean-EOF row above — same setup, same park, other exit.
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
    // The frame has to be read — and its turn parked — before the body dies, or this row would
    // prove nothing about draining.
    await settle()
    await settle()

    // Truncation is specifically MID-FRAME: a length prefix that promises more bytes than ever
    // arrive. A body that merely stops at a frame boundary is a clean EOF as far as `StreamReader`
    // is concerned (`pullChunk` reports a fault as end-of-stream), and would take the exit the row
    // above already covers.
    controller.enqueue(lengthPrefixed(encode.ping()).subarray(0, 6) as Uint8Array<ArrayBuffer>)
    controller.close()

    // INSTRUMENT CHECK: the read loop has already thrown and the event loop has turned, yet the
    // response is still pending because the frame it carried is still parked.
    await settle()
    await settle()
    expect(resolved).toBe(false)

    globalRegisterChannel(parkedId)

    // 400, not 200: the truncation is a real read failure that reached `handleRequest`'s sink.
    expect((await response)?.statusCode).toBe(400)
  })
})
