// I8 — delivery within one lane is attempt-ordered (frame N+1's attempt begins only after frame N's
// attempt settled), and cross-lane attempts are independent. The exact chain algorithm is
// readiness-ordering.md §3; its normative semantics 1-6 are the scenarios below.
//
// The head-of-line and failure scenarios need a target that can be made slow or made to fail, which the
// contract declares a per-backend trace rather than a common guarantee (I5). Those scenarios are gated on
// the trace; the ordering scenarios that every backend must satisfy are not.

import { afterEach, beforeEach, describe, expect, it } from 'vitest'
import { type BackendFixture, installedBackends } from './harness.js'
import {
  accepted,
  binaryLane,
  bytes,
  collector,
  CONTROL,
  enterClosing,
  finalizeClose,
  inboxLane,
  nextId,
  okHead,
  openRoom,
  readHeadOrThrow,
  SEMANTIC,
  settled,
  stallingReceiver,
  throwingReceiver,
} from './scenario.js'

for (const harness of installedBackends) {
  describe(`ordering — ${harness.name}`, () => {
    let fx: BackendFixture
    let roomId: string
    let inc: string

    beforeEach(async () => {
      fx = await harness.create()
      roomId = nextId('room')
      const opened = await openRoom(fx.backend, roomId)
      inc = opened.inc
    })

    afterEach(async () => {
      await fx.dispose()
    })

    it('keeps one domain cursor monotonic across time changes, inactivity, maintenance and reconstruction', async () => {
      fx.orderControl.setAuthority(1_000)
      const first = accepted(await fx.backend.commitLane(roomId, inc, SEMANTIC, bytes('one')))
      const second = accepted(await fx.backend.commitLane(roomId, inc, SEMANTIC, bytes('two')))
      expect([first.timestamp, first.seq]).toEqual([1_000, 1])
      expect([second.timestamp, second.seq]).toEqual([1_000, 2])

      fx.orderControl.setAuthority(2_000)
      const later = accepted(await fx.backend.commitLane(roomId, inc, SEMANTIC, bytes('later')))
      expect([later.timestamp, later.seq]).toEqual([2_000, 3])

      fx.orderControl.setAuthority(1_500)
      const rollback = accepted(await fx.backend.commitLane(roomId, inc, SEMANTIC, bytes('rollback')))
      expect([rollback.timestamp, rollback.seq]).toEqual([2_000, 4])

      fx.advanceAuthority(7 * 24 * 60 * 60 * 1_000)
      await fx.orderControl.runMaintenance(roomId)
      const afterMaintenance = accepted(await fx.backend.commitLane(roomId, inc, SEMANTIC, bytes('after-maintenance')))
      expect(afterMaintenance.seq).toBe(5)

      await fx.orderControl.reconstructBackend(roomId)
      const afterReconstruction = accepted(
        await fx.backend.commitLane(roomId, inc, SEMANTIC, bytes('after-reconstruction'), { retain: true }),
      )
      expect(afterReconstruction.seq).toBe(6)
      expect(await fx.backend.readRetained(roomId, inc, SEMANTIC)).toMatchObject({
        seq: 6,
        timestamp: afterReconstruction.timestamp,
      })
      expect(accepted(await fx.backend.commitLane(roomId, inc, SEMANTIC, bytes('after-replay'))).seq).toBe(7)
    })

    it('round-trips a Room position above the unsigned-32-bit boundary through live and retained paths', async () => {
      const latch = collector()
      const sub = fx.backend.subscribeLane(roomId, inc, SEMANTIC, latch.receiver)
      await sub.ready
      await fx.orderControl.seedWatermark(roomId, inc, SEMANTIC, 0xffff_ffff, fx.authorityNow())

      const committed = accepted(
        await fx.backend.commitLane(roomId, inc, SEMANTIC, bytes('wide-sequence'), { retain: true }),
      )
      await committed.delivery
      await latch.waitFor(1)
      const expected = 0x1_0000_0000
      expect(committed.seq).toBe(expected)
      expect(latch.frames[0]?.seq).toBe(expected)
      expect(await fx.backend.readRetained(roomId, inc, SEMANTIC)).toMatchObject({ seq: expected })
      await sub.unsubscribe()
    })

    it('rejects safe-integer exhaustion before retained or callback effects', async () => {
      const latch = collector()
      const sub = fx.backend.subscribeLane(roomId, inc, SEMANTIC, latch.receiver)
      await sub.ready
      await fx.orderControl.seedWatermark(roomId, inc, SEMANTIC, Number.MAX_SAFE_INTEGER, fx.authorityNow())

      await expect(
        fx.backend.commitLane(roomId, inc, SEMANTIC, bytes('must-not-commit'), { retain: true }),
      ).rejects.toThrow('commitLane: sequence exhausted for the ordering domain')
      expect(await fx.backend.readRetained(roomId, inc, SEMANTIC)).toBeNull()
      expect(latch.frames).toEqual([])
      await expect(fx.backend.commitLane(roomId, inc, SEMANTIC, bytes('still-exhausted'))).rejects.toThrow(
        'commitLane: sequence exhausted for the ordering domain',
      )
      expect(latch.frames).toEqual([])
      await sub.unsubscribe()
    })

    it('delivers a lane in seq order', async () => {
      const latch = collector()
      const sub = fx.backend.subscribeLane(roomId, inc, SEMANTIC, latch.receiver)
      await sub.ready

      const expected: string[] = []
      for (let n = 0; n < 100; n++) {
        expected.push(`frame-${n}`)
        accepted(await fx.backend.commitLane(roomId, inc, SEMANTIC, bytes(`frame-${n}`)))
      }
      await latch.waitFor(100)
      expect(latch.payloads()).toEqual(expected)
      expect(latch.frames.map((frame) => frame.seq)).toEqual(latch.frames.map((_, index) => index + 1))
    })

    it('settles delivery promises in commit order on one lane', async () => {
      const latch = collector()
      const sub = fx.backend.subscribeLane(roomId, inc, SEMANTIC, latch.receiver)
      await sub.ready

      const order: number[] = []
      const attempts = []
      for (let n = 0; n < 10; n++) {
        const result = accepted(await fx.backend.commitLane(roomId, inc, SEMANTIC, bytes(`frame-${n}`)))
        attempts.push(result.delivery.then(() => order.push(n)))
      }
      await Promise.all(attempts)
      expect(order).toEqual([0, 1, 2, 3, 4, 5, 6, 7, 8, 9])
    })

    it('keeps each lane on its own chain — a stalled lane never reorders another', async () => {
      // KILLER: sharing one chain across lanes turns this red.
      if (!fx.traces.handoffAwaitsReceiver) return
      const stall = stallingReceiver()
      const stalled = fx.backend.subscribeLane(roomId, inc, SEMANTIC, stall.receiver)
      const other = collector()
      const free = fx.backend.subscribeLane(roomId, inc, CONTROL, other.receiver)
      await Promise.all([stalled.ready, free.ready])

      const blocked = accepted(await fx.backend.commitLane(roomId, inc, SEMANTIC, bytes('stuck')))
      const independent = accepted(await fx.backend.commitLane(roomId, inc, CONTROL, bytes('free')))

      // the control lane delivers while the semantic lane's attempt is still in flight
      await independent.delivery
      await other.waitFor(1)
      expect(other.payloads()).toEqual(['free'])

      await stall.release()
      await blocked.delivery
    })

    it('starts frame N+1 only after frame N settled on the same lane', async () => {
      if (!fx.traces.handoffAwaitsReceiver) return
      const entries: string[] = []
      const stall = stallingReceiver(() => entries.push('enter'))
      const sub = fx.backend.subscribeLane(roomId, inc, SEMANTIC, stall.receiver)
      await sub.ready

      const one = accepted(await fx.backend.commitLane(roomId, inc, SEMANTIC, bytes('one')))
      const two = accepted(await fx.backend.commitLane(roomId, inc, SEMANTIC, bytes('two')))
      await stall.waitForEntry()
      expect(entries).toEqual(['enter']) // frame two's attempt has not begun

      await stall.release()
      await Promise.all([one.delivery, two.delivery])
      expect(entries).toEqual(['enter', 'enter'])
    })

    it('does not let a failed frame poison the lane, and rejects only its own promise', async () => {
      // KILLER: gating the chain on the attempt itself rather than on its SETTLEMENT turns this red.
      if (!fx.traces.perTargetFailure) return
      const seen = collector()
      const failing = throwingReceiver('handoff failed', 'bad')
      const badSub = fx.backend.subscribeLane(roomId, inc, SEMANTIC, failing.receiver)
      const sub = fx.backend.subscribeLane(roomId, inc, SEMANTIC, seen.receiver)
      await Promise.all([badSub.ready, sub.ready])

      const bad = accepted(await fx.backend.commitLane(roomId, inc, SEMANTIC, bytes('bad')))
      const good = accepted(await fx.backend.commitLane(roomId, inc, SEMANTIC, bytes('good')))

      expect(await settled(bad.delivery)).toBe('rejected')
      expect(await settled(good.delivery)).toBe('resolved')
      expect(seen.payloads()).toEqual(['bad', 'good'])
      expect(failing.calls()).toBe(2)
    })

    it('interleaves independent lanes without cross-contaminating their order', async () => {
      const semantic = collector()
      const inbox = collector()
      const binary = collector()
      const subs = [
        fx.backend.subscribeLane(roomId, inc, SEMANTIC, semantic.receiver),
        fx.backend.subscribeLane(roomId, inc, inboxLane('alice'), inbox.receiver),
        fx.backend.subscribeLane(roomId, inc, binaryLane('alice', 'cam'), binary.receiver),
      ]
      await Promise.all(subs.map((sub) => sub.ready))

      for (let n = 0; n < 5; n++) {
        accepted(await fx.backend.commitLane(roomId, inc, SEMANTIC, bytes(`s${n}`)))
        accepted(await fx.backend.commitLane(roomId, inc, inboxLane('alice'), bytes(`i${n}`)))
        accepted(await fx.backend.commitLane(roomId, inc, binaryLane('alice', 'cam'), bytes(`b${n}`)))
      }
      await Promise.all([semantic.waitFor(5), inbox.waitFor(5), binary.waitFor(5)])

      expect(semantic.payloads()).toEqual(['s0', 's1', 's2', 's3', 's4'])
      expect(inbox.payloads()).toEqual(['i0', 'i1', 'i2', 'i3', 'i4'])
      expect(binary.payloads()).toEqual(['b0', 'b1', 'b2', 'b3', 'b4'])
      for (const latch of [semantic, inbox, binary]) {
        expect(latch.frames.map((frame) => frame.seq)).toEqual([1, 2, 3, 4, 5])
      }
      await Promise.all(subs.map((sub) => sub.unsubscribe()))
    })

    it('starts clean chains in a new incarnation — no chain state survives a recreation', async () => {
      if (!fx.traces.handoffAwaitsReceiver) return
      const stall = stallingReceiver()
      const stalled = fx.backend.subscribeLane(roomId, inc, SEMANTIC, stall.receiver)
      await stalled.ready
      const stuck = accepted(await fx.backend.commitLane(roomId, inc, SEMANTIC, bytes('stuck-in-old-gen')))

      const head = await readHeadOrThrow(fx.backend, roomId)
      const { head: closing, leaseId } = await enterClosing(fx.backend, roomId, head)
      accepted(await fx.backend.commitLane(roomId, inc, CONTROL, bytes('closed'), { closingLease: leaseId }))
      const tombstone = okHead(await finalizeClose(fx.backend, roomId, closing, leaseId))
      const recreated = await openRoom(fx.backend, roomId, { prior: tombstone })

      // the new incarnation's chain is not behind the old generation's stalled attempt
      const latch = collector()
      const fresh = fx.backend.subscribeLane(roomId, recreated.inc, SEMANTIC, latch.receiver)
      await fresh.ready
      const recreatedCommit = accepted(await fx.backend.commitLane(roomId, recreated.inc, SEMANTIC, bytes('new-gen')))
      expect(recreatedCommit.seq).toBe(1)
      await recreatedCommit.delivery
      expect(latch.payloads()).toEqual(['new-gen'])

      await stall.release()
      await stuck.delivery
    })

    it("discards a dropped generation's accepted attempt before legal reuse of the same incarnation id", async () => {
      if (!fx.traces.handoffAwaitsReceiver) return
      const stall = stallingReceiver()
      const old = fx.backend.subscribeLane(roomId, inc, SEMANTIC, stall.receiver)
      await old.ready
      const blocker = accepted(await fx.backend.commitLane(roomId, inc, SEMANTIC, bytes('blocker')))
      await stall.waitForEntry()
      const droppedAttempt = accepted(await fx.backend.commitLane(roomId, inc, SEMANTIC, bytes('old-frame')))

      const head = await readHeadOrThrow(fx.backend, roomId)
      const { head: closing, leaseId } = await enterClosing(fx.backend, roomId, head)
      accepted(await fx.backend.commitLane(roomId, inc, CONTROL, bytes('closed'), { closingLease: leaseId }))
      const tombstone = okHead(await finalizeClose(fx.backend, roomId, closing, leaseId))
      await fx.backend.dropGeneration(roomId, inc)

      const reused = okHead(
        await fx.backend.compareExchangeHead(
          roomId,
          { expect: { rev: tombstone.rev } },
          { head: { currentInc: inc, state: 'open', config: tombstone.config } },
        ),
      )
      expect(reused.currentInc).toBe(inc)
      const fresh = collector()
      const freshSub = fx.backend.subscribeLane(roomId, inc, SEMANTIC, fresh.receiver)
      await freshSub.ready
      const reusedCommit = accepted(await fx.backend.commitLane(roomId, inc, SEMANTIC, bytes('new-frame')))
      expect(reusedCommit.seq).toBe(1)
      await reusedCommit.delivery

      await stall.release()
      await Promise.all([blocker.delivery, droppedAttempt.delivery])
      expect(fresh.payloads()).toEqual(['new-frame'])
      await freshSub.unsubscribe()
    })
  })
}
