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
  deferred,
  enterClosing,
  finalizeClose,
  flush,
  inboxLane,
  nextId,
  okHead,
  openRoom,
  readHeadOrThrow,
  SEMANTIC,
  settled,
  stallingReceiver,
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
      const gate = deferred()
      const stalled = fx.backend.subscribeLane(roomId, inc, SEMANTIC, stallingReceiver(gate.promise))
      const other = collector()
      const free = fx.backend.subscribeLane(roomId, inc, CONTROL, other.receiver)
      await Promise.all([stalled.ready, free.ready])

      const blocked = accepted(await fx.backend.commitLane(roomId, inc, SEMANTIC, bytes('stuck')))
      const independent = accepted(await fx.backend.commitLane(roomId, inc, CONTROL, bytes('free')))

      // the control lane delivers while the semantic lane's attempt is still in flight
      await independent.delivery
      await other.waitFor(1)
      expect(other.payloads()).toEqual(['free'])

      gate.resolve()
      await blocked.delivery
    })

    it('starts frame N+1 only after frame N settled on the same lane', async () => {
      if (!fx.traces.handoffAwaitsReceiver) return
      const first = deferred()
      const entries: string[] = []
      const sub = fx.backend.subscribeLane(
        roomId,
        inc,
        SEMANTIC,
        stallingReceiver(first.promise, () => entries.push('enter')),
      )
      await sub.ready

      const one = accepted(await fx.backend.commitLane(roomId, inc, SEMANTIC, bytes('one')))
      const two = accepted(await fx.backend.commitLane(roomId, inc, SEMANTIC, bytes('two')))
      await flush()
      expect(entries).toEqual(['enter']) // frame two's attempt has not begun

      first.resolve()
      await Promise.all([one.delivery, two.delivery])
      expect(entries).toEqual(['enter', 'enter'])
    })

    it('does not let a failed frame poison the lane, and rejects only its own promise', async () => {
      // KILLER: gating the chain on the attempt itself rather than on its SETTLEMENT turns this red.
      if (!fx.traces.perTargetFailure) return
      const seen: string[] = []
      const sub = fx.backend.subscribeLane(roomId, inc, SEMANTIC, (payload) => {
        const value = new TextDecoder().decode(payload)
        seen.push(value)
        if (value === 'bad') throw new Error('handoff failed')
      })
      await sub.ready

      const bad = accepted(await fx.backend.commitLane(roomId, inc, SEMANTIC, bytes('bad')))
      const good = accepted(await fx.backend.commitLane(roomId, inc, SEMANTIC, bytes('good')))

      expect(await settled(bad.delivery)).toBe('rejected')
      expect(await settled(good.delivery)).toBe('resolved')
      expect(seen).toEqual(['bad', 'good'])
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
      const gate = deferred()
      const stalled = fx.backend.subscribeLane(roomId, inc, SEMANTIC, stallingReceiver(gate.promise))
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
      await accepted(await fx.backend.commitLane(roomId, recreated.inc, SEMANTIC, bytes('new-gen'))).delivery
      expect(latch.payloads()).toEqual(['new-gen'])

      gate.resolve()
      await stuck.delivery
    })

    it("discards a dropped generation's accepted attempt before legal reuse of the same incarnation id", async () => {
      if (!fx.traces.handoffAwaitsReceiver) return
      const gate = deferred()
      const entered = deferred()
      const old = fx.backend.subscribeLane(roomId, inc, SEMANTIC, async () => {
        entered.resolve()
        await gate.promise
      })
      await old.ready
      const blocker = accepted(await fx.backend.commitLane(roomId, inc, SEMANTIC, bytes('blocker')))
      await entered.promise
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
      await accepted(await fx.backend.commitLane(roomId, inc, SEMANTIC, bytes('new-frame'))).delivery

      gate.resolve()
      await Promise.all([blocker.delivery, droppedAttempt.delivery])
      expect(fresh.payloads()).toEqual(['new-frame'])
      await freshSub.unsubscribe()
    })
  })
}
