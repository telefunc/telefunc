// I4 — acceptance is atomic with the order advance and the retained install, and no two accepted commits
// on one lane share or reorder (seq, timestamp) in that lane's domain.
// I5 — acceptance and delivery are separated: the handoff attempt may fail without rolling acceptance
// back, without retrying, and without poisoning the lane.
// I6 — a retained read after an accepted retain commit returns that generation or newer, never older.

import { afterEach, beforeEach, describe, expect, it } from 'vitest'
import type { LaneId } from '../spi.js'
import { expectedReceiverCount, type BackendFixture, installedBackends } from './harness.js'
import {
  accepted,
  binaryLane,
  bytes,
  collector,
  CONTROL,
  enterClosing,
  finalizeClose,
  inboxLane,
  isStale,
  nextId,
  okHead,
  openRoom,
  readHeadOrThrow,
  SEMANTIC,
  settled,
  stallingReceiver,
  text,
  throwingReceiver,
} from './scenario.js'

for (const harness of installedBackends) {
  describe(`commit — ${harness.name}`, () => {
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

    const commit = (lane: LaneId, payload: string, opts?: { retain?: boolean }) =>
      fx.backend.commitLane(roomId, inc, lane, bytes(payload), opts)

    // ── I4 ──

    describe('I4 — atomic acceptance and lane order', () => {
      it('assigns a strictly increasing seq and a non-decreasing timestamp within a lane', async () => {
        const marks: Array<{ seq: number; timestamp: number }> = []
        for (let n = 0; n < 50; n++) {
          const result = accepted(await commit(SEMANTIC, `frame-${n}`))
          marks.push({ seq: result.seq, timestamp: result.timestamp })
        }
        for (let n = 1; n < marks.length; n++) {
          expect((marks[n] as { seq: number }).seq).toBeGreaterThan((marks[n - 1] as { seq: number }).seq)
          expect((marks[n] as { timestamp: number }).timestamp).toBeGreaterThanOrEqual(
            (marks[n - 1] as { timestamp: number }).timestamp,
          )
        }
        expect(new Set(marks.map((mark) => mark.seq)).size).toBe(marks.length)
      })

      it('shares one order domain between text and announce on the semantic lane', async () => {
        const first = accepted(await commit(SEMANTIC, 'text'))
        const second = accepted(await commit(SEMANTIC, 'announce'))
        expect(second.seq).toBe(first.seq + 1)
      })

      it('keeps binary tracks, inboxes and control in separate order domains', async () => {
        const lanes: LaneId[] = [
          binaryLane('alice', 'cam'),
          binaryLane('alice', 'mic'),
          binaryLane('bob', 'cam'),
          inboxLane('alice'),
          inboxLane('bob'),
          CONTROL,
          SEMANTIC,
        ]
        for (const lane of lanes) expect(accepted(await commit(lane, 'first')).seq).toBe(1)
        for (const lane of lanes) expect(accepted(await commit(lane, 'second')).seq).toBe(2)
      })

      it('is stale for a foreign incarnation and once the head is no longer open', async () => {
        // KILLER: accepting a commit without the head check turns this red.
        expect(isStale(await fx.backend.commitLane(roomId, 'inc-that-never-was', SEMANTIC, bytes('x')))).toBe(true)

        const head = await readHeadOrThrow(fx.backend, roomId)
        const { head: closing, leaseId } = await enterClosing(fx.backend, roomId, head)
        expect(isStale(await commit(SEMANTIC, 'while-closing'))).toBe(true)

        accepted(await fx.backend.commitLane(roomId, inc, CONTROL, bytes('closed'), { closingLease: leaseId }))
        okHead(await finalizeClose(fx.backend, roomId, closing, leaseId))
        expect(isStale(await commit(SEMANTIC, 'after-closed'))).toBe(true)
      })

      it('does not advance the order domain when an over-cap retain is rejected', async () => {
        const before = accepted(await commit(SEMANTIC, 'before'))
        const oversized = new Uint8Array(fx.backend.capabilities.maxRetainedPayloadBytes + 1)
        await expect(fx.backend.commitLane(roomId, inc, SEMANTIC, oversized, { retain: true })).rejects.toThrow()
        expect(accepted(await commit(SEMANTIC, 'after')).seq).toBe(before.seq + 1)
      })

      it('rejects a retain that would exceed the aggregate cap across lanes', async () => {
        const half = Math.ceil(fx.backend.capabilities.maxRetainedPayloadBytes / 2) + 1
        const first = new Uint8Array(half)
        const section = 1024 * 1024
        for (let offset = 0, value = 1; offset < first.byteLength; offset += section, value += 1) {
          first.fill(value, offset, Math.min(first.byteLength, offset + section))
        }
        accepted(await fx.backend.commitLane(roomId, inc, SEMANTIC, first, { retain: true }))
        const retained = await fx.backend.readRetained(roomId, inc, SEMANTIC)
        expect(retained?.payload.byteLength).toBe(first.byteLength)
        for (let offset = 0; offset < first.byteLength; offset += section) {
          expect(retained?.payload[offset]).toBe(first[offset])
        }
        await expect(
          fx.backend.commitLane(roomId, inc, CONTROL, new Uint8Array(half), { retain: true }),
        ).rejects.toThrow()
      })
    })

    // ── I5 ──

    describe('I5 — acceptance and delivery are separated', () => {
      it('counts snapshot targets in the declared authority granularity and reaches both local callbacks', async () => {
        const first = collector()
        const second = collector()
        const firstSub = fx.backend.subscribeLane(roomId, inc, SEMANTIC, first.receiver)
        const secondSub = fx.backend.subscribeLane(roomId, inc, SEMANTIC, second.receiver)
        await Promise.all([firstSub.ready, secondSub.ready])

        const result = accepted(await commit(SEMANTIC, 'two-local'))
        expect(result.receivers).toBe(
          await expectedReceiverCount(fx, roomId, inc, SEMANTIC, fx.expectedReceivers.twoLocalSubscriptionsSameLane),
        )
        await result.delivery
        await Promise.all([first.waitFor(1), second.waitFor(1)])
        expect(first.payloads()).toEqual(['two-local'])
        expect(second.payloads()).toEqual(['two-local'])
      })

      it('counts receivers as the targets snapshotted at acceptance, not successful deliveries', async () => {
        if (!fx.traces.perTargetFailure) return
        const good = collector()
        const badReceiver = throwingReceiver('receiver blew up')
        const bad = fx.backend.subscribeLane(roomId, inc, SEMANTIC, badReceiver.receiver)
        const goodSub = fx.backend.subscribeLane(roomId, inc, SEMANTIC, good.receiver)
        await Promise.all([bad.ready, goodSub.ready])

        const result = accepted(await commit(SEMANTIC, 'K'))
        expect(await settled(result.delivery)).toBe('rejected')
        // acceptance stood: the healthy target still got the frame and the order advanced
        await good.waitFor(1)
        expect(badReceiver.calls()).toBe(1)
        expect(good.payloads()).toEqual(['K'])
        expect(accepted(await commit(SEMANTIC, 'next')).seq).toBe(result.seq + 1)
      })

      it('never retries a failed handoff and never poisons the next frame', async () => {
        if (!fx.traces.perTargetFailure) return
        const failingReceiver = throwingReceiver('receiver blew up')
        const failing = fx.backend.subscribeLane(roomId, inc, SEMANTIC, failingReceiver.receiver)
        await failing.ready

        expect(await settled(accepted(await commit(SEMANTIC, 'one')).delivery)).toBe('rejected')
        expect(failingReceiver.calls()).toBe(1) // exactly one attempt — at-most-once

        // the lane is not poisoned: the next frame is attempted and reaches a healthy target
        await failing.unsubscribe()
        const good = collector()
        const healthy = fx.backend.subscribeLane(roomId, inc, SEMANTIC, good.receiver)
        await healthy.ready
        await accepted(await commit(SEMANTIC, 'two')).delivery
        expect(good.payloads()).toEqual(['two'])
        expect(failingReceiver.calls()).toBe(1)
      })

      it('settles delivery for a frame with no subscribers', async () => {
        const result = accepted(await commit(SEMANTIC, 'nobody-listening'))
        expect(result.receivers).toBe(0)
        expect(await settled(result.delivery)).toBe('resolved')
      })

      it('does not run a receiver reentrantly inside commitLane', async () => {
        const seen = collector()
        const sub = fx.backend.subscribeLane(roomId, inc, SEMANTIC, seen.receiver)
        await sub.ready
        // The reentrancy inversion the attempt chain exists to prevent is a receiver running INSIDE the
        // commitLane call, so the call is deliberately not awaited before the assertion.
        const pending = commit(SEMANTIC, 'K')
        expect(seen.payloads()).toEqual([])
        const result = accepted(await pending)
        await result.delivery
        expect(seen.payloads()).toEqual(['K'])
      })
    })

    // ── I6 ──

    describe('I6 — retained is consistent', () => {
      it('returns the accepted generation or newer, never older', async () => {
        // KILLER: serving retained from a lagging (non-consistent) tier turns this red.
        const first = accepted(await commit(SEMANTIC, 'K1', { retain: true }))
        const afterFirst = await fx.backend.readRetained(roomId, inc, SEMANTIC)
        expect(afterFirst?.seq).toBe(first.seq)
        expect(text(afterFirst?.payload ?? bytes(''))).toBe('K1')

        const second = accepted(await commit(SEMANTIC, 'K2', { retain: true }))
        const afterSecond = await fx.backend.readRetained(roomId, inc, SEMANTIC)
        expect(afterSecond?.seq).toBe(second.seq)
        expect(afterSecond?.seq).toBeGreaterThan(first.seq)
        expect(text(afterSecond?.payload ?? bytes(''))).toBe('K2')
      })

      it('does not install retained for a non-retained commit', async () => {
        accepted(await commit(SEMANTIC, 'transient'))
        expect(await fx.backend.readRetained(roomId, inc, SEMANTIC)).toBeNull()
      })

      it('keeps retained per lane, and lists and deletes by lane', async () => {
        accepted(await commit(SEMANTIC, 'S', { retain: true }))
        accepted(await commit(binaryLane('alice', 'cam'), 'B', { retain: true }))
        const listed = await fx.backend.listRetained(roomId, inc)
        expect(listed).toHaveLength(2)
        expect(listed).toContainEqual(SEMANTIC)
        expect(listed).toContainEqual(binaryLane('alice', 'cam'))

        await fx.backend.deleteRetained(roomId, inc, SEMANTIC)
        expect(await fx.backend.readRetained(roomId, inc, SEMANTIC)).toBeNull()
        expect(await fx.backend.readRetained(roomId, inc, binaryLane('alice', 'cam'))).not.toBeNull()

        await fx.backend.deleteRetained(roomId, inc)
        expect(await fx.backend.listRetained(roomId, inc)).toEqual([])
      })

      it('exposes retained to a reader that never subscribed', async () => {
        const result = accepted(await commit(SEMANTIC, 'K1', { retain: true }))
        const seed = await fx.backend.readRetained(roomId, inc, SEMANTIC)
        expect(seed?.seq).toBe(result.seq)
        expect(seed?.timestamp).toBe(result.timestamp)
      })

      it('installs retained atomically with acceptance — visible before delivery settles', async () => {
        const stalled = stallingReceiver()
        const sub = fx.backend.subscribeLane(roomId, inc, SEMANTIC, stalled.receiver)
        await sub.ready
        const result = accepted(await commit(SEMANTIC, 'K1', { retain: true }))
        if (fx.traces.handoffAwaitsReceiver) {
          // the handoff is still in flight, yet the retained generation is already readable
          const seed = await fx.backend.readRetained(roomId, inc, SEMANTIC)
          expect(text(seed?.payload ?? bytes(''))).toBe('K1')
        }
        await stalled.release()
        await result.delivery
      })
    })
  })
}
