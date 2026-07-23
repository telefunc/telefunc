// I7 — the live-readiness test of readiness-ordering.md §2.1: `ready` resolves only when a commit
// accepted afterwards actually reaches the receiver; initial failure rejects (fail-closed).
// I11 — subscriptions are incarnation-scoped end to end: a surviving old-inc subscription observes
// nothing from a recreated room.
// Plus both retained-overlap schedules of §2.2, driven through the SeedGate model in scenario.ts.

import { afterEach, beforeEach, describe, expect, it } from 'vitest'
import type { ReadinessState } from '../spi.js'
import { expectedReceiverCount, type BackendFixture, installedBackends } from './harness.js'
import {
  accepted,
  bytes,
  collector,
  CONTROL,
  enterClosing,
  finalizeClose,
  flush,
  nextId,
  noopReceiver,
  okHead,
  openRoom,
  readHeadOrThrow,
  SEMANTIC,
  seededSubscriber,
  settled,
} from './scenario.js'

for (const harness of installedBackends) {
  describe(`readiness — ${harness.name}`, () => {
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

    // ── I7 ──

    describe('I7 — live readiness (readiness-ordering §2.1)', () => {
      it('delivers a frame committed strictly after ready resolved', async () => {
        // KILLER: resolving `ready` before the registration is durable turns this red — the commit that
        // follows the barrier snapshots zero targets and the latch never fires.
        const latch = collector()
        const sub = fx.backend.subscribeLane(roomId, inc, SEMANTIC, latch.receiver)
        await sub.ready // barrier: the next step starts only after resolution
        expect(sub.state()).toBe('ready')

        const result = accepted(await fx.backend.commitLane(roomId, inc, SEMANTIC, bytes('K')))
        expect(result.receivers).toBe(await expectedReceiverCount(fx, roomId, inc, SEMANTIC, 1))
        await result.delivery // the backend's one at-most-once handoff attempt settled
        await latch.waitFor(1) // receiver completion is not a delivery guarantee — wait for it, bounded
        expect(latch.payloads()).toEqual(['K'])
      })

      it('keeps the shared route live until the last local sibling detaches', async () => {
        const survivor = collector()
        const detached = collector()
        const first = fx.backend.subscribeLane(roomId, inc, SEMANTIC, survivor.receiver)
        const second = fx.backend.subscribeLane(roomId, inc, SEMANTIC, detached.receiver)
        await Promise.all([first.ready, second.ready])

        // Detach the most recently attached sibling. Independent lease lifecycles would let this one
        // delete the representative's current route and strand the still-ready first subscription.
        await second.unsubscribe()
        expect(second.state()).toBe('closed')
        expect(first.state()).toBe('ready')

        const result = accepted(await fx.backend.commitLane(roomId, inc, SEMANTIC, bytes('survivor')))
        expect(result.receivers).toBe(
          await expectedReceiverCount(
            fx,
            roomId,
            inc,
            SEMANTIC,
            fx.expectedReceivers.oneLocalSubscriptionAfterSiblingDetach,
          ),
        )
        await result.delivery
        await survivor.waitFor(1)
        expect(detached.payloads()).toEqual([])
        expect(survivor.payloads()).toEqual(['survivor'])
        await first.unsubscribe()
      })

      it('treats repeated attachment of one receiver function as distinct subscriptions', async () => {
        for (const detachFirst of [0, 1] as const) {
          const scopedRoomId = `${roomId}-same-receiver-${detachFirst}`
          const { inc: scopedInc } = await openRoom(fx.backend, scopedRoomId)
          const receiver = collector()
          const subscriptions = [
            fx.backend.subscribeLane(scopedRoomId, scopedInc, SEMANTIC, receiver.receiver),
            fx.backend.subscribeLane(scopedRoomId, scopedInc, SEMANTIC, receiver.receiver),
          ] as const
          await Promise.all(subscriptions.map((sub) => sub.ready))

          await accepted(await fx.backend.commitLane(scopedRoomId, scopedInc, SEMANTIC, bytes('both'))).delivery
          expect(receiver.payloads()).toEqual(['both', 'both'])

          await subscriptions[detachFirst].unsubscribe()
          await accepted(await fx.backend.commitLane(scopedRoomId, scopedInc, SEMANTIC, bytes('one'))).delivery
          expect(receiver.payloads()).toEqual(['both', 'both', 'one'])

          await subscriptions[detachFirst === 0 ? 1 : 0].unsubscribe()
          expect(
            accepted(await fx.backend.commitLane(scopedRoomId, scopedInc, SEMANTIC, bytes('none'))).receivers,
          ).toBe(0)
        }
      })

      it('rejects ready for an incarnation that is not the open one (fail-closed)', async () => {
        const sub = fx.backend.subscribeLane(roomId, 'inc-that-never-was', SEMANTIC, noopReceiver())
        expect(await settled(sub.ready)).toBe('rejected')
        expect(sub.state()).toBe('closed')
      })

      it('rejects ready once the head has left open', async () => {
        const head = await readHeadOrThrow(fx.backend, roomId)
        const { head: closing, leaseId } = await enterClosing(fx.backend, roomId, head)
        const whileClosing = fx.backend.subscribeLane(roomId, inc, SEMANTIC, noopReceiver())
        expect(await settled(whileClosing.ready)).toBe('rejected')

        accepted(await fx.backend.commitLane(roomId, inc, CONTROL, bytes('closed'), { closingLease: leaseId }))
        okHead(await finalizeClose(fx.backend, roomId, closing, leaseId))
        const afterClosed = fx.backend.subscribeLane(roomId, inc, SEMANTIC, noopReceiver())
        expect(await settled(afterClosed.ready)).toBe('rejected')
      })

      it('reports state transitions and stops delivery on unsubscribe', async () => {
        const latch = collector()
        const seen: ReadinessState[] = []
        const sub = fx.backend.subscribeLane(roomId, inc, SEMANTIC, latch.receiver)
        const off = sub.onStateChange((state) => seen.push(state))
        await sub.ready
        await accepted(await fx.backend.commitLane(roomId, inc, SEMANTIC, bytes('before'))).delivery

        await sub.unsubscribe()
        expect(sub.state()).toBe('closed')
        expect(seen).toEqual(['closed'])
        off()

        const after = accepted(await fx.backend.commitLane(roomId, inc, SEMANTIC, bytes('after')))
        expect(after.receivers).toBe(0)
        await after.delivery
        expect(latch.payloads()).toEqual(['before'])
      })
    })

    // ── I11 ──

    describe('I11 — subscriptions are incarnation-scoped', () => {
      it('delivers nothing from a recreated room to a surviving old-inc subscription', async () => {
        // KILLER: keying channels by lane alone (dropping the incarnation) turns this red.
        const stale = collector()
        const oldSub = fx.backend.subscribeLane(roomId, inc, SEMANTIC, stale.receiver)
        await oldSub.ready
        await accepted(await fx.backend.commitLane(roomId, inc, SEMANTIC, bytes('old-gen'))).delivery
        expect(stale.payloads()).toEqual(['old-gen'])

        // close and recreate WITHOUT dropping the old generation, so the old subscription survives
        const head = await readHeadOrThrow(fx.backend, roomId)
        const { head: closing, leaseId } = await enterClosing(fx.backend, roomId, head)
        accepted(await fx.backend.commitLane(roomId, inc, CONTROL, bytes('closed'), { closingLease: leaseId }))
        const tombstone = okHead(await finalizeClose(fx.backend, roomId, closing, leaseId))
        const recreated = await openRoom(fx.backend, roomId, { prior: tombstone })

        const fresh = collector()
        const newSub = fx.backend.subscribeLane(roomId, recreated.inc, SEMANTIC, fresh.receiver)
        await newSub.ready
        const result = accepted(await fx.backend.commitLane(roomId, recreated.inc, SEMANTIC, bytes('new-gen')))
        await result.delivery
        await flush()

        expect(fresh.payloads()).toEqual(['new-gen'])
        expect(result.receivers).toBe(await expectedReceiverCount(fx, roomId, recreated.inc, SEMANTIC, 1)) // the old subscription is not even a target
        expect(stale.payloads()).toEqual(['old-gen']) // zero frames from the new incarnation
      })
    })

    // ── retained overlap (readiness-ordering §2.2) ──

    describe('retained overlap — exactly once through both schedules', () => {
      it('schedule A (commit wins): the subscriber observes K1 exactly once, via the seed', async () => {
        // KILLER: serving retained from a lagging tier turns this red — the seed is the ONLY path to K1
        // here, because the commit predates the registration.
        accepted(await fx.backend.commitLane(roomId, inc, SEMANTIC, bytes('K1'), { retain: true }))

        const subscriber = seededSubscriber(fx.backend, roomId, inc, SEMANTIC)
        await subscriber.sub.ready
        await subscriber.seed()
        await flush()

        expect(subscriber.observed).toEqual([{ payload: 'K1', seq: 1, source: 'seed' }])
        await subscriber.close()
      })

      it('schedule B (subscribe wins, acceptance before the seed read): K2 exactly once', async () => {
        const subscriber = seededSubscriber(fx.backend, roomId, inc, SEMANTIC)
        await subscriber.sub.ready

        // acceptance lands first, then the seed read races the live frame
        const pending = fx.backend.commitLane(roomId, inc, SEMANTIC, bytes('K2'), { retain: true })
        await subscriber.seed()
        accepted(await pending)
        await flush()

        expect(subscriber.observed.filter((frame) => frame.payload === 'K2')).toHaveLength(1)
        expect(subscriber.observed).toHaveLength(1)
        await subscriber.close()
      })

      it('schedule B (subscribe wins, seed read before acceptance): K2 exactly once, live', async () => {
        const subscriber = seededSubscriber(fx.backend, roomId, inc, SEMANTIC)
        await subscriber.sub.ready

        await subscriber.seed() // nothing retained yet
        expect(subscriber.observed).toEqual([])
        await accepted(await fx.backend.commitLane(roomId, inc, SEMANTIC, bytes('K2'), { retain: true })).delivery
        await flush()

        expect(subscriber.observed).toEqual([{ payload: 'K2', seq: 1, source: 'live' }])
        await subscriber.close()
      })

      it('dedupes a seed that a live frame already covered', async () => {
        accepted(await fx.backend.commitLane(roomId, inc, SEMANTIC, bytes('K1'), { retain: true }))
        const subscriber = seededSubscriber(fx.backend, roomId, inc, SEMANTIC)
        await subscriber.sub.ready

        // a newer retained generation arrives live before the seed read happens
        await accepted(await fx.backend.commitLane(roomId, inc, SEMANTIC, bytes('K2'), { retain: true })).delivery
        await subscriber.seed()
        await flush()

        // the seed returns K2 (consistent tier) and the live K2 is deduped by the watermark
        expect(subscriber.observed).toHaveLength(1)
        expect(subscriber.observed[0]?.payload).toBe('K2')
        await subscriber.close()
      })
    })
  })
}
