// CF-specific scenarios that the common conformance suite cannot express, because they exercise the
// Cloudflare route table and delivery mechanics directly (convergence §W2 + readiness-ordering §2.3/§3):
//
//  - route re-establishment as an atomic UPSERT (one row per (inc,lane,subscriber)) → no duplicate delivery
//  - renewal-loss-before-expiry: the new lease replaces the UNEXPIRED old one → exactly-once
//  - unsubscribe is lease-guarded (all four fields) → a racing old lease can't remove the new route
//  - eviction between acceptance and attempt → at-most-once loss, no retry, acceptance durable
//  - the barrier-forced concurrent I13(c) finalization race (the async-CX obligation of the harness)
//
// Runs only in the Cloudflare lane (vitest.cloudflare.config.ts). Uses the low-level DO stub + a
// hand-driven delivery chain where the property lives below the SPI facade.

import { afterEach, beforeEach, describe, expect, it } from 'vitest'
import { laneKey as laneKeyOf } from '../../../server/adapter/cloudflare/room/codec.js'
import { Fanout } from '../../../server/adapter/cloudflare/room/fanout.js'
import type { CommitWire } from '../../../server/adapter/cloudflare/room/do.js'
import type { BackendFixture } from '../harness.js'
import {
  accepted,
  bytes,
  CLOSE_LEASE_MS,
  collector,
  CONTROL,
  conflicted,
  enterClosing,
  finalizeClose,
  type HeadCxResult,
  nextId,
  okHead,
  openRoom,
  readHeadOrThrow,
  SEMANTIC,
  takeoverClose,
  text,
} from '../scenario.js'
import {
  cloudflareHarness,
  cloudflareRoomStub,
  deliverToReceiver,
  evictReceiver,
  installReceiver,
  type RoomStub,
} from './fixture.js'

function acceptedCommit(wire: CommitWire): { seq: number; timestamp: number; receivers: number; targets: string[] } {
  if (!('accepted' in wire)) throw new Error(`expected an accepted commit, got ${JSON.stringify(wire)}`)
  return wire
}

describe('cloudflare — CF-specific mechanics', () => {
  let fx: BackendFixture

  beforeEach(async () => {
    fx = await cloudflareHarness.create()
  })

  afterEach(async () => {
    await fx.dispose()
  })

  const openCfRoom = async (): Promise<{ roomId: string; inc: string; stub: RoomStub }> => {
    const roomId = nextId('cf-room')
    const { inc } = await openRoom(fx.backend, roomId)
    const stub = await cloudflareRoomStub(roomId)
    return { roomId, inc, stub }
  }

  // ── route lease lifecycle (readiness-ordering §2.3) ──

  describe('route lease lifecycle — UPSERT replacement (readiness-ordering §2.3)', () => {
    it('re-establishment replaces the lease in one row: the acceptance snapshot has no duplicate target', async () => {
      const { inc, stub } = await openCfRoom()
      const lk = laneKeyOf(SEMANTIC)
      const subscriber = 'subscriber-do-a'

      expect('ok' in (await stub.registerRoute(inc, lk, subscriber, 'lease-A', null))).toBe(true)
      // renewal loss → re-establish with a NEW lease id: the UPSERT replaces the prior row for the same
      // (inc, lane, subscriber) rather than inserting a second one.
      expect('ok' in (await stub.registerRoute(inc, lk, subscriber, 'lease-B', null))).toBe(true)

      const commit = acceptedCommit(await stub.commitLane(inc, SEMANTIC, bytes('K')))
      expect(commit.targets).toEqual([subscriber]) // exactly one target — the two leases never coexisted
      expect(commit.receivers).toBe(1)

      // and the superseded lease can no longer renew; only the current one can (four-field compare)
      expect((await stub.renewRoute(inc, lk, subscriber, 'lease-A')).ok).toBe(false)
      expect((await stub.renewRoute(inc, lk, subscriber, 'lease-B')).ok).toBe(true)
    })

    it('renewal-loss-before-expiry is exactly-once: a new lease replaces the UNEXPIRED old one', async () => {
      // KILLER: an INSERT (instead of UPSERT) on re-establishment would leave the unexpired old row in
      // place, so the acceptance snapshot would carry the subscriber twice and it would be delivered twice.
      const { inc, stub } = await openCfRoom()
      const lk = laneKeyOf(SEMANTIC)
      const subscriber = 'subscriber-do-b'

      // establish; never advance the clock, so the old lease stays UNEXPIRED throughout
      await stub.registerRoute(inc, lk, subscriber, 'lease-old', null)
      // K consecutive renewal losses drive a re-establish with a fresh lease while the old is still live
      await stub.registerRoute(inc, lk, subscriber, 'lease-new', null)

      let deliveries = 0
      installReceiver(subscriber, () => {
        deliveries += 1
      })
      const fanout = new Fanout(deliverToReceiver)
      const commit = acceptedCommit(await stub.commitLane(inc, SEMANTIC, bytes('once')))
      await fanout.await(fanout.enqueue(inc, lk, commit.targets, bytes('once'), { seq: commit.seq, timestamp: commit.timestamp }))
      evictReceiver(subscriber)

      expect(commit.targets).toEqual([subscriber])
      expect(deliveries).toBe(1) // exactly once — no coexisting old lease to double-deliver
    })

    it('unsubscribe is lease-guarded: a racing stale lease cannot remove the re-established route', async () => {
      const { inc, stub } = await openCfRoom()
      const lk = laneKeyOf(SEMANTIC)
      const subscriber = 'subscriber-do-c'

      await stub.registerRoute(inc, lk, subscriber, 'lease-old', null)
      await stub.registerRoute(inc, lk, subscriber, 'lease-new', null)

      // a delayed unsubscribe carrying the superseded lease matches nothing (all four fields compared)
      await stub.unsubscribeRoute(inc, lk, subscriber, 'lease-old')
      expect(acceptedCommit(await stub.commitLane(inc, SEMANTIC, bytes('x'))).receivers).toBe(1)

      // the current lease does remove it
      await stub.unsubscribeRoute(inc, lk, subscriber, 'lease-new')
      expect(acceptedCommit(await stub.commitLane(inc, SEMANTIC, bytes('y'))).receivers).toBe(0)
    })
  })

  // ── delivery at-most-once (readiness-ordering §3.4) ──

  describe('delivery at-most-once — eviction between acceptance and attempt', () => {
    it('loses the evicted frame without retry, keeps acceptance durable, and does not poison the lane', async () => {
      const { roomId, inc, stub } = await openCfRoom()
      const lk = laneKeyOf(SEMANTIC)
      const subscriber = 'subscriber-do-evict'
      await stub.registerRoute(inc, lk, subscriber, 'lease-1', null)

      const got: string[] = []
      installReceiver(subscriber, (payload) => {
        got.push(text(payload))
      })
      const fanout = new Fanout(deliverToReceiver)

      // frame 1: accepted + retained (durable in SQL), then the subscriber isolate is EVICTED before the
      // handoff attempt runs — the frame is lost at-most-once, never retried.
      const c1 = acceptedCommit(await stub.commitLane(inc, SEMANTIC, bytes('K1'), { retain: true }))
      evictReceiver(subscriber)
      await fanout.await(fanout.enqueue(inc, lk, c1.targets, bytes('K1'), { seq: c1.seq, timestamp: c1.timestamp }))
      expect(got).toEqual([]) // lost — no delivery, no retry

      // acceptance stood: the retained generation and the advanced order are durable regardless
      const retained1 = await fx.backend.readRetained(roomId, inc, SEMANTIC)
      expect(retained1 === null ? null : text(retained1.payload)).toBe('K1')
      expect(c1.seq).toBe(1)

      // the lane is not poisoned: a re-installed receiver gets the next frame, order keeps advancing
      installReceiver(subscriber, (payload) => {
        got.push(text(payload))
      })
      const c2 = acceptedCommit(await stub.commitLane(inc, SEMANTIC, bytes('K2'), { retain: true }))
      await fanout.await(fanout.enqueue(inc, lk, c2.targets, bytes('K2'), { seq: c2.seq, timestamp: c2.timestamp }))
      evictReceiver(subscriber)

      expect(got).toEqual(['K2'])
      expect(c2.seq).toBe(2)
      const retained2 = await fx.backend.readRetained(roomId, inc, SEMANTIC)
      expect(retained2 === null ? null : text(retained2.payload)).toBe('K2')
    })
  })

  // ── the barrier-forced concurrent I13(c) variant (the async-CX obligation) ──

  describe('I13(c) finalization race — barrier-forced concurrent (async CX)', () => {
    // The room DO applies each CX asynchronously (an RPC hop), so — per the spi.md race-realization note —
    // beyond the two serial linearizations the common suite runs inline, this lane must additionally force
    // both CXs concurrent through the harness barrier and assert each release order's outcome.
    const pauseAfterClosedEvent = async (): Promise<{
      roomId: string
      inc: string
      paused: Awaited<ReturnType<typeof readHeadOrThrow>>
      originalLease: string
      control: ReturnType<typeof collector>
    }> => {
      const roomId = nextId('cf-room')
      const { inc, head } = await openRoom(fx.backend, roomId)
      const control = collector()
      const sub = fx.backend.subscribeLane(roomId, inc, CONTROL, control.receiver)
      await sub.ready
      const { head: closing, leaseId } = await enterClosing(fx.backend, roomId, head)
      const event = accepted(
        await fx.backend.commitLane(roomId, inc, CONTROL, bytes('closed-by-original'), { closingLease: leaseId }),
      )
      await event.delivery
      fx.advanceAuthority(CLOSE_LEASE_MS + 1)
      return { roomId, inc, paused: closing, originalLease: leaseId, control }
    }

    it('release order finalize→takeover: the original finalizes and the takeover conflicts', async () => {
      if (fx.concurrentHeadCxBarrier === undefined) throw new Error('CF fixture must supply concurrentHeadCxBarrier')
      const { roomId, paused, originalLease, control } = await pauseAfterClosedEvent()

      const [finalized, taken] = await fx.concurrentHeadCxBarrier<HeadCxResult>(
        () => finalizeClose(fx.backend, roomId, paused, originalLease),
        () => takeoverClose(fx.backend, roomId, paused).then((r) => r.result),
      )

      const tombstone = okHead(finalized)
      expect(tombstone.state).toBe('closed')
      expect(tombstone.currentInc).toBeNull()
      conflicted(taken)
      expect(control.payloads()).toEqual(['closed-by-original']) // head went null only after the winner's event
    })

    it('release order takeover→finalize: the takeover wins and the original aborts', async () => {
      if (fx.concurrentHeadCxBarrier === undefined) throw new Error('CF fixture must supply concurrentHeadCxBarrier')
      const { roomId, inc, paused, originalLease } = await pauseAfterClosedEvent()

      const [taken, finalized] = await fx.concurrentHeadCxBarrier<HeadCxResult>(
        () => takeoverClose(fx.backend, roomId, paused).then((r) => r.result),
        () => finalizeClose(fx.backend, roomId, paused, originalLease),
      )

      const fresh = okHead(taken)
      expect(fresh.state).toBe('closing')
      conflicted(finalized) // the superseded closer aborts its tail — it can never clear the head

      const still = await readHeadOrThrow(fx.backend, roomId)
      expect(still.state).toBe('closing')
      expect(still.currentInc).toBe(inc)
    })
  })
})
