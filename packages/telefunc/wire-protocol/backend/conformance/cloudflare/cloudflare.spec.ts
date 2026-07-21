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
import type { CommitWire } from '../../../server/adapter/cloudflare/room/do.js'
import { Fanout } from '../../../server/adapter/cloudflare/room/fanout.js'
import { ROUTE_RENEW_EVERY_MS, ROUTE_TTL_MS } from '../../../server/adapter/cloudflare/room/routes.js'
import {
  CloudflareLaneSubscription,
  CloudflareLaneSubscriptionMultiplexer,
} from '../../../server/adapter/cloudflare/room/subscription.js'
import { GEN_ORPHAN_GRACE_MS } from '../../../server/adapter/cloudflare/room/storage.js'
import type { BackendFixture } from '../harness.js'
import {
  accepted,
  bytes,
  cellsOf,
  CLOSE_LEASE_MS,
  collector,
  CONTROL,
  conflicted,
  enterClosing,
  deferred,
  finalizeClose,
  flush,
  type HeadCxResult,
  nextId,
  okHead,
  openRoom,
  readHeadOrThrow,
  SEMANTIC,
  settled,
  takeoverClose,
  text,
} from '../scenario.js'
import {
  cloudflareHarness,
  cloudflareRenewalControls,
  cloudflareRoomStub,
  disposeCloudflareRoomStubs,
  evictReceiver,
  failReceiverUninstalls,
  flushCloudflareAuthorityClock,
  holdReceiverDeliveries,
  holdReceiverProbes,
  installReceiver,
  releaseReceiverDeliveries,
  releaseReceiverProbes,
  type RoomStub,
  waitForReceiverDelivery,
  waitForReceiverProbe,
} from './fixture.js'

function acceptedCommit(wire: CommitWire): {
  seq: number
  timestamp: number
  receivers: number
  deliveryToken: string
} {
  if (!('accepted' in wire)) throw new Error(`expected an accepted commit, got ${JSON.stringify(wire)}`)
  return wire
}

async function waitUntil(predicate: () => Promise<boolean>, timeoutMs: number = 2_000): Promise<void> {
  const deadline = Date.now() + timeoutMs
  while (!(await predicate())) {
    if (Date.now() >= deadline) throw new Error('condition did not become true before timeout')
    await new Promise<void>((resolve) => setTimeout(resolve, 10))
  }
}

describe('cloudflare — CF-specific mechanics', () => {
  let fx: BackendFixture

  beforeEach(async () => {
    fx = await cloudflareHarness.create()
  })

  afterEach(async () => {
    await fx.dispose()
    disposeCloudflareRoomStubs()
  })

  const openCfRoom = async (): Promise<{ roomId: string; inc: string; stub: RoomStub }> => {
    const roomId = nextId('cf-room')
    const { inc } = await openRoom(fx.backend, roomId)
    const stub = await cloudflareRoomStub(roomId)
    return { roomId, inc, stub }
  }

  // ── route lease lifecycle (readiness-ordering §2.3) ──

  describe('route lease lifecycle — UPSERT replacement (readiness-ordering §2.3)', () => {
    it('rejects an unaddressable subscriber before persisting its route', async () => {
      const { roomId, inc, stub } = await openCfRoom()
      const lk = laneKeyOf(SEMANTIC)
      const subscriber = 'subscriber-do-missing'

      const registration = await stub.registerRoute(roomId, inc, lk, subscriber, 'lease-missing', null)
      expect(registration).toEqual({ rejected: true, reason: `subscriber '${subscriber}' is not addressable` })
      expect(acceptedCommit(await stub.commitLane(roomId, inc, SEMANTIC, bytes('no-target'))).receivers).toBe(0)
    })

    it('mints the full route TTL from a clock sample taken after the awaited subscriber probe', async () => {
      const { roomId, inc, stub } = await openCfRoom()
      const laneKey = laneKeyOf(SEMANTIC)
      const subscriber = 'subscriber-do-slow-probe'
      const leaseId = 'lease-slow-probe'
      await installReceiver(roomId, inc, laneKey, subscriber, leaseId, () => {})
      await holdReceiverProbes(subscriber)

      const beforeProbe = fx.authorityNow()
      const pending = stub.registerRoute(roomId, inc, laneKey, subscriber, leaseId, null)
      await waitForReceiverProbe(subscriber)
      fx.advanceAuthority(10_000)
      await flushCloudflareAuthorityClock()
      await releaseReceiverProbes(subscriber)

      const registration = await pending
      if (!('ok' in registration)) throw new Error(`expected registration, got ${registration.reason}`)
      expect(registration.expiresAt).toBe(fx.authorityNow() + ROUTE_TTL_MS)
      expect(registration.expiresAt).toBeGreaterThan(beforeProbe + ROUTE_TTL_MS)
      await evictReceiver(roomId, inc, laneKey, subscriber, leaseId)
    })

    it('rejects a held old-generation probe after drop and legal reuse of the same incarnation id', async () => {
      const { roomId, inc, stub } = await openCfRoom()
      const laneKey = laneKeyOf(SEMANTIC)
      const subscriber = 'subscriber-do-probe-generation-fence'
      const leaseId = 'lease-old-probe'
      await installReceiver(roomId, inc, laneKey, subscriber, leaseId, () => {})
      await holdReceiverProbes(subscriber)

      const pending = stub.registerRoute(roomId, inc, laneKey, subscriber, leaseId, null)
      await waitForReceiverProbe(subscriber)
      const head = await readHeadOrThrow(fx.backend, roomId)
      const { head: closing, leaseId: closeLease } = await enterClosing(fx.backend, roomId, head)
      accepted(await fx.backend.commitLane(roomId, inc, CONTROL, bytes('closed'), { closingLease: closeLease }))
      const tombstone = okHead(await finalizeClose(fx.backend, roomId, closing, closeLease))
      await fx.backend.dropGeneration(roomId, inc)
      okHead(
        await fx.backend.compareExchangeHead(
          roomId,
          { expect: { rev: tombstone.rev } },
          { head: { currentInc: inc, state: 'open', config: tombstone.config } },
        ),
      )

      await releaseReceiverProbes(subscriber)
      expect(await pending).toEqual({
        rejected: true,
        reason: `generation '${inc}' was invalidated`,
        terminal: true,
      })
      expect(acceptedCommit(await stub.commitLane(roomId, inc, SEMANTIC, bytes('no-stale-route'))).receivers).toBe(0)
      await evictReceiver(roomId, inc, laneKey, subscriber, leaseId)
    })

    it('closes locally even when unsubscribe transport teardown fails', async () => {
      let closedCalls = 0
      const states: string[] = []
      const sub = new CloudflareLaneSubscription({
        establish: async () => ({ ready: true }),
        renew: async () => true,
        unsubscribe: async () => {
          throw new Error('transport down')
        },
        closed: () => {
          closedCalls += 1
        },
      })
      sub.onStateChange((state) => states.push(state))
      sub.start()
      await sub.ready

      await expect(sub.unsubscribe()).rejects.toThrow('transport down')
      expect(sub.state()).toBe('closed')
      expect(states).toEqual(['closed'])
      expect(closedCalls).toBe(1)
    })

    it('isolates throwing state observers so attachment and shared-route teardown still complete', async () => {
      let remoteUnsubscribes = 0
      let localDetaches = 0
      let emptyCalls = 0
      const sharedStates: string[] = []
      const shared = new CloudflareLaneSubscription({
        establish: async () => ({ ready: true }),
        renew: async () => true,
        unsubscribe: async () => {
          remoteUnsubscribes += 1
        },
      })
      shared.onStateChange(() => {
        throw new Error('shared observer failure')
      })
      shared.onStateChange((state) => sharedStates.push(state))
      const mux = new CloudflareLaneSubscriptionMultiplexer(shared, () => {
        emptyCalls += 1
      })
      const attachment = mux.attach(() => {
        localDetaches += 1
      })
      attachment.onStateChange(() => {
        throw new Error('attachment observer failure')
      })
      shared.start()
      await attachment.ready

      await attachment.unsubscribe()
      expect(attachment.state()).toBe('closed')
      expect(shared.state()).toBe('closed')
      expect({ remoteUnsubscribes, localDetaches, emptyCalls }).toEqual({
        remoteUnsubscribes: 1,
        localDetaches: 1,
        emptyCalls: 1,
      })
      expect(sharedStates).toEqual(['closed'])
    })

    it('re-establishment replaces the lease in one row: the acceptance snapshot has no duplicate target', async () => {
      const { roomId, inc, stub } = await openCfRoom()
      const lk = laneKeyOf(SEMANTIC)
      const subscriber = 'subscriber-do-a'
      const receiver = () => {}
      await installReceiver(roomId, inc, lk, subscriber, 'lease-A', receiver)

      expect('ok' in (await stub.registerRoute(roomId, inc, lk, subscriber, 'lease-A', null))).toBe(true)
      // renewal loss → re-establish with a NEW lease id: the UPSERT replaces the prior row for the same
      // (inc, lane, subscriber) rather than inserting a second one.
      await installReceiver(roomId, inc, lk, subscriber, 'lease-B', receiver)
      expect('ok' in (await stub.registerRoute(roomId, inc, lk, subscriber, 'lease-B', null))).toBe(true)

      const commit = acceptedCommit(await stub.commitLane(roomId, inc, SEMANTIC, bytes('K')))
      expect(commit.receivers).toBe(1)

      // and the superseded lease can no longer renew; only the current one can (four-field compare)
      expect((await stub.renewRoute(inc, lk, subscriber, 'lease-A')).ok).toBe(false)
      expect((await stub.renewRoute(inc, lk, subscriber, 'lease-B')).ok).toBe(true)
    })

    it('renewal-loss-before-expiry is exactly-once: a new lease replaces the UNEXPIRED old one', async () => {
      // KILLER: an INSERT (instead of UPSERT) on re-establishment would leave the unexpired old row in
      // place, so the acceptance snapshot would carry the subscriber twice and it would be delivered twice.
      const { roomId, inc, stub } = await openCfRoom()
      const lk = laneKeyOf(SEMANTIC)
      const subscriber = 'subscriber-do-b'
      let deliveries = 0
      const receiver = () => {
        deliveries += 1
      }
      await installReceiver(roomId, inc, lk, subscriber, 'lease-old', receiver)

      // establish; never advance the clock, so the old lease stays UNEXPIRED throughout
      await stub.registerRoute(roomId, inc, lk, subscriber, 'lease-old', null)
      // K consecutive renewal losses drive a re-establish with a fresh lease while the old is still live
      await installReceiver(roomId, inc, lk, subscriber, 'lease-new', receiver)
      await stub.registerRoute(roomId, inc, lk, subscriber, 'lease-new', null)

      const commit = acceptedCommit(await stub.commitLane(roomId, inc, SEMANTIC, bytes('once')))
      await stub.awaitDelivery(commit.deliveryToken)
      await evictReceiver(roomId, inc, lk, subscriber, 'lease-new')

      expect(deliveries).toBe(1) // exactly once — no coexisting old lease to double-deliver
    })

    it('the live TTL/3 lifecycle marks two renewal losses lost, re-establishes, and delivers exactly once', async () => {
      const { roomId, inc } = await openCfRoom()
      const received = collector()
      const sub = fx.backend.subscribeLane(roomId, inc, SEMANTIC, received.receiver)
      await sub.ready
      const states: string[] = []
      sub.onStateChange((state) => states.push(state))

      // Authority time does not move, so the first lease remains unexpired. Two consecutive timer-driven
      // renewal losses must transition lost, mint a new lease, UPSERT it, then transition ready again.
      const renewals = cloudflareRenewalControls(fx.backend)
      renewals.forceFailures(2)
      await renewals.advance(ROUTE_RENEW_EVERY_MS * 2)
      expect(states).toEqual(['lost', 'ready'])

      const event = accepted(await fx.backend.commitLane(roomId, inc, SEMANTIC, bytes('after-reestablish')))
      await event.delivery
      expect(event.receivers).toBe(1)
      expect(received.payloads()).toEqual(['after-reestablish'])
      await sub.unsubscribe()
    })

    it('uses the production bounded retry path after an initial transient establishment failure', async () => {
      const { roomId, inc } = await openCfRoom()
      const controls = cloudflareRenewalControls(fx.backend)
      controls.forceEstablishmentFailures(2)
      const sub = fx.backend.subscribeLane(roomId, inc, SEMANTIC, () => {})
      const states: string[] = []
      sub.onStateChange((state) => states.push(state))

      expect(await settled(sub.ready)).toBe('rejected')
      expect(sub.state()).toBe('establishing')
      await controls.advance(250 + 500)
      expect(sub.state()).toBe('ready')
      expect(states).toEqual(['ready'])
      await sub.unsubscribe()
    })

    it('closes after five bounded re-establishment attempts following renewal loss', async () => {
      const { roomId, inc } = await openCfRoom()
      const sub = fx.backend.subscribeLane(roomId, inc, SEMANTIC, () => {})
      await sub.ready
      const states: string[] = []
      sub.onStateChange((state) => states.push(state))
      const controls = cloudflareRenewalControls(fx.backend)
      controls.forceFailures(2)
      controls.forceEstablishmentFailures(5)

      await controls.advance(ROUTE_RENEW_EVERY_MS * 2 + 250 + 500 + 1_000 + 2_000)
      expect(sub.state()).toBe('closed')
      expect(states).toEqual(['lost', 'closed'])
    })

    it('unsubscribe is lease-guarded: a racing stale lease cannot remove the re-established route', async () => {
      const { roomId, inc, stub } = await openCfRoom()
      const lk = laneKeyOf(SEMANTIC)
      const subscriber = 'subscriber-do-c'
      const receiver = () => {}
      await installReceiver(roomId, inc, lk, subscriber, 'lease-old', receiver)

      await stub.registerRoute(roomId, inc, lk, subscriber, 'lease-old', null)
      await installReceiver(roomId, inc, lk, subscriber, 'lease-new', receiver)
      await stub.registerRoute(roomId, inc, lk, subscriber, 'lease-new', null)

      // a delayed unsubscribe carrying the superseded lease matches nothing (all four fields compared)
      await stub.unsubscribeRoute(inc, lk, subscriber, 'lease-old')
      expect(acceptedCommit(await stub.commitLane(roomId, inc, SEMANTIC, bytes('x'))).receivers).toBe(1)

      // the current lease does remove it
      await stub.unsubscribeRoute(inc, lk, subscriber, 'lease-new')
      expect(acceptedCommit(await stub.commitLane(roomId, inc, SEMANTIC, bytes('y'))).receivers).toBe(0)
    })
  })

  // ── delivery at-most-once (readiness-ordering §3.4) ──

  describe('delivery at-most-once — eviction between acceptance and attempt', () => {
    it('loses the evicted frame without retry, keeps acceptance durable, and does not poison the lane', async () => {
      const { roomId, inc, stub } = await openCfRoom()
      const lk = laneKeyOf(SEMANTIC)
      const subscriber = 'subscriber-do-evict'
      const got: string[] = []
      const receiver = (payload: Uint8Array) => {
        got.push(text(payload))
      }
      await installReceiver(roomId, inc, lk, subscriber, 'lease-1', receiver)
      await stub.registerRoute(roomId, inc, lk, subscriber, 'lease-1', null)
      await holdReceiverDeliveries(subscriber)

      // frame 1: accepted + retained (durable in SQL), then the subscriber isolate is EVICTED before the
      // handoff attempt runs — the frame is lost at-most-once, never retried.
      const c1 = acceptedCommit(await stub.commitLane(roomId, inc, SEMANTIC, bytes('K1'), { retain: true }))
      await evictReceiver(roomId, inc, lk, subscriber, 'lease-1')
      await releaseReceiverDeliveries(subscriber)
      await stub.awaitDelivery(c1.deliveryToken)
      expect(got).toEqual([]) // lost — no delivery, no retry

      // acceptance stood: the retained generation and the advanced order are durable regardless
      const retained1 = await fx.backend.readRetained(roomId, inc, SEMANTIC)
      expect(retained1 === null ? null : text(retained1.payload)).toBe('K1')
      expect(c1.seq).toBe(1)

      // the lane is not poisoned: a re-installed receiver gets the next frame, order keeps advancing
      await installReceiver(roomId, inc, lk, subscriber, 'lease-1', receiver)
      const c2 = acceptedCommit(await stub.commitLane(roomId, inc, SEMANTIC, bytes('K2'), { retain: true }))
      await stub.awaitDelivery(c2.deliveryToken)
      await evictReceiver(roomId, inc, lk, subscriber, 'lease-1')

      expect(got).toEqual(['K2'])
      expect(c2.seq).toBe(2)
      const retained2 = await fx.backend.readRetained(roomId, inc, SEMANTIC)
      expect(retained2 === null ? null : text(retained2.payload)).toBe('K2')
    })

    it('fences a held old lease after dropGeneration and legal reuse of the same incarnation id', async () => {
      const { roomId, inc, stub } = await openCfRoom()
      const laneKey = laneKeyOf(SEMANTIC)
      const subscriber = 'subscriber-do-reused-inc'
      const seen: string[] = []
      await installReceiver(roomId, inc, laneKey, subscriber, 'lease-old', (payload) => seen.push(text(payload)))
      await stub.registerRoute(roomId, inc, laneKey, subscriber, 'lease-old', null)
      await holdReceiverDeliveries(subscriber)
      const oldAttempt = acceptedCommit(await stub.commitLane(roomId, inc, SEMANTIC, bytes('old-frame')))
      await waitForReceiverDelivery(subscriber)

      const head = await readHeadOrThrow(fx.backend, roomId)
      const { head: closing, leaseId } = await enterClosing(fx.backend, roomId, head)
      accepted(await fx.backend.commitLane(roomId, inc, CONTROL, bytes('closed'), { closingLease: leaseId }))
      const tombstone = okHead(await finalizeClose(fx.backend, roomId, closing, leaseId))
      await fx.backend.dropGeneration(roomId, inc)
      await evictReceiver(roomId, inc, laneKey, subscriber, 'lease-old')

      okHead(
        await fx.backend.compareExchangeHead(
          roomId,
          { expect: { rev: tombstone.rev } },
          { head: { currentInc: inc, state: 'open', config: tombstone.config } },
        ),
      )
      await installReceiver(roomId, inc, laneKey, subscriber, 'lease-new', (payload) => seen.push(text(payload)))
      await stub.registerRoute(roomId, inc, laneKey, subscriber, 'lease-new', null)

      await releaseReceiverDeliveries(subscriber)
      await stub.awaitDelivery(oldAttempt.deliveryToken)
      const fresh = acceptedCommit(await stub.commitLane(roomId, inc, SEMANTIC, bytes('new-frame')))
      await stub.awaitDelivery(fresh.deliveryToken)
      expect(seen).toEqual(['new-frame'])
      await evictReceiver(roomId, inc, laneKey, subscriber, 'lease-new')
    })
  })

  describe('room-authority delivery ownership', () => {
    it('orders one lane across two backend facade instances sharing the same room DO', async () => {
      const second = await cloudflareHarness.create()
      try {
        const { roomId, inc } = await openCfRoom()
        const gate = deferred()
        const entries: string[] = []
        const sub = fx.backend.subscribeLane(roomId, inc, SEMANTIC, async (payload) => {
          const value = text(payload)
          entries.push(value)
          if (value === 'one') await gate.promise
        })
        await sub.ready

        const one = accepted(await fx.backend.commitLane(roomId, inc, SEMANTIC, bytes('one')))
        const two = accepted(await second.backend.commitLane(roomId, inc, SEMANTIC, bytes('two')))
        await flush()
        expect(entries).toEqual(['one'])

        gate.resolve()
        await Promise.all([one.delivery, two.delivery])
        expect(entries).toEqual(['one', 'two'])
      } finally {
        await second.dispose()
      }
    })

    it('snapshots mutable payload bytes before a deferred delivery attempt', async () => {
      const { roomId, inc } = await openCfRoom()
      const gate = deferred()
      const seen: string[] = []
      const sub = fx.backend.subscribeLane(roomId, inc, SEMANTIC, async (payload) => {
        const value = text(payload)
        seen.push(value)
        if (value === 'first') await gate.promise
      })
      await sub.ready
      const first = accepted(await fx.backend.commitLane(roomId, inc, SEMANTIC, bytes('first')))
      const mutable = bytes('good')
      const second = accepted(await fx.backend.commitLane(roomId, inc, SEMANTIC, mutable))
      mutable.set(bytes('evil'))
      await flush()
      expect(seen).toEqual(['first'])
      gate.resolve()
      await Promise.all([first.delivery, second.delivery])
      expect(seen).toEqual(['first', 'good'])
    })

    it('the production Fanout snapshots bytes at enqueue before its lane tail runs', async () => {
      const gate = deferred()
      const seen: string[] = []
      const fanout = new Fanout(async (_target, payload) => {
        const value = text(payload)
        seen.push(value)
        if (value === 'first') await gate.promise
      })
      const info = { roomId: 'room', inc: 'inc', laneKey: 'semantic', seq: 1, timestamp: 1 }
      const target = [{ subscriber: 'subscriber', leaseId: 'lease' }]
      const first = fanout.enqueue('inc', 'semantic', target, bytes('first'), info)
      const mutable = bytes('good')
      const second = fanout.enqueue('inc', 'semantic', target, mutable, { ...info, seq: 2 })
      mutable.set(bytes('evil'))
      await flush()
      expect(seen).toEqual(['first'])
      gate.resolve()
      await Promise.all([fanout.await(first), fanout.await(second)])
      expect(seen).toEqual(['first', 'good'])
    })

    it('scopes one subscriber DO name by room, incarnation, and lane', async () => {
      const first = await openCfRoom()
      const second = await openCfRoom()
      const subscriber = 'shared-representative-do'
      const semantic = laneKeyOf(SEMANTIC)
      const control = laneKeyOf(CONTROL)
      const seen: string[] = []
      await installReceiver(first.roomId, first.inc, semantic, subscriber, 'lease-a-s', () =>
        seen.push('room-a-semantic'),
      )
      await installReceiver(first.roomId, first.inc, control, subscriber, 'lease-a-c', () =>
        seen.push('room-a-control'),
      )
      await installReceiver(second.roomId, second.inc, semantic, subscriber, 'lease-b-s', () =>
        seen.push('room-b-semantic'),
      )
      await first.stub.registerRoute(first.roomId, first.inc, semantic, subscriber, 'lease-a-s', null)
      await first.stub.registerRoute(first.roomId, first.inc, control, subscriber, 'lease-a-c', null)
      await second.stub.registerRoute(second.roomId, second.inc, semantic, subscriber, 'lease-b-s', null)

      const aSemantic = acceptedCommit(
        await first.stub.commitLane(first.roomId, first.inc, SEMANTIC, bytes('a-semantic')),
      )
      const aControl = acceptedCommit(await first.stub.commitLane(first.roomId, first.inc, CONTROL, bytes('a-control')))
      const bSemantic = acceptedCommit(
        await second.stub.commitLane(second.roomId, second.inc, SEMANTIC, bytes('b-semantic')),
      )
      await Promise.all([
        first.stub.awaitDelivery(aSemantic.deliveryToken),
        first.stub.awaitDelivery(aControl.deliveryToken),
        second.stub.awaitDelivery(bSemantic.deliveryToken),
      ])

      expect(seen.sort()).toEqual(['room-a-control', 'room-a-semantic', 'room-b-semantic'])
    })
  })

  describe('post-ready target failure eviction', () => {
    it('drops the guarded route after K=3 consecutive target failures', async () => {
      const { roomId, inc } = await openCfRoom()
      const sub = fx.backend.subscribeLane(roomId, inc, SEMANTIC, () => {
        throw new Error('target failed')
      })
      await sub.ready

      for (let attempt = 1; attempt <= 3; attempt++) {
        const frame = accepted(await fx.backend.commitLane(roomId, inc, SEMANTIC, bytes(`bad-${attempt}`)))
        expect(frame.receivers).toBe(1)
        expect(await settled(frame.delivery)).toBe('rejected')
      }
      const evicted = accepted(await fx.backend.commitLane(roomId, inc, SEMANTIC, bytes('fourth')))
      expect(evicted.receivers).toBe(0)
      expect(await settled(evicted.delivery)).toBe('resolved')
    })

    it('makes a late mux attachment await re-establishment after K=3 authority eviction', async () => {
      const { roomId, inc } = await openCfRoom()
      let failuresRemaining = 3
      const firstSeen: string[] = []
      const lateSeen: string[] = []
      const first = fx.backend.subscribeLane(roomId, inc, SEMANTIC, (payload) => {
        if (failuresRemaining > 0) {
          failuresRemaining -= 1
          throw new Error('target failed')
        }
        firstSeen.push(text(payload))
      })
      await first.ready
      for (let attempt = 1; attempt <= 3; attempt++) {
        const frame = accepted(await fx.backend.commitLane(roomId, inc, SEMANTIC, bytes(`bad-${attempt}`)))
        expect(await settled(frame.delivery)).toBe('rejected')
      }

      // Keep the first re-establishment attempt pending behind the production retry scheduler. A stale
      // handout of the shared historical ready promise would resolve the late attachment immediately.
      const controls = cloudflareRenewalControls(fx.backend)
      controls.forceEstablishmentFailures(1)
      const late = fx.backend.subscribeLane(roomId, inc, SEMANTIC, (payload) => lateSeen.push(text(payload)))
      let lateReady = false
      void late.ready.then(() => {
        lateReady = true
      })
      await waitUntil(async () => late.state() === 'lost')
      expect(lateReady).toBe(false)

      await controls.advance(250)
      await late.ready
      const fresh = accepted(await fx.backend.commitLane(roomId, inc, SEMANTIC, bytes('fresh')))
      expect(fresh.receivers).toBe(1)
      await fresh.delivery
      expect(firstSeen).toEqual(['fresh'])
      expect(lateSeen).toEqual(['fresh'])
      await late.unsubscribe()
      await first.unsubscribe()
    })

    it('resets the consecutive counter after a successful target handoff', async () => {
      const { roomId, inc } = await openCfRoom()
      let fail = true
      const sub = fx.backend.subscribeLane(roomId, inc, SEMANTIC, () => {
        if (fail) throw new Error('target failed')
      })
      await sub.ready
      for (let index = 0; index < 2; index++) {
        const frame = accepted(await fx.backend.commitLane(roomId, inc, SEMANTIC, bytes('bad')))
        expect(await settled(frame.delivery)).toBe('rejected')
      }
      fail = false
      await accepted(await fx.backend.commitLane(roomId, inc, SEMANTIC, bytes('good'))).delivery
      fail = true
      for (let index = 0; index < 2; index++) {
        const frame = accepted(await fx.backend.commitLane(roomId, inc, SEMANTIC, bytes('bad-again')))
        expect(await settled(frame.delivery)).toBe('rejected')
      }
      const stillRouted = accepted(await fx.backend.commitLane(roomId, inc, SEMANTIC, bytes('still-routed')))
      expect(stillRouted.receivers).toBe(1)
      expect(await settled(stillRouted.delivery)).toBe('rejected')
    })

    it('does not let late failures from a replaced lease evict the current route', async () => {
      const { roomId, inc, stub } = await openCfRoom()
      const laneKey = laneKeyOf(SEMANTIC)
      const subscriber = 'guarded-failure-subscriber'
      const receiver = () => {
        throw new Error('old attempt failed')
      }
      await installReceiver(roomId, inc, laneKey, subscriber, 'lease-old', receiver)
      await stub.registerRoute(roomId, inc, laneKey, subscriber, 'lease-old', null)
      await holdReceiverDeliveries(subscriber)
      const oldAttempts = await Promise.all(
        [1, 2, 3].map(async (index) =>
          acceptedCommit(await stub.commitLane(roomId, inc, SEMANTIC, bytes(`old-${index}`))),
        ),
      )
      await installReceiver(roomId, inc, laneKey, subscriber, 'lease-current', receiver)
      await stub.registerRoute(roomId, inc, laneKey, subscriber, 'lease-current', null)
      // Re-install the snapshotted lease so the held old attempts genuinely fail at the callback; the
      // room's lease-guarded failure accounting must still leave the replacement route intact.
      await installReceiver(roomId, inc, laneKey, subscriber, 'lease-old', receiver)
      await releaseReceiverDeliveries(subscriber)
      for (const attempt of oldAttempts) {
        await expect(stub.awaitDelivery(attempt.deliveryToken)).rejects.toThrow()
      }

      await installReceiver(roomId, inc, laneKey, subscriber, 'lease-current', receiver)
      const current = acceptedCommit(await stub.commitLane(roomId, inc, SEMANTIC, bytes('current')))
      expect(current.receivers).toBe(1)
      await expect(stub.awaitDelivery(current.deliveryToken)).rejects.toThrow()
    })
  })

  describe('SQLite supplementary-Unicode prefix parity', () => {
    it('returns astral-plane cell and directory keys from exact prefix scans', async () => {
      const { roomId, inc } = await openCfRoom()
      const initial = cellsOf(await fx.backend.readCells(roomId, inc, { prefix: 'member/' }))
      expect(
        await fx.backend.compareExchangeCells(roomId, inc, initial.revision, [
          { key: 'member/alice', set: { bytes: bytes('a') } },
          { key: 'member/😀', set: { bytes: bytes('emoji') } },
          { key: 'other/😀', set: { bytes: bytes('other') } },
        ]),
      ).toBe('committed')
      const cells = cellsOf(await fx.backend.readCells(roomId, inc, { prefix: 'member/' }))
      expect([...cells.cells.keys()].sort()).toEqual(['member/alice', 'member/😀'].sort())

      await fx.backend.directoryPut('group/alice', 'inc-a')
      await fx.backend.directoryPut('group/😀', 'inc-emoji')
      await fx.backend.directoryPut('other/😀', 'inc-other')
      const directory = await fx.backend.directoryList('group/')
      expect(directory.entries.map((entry) => entry.roomId).sort()).toEqual(['group/alice', 'group/😀'].sort())
    })
  })

  describe('alarm janitor — observation-aged orphan generations', () => {
    it('keeps the durable generation retry source when subscriber uninstall fails', async () => {
      const { roomId, inc, stub } = await openCfRoom()
      const laneKey = laneKeyOf(SEMANTIC)
      const subscriber = 'subscriber-do-retryable-generation-drop'
      const leaseId = 'lease-retryable-drop'
      await installReceiver(roomId, inc, laneKey, subscriber, leaseId, () => {})
      await stub.registerRoute(roomId, inc, laneKey, subscriber, leaseId, null)

      const head = await readHeadOrThrow(fx.backend, roomId)
      const { head: closing, leaseId: closeLease } = await enterClosing(fx.backend, roomId, head)
      accepted(await fx.backend.commitLane(roomId, inc, CONTROL, bytes('closed'), { closingLease: closeLease }))
      await finalizeClose(fx.backend, roomId, closing, closeLease)

      await failReceiverUninstalls(subscriber, 1)
      await expect(stub.dropGeneration(inc)).rejects.toThrow()
      expect(await stub.listGenerations()).toContain(inc)

      const retried = await stub.dropGeneration(inc)
      if ('error' in retried) throw new Error(retried.error)
      expect(retried.droppedSubscribers).toEqual([[laneKey, subscriber]])
      expect(await stub.listGenerations()).not.toContain(inc)
      await evictReceiver(roomId, inc, laneKey, subscriber, leaseId)
    })

    it('keeps an orphan for the full 60s grace, then deletes only that non-current generation', async () => {
      const roomId = nextId('cf-room')
      const { inc: orphan, head } = await openRoom(fx.backend, roomId)
      const { head: closing, leaseId } = await enterClosing(fx.backend, roomId, head)
      accepted(await fx.backend.commitLane(roomId, orphan, CONTROL, bytes('closed'), { closingLease: leaseId }))
      const tombstone = okHead(await finalizeClose(fx.backend, roomId, closing, leaseId))
      const { inc: current } = await openRoom(fx.backend, roomId, { prior: tombstone })
      const stub = await cloudflareRoomStub(roomId)

      await stub.runJanitor() // first non-current observation starts orphan_since
      expect((await stub.listGenerations()).sort()).toEqual([current, orphan].sort())

      fx.advanceAuthority(GEN_ORPHAN_GRACE_MS - 1)
      await fx.backend.readHead(roomId) // flush the controlled authority clock into workerd
      await stub.runJanitor()
      expect((await stub.listGenerations()).sort()).toEqual([current, orphan].sort())

      fx.advanceAuthority(1)
      await fx.backend.readHead(roomId)
      await stub.runJanitor()
      expect(await stub.listGenerations()).toEqual([current])
      expect((await readHeadOrThrow(fx.backend, roomId)).currentInc).toBe(current)
    })

    it('invalidates a held subscriber attempt when the alarm sweep drops its orphan generation', async () => {
      const roomId = nextId('cf-alarm-held-room')
      const { inc: orphan, head } = await openRoom(fx.backend, roomId)
      const stub = await cloudflareRoomStub(roomId)
      const laneKey = laneKeyOf(SEMANTIC)
      const subscriber = 'subscriber-do-alarm-orphan'
      const seen: string[] = []
      await installReceiver(roomId, orphan, laneKey, subscriber, 'lease-orphan', (payload) => seen.push(text(payload)))
      await stub.registerRoute(roomId, orphan, laneKey, subscriber, 'lease-orphan', null)
      await holdReceiverDeliveries(subscriber)
      const held = acceptedCommit(await stub.commitLane(roomId, orphan, SEMANTIC, bytes('must-discard')))
      await waitForReceiverDelivery(subscriber)

      const { head: closing, leaseId } = await enterClosing(fx.backend, roomId, head)
      accepted(await fx.backend.commitLane(roomId, orphan, CONTROL, bytes('closed'), { closingLease: leaseId }))
      const tombstone = okHead(await finalizeClose(fx.backend, roomId, closing, leaseId))
      await openRoom(fx.backend, roomId, { prior: tombstone })

      await stub.runJanitor()
      fx.advanceAuthority(GEN_ORPHAN_GRACE_MS)
      await fx.backend.readHead(roomId)
      await stub.runJanitor()
      await releaseReceiverDeliveries(subscriber)
      await stub.awaitDelivery(held.deliveryToken)
      expect(seen).toEqual([])
    })

    it('closes an alarm-dropped mux instead of renewing it into a reused incarnation id', async () => {
      const roomId = nextId('cf-alarm-mux-room')
      const { inc: orphan, head } = await openRoom(fx.backend, roomId)
      const oldSeen: string[] = []
      const old = fx.backend.subscribeLane(roomId, orphan, SEMANTIC, (payload) => oldSeen.push(text(payload)))
      await old.ready

      const { head: orphanClosing, leaseId: orphanLease } = await enterClosing(fx.backend, roomId, head)
      accepted(await fx.backend.commitLane(roomId, orphan, CONTROL, bytes('closed'), { closingLease: orphanLease }))
      const firstTombstone = okHead(await finalizeClose(fx.backend, roomId, orphanClosing, orphanLease))
      const { inc: current, head: currentHead } = await openRoom(fx.backend, roomId, { prior: firstTombstone })
      const stub = await cloudflareRoomStub(roomId)

      await stub.runJanitor()
      fx.advanceAuthority(GEN_ORPHAN_GRACE_MS)
      await fx.backend.readHead(roomId)
      await stub.runJanitor()
      expect(await stub.listGenerations()).not.toContain(orphan)

      const { head: currentClosing, leaseId: currentLease } = await enterClosing(fx.backend, roomId, currentHead)
      accepted(await fx.backend.commitLane(roomId, current, CONTROL, bytes('closed'), { closingLease: currentLease }))
      const secondTombstone = okHead(await finalizeClose(fx.backend, roomId, currentClosing, currentLease))
      okHead(
        await fx.backend.compareExchangeHead(
          roomId,
          { expect: { rev: secondTombstone.rev } },
          { head: { currentInc: orphan, state: 'open', config: secondTombstone.config } },
        ),
      )

      const controls = cloudflareRenewalControls(fx.backend)
      await controls.advance(ROUTE_RENEW_EVERY_MS)
      expect(old.state()).toBe('closed')
      const withoutFreshSubscription = accepted(
        await fx.backend.commitLane(roomId, orphan, SEMANTIC, bytes('must-have-no-old-target')),
      )
      expect(withoutFreshSubscription.receivers).toBe(0)
      await withoutFreshSubscription.delivery

      const freshSeen: string[] = []
      const fresh = fx.backend.subscribeLane(roomId, orphan, SEMANTIC, (payload) => freshSeen.push(text(payload)))
      await fresh.ready
      const onlyFresh = accepted(await fx.backend.commitLane(roomId, orphan, SEMANTIC, bytes('fresh-only')))
      expect(onlyFresh.receivers).toBe(1)
      await onlyFresh.delivery
      expect(oldSeen).toEqual([])
      expect(freshSeen).toEqual(['fresh-only'])
      await fresh.unsubscribe()
    })

    it('schedules and re-arms a real workerd alarm without the runJanitor test RPC', async () => {
      const roomId = nextId('cf-alarm-room')
      const { inc: orphan, head } = await openRoom(fx.backend, roomId)
      const { head: closing, leaseId } = await enterClosing(fx.backend, roomId, head)
      accepted(await fx.backend.commitLane(roomId, orphan, CONTROL, bytes('closed'), { closingLease: leaseId }))
      const tombstone = okHead(await finalizeClose(fx.backend, roomId, closing, leaseId))
      const { inc: current } = await openRoom(fx.backend, roomId, { prior: tombstone })
      const stub = await cloudflareRoomStub(roomId)

      // The 500ms fixture binding shortens only the production setAlarm cadence. Give one alarm turn time
      // to observe/stamp the orphan, then advance the authority clock through the normative grace window.
      await new Promise<void>((resolve) => setTimeout(resolve, 600))
      fx.advanceAuthority(GEN_ORPHAN_GRACE_MS)
      await fx.backend.readHead(roomId)
      await waitUntil(async () => !(await stub.listGenerations()).includes(orphan))

      expect(await stub.listGenerations()).toEqual([current])
      expect((await readHeadOrThrow(fx.backend, roomId)).currentInc).toBe(current)
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
