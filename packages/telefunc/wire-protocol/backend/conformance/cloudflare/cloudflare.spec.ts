import { afterAll, afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { Miniflare } from 'miniflare'
import { disposeRoomBackend, installRoomBackend } from '../../install.js'
import type { LaneId, LaneReceiver, ReadinessState, RoomBackendSpi } from '../../spi.js'
import { CHANNEL_PING_INTERVAL_MS } from '../../../constants.js'
import { frameRoomBinaryOrder, unframeRoomBinaryOrder } from '../../../room/protocol.js'
import { Room, ServerRoom } from '../../../room/server.js'
import {
  CloudflareRoomBackend,
  CLOUDFLARE_ROOM_CONTEXT_ERROR,
  requireCloudflareRoomNamespace,
} from '../../../server/adapter/cloudflare/room/backend.js'
import { laneKey as laneKeyOf } from '../../../server/adapter/cloudflare/room/codec.js'
import {
  ROUTE_CAPTURE_TTL_MS,
  ROUTE_RENEW_EVERY_MS,
  ROUTE_TTL_MS,
} from '../../../server/adapter/cloudflare/room/routes.js'
import { GEN_ORPHAN_GRACE_MS } from '../../../server/adapter/cloudflare/room/storage.js'
import type { BackendFixture } from '../harness.js'
import { conformanceReceiver } from '../receiver.js'
import {
  accepted,
  bytes,
  collector,
  CONTROL,
  enterClosing,
  finalizeClose,
  nextId,
  noopReceiver,
  okHead,
  openRoom,
  readHeadOrThrow,
  SEMANTIC,
  sequencedReceiver,
  settled,
  stallingReceiver,
  text,
  throwingReceiver,
} from '../scenario.js'
import {
  cloudflareCommitFromSession,
  cloudflareClearOwnershipProbes,
  cloudflareContextProbe,
  cloudflareHarness,
  cloudflareMissingBindingSubscriptionProbe,
  cloudflareRenewalControls,
  cloudflareRoomStub,
  cloudflareSessionId,
  cloudflareSharedBackendOwnershipProbe,
  disposeCloudflareRoomStubs,
  disposeSharedMiniflare,
  flushCloudflareAuthorityClock,
  runCloudflareSocketRecoverySchedule,
  type RoomStub,
} from './fixture.js'
import { bundleWorker } from './bundle.js'

type Opened = { roomId: string; inc: string; stub: RoomStub }

describe('cloudflare — production session-manager mechanics', () => {
  let fx: BackendFixture

  beforeEach(async () => {
    fx = await cloudflareHarness.create()
  })

  afterEach(async () => {
    disposeCloudflareRoomStubs()
    await fx.dispose()
  })

  afterAll(async () => {
    await disposeSharedMiniflare()
  })

  async function opened(prefix: string = 'cf-room'): Promise<Opened> {
    const roomId = nextId(prefix)
    const { inc } = await openRoom(fx.backend, roomId)
    return { roomId, inc, stub: await cloudflareRoomStub(roomId) }
  }

  async function close(roomId: string, inc: string) {
    const head = await readHeadOrThrow(fx.backend, roomId)
    const { head: closing, leaseId } = await enterClosing(fx.backend, roomId, head)
    accepted(await fx.backend.commitLane(roomId, inc, CONTROL, bytes('closed'), { closingLease: leaseId }))
    return okHead(await finalizeClose(fx.backend, roomId, closing, leaseId))
  }

  async function directRegistration(
    target: Opened,
    subscriberDoId: string,
    leaseId: string,
    lane: LaneId = SEMANTIC,
    attempt?: { id: string; createdAt: number },
  ) {
    const capture = await target.stub.captureRouteGeneration(incOf(target), attempt?.id, attempt?.createdAt)
    if ('rejected' in capture) return capture
    return target.stub.registerRoute(
      target.roomId,
      target.inc,
      laneKeyOf(lane),
      subscriberDoId,
      leaseId,
      capture.generationToken,
      attempt?.id,
      attempt?.createdAt,
    )
  }

  function incOf(target: Opened): string {
    return target.inc
  }

  async function waitUntil(predicate: () => Promise<boolean>, timeoutMs: number = 3_000): Promise<void> {
    const deadline = Date.now() + timeoutMs
    while (!(await predicate())) {
      if (Date.now() >= deadline) throw new Error('condition did not become true before timeout')
      await new Promise<void>((resolve) => setTimeout(resolve, 50))
    }
  }

  it('round-trips the 3/8/16 MiB retained chunk matrix through local workerd', async () => {
    for (const mib of [3, 8, 16]) {
      const { roomId, inc } = await opened(`retained-${mib}mib`)
      const payload = new Uint8Array(mib * 1024 * 1024)
      for (let offset = 0, value = 1; offset < payload.byteLength; offset += 1024 * 1024, value += 1) {
        payload.fill(value, offset, Math.min(payload.byteLength, offset + 1024 * 1024))
      }

      const committed = accepted(await fx.backend.commitLane(roomId, inc, SEMANTIC, payload, { retain: true }))
      await committed.delivery
      const retained = await fx.backend.readRetained(roomId, inc, SEMANTIC)

      expect(retained?.payload.byteLength).toBe(payload.byteLength)
      expect(retained?.seq).toBe(committed.seq)
      expect(retained?.timestamp).toBe(committed.timestamp)
      for (let offset = 0; offset < payload.byteLength; offset += 1024 * 1024) {
        expect(retained?.payload[offset]).toBe(payload[offset])
      }
    }
  }, 60_000)

  it('round-trips a binary cursor above u32 through workerd live delivery, SQLite retained state and the Room client decoder', async () => {
    const { roomId, inc, stub } = await opened('wide-binary')
    const lane = { kind: 'binary', member: crypto.randomUUID(), track: 'camera' } satisfies LaneId
    const seen = collector()
    const sub = fx.backend.subscribeLane(roomId, inc, lane, seen.receiver)
    try {
      await sub.ready
      await stub.telefuncRoomSeedOrderWatermarkForTest(inc, lane, 0xffff_ffff, 10)
      const payload = bytes('cloudflare-wide-binary')
      const committed = accepted(await fx.backend.commitLane(roomId, inc, lane, payload, { retain: true }))
      await committed.delivery
      await seen.waitFor(1)

      expect(committed.seq).toBe(0x1_0000_0000)
      expect(seen.frames[0]?.seq).toBe(0x1_0000_0000)
      const liveDecoded = unframeRoomBinaryOrder(
        frameRoomBinaryOrder(bytes(seen.frames[0]?.payload as string), {
          seq: seen.frames[0]?.seq as number,
          timestamp: seen.frames[0]?.timestamp as number,
        }),
      )
      expect(liveDecoded?.info.seq).toBe(0x1_0000_0000)
      expect(text(liveDecoded?.payload as Uint8Array)).toBe('cloudflare-wide-binary')

      const retained = await fx.backend.readRetained(roomId, inc, lane)
      expect(retained?.seq).toBe(0x1_0000_0000)
      const retainedDecoded = unframeRoomBinaryOrder(
        frameRoomBinaryOrder(retained?.payload as Uint8Array, {
          seq: retained?.seq as number,
          timestamp: retained?.timestamp as number,
        }),
      )
      expect(retainedDecoded?.info.seq).toBe(0x1_0000_0000)
      expect(text(retainedDecoded?.payload as Uint8Array)).toBe('cloudflare-wide-binary')
    } finally {
      await sub.unsubscribe().catch(() => {})
    }
  })

  it('recovers a public Room lane after real workerd loss and a synchronous first replacement failure', async () => {
    const roomId = nextId('managed-room-renewal')
    const states: ReadinessState[] = []
    const observed: string[] = []
    const remoteReceivers = new Set<LaneReceiver>()
    const report = vi.spyOn(console, 'error').mockImplementation(() => {})
    const baseRoomBackend = roomCapableBackend(
      fx.backend,
      (id, lane, state) => {
        if (id === roomId && lane.kind === 'semantic') states.push(state)
      },
      remoteReceivers,
    )
    let semanticCalls = 0
    let closeQueued = false
    let semanticSubscription: ReturnType<RoomBackendSpi['subscribeLane']> | undefined
    const roomBackend = new Proxy(baseRoomBackend, {
      get(target, property) {
        if (property !== 'subscribeLane') {
          const value = Reflect.get(target, property, target)
          return typeof value === 'function' ? value.bind(target) : value
        }
        return (id: string, inc: string, lane: LaneId, receiver: LaneReceiver) => {
          if (id === roomId && lane.kind === 'semantic') {
            semanticCalls++
            if (semanticCalls === 2) throw new Error('synthetic workerd replacement subscribe failure')
          }
          const subscription = target.subscribeLane(id, inc, lane, receiver)
          if (id === roomId && lane.kind === 'semantic') {
            semanticSubscription = subscription
            if (semanticCalls === 1) {
              subscription.onStateChange((state) => {
                if (state === 'lost' && !closeQueued) {
                  closeQueued = true
                  // Preserve the real workerd lost event, then terminally end the old generation so
                  // Room must execute its replacement-subscribe recovery branch.
                  queueMicrotask(() => void subscription.unsubscribe().catch(() => {}))
                }
              })
            }
          }
          return subscription
        }
      },
    })
    installRoomBackend(() => roomBackend)
    try {
      const room = await Room.create(roomId)
      room.onAnnounce((data) => observed.push(String(data)))
      await Room.announce(roomId, 'before-loss')
      expect(observed).toEqual(['before-loss'])

      const controls = cloudflareRenewalControls(fx.backend)
      // Room owns a fixed control lane and the semantic lane under test. Two consecutive renewal
      // failures per route drive each production subscription through lost -> replacement -> ready.
      controls.forceFailures(4)
      await controls.advance(ROUTE_RENEW_EVERY_MS * 2)
      await waitUntil(async () => semanticCalls === 3 && semanticSubscription?.state() === 'ready')
      states.push(semanticSubscription?.state() as ReadinessState)
      expect(semanticCalls).toBe(3)
      expect(states).toEqual(['lost', 'ready', 'closed', 'ready'])
      expect(
        report.mock.calls.some(([error]) => String(error).includes('synthetic workerd replacement subscribe failure')),
      ).toBe(true)

      await Room.announce(roomId, 'after-recovery')
      expect(observed).toEqual(['before-loss', 'after-recovery'])
      console.log(
        `[w5-integration] cloudflare-room-renewal states=${states.join(',')} subscribeCalls=${semanticCalls} replacementFailures=1 delivered=${observed.length}`,
      )
      await Room.close(roomId)
    } finally {
      report.mockRestore()
      remoteReceivers.clear()
      await disposeRoomBackend().catch(() => {})
    }
  })

  it('closes an observed public Room on workerd without failure injection', async () => {
    const roomId = nextId('managed-room-close-probe')
    const remoteReceivers = new Set<LaneReceiver>()
    const roomBackend = roomCapableBackend(fx.backend, () => {}, remoteReceivers)
    installRoomBackend(() => roomBackend)
    try {
      const room = await Room.create(roomId)
      const firstInc = (room as ServerRoom)._inc
      const unlisten = room.onAnnounce(() => {})
      await Room.announce(roomId, 'ready')
      await Room.close(roomId)
      expect((await fx.backend.readHead(roomId))?.head).toMatchObject({ state: 'closed', currentInc: null })
      expect(await fx.backend.listGenerations(roomId)).toEqual([])
      expect((await fx.backend.directoryList(roomId)).entries).toEqual([])
      const recreated = await Room.create(roomId)
      expect((recreated as ServerRoom)._inc).not.toBe(firstInc)
      await Room.close(roomId)
      unlisten()
    } finally {
      remoteReceivers.clear()
      await disposeRoomBackend().catch(() => {})
    }
  })

  it('rolls the order advance back when the retained install fails inside the acceptance transaction', async () => {
    const { roomId, inc, stub } = await opened('retained-atomicity')
    await stub.telefuncRoomDropRetainedChunksForTest()

    await expect(fx.backend.commitLane(roomId, inc, SEMANTIC, bytes('fails'), { retain: true })).rejects.toThrow()
    const next = accepted(await fx.backend.commitLane(roomId, inc, SEMANTIC, bytes('next')))

    expect(next.seq).toBe(1)
  })

  describe('topology and receiver ownership', () => {
    it('rejects a non-canonical session id before persisting a route', async () => {
      const target = await opened()
      await expect(directRegistration(target, 'subscriber-do-missing', 'lease')).resolves.toEqual({
        rejected: true,
        reason: "subscriber Durable Object id 'subscriber-do-missing' is invalid",
        terminal: true,
      })
      expect(
        acceptedWire(await target.stub.commitLane(target.roomId, target.inc, SEMANTIC, bytes('none'))).receivers,
      ).toBe(0)
    })

    it('kills any Node callback replay path', async () => {
      const { roomId, inc } = await opened()
      expect(() => fx.backend.subscribeLane(roomId, inc, SEMANTIC, () => {})).toThrow(
        'Cloudflare conformance forbids Node receiver callbacks; use a receiver command',
      )
    })

    it('delivers through the exact worker-owned production manager', async () => {
      const { roomId, inc } = await opened()
      const seen = collector()
      const sub = fx.backend.subscribeLane(roomId, inc, SEMANTIC, seen.receiver)
      await sub.ready
      await accepted(await fx.backend.commitLane(roomId, inc, SEMANTIC, bytes('worker'))).delivery
      expect(seen.payloads()).toEqual(['worker'])
    })

    it('unwinds self-fanout before the same session shard callback runs', async () => {
      const { roomId, inc } = await opened()
      const seen = collector()
      const sub = fx.backend.subscribeLane(roomId, inc, SEMANTIC, seen.receiver)
      await sub.ready
      const result = accepted(await cloudflareCommitFromSession(fx.backend, roomId, inc, SEMANTIC, bytes('self')))
      expect(result.receivers).toBe(1)
      await result.delivery
      expect(seen.payloads()).toEqual(['self'])
    })

    it('multiplexes sibling callbacks behind one authority route', async () => {
      const { roomId, inc } = await opened()
      const a = collector()
      const b = collector()
      const one = fx.backend.subscribeLane(roomId, inc, SEMANTIC, a.receiver)
      const two = fx.backend.subscribeLane(roomId, inc, SEMANTIC, b.receiver)
      await Promise.all([one.ready, two.ready])
      const result = accepted(await fx.backend.commitLane(roomId, inc, SEMANTIC, bytes('both')))
      expect(result.receivers).toBe(1)
      await result.delivery
      expect([a.payloads(), b.payloads()]).toEqual([['both'], ['both']])
    })

    it('addresses two session shards as two exact targets without cross-delivery', async () => {
      const second = await cloudflareHarness.create()
      try {
        const { roomId, inc } = await opened()
        const firstReceiver = collector()
        const secondReceiver = collector()
        const first = fx.backend.subscribeLane(roomId, inc, SEMANTIC, firstReceiver.receiver)
        const other = second.backend.subscribeLane(roomId, inc, SEMANTIC, secondReceiver.receiver)
        await Promise.all([first.ready, other.ready])

        const result = accepted(await fx.backend.commitLane(roomId, inc, SEMANTIC, bytes('two-shards')))
        expect(result.receivers).toBe(2)
        await result.delivery
        await Promise.all([firstReceiver.waitFor(1), secondReceiver.waitFor(1)])
        expect(firstReceiver.payloads()).toEqual(['two-shards'])
        expect(secondReceiver.payloads()).toEqual(['two-shards'])
      } finally {
        await second.dispose()
      }
    })

    it('keeps the exact shard manager across interleaved awaits', async () => {
      const second = await cloudflareHarness.create()
      try {
        const firstProbe = cloudflareContextProbe(fx.backend, 10)
        await new Promise<void>((resolve) => setTimeout(resolve, 1))
        const secondProbe = cloudflareContextProbe(second.backend, 30)
        await expect(Promise.all([firstProbe, secondProbe])).resolves.toEqual([true, true])
      } finally {
        await second.dispose()
      }
    })

    it('resolves one shared backend to each event-local manager across interleaved awaits', async () => {
      const second = await cloudflareHarness.create()
      try {
        const { roomId, inc } = await opened('shared-proxy')
        const [firstId, secondId] = await Promise.all([
          cloudflareSharedBackendOwnershipProbe(fx.backend, roomId, inc, 30),
          cloudflareSharedBackendOwnershipProbe(second.backend, roomId, inc, 10),
        ])
        expect(firstId).not.toBe(secondId)
        expect(accepted(await fx.backend.commitLane(roomId, inc, SEMANTIC, bytes('two-managers'))).receivers).toBe(2)
      } finally {
        await Promise.all([cloudflareClearOwnershipProbes(fx.backend), cloudflareClearOwnershipProbes(second.backend)])
        await second.dispose()
      }
    })

    it('recovers an actual Room-want socket when delivery reaches the empty epoch first', async () => {
      const result = await runCloudflareSocketRecoverySchedule('delivery')
      expect(result.close).toEqual({ code: 1012, reason: 'Telefunc session reset; reconnect' })
      expect(result.oldDelivery).toBe('rejected')
      expect(result.observation.wants).toBeGreaterThanOrEqual(2)
      expect(result.observation.readyEpochs).toBeGreaterThanOrEqual(2)
      expect(result.observation.payloads).toEqual(['fresh-delivery'])
      expect(result.observation.errors).toEqual([])
    })

    it('recovers an actual Room-want socket when the reconnect message reaches the empty epoch first', async () => {
      const result = await runCloudflareSocketRecoverySchedule('message')
      expect(result.close).toEqual({ code: 1012, reason: 'Telefunc session reset; reconnect' })
      expect(result.oldDelivery).toBe('not-attempted')
      expect(result.observation.wants).toBeGreaterThanOrEqual(2)
      expect(result.observation.readyEpochs).toBeGreaterThanOrEqual(2)
      expect(result.observation.payloads).toEqual(['fresh-message'])
      expect(result.observation.errors).toEqual([])
    })

    it('keeps the authority route until the final local detach', async () => {
      const { roomId, inc } = await opened()
      const a = collector()
      const b = collector()
      const one = fx.backend.subscribeLane(roomId, inc, SEMANTIC, a.receiver)
      const two = fx.backend.subscribeLane(roomId, inc, SEMANTIC, b.receiver)
      await Promise.all([one.ready, two.ready])
      await two.unsubscribe()
      const live = accepted(await fx.backend.commitLane(roomId, inc, SEMANTIC, bytes('one')))
      expect(live.receivers).toBe(1)
      await live.delivery
      await one.unsubscribe()
      expect(accepted(await fx.backend.commitLane(roomId, inc, SEMANTIC, bytes('zero'))).receivers).toBe(0)
    })

    it('uses distinct worker manager routes for distinct lanes', async () => {
      const { roomId, inc } = await opened()
      const semantic = collector()
      const control = collector()
      await Promise.all([
        fx.backend.subscribeLane(roomId, inc, SEMANTIC, semantic.receiver).ready,
        fx.backend.subscribeLane(roomId, inc, CONTROL, control.receiver).ready,
      ])
      await accepted(await fx.backend.commitLane(roomId, inc, SEMANTIC, bytes('s'))).delivery
      await accepted(await fx.backend.commitLane(roomId, inc, CONTROL, bytes('c'))).delivery
      expect([semantic.payloads(), control.payloads()]).toEqual([['s'], ['c']])
    })

    it('does not expose a subscriber Durable Object or relay binding', async () => {
      const source = await bundleWorker()
      expect(source).not.toContain('TelefuncRoomSubscriberDurableObject')
      expect(source).not.toContain('TELEFUNC_ROOM_DELIVERY_RELAY')
    })

    it('fails closed outside await-safe raw context', () => {
      const backend = new CloudflareRoomBackend()
      expect(() => backend.subscribeLane('room', 'inc', SEMANTIC, () => {})).toThrow(CLOUDFLARE_ROOM_CONTEXT_ERROR)
    })

    it('fails early with the exact missing authority-binding diagnostic', () => {
      expect(() => requireCloudflareRoomNamespace({}, 'CustomRoomAuthority')).toThrow(
        'Missing Cloudflare Room Durable Object binding "CustomRoomAuthority". Add it to your wrangler.jsonc.',
      )
    })

    it('rejects a production subscription before local mutation when the authority binding is missing', async () => {
      const { roomId, inc } = await opened('missing-binding-subscribe')
      await expect(cloudflareMissingBindingSubscriptionProbe(fx.backend, roomId, inc)).resolves.toBe(
        'Missing Cloudflare Room Durable Object binding "ROOM". Add it to your wrangler.jsonc.',
      )
    })

    it('fails the SQLite authority constructor with the exact missing session-binding diagnostic', async () => {
      const mf = new Miniflare({
        modules: true,
        script: await bundleWorker(),
        compatibilityDate: '2025-01-01',
        compatibilityFlags: ['nodejs_compat'],
        durableObjects: { ROOM: { className: 'TelefuncRoomDurableObject', useSQLite: true } },
      })
      try {
        const namespace = await mf.getDurableObjectNamespace('ROOM')
        await expect(
          Promise.resolve().then(async () => {
            const stub = namespace.get(namespace.idFromName(nextId('missing-session')))
            try {
              await (stub as unknown as RoomStub).readHead()
            } finally {
              if (Symbol.dispose in stub) stub[Symbol.dispose]()
            }
          }),
        ).rejects.toThrow(
          'Missing Cloudflare session Durable Object binding "TelefuncDurableObject" in TelefuncRoomDurableObject constructor.',
        )
      } finally {
        await mf.dispose()
      }
    })

    it('keeps receiver handles exact across independent subscriptions', async () => {
      const { roomId, inc } = await opened()
      const old = stallingReceiver()
      const oldSub = fx.backend.subscribeLane(roomId, inc, SEMANTIC, old.receiver)
      await oldSub.ready
      await oldSub.unsubscribe()
      const fresh = collector()
      const freshSub = fx.backend.subscribeLane(roomId, inc, SEMANTIC, fresh.receiver)
      await freshSub.ready
      await old.release()
      await accepted(await fx.backend.commitLane(roomId, inc, SEMANTIC, bytes('fresh'))).delivery
      expect(fresh.payloads()).toEqual(['fresh'])
    })
  })

  describe('readiness, renewal and replacement', () => {
    it('re-establishment replaces the unexpired lease in one row', async () => {
      const { roomId, inc } = await opened()
      const seen = collector()
      const sub = fx.backend.subscribeLane(roomId, inc, SEMANTIC, seen.receiver)
      await sub.ready
      const controls = cloudflareRenewalControls(fx.backend)
      controls.forceFailures(2)
      await controls.advance(ROUTE_RENEW_EVERY_MS * 2)
      const result = accepted(await fx.backend.commitLane(roomId, inc, SEMANTIC, bytes('once')))
      expect(result.receivers).toBe(1)
      await result.delivery
      expect(seen.payloads()).toEqual(['once'])
    })

    it('reports lost then ready for a replacement lifecycle', async () => {
      const { roomId, inc } = await opened()
      const sub = fx.backend.subscribeLane(roomId, inc, SEMANTIC, noopReceiver())
      await sub.ready
      const states: ReadinessState[] = []
      sub.onStateChange((state) => states.push(state))
      const controls = cloudflareRenewalControls(fx.backend)
      controls.forceFailures(2)
      await controls.advance(ROUTE_RENEW_EVERY_MS * 2)
      expect(states).toEqual(['lost', 'ready'])
    })

    it('recovers through the bounded initial establishment retry path', async () => {
      const { roomId, inc } = await opened()
      const controls = cloudflareRenewalControls(fx.backend)
      controls.forceEstablishmentFailures(2)
      const sub = fx.backend.subscribeLane(roomId, inc, SEMANTIC, noopReceiver())
      expect(await settled(sub.ready)).toBe('rejected')
      await controls.advance(250 + 500)
      expect(sub.state()).toBe('ready')
    })

    it('reuses one fresh lease when a replacement registration commits but its acknowledgement is lost', async () => {
      const { roomId, inc } = await opened('lost-register-ack')
      const sub = fx.backend.subscribeLane(roomId, inc, SEMANTIC, noopReceiver())
      await sub.ready
      const controls = cloudflareRenewalControls(fx.backend)
      controls.forceFailures(2)
      controls.forcePostCommitEstablishmentFailures(1)
      await controls.advance(ROUTE_RENEW_EVERY_MS * 2 + 250)
      expect(sub.state()).toBe('ready')
      const leases = await controls.registrationLeaseHistory()
      expect(leases).toHaveLength(2)
      expect(new Set(leases).size).toBe(1)
    })

    it('removes a committed-but-unacknowledged replacement after bounded exhaustion', async () => {
      const target = await opened('lost-register-ack-exhaustion')
      const sub = fx.backend.subscribeLane(target.roomId, target.inc, SEMANTIC, noopReceiver())
      await sub.ready
      const controls = cloudflareRenewalControls(fx.backend)
      controls.forceFailures(2)
      controls.forcePostCommitEstablishmentFailures(5)
      await controls.advance(ROUTE_RENEW_EVERY_MS * 2 + 250 + 500 + 1_000 + 2_000)
      expect(sub.state()).toBe('closed')
      expect(new Set(await controls.registrationLeaseHistory()).size).toBe(1)
      expect(
        acceptedWire(await target.stub.commitLane(target.roomId, target.inc, SEMANTIC, bytes('no-ghost'))).receivers,
      ).toBe(0)
    })

    it('closes locally, reports teardown transport failure, and expires the remote route safely', async () => {
      const target = await opened('teardown-failure')
      const sub = fx.backend.subscribeLane(target.roomId, target.inc, SEMANTIC, noopReceiver())
      await sub.ready
      const controls = cloudflareRenewalControls(fx.backend)
      controls.forceUnsubscribeFailures(1)
      await expect(sub.unsubscribe()).rejects.toThrow('forced unsubscribe transport failure')
      expect(sub.state()).toBe('closed')
      expect(
        acceptedWire(await target.stub.commitLane(target.roomId, target.inc, SEMANTIC, bytes('still-durable')))
          .receivers,
      ).toBe(1)
      fx.advanceAuthority(ROUTE_TTL_MS + 1)
      await flushCloudflareAuthorityClock()
      expect(await target.stub.telefuncRoomRunMaintenance()).toEqual({ prunedRoutes: 1 })
      expect(
        acceptedWire(await target.stub.commitLane(target.roomId, target.inc, SEMANTIC, bytes('expired'))).receivers,
      ).toBe(0)
    })

    it('recovers a lost generation-capture response with the same attempt identity', async () => {
      const { roomId, inc } = await opened()
      const controls = cloudflareRenewalControls(fx.backend)
      controls.forceGenerationCaptureFailures(2)
      const sub = fx.backend.subscribeLane(roomId, inc, SEMANTIC, noopReceiver())
      expect(await settled(sub.ready)).toBe('rejected')
      await controls.advance(250 + 500)
      expect(sub.state()).toBe('ready')
    })

    it('closes after five bounded initial capture failures', async () => {
      const { roomId, inc } = await opened()
      const controls = cloudflareRenewalControls(fx.backend)
      controls.forceGenerationCaptureFailures(5)
      const sub = fx.backend.subscribeLane(roomId, inc, SEMANTIC, noopReceiver())
      expect(await settled(sub.ready)).toBe('rejected')
      await controls.advance(250 + 500 + 1_000 + 2_000)
      expect(sub.state()).toBe('closed')
    })

    it('closes after bounded re-establishment exhaustion', async () => {
      const { roomId, inc } = await opened()
      const sub = fx.backend.subscribeLane(roomId, inc, SEMANTIC, noopReceiver())
      await sub.ready
      const controls = cloudflareRenewalControls(fx.backend)
      controls.forceFailures(2)
      controls.forceEstablishmentFailures(5)
      await controls.advance(ROUTE_RENEW_EVERY_MS * 2 + 250 + 500 + 1_000 + 2_000)
      expect(sub.state()).toBe('closed')
    })

    it('rejects readiness for a non-open incarnation', async () => {
      const { roomId } = await opened()
      const sub = fx.backend.subscribeLane(roomId, 'missing', SEMANTIC, noopReceiver())
      expect(await settled(sub.ready)).toBe('rejected')
      expect(sub.state()).toBe('closed')
    })

    it('rejects readiness after the head enters closing', async () => {
      const { roomId, inc } = await opened()
      const head = await readHeadOrThrow(fx.backend, roomId)
      await enterClosing(fx.backend, roomId, head)
      expect(await settled(fx.backend.subscribeLane(roomId, inc, SEMANTIC, noopReceiver()).ready)).toBe('rejected')
    })

    it('does not rebind a lost capture across drop and exact id reuse', async () => {
      const { roomId, inc } = await opened()
      const controls = cloudflareRenewalControls(fx.backend)
      controls.forceGenerationCaptureFailures(1)
      const stale = fx.backend.subscribeLane(roomId, inc, SEMANTIC, noopReceiver())
      expect(await settled(stale.ready)).toBe('rejected')
      const tombstone = await close(roomId, inc)
      await fx.backend.dropGeneration(roomId, inc)
      okHead(
        await fx.backend.compareExchangeHead(
          roomId,
          { expect: { rev: tombstone.rev } },
          {
            head: { currentInc: inc, state: 'open', config: tombstone.config },
          },
        ),
      )
      await controls.advance(250)
      expect(stale.state()).toBe('closed')
      expect(accepted(await fx.backend.commitLane(roomId, inc, SEMANTIC, bytes('new'))).receivers).toBe(0)
    })

    it('guards unsubscribe from removing a replacement route', async () => {
      const target = await opened()
      const id = await cloudflareSessionId(nextId('session'))
      await directRegistration(target, id, 'old')
      await directRegistration(target, id, 'current')
      await target.stub.unsubscribeRoute(target.inc, laneKeyOf(SEMANTIC), id, 'old')
      expect(
        acceptedWire(await target.stub.commitLane(target.roomId, target.inc, SEMANTIC, bytes('target'))).receivers,
      ).toBe(1)
    })
  })

  describe('delivery, ordering and failure fences', () => {
    it('attempts a failing callback exactly once', async () => {
      const { roomId, inc } = await opened()
      const failed = throwingReceiver('target failed')
      await fx.backend.subscribeLane(roomId, inc, SEMANTIC, failed.receiver).ready
      expect(await settled(accepted(await fx.backend.commitLane(roomId, inc, SEMANTIC, bytes('x'))).delivery)).toBe(
        'rejected',
      )
      expect(failed.calls()).toBe(1)
    })

    it('makes a route non-live after three consecutive target failures', async () => {
      const { roomId, inc } = await opened()
      const failed = throwingReceiver('target failed')
      await fx.backend.subscribeLane(roomId, inc, SEMANTIC, failed.receiver).ready
      for (let n = 0; n < 3; n++)
        expect(
          await settled(accepted(await fx.backend.commitLane(roomId, inc, SEMANTIC, bytes(String(n)))).delivery),
        ).toBe('rejected')
      expect(accepted(await fx.backend.commitLane(roomId, inc, SEMANTIC, bytes('gone'))).receivers).toBe(0)
    })

    it('re-establishes after a late attachment observes K=3 eviction', async () => {
      const { roomId, inc } = await opened()
      const failed = throwingReceiver('target failed')
      const first = fx.backend.subscribeLane(roomId, inc, SEMANTIC, failed.receiver)
      await first.ready
      for (let n = 0; n < 3; n++)
        await settled(accepted(await fx.backend.commitLane(roomId, inc, SEMANTIC, bytes(String(n)))).delivery)
      const late = collector()
      const lateSub = fx.backend.subscribeLane(roomId, inc, SEMANTIC, late.receiver)
      await lateSub.ready
      const recovered = accepted(await fx.backend.commitLane(roomId, inc, SEMANTIC, bytes('recovered')))
      expect(recovered.receivers).toBe(1)
      expect(await settled(recovered.delivery)).toBe('rejected')
      expect(late.payloads()).toEqual(['recovered'])
    })

    it('resets the consecutive failure counter after success', async () => {
      const { roomId, inc } = await opened()
      const receiver = sequencedReceiver(['throw', 'throw', 'collect', 'throw', 'throw', 'throw'])
      await fx.backend.subscribeLane(roomId, inc, SEMANTIC, receiver.receiver).ready
      for (const expected of ['rejected', 'rejected', 'resolved', 'rejected', 'rejected']) {
        expect(
          await settled(accepted(await fx.backend.commitLane(roomId, inc, SEMANTIC, bytes(expected))).delivery),
        ).toBe(expected)
      }
      expect(accepted(await fx.backend.commitLane(roomId, inc, SEMANTIC, bytes('still-targeted'))).receivers).toBe(1)
    })

    it('keeps independent lanes moving while one callback is stalled', async () => {
      const { roomId, inc } = await opened()
      const stalled = stallingReceiver()
      const free = collector()
      await Promise.all([
        fx.backend.subscribeLane(roomId, inc, SEMANTIC, stalled.receiver).ready,
        fx.backend.subscribeLane(roomId, inc, CONTROL, free.receiver).ready,
      ])
      const blocked = accepted(await fx.backend.commitLane(roomId, inc, SEMANTIC, bytes('blocked')))
      await accepted(await fx.backend.commitLane(roomId, inc, CONTROL, bytes('free'))).delivery
      expect(free.payloads()).toEqual(['free'])
      await stalled.release()
      await blocked.delivery
    })

    it('does not start a second same-lane callback before the first settles', async () => {
      const { roomId, inc } = await opened()
      const stalled = stallingReceiver()
      await fx.backend.subscribeLane(roomId, inc, SEMANTIC, stalled.receiver).ready
      const one = accepted(await fx.backend.commitLane(roomId, inc, SEMANTIC, bytes('one')))
      const two = accepted(await fx.backend.commitLane(roomId, inc, SEMANTIC, bytes('two')))
      await stalled.waitForEntry()
      expect(stalled.entries()).toBe(1)
      await stalled.release()
      await Promise.all([one.delivery, two.delivery])
      expect(stalled.entries()).toBe(2)
    })

    it('orders one lane across two facade instances at the room authority', async () => {
      const second = await cloudflareHarness.create()
      try {
        const { roomId, inc } = await opened()
        const stalled = stallingReceiver()
        await fx.backend.subscribeLane(roomId, inc, SEMANTIC, stalled.receiver).ready
        const one = accepted(await fx.backend.commitLane(roomId, inc, SEMANTIC, bytes('one')))
        const two = accepted(await second.backend.commitLane(roomId, inc, SEMANTIC, bytes('two')))
        await stalled.waitForEntry()
        expect(stalled.entries()).toBe(1)
        await stalled.release()
        await Promise.all([one.delivery, two.delivery])
        expect(stalled.entries()).toBe(2)
      } finally {
        await second.dispose()
      }
    })

    it('snapshots mutable payload bytes before a deferred attempt', async () => {
      const { roomId, inc } = await opened()
      const stalled = stallingReceiver()
      await fx.backend.subscribeLane(roomId, inc, SEMANTIC, stalled.receiver).ready
      const first = accepted(await fx.backend.commitLane(roomId, inc, SEMANTIC, bytes('first')))
      const mutable = bytes('good')
      const second = accepted(await fx.backend.commitLane(roomId, inc, SEMANTIC, mutable))
      mutable.set(bytes('evil'))
      await stalled.waitForEntry()
      await stalled.release()
      await Promise.all([first.delivery, second.delivery])
      expect(stalled.payloads()).toEqual(['first', 'good'])
    })

    it('keeps generation delivery isolated after room recreation', async () => {
      const { roomId, inc } = await opened()
      const stale = collector()
      await fx.backend.subscribeLane(roomId, inc, SEMANTIC, stale.receiver).ready
      const tombstone = await close(roomId, inc)
      const freshInc = (await openRoom(fx.backend, roomId, { prior: tombstone })).inc
      const fresh = collector()
      await fx.backend.subscribeLane(roomId, freshInc, SEMANTIC, fresh.receiver).ready
      await accepted(await fx.backend.commitLane(roomId, freshInc, SEMANTIC, bytes('fresh'))).delivery
      expect(stale.payloads()).toEqual([])
      expect(fresh.payloads()).toEqual(['fresh'])
    })

    it('closes the worker mux on explicit generation invalidation', async () => {
      const { roomId, inc } = await opened()
      const sub = fx.backend.subscribeLane(roomId, inc, SEMANTIC, noopReceiver())
      await sub.ready
      await close(roomId, inc)
      await fx.backend.dropGeneration(roomId, inc)
      expect(sub.state()).toBe('closed')
    })
  })

  describe('authority route, capture and janitor mechanics', () => {
    it('keeps the 5-second Channel heartbeat independent from the 30-second route renewal', () => {
      expect(CHANNEL_PING_INTERVAL_MS).toBe(5_000)
      expect(ROUTE_RENEW_EVERY_MS).toBe(30_000)
    })
    it('replaces a route descriptor under one primary key', async () => {
      const target = await opened()
      const id = await cloudflareSessionId(nextId('session'))
      await directRegistration(target, id, 'one')
      await directRegistration(target, id, 'two')
      expect(
        acceptedWire(await target.stub.commitLane(target.roomId, target.inc, SEMANTIC, bytes('one-target'))).receivers,
      ).toBe(1)
    })

    it('renews only the exact current lease', async () => {
      const target = await opened()
      const id = await cloudflareSessionId(nextId('session'))
      const registered = await directRegistration(target, id, 'current')
      if (!('ok' in registered)) throw new Error('registration failed')
      expect(
        (await target.stub.renewRoute(target.inc, laneKeyOf(SEMANTIC), id, 'stale', registered.generationToken)).ok,
      ).toBe(false)
      expect(
        (await target.stub.renewRoute(target.inc, laneKeyOf(SEMANTIC), id, 'current', registered.generationToken)).ok,
      ).toBe(true)
    })

    it('mints route TTL from authority time', async () => {
      const target = await opened()
      const id = await cloudflareSessionId(nextId('session'))
      const registered = await directRegistration(target, id, 'lease')
      if (!('ok' in registered)) throw new Error('registration failed')
      expect(registered.expiresAt).toBe(fx.authorityNow() + ROUTE_TTL_MS)
    })

    it('rejects an expired capture attempt instead of rebinding it', async () => {
      const target = await opened()
      const id = await cloudflareSessionId(nextId('session'))
      const attempt = { id: nextId('capture'), createdAt: fx.authorityNow() }
      const first = await directRegistration(target, id, 'lease', SEMANTIC, attempt)
      expect('ok' in first).toBe(true)
      await target.stub.releaseRouteGenerationCapture(attempt.id)
      fx.advanceAuthority(ROUTE_CAPTURE_TTL_MS + 1)
      await flushCloudflareAuthorityClock()
      expect(await directRegistration(target, id, 'late', SEMANTIC, attempt)).toMatchObject({
        rejected: true,
        terminal: true,
      })
    })

    it('does not refresh a capture pin through a stale exact renewal', async () => {
      const target = await opened()
      const id = await cloudflareSessionId(nextId('session'))
      const attempt = { id: nextId('capture'), createdAt: fx.authorityNow() }
      const first = await directRegistration(target, id, 'lease', SEMANTIC, attempt)
      if (!('ok' in first)) throw new Error('registration failed')
      fx.advanceAuthority(ROUTE_CAPTURE_TTL_MS + 1)
      await flushCloudflareAuthorityClock()
      expect(
        (
          await target.stub.renewRoute(
            target.inc,
            laneKeyOf(SEMANTIC),
            id,
            'stale',
            first.generationToken,
            attempt.id,
            attempt.createdAt,
          )
        ).ok,
      ).toBe(false)
      await target.stub.telefuncRoomRunMaintenance()
      expect(await target.stub.countRouteGenerationCaptures()).toBe(0)
    })

    it('keeps a live capture pin past its original deadline only through exact renewal', async () => {
      const target = await opened()
      const id = await cloudflareSessionId(nextId('session'))
      const attempt = { id: nextId('capture'), createdAt: fx.authorityNow() }
      const first = await directRegistration(target, id, 'lease', SEMANTIC, attempt)
      if (!('ok' in first)) throw new Error('registration failed')

      fx.advanceAuthority(ROUTE_RENEW_EVERY_MS)
      await flushCloudflareAuthorityClock()
      expect(
        (
          await target.stub.renewRoute(
            target.inc,
            laneKeyOf(SEMANTIC),
            id,
            'lease',
            first.generationToken,
            attempt.id,
            attempt.createdAt,
          )
        ).ok,
      ).toBe(true)
      fx.advanceAuthority(ROUTE_CAPTURE_TTL_MS - ROUTE_RENEW_EVERY_MS)
      await flushCloudflareAuthorityClock()
      await target.stub.telefuncRoomRunMaintenance()
      expect(await target.stub.countRouteGenerationCaptures()).toBe(1)
    })

    it('prunes an expired route through the real janitor path', async () => {
      const target = await opened()
      const id = await cloudflareSessionId(nextId('session'))
      await directRegistration(target, id, 'lease')
      fx.advanceAuthority(ROUTE_TTL_MS + 1)
      await flushCloudflareAuthorityClock()
      expect(await target.stub.telefuncRoomRunMaintenance()).toEqual({ prunedRoutes: 1 })
      expect(
        acceptedWire(await target.stub.commitLane(target.roomId, target.inc, SEMANTIC, bytes('none'))).receivers,
      ).toBe(0)
    })

    it('retains an expired exact route when invalidation fails, then retries it', async () => {
      const target = await opened('janitor-retry')
      const receiver = collector()
      const sub = fx.backend.subscribeLane(target.roomId, target.inc, SEMANTIC, receiver.receiver)
      await sub.ready
      const controls = cloudflareRenewalControls(fx.backend)
      controls.forceInvalidationFailures(1)
      fx.advanceAuthority(ROUTE_TTL_MS + 1)
      await flushCloudflareAuthorityClock()

      expect(await target.stub.telefuncRoomRunMaintenance()).toEqual({ prunedRoutes: 0 })
      expect(await target.stub.telefuncRoomRunMaintenance()).toEqual({ prunedRoutes: 1 })
      await controls.advance(0)
      expect(sub.state()).toBe('closed')
    })

    it('keeps an orphan for its grace period then drops it', async () => {
      const target = await opened('orphan')
      const tombstone = await close(target.roomId, target.inc)
      await openRoom(fx.backend, target.roomId, { prior: tombstone })
      await target.stub.telefuncRoomRunMaintenance()
      fx.advanceAuthority(GEN_ORPHAN_GRACE_MS - 1)
      await flushCloudflareAuthorityClock()
      await target.stub.telefuncRoomRunMaintenance()
      expect(await target.stub.listGenerations()).toContain(target.inc)
      fx.advanceAuthority(1)
      await flushCloudflareAuthorityClock()
      await target.stub.telefuncRoomRunMaintenance()
      expect(await target.stub.listGenerations()).not.toContain(target.inc)
    })

    it('schedules and re-arms the real workerd alarm', async () => {
      const target = await opened('alarm')
      const tombstone = await close(target.roomId, target.inc)
      const current = (await openRoom(fx.backend, target.roomId, { prior: tombstone })).inc

      // The fixture shortens only the production alarm cadence. The first alarm observes the orphan;
      // after the authority clock crosses the grace window, a later re-armed alarm removes it.
      await new Promise<void>((resolve) => setTimeout(resolve, 650))
      fx.advanceAuthority(GEN_ORPHAN_GRACE_MS)
      await flushCloudflareAuthorityClock()
      await waitUntil(async () => !(await target.stub.listGenerations()).includes(target.inc))
      expect(await target.stub.listGenerations()).toEqual([current])
    })

    it('preserves supplementary-Unicode prefix parity', async () => {
      const target = await opened('unicode')
      const key = 'room/😀/member'
      const read = await fx.backend.readCells(target.roomId, target.inc, { prefix: 'room/😀' })
      if ('staleInc' in read) throw new Error('stale')
      await fx.backend.compareExchangeCells(target.roomId, target.inc, read.revision, [
        { key, set: { bytes: bytes('v') } },
      ])
      const after = await fx.backend.readCells(target.roomId, target.inc, { prefix: 'room/😀' })
      if ('staleInc' in after) throw new Error('stale')
      expect([...after.cells.keys()]).toEqual([key])
    })

    it('refuses to drop the current generation', async () => {
      const target = await opened()
      await expect(fx.backend.dropGeneration(target.roomId, target.inc)).rejects.toThrow(
        'refusing to drop the current incarnation',
      )
    })

    it('lists only durable generation subtrees', async () => {
      const target = await opened()
      expect(await fx.backend.listGenerations(target.roomId)).toEqual([target.inc])
    })

    it('tag-guards directory deletion through the production authority', async () => {
      const roomId = `😀/${nextId('directory')}`
      await fx.backend.directoryPut(roomId, 'current')
      await fx.backend.directoryDelete(roomId, 'stale')
      expect((await fx.backend.directoryList('😀/')).entries).toContainEqual({ roomId, incTag: 'current' })
      await fx.backend.directoryDelete(roomId, 'current')
      expect((await fx.backend.directoryList('😀/')).entries).not.toContainEqual({ roomId, incTag: 'current' })
    })
  })
})

function roomCapableBackend(
  backend: RoomBackendSpi,
  observeState: (roomId: string, lane: LaneId, state: ReadinessState) => void,
  ownedReceivers: Set<LaneReceiver>,
): RoomBackendSpi {
  return new Proxy(backend, {
    get(target, property) {
      if (property === 'subscribeLane') {
        return (roomId: string, inc: string, lane: LaneId, receiver: LaneReceiver) => {
          // The workerd conformance transport accepts only serializable receiver commands. Adapt the
          // production Room receiver to that command without changing the backend or Room lifecycle:
          // remote observations are replayed through the exact receiver Room installed.
          const remoteReceiver = conformanceReceiver({ kind: 'collect' }, receiver, (observations) => {
            for (const observation of observations) {
              void receiver(bytes(observation.payload), {
                seq: observation.seq,
                timestamp: observation.timestamp,
              })
            }
          })
          ownedReceivers.add(remoteReceiver)
          const subscription = target.subscribeLane(roomId, inc, lane, remoteReceiver)
          subscription.onStateChange((state) => observeState(roomId, lane, state))
          return subscription
        }
      }
      const value = Reflect.get(target, property, target)
      return typeof value === 'function' ? value.bind(target) : value
    },
  })
}

function acceptedWire(wire: Awaited<ReturnType<RoomStub['commitLane']>>) {
  if (!('accepted' in wire)) throw new Error(`expected accepted wire, got ${JSON.stringify(wire)}`)
  return wire
}
