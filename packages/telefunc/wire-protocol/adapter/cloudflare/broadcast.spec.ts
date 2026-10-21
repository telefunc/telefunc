import { afterEach, describe, expect, it } from 'vitest'
import {
  DEFAULT_BROADCAST_BUCKETS,
  assertLocationFallbackIsScaled,
  getBucketCoordinatorShardIndices,
  getDeterministicKeyBucketIndex,
  resolveCloudflareLocationHint,
  resolveSessionRoutingTarget,
} from './routing.js'
import '../../../node/server/async_hooks.js'
import { CloudflareBroadcastAuthorityState, CloudflareBroadcast } from './broadcast.js'
import type { BroadcastCalls, BroadcastPresenceRequest, CloudflareBroadcastMember } from './broadcast.js'
import { withCloudflareSession } from './session.js'
import type { BroadcastRoute } from '../../backend/broadcast/contract.js'
import { broadcastRouteKey } from '../../backend/broadcast/routeKey.js'
import { OrderedStubs } from './OrderedStubs.js'
import type { TelefuncDurableObjectNamespace } from './namespace.js'
import { CLOUDFLARE_COLO_LOCATION_HINT_MAP } from './coloLocationHintMap.js'
import { ServerBroadcast } from '../../broadcast/ServerBroadcast.js'
import { disposeBackend, installBackend } from '../../backend/install.js'
import { CloudflareBackend } from './backend.js'
import { CloudflareRoomSessionManager } from './room/subscription.js'
import { ChannelMux } from '../../channel/server/mux.js'
import type { SubscriptionAttempt, SubscriptionState } from '../../backend/subscription.js'
import { sqliteStorage } from '../../test/cloudflareSql.js'

/** Resolves once the attempt is ready; rejects if it ends first. */
function untilReady(attempt: SubscriptionAttempt): Promise<void> {
  return new Promise((resolve, reject) => {
    const settle = (state: SubscriptionState, reason?: Error): boolean => {
      if (state === 'ready') resolve()
      else if (state === 'closed') reject(reason ?? new Error(`attempt ${state}`))
      else return false
      return true
    }
    if (settle(attempt.state())) return
    const stop = attempt.onStateChange((state, reason) => {
      if (settle(state, reason)) stop()
    })
  })
}

type CloudflareRequest = Request & { cf?: { colo?: string; continent?: string } }

afterEach(async () => {
  await disposeBackend()
})

function installCloudflareBroadcast(broadcast: CloudflareBroadcast): void {
  installBackend(
    () =>
      new CloudflareBackend({
        rooms: () => {
          throw new Error('Broadcast specs use no Room namespace')
        },
        broadcast,
      }),
  )
}

function createCloudflareRequest({ colo, continent }: { colo?: string; continent?: string } = {}): CloudflareRequest {
  const request = new Request('https://telefunc.test') as CloudflareRequest
  request.cf = { colo, continent }
  return request
}

function createAuthorityState() {
  return new CloudflareBroadcastAuthorityState({ storage: sqliteStorage().storage } as DurableObjectState)
}

type PresenceHooks = { beforeRecord?: () => Promise<void>; beforeWithdraw?: () => Promise<void> }

/** The key's authority as a binding reaches it: presence writes arrive in call order, as through one stub, each after
 *  its hook, and land in `authority`. */
function presenceAt(authority: CloudflareBroadcastAuthorityState, hooks: PresenceHooks = {}) {
  let arrival = Promise.resolve()
  return (_id: unknown, request: BroadcastPresenceRequest): Promise<void> =>
    (arrival = arrival.then(async () => {
      await (request.bucket === null ? hooks.beforeWithdraw : hooks.beforeRecord)?.()
      authority.setPresence(request)
    }))
}

function liveMembers(authority: CloudflareBroadcastAuthorityState, route: BroadcastRoute) {
  return authority.livePresence(broadcastRouteKey(route), Date.now())
}

async function flushMicrotasks(turns = 6): Promise<void> {
  for (let index = 0; index < turns; index++) {
    await Promise.resolve()
  }
}

async function flushCoordinatorTurn(): Promise<void> {
  await flushMicrotasks()
  await new Promise<void>((resolve) => setTimeout(resolve, 0))
}

function createBasicBinding(
  overrides?: Partial<{
    onPublish: (id: { name: string }, request: any) => any
    onForward: (id: { name: string }, request: any) => any
    onDeliver: (id: { name: string }, request: any) => any
    onPresence: (id: { name: string }, request: any) => any
  }>,
) {
  return {
    idFromName(name: string) {
      return { name }
    },
    idFromString(id: string) {
      return { name: id }
    },
    get(id: { name: string }) {
      return {
        telefuncBroadcastPublish(request: any) {
          return overrides?.onPublish?.(id, request) ?? Promise.resolve({ seq: 1, timestamp: Date.now() })
        },
        telefuncBroadcastForward(request: any) {
          return overrides?.onForward?.(id, request) ?? Promise.resolve()
        },
        telefuncBroadcastDeliver(request: any) {
          return overrides?.onDeliver?.(id, request) ?? Promise.resolve()
        },
        telefuncBroadcastPresence(request: any) {
          return overrides?.onPresence?.(id, request) ?? Promise.resolve()
        },
      }
    },
  } as unknown as TelefuncDurableObjectNamespace
}

function createBroadcast(binding = createBasicBinding()): CloudflareBroadcast {
  return new CloudflareBroadcast({
    baseInstanceName: 'telefunc',
    scale: 1,
    locationFallback: 'weur',
    namespace: () => binding,
  })
}

/** A session DO's Broadcast membership, placed in weur. */
function createMember(broadcast: CloudflareBroadcast, id = 'member-weur-0'): CloudflareBroadcastMember {
  const member = broadcast.member(id, new OrderedStubs())
  member.locate('weur')
  return member
}

/** Runs `fn` as code in the member's session DO. */
function inSession<T>(member: CloudflareBroadcastMember, fn: () => T): T {
  return withCloudflareSession(
    { room: new CloudflareRoomSessionManager('session'), broadcast: member, mux: new ChannelMux() },
    fn,
  )
}

describe('cloudflare broadcast routing', () => {
  it('uses the six default canonical buckets for mapped Cloudflare locations', () => {
    expect(DEFAULT_BROADCAST_BUCKETS).toEqual(['wnam', 'enam', 'weur', 'eeur', 'apac', 'oc'])
  })

  it('stores direct session-placement buckets for colos', () => {
    expect(CLOUDFLARE_COLO_LOCATION_HINT_MAP.LAX).toBe('wnam')
    expect(CLOUDFLARE_COLO_LOCATION_HINT_MAP.ORD).toBe('enam')
    expect(CLOUDFLARE_COLO_LOCATION_HINT_MAP.LHR).toBe('weur')
    expect(CLOUDFLARE_COLO_LOCATION_HINT_MAP.WAW).toBe('eeur')
    expect(CLOUDFLARE_COLO_LOCATION_HINT_MAP.BOM).toBe('apac')
    expect(CLOUDFLARE_COLO_LOCATION_HINT_MAP.SYD).toBe('oc')
  })

  it('resolves request colos to canonical location hints', () => {
    const losAngeles = createCloudflareRequest({ colo: 'LAX' })
    const chicago = createCloudflareRequest({ colo: 'ORD' })
    const london = createCloudflareRequest({ colo: 'LHR' })
    const warsaw = createCloudflareRequest({ colo: 'WAW' })
    const mumbai = createCloudflareRequest({ colo: 'BOM' })
    const sydney = createCloudflareRequest({ colo: 'SYD' })

    expect(resolveCloudflareLocationHint(losAngeles, 'weur')).toBe('wnam')
    expect(resolveCloudflareLocationHint(chicago, 'weur')).toBe('enam')
    expect(resolveCloudflareLocationHint(london, 'weur')).toBe('weur')
    expect(resolveCloudflareLocationHint(warsaw, 'weur')).toBe('eeur')
    expect(resolveCloudflareLocationHint(mumbai, 'weur')).toBe('apac')
    expect(resolveCloudflareLocationHint(sydney, 'weur')).toBe('oc')
  })

  it('maps unambiguous continents directly to session-placement buckets', () => {
    expect(resolveCloudflareLocationHint(createCloudflareRequest({ continent: 'AF' }), 'weur')).toBe('weur')
    expect(resolveCloudflareLocationHint(createCloudflareRequest({ continent: 'AS' }), 'weur')).toBe('apac')
    expect(resolveCloudflareLocationHint(createCloudflareRequest({ continent: 'OC' }), 'weur')).toBe('oc')
    expect(resolveCloudflareLocationHint(createCloudflareRequest({ continent: 'SA' }), 'weur')).toBe('enam')
  })

  it('prefers a mapped continent bucket when the colo is unmapped', () => {
    const unknown = createCloudflareRequest({ colo: 'ZZZ', continent: 'AF' })

    expect(resolveCloudflareLocationHint(unknown, 'weur')).toBe('weur')
  })

  it('falls back to locationFallback for ambiguous continents', () => {
    const request = createCloudflareRequest({ colo: 'ZZZ', continent: 'EU' })

    expect(resolveCloudflareLocationHint(request, 'weur')).toBe('weur')
    expect(resolveCloudflareLocationHint(request, 'apac')).toBe('apac')
  })

  it('falls back to locationFallback when cf.continent is unavailable', () => {
    const request = createCloudflareRequest({ colo: 'ZZZ' })

    expect(resolveCloudflareLocationHint(request, 'weur')).toBe('weur')
  })

  it('falls back to locationFallback when neither colo nor continent exists', () => {
    expect(resolveCloudflareLocationHint(createCloudflareRequest(), 'weur')).toBe('weur')
  })

  it('maps the same room to the same bucket-coordinator offset for a bucket', () => {
    const shardIndices = getBucketCoordinatorShardIndices(2, 'weur')

    expect(getDeterministicKeyBucketIndex('room/alpha', shardIndices.length)).toBe(
      getDeterministicKeyBucketIndex('room/alpha', shardIndices.length),
    )
  })

  it('assigns room keys only within the bucket-coordinator subset', () => {
    const weurShards = getBucketCoordinatorShardIndices(2, 'weur')
    const apacShards = getBucketCoordinatorShardIndices(2, 'apac')
    const ocShards = getBucketCoordinatorShardIndices(2, 'oc')

    expect(weurShards).toContain(weurShards[getDeterministicKeyBucketIndex('room/alpha', weurShards.length)]!)
    expect(apacShards).toContain(apacShards[getDeterministicKeyBucketIndex('room/alpha', apacShards.length)]!)
    expect(ocShards).toContain(ocShards[getDeterministicKeyBucketIndex('room/alpha', ocShards.length)]!)
  })

  it('partitions bucket coordinators at ceil(sessionScale / 2) per bucket', () => {
    expect(getBucketCoordinatorShardIndices(1, 'wnam')).toEqual([0])
    expect(getBucketCoordinatorShardIndices(2, 'wnam')).toEqual([0])
    expect(getBucketCoordinatorShardIndices(3, 'wnam')).toEqual([0, 1])
    expect(getBucketCoordinatorShardIndices(4, 'wnam')).toEqual([0, 1])
    expect(getBucketCoordinatorShardIndices(4, 'enam')).toEqual([0, 1])
  })

  it('uses scale maps to control the bucket-coordinator shard subsets', () => {
    expect(getBucketCoordinatorShardIndices({ weur: 2, enam: 1 }, 'enam')).toEqual([0])
    expect(getBucketCoordinatorShardIndices({ weur: 2, enam: 1 }, 'weur')).toEqual([0])
    expect(getBucketCoordinatorShardIndices({ weur: 3, enam: 1 }, 'weur')).toEqual([0, 1])
  })

  it('uses only canonical bucket scale entries', () => {
    expect(getBucketCoordinatorShardIndices({ weur: 2, apac: 1 }, 'apac')).toEqual([0])
  })

  it('resolves session targets from request location and scale', () => {
    const exactRequest = createCloudflareRequest({ colo: 'LHR' })
    const unknownRequest = createCloudflareRequest({ continent: 'EU' })
    const exactTarget = resolveSessionRoutingTarget('telefunc', { weur: 2, apac: 1 }, exactRequest, 'weur', 'token')
    const fallbackTarget = resolveSessionRoutingTarget(
      'telefunc',
      { weur: 1, apac: 1 },
      unknownRequest,
      'weur',
      'token',
    )

    expect(exactTarget).toMatchObject({
      sessionInstanceName: expect.stringMatching(/^telefunc-shard-weur-/),
      locationBucket: 'weur',
    })
    expect(fallbackTarget).toMatchObject({
      sessionInstanceName: 'telefunc-shard-weur-0',
      locationBucket: 'weur',
      shardOrdinal: 0,
    })
  })

  it('routes a recognized region missing from the scale map to locationFallback instead of throwing', () => {
    // `ABQ` resolves to `wnam`, which is absent from this per-region scale map.
    const wnamRequest = createCloudflareRequest({ colo: 'ABQ' })
    const target = resolveSessionRoutingTarget('telefunc', { weur: 2, apac: 1 }, wnamRequest, 'weur', 'token')

    expect(target).toMatchObject({
      sessionInstanceName: expect.stringMatching(/^telefunc-shard-weur-/),
      locationBucket: 'weur',
    })
    expect([0, 1]).toContain(target.shardOrdinal)
  })

  it('rejects a per-region scale map whose locationFallback region has no shards', () => {
    // `locationFallback` is where unlisted regions land, so it must itself be scaled.
    expect(() => assertLocationFallbackIsScaled({ enam: 3, apac: 2 }, 'weur')).toThrow(/locationFallback/)
    expect(() => assertLocationFallbackIsScaled({ weur: 2, apac: 1 }, 'weur')).not.toThrow()
    // A uniform numeric scale (or the default) applies to every region, so any fallback is fine.
    expect(() => assertLocationFallbackIsScaled(4, 'weur')).not.toThrow()
    expect(() => assertLocationFallbackIsScaled(undefined, 'weur')).not.toThrow()
  })

  it("records presence at the key's authority on subscribe and reads it during publish fanout", async () => {
    const authority = createAuthorityState()
    const calls: BroadcastCalls = new OrderedStubs()
    const broadcast = createBroadcast(
      createBasicBinding({
        onPresence: presenceAt(authority),
        onPublish: (_, request) => broadcast.publishToSubscribers(authority, calls, request),
      }),
    )
    const member = createMember(broadcast)
    const route = { key: 'room:test', kind: 'text' } as const
    const subscription = member.openSubscription(route, () => {})
    await untilReady(subscription)
    expect(liveMembers(authority, route)).toEqual([{ member: 'member-weur-0', bucket: 'weur' }])
    const binary = await broadcast.publish({ key: 'room:test', kind: 'binary' }, new Uint8Array([1]))
    const text = await broadcast.publish(route, '"text"')
    expect([binary.receivers, text.receivers]).toEqual([0, 1])
    await subscription.unsubscribe()
  })

  it('holds a publish until the authority records the subscription, then delivers it with the authority receipt', async () => {
    const authority = createAuthorityState()
    const calls: BroadcastCalls = new OrderedStubs()
    const coordinatorCalls: BroadcastCalls = new OrderedStubs()
    const recorded = Promise.withResolvers<void>()
    const received: string[] = []
    const broadcast: CloudflareBroadcast = createBroadcast(
      createBasicBinding({
        onPresence: presenceAt(authority, { beforeRecord: () => recorded.promise }),
        onPublish(_id, request) {
          return broadcast.publishToSubscribers(authority, calls, request)
        },
        onForward(_id, request) {
          return broadcast.forwardToBucket(coordinatorCalls, request)
        },
        onDeliver(_id, request) {
          return member.deliver(request)
        },
      }),
    )
    installCloudflareBroadcast(broadcast)
    const member = createMember(broadcast)
    await inSession(member, async () => {
      const subscriber = new ServerBroadcast<{ text: string }>({ key: 'room:test' })
      subscriber.subscribe((message) => {
        received.push(message.text)
      })
      const publisher = new ServerBroadcast<{ text: string }>({ key: 'room:test' })
      const receipt = publisher.publish({ text: 'hello' })
      await flushMicrotasks(2)
      expect(received).toEqual([])
      recorded.resolve()
      await expect(receipt).resolves.toStrictEqual({
        key: 'room:test',
        seq: 1,
        timestamp: expect.any(Number),
        receivers: 1,
      })
      expect(received).toEqual(['hello'])
    })
  })

  it('authority forwards once to each populated bucket coordinator', async () => {
    const authorityState = createAuthorityState()
    const calls: BroadcastCalls = new OrderedStubs()
    const coordinators: string[] = []
    const broadcast: CloudflareBroadcast = createBroadcast(
      createBasicBinding({
        onForward(id) {
          coordinators.push(id.name)
          return Promise.resolve()
        },
      }),
    )
    for (const bucket of ['weur', 'apac', 'eeur'] as const) {
      authorityState.setPresence({ key: 'room:test', kind: 'text', member: `telefunc-shard-${bucket}-0`, bucket })
    }
    await broadcast.publishToSubscribers(authorityState, calls, {
      key: 'room:test',
      kind: 'text',
      payload: '{"text":"hello"}',
    })
    expect(coordinators.sort()).toEqual([
      'telefunc:broadcast:apac:0',
      'telefunc:broadcast:eeur:0',
      'telefunc:broadcast:weur:0',
    ])
  })

  it('forwarded publish delivers to DO names listed in the request', async () => {
    const deliveredTo: string[] = []
    const broadcast = createBroadcast(
      createBasicBinding({
        onDeliver(id) {
          deliveredTo.push(id.name)
          return Promise.resolve()
        },
      }),
    )

    await broadcast.forwardToBucket(new OrderedStubs(), {
      key: 'room:test',
      kind: 'text',
      payload: '{"text":"hello"}',
      info: { seq: 1, timestamp: Date.now() },
      members: ['telefunc-shard-weur-0', 'telefunc-shard-weur-1'],
    })

    expect(deliveredTo.sort()).toEqual(['telefunc-shard-weur-0', 'telefunc-shard-weur-1'])
  })

  it("publishes from outside a session, as from a cron trigger, to the key's authority", async () => {
    const coordinatorPublishes: Array<{ name: string; key: string; text: string }> = []
    const broadcast: CloudflareBroadcast = createBroadcast(
      createBasicBinding({
        onPublish(id, { key, payload }) {
          coordinatorPublishes.push({ name: id.name, key, text: payload })
          return Promise.resolve({ seq: 1, timestamp: Date.now() })
        },
      }),
    )
    installCloudflareBroadcast(broadcast)
    const room = new ServerBroadcast<{ text: string }>({ key: 'room:test:no-ctx' })

    expect(() => room.publish({ text: 'hello' })).not.toThrow()

    await flushCoordinatorTurn()

    expect(coordinatorPublishes).toEqual([
      {
        name: 'telefunc:broadcast:authority:room:test:no-ctx',
        key: 'room:test:no-ctx',
        text: '{"text":"hello"}',
      },
    ])
  })

  it('a member registers presence only once it knows its bucket', async () => {
    const broadcast = createBroadcast()
    const member = broadcast.member('member-unplaced', new OrderedStubs())
    const subscription = member.openSubscription({ key: 'room:test', kind: 'text' }, () => {})
    await expect(untilReady(subscription)).rejects.toThrow('knows its bucket')
  })

  it('serializes authority dispatch without blocking later publishes on remote delivery completion', async () => {
    const authorityState = createAuthorityState()
    const calls: BroadcastCalls = new OrderedStubs()
    const coordinatorPublishes: string[] = []
    const firstRemotePublish = Promise.withResolvers<void>()
    const broadcast = createBroadcast(
      createBasicBinding({
        onForward(id, { payload: text }) {
          coordinatorPublishes.push(`${id.name}:${text}`)
          if (id.name.includes(':broadcast:apac:') && text === '{"text":"first"}') return firstRemotePublish.promise
          return Promise.resolve()
        },
      }),
    )
    for (const bucket of ['weur', 'apac'] as const) {
      authorityState.setPresence({ key: 'room:test', kind: 'text', member: `telefunc-shard-${bucket}-0`, bucket })
    }
    const firstPublish = broadcast.publishToSubscribers(authorityState, calls, {
      key: 'room:test',
      kind: 'text',
      payload: '{"text":"first"}',
    })
    await flushMicrotasks(8)
    const secondPublish = broadcast.publishToSubscribers(authorityState, calls, {
      key: 'room:test',
      kind: 'text',
      payload: '{"text":"second"}',
    })
    await flushMicrotasks(8)

    expect(coordinatorPublishes).toContain('telefunc:broadcast:weur:0:{"text":"first"}')
    expect(coordinatorPublishes).toContain('telefunc:broadcast:apac:0:{"text":"first"}')
    expect(coordinatorPublishes).toContain('telefunc:broadcast:weur:0:{"text":"second"}')
    expect(coordinatorPublishes).toContain('telefunc:broadcast:apac:0:{"text":"second"}')

    firstRemotePublish.resolve()
    await Promise.all([firstPublish, secondPublish])

    expect(coordinatorPublishes).toContain('telefunc:broadcast:weur:0:{"text":"second"}')
    expect(coordinatorPublishes).toContain('telefunc:broadcast:apac:0:{"text":"second"}')
  })

  it('withdraws presence at the authority on unsubscribe', async () => {
    const authority = createAuthorityState()
    const broadcast = createBroadcast(createBasicBinding({ onPresence: presenceAt(authority) }))
    const member = createMember(broadcast)
    const route = { key: 'room:test', kind: 'text' } as const
    const subscription = member.openSubscription(route, () => {})
    await untilReady(subscription)
    expect(liveMembers(authority, route)).toEqual([{ member: 'member-weur-0', bucket: 'weur' }])
    await subscription.unsubscribe()
    expect(liveMembers(authority, route)).toEqual([])
  })
})
