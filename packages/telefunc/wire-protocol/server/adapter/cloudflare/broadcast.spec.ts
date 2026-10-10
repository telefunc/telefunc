import { DatabaseSync, type SQLInputValue } from 'node:sqlite'
import { afterEach, describe, expect, it, vi } from 'vitest'
import {
  DEFAULT_BROADCAST_BUCKETS,
  assertLocationFallbackIsScaled,
  getBucketCoordinatorShardIndices,
  getDeterministicKeyBucketIndex,
  getShardIndicesForBucket,
  resolveCloudflareLocationHint,
  resolveSessionRoutingTarget,
} from './routing.js'
import '../../../../node/server/async_hooks.js'
import { CloudflareBroadcastAuthorityState, CloudflareBroadcast } from './broadcast.js'
import type { BroadcastCalls, BroadcastPresenceRequest, CloudflareBroadcastMember } from './broadcast.js'
import { withCloudflareSession } from './session.js'
import type { BroadcastRoute } from '../../../backend/broadcast/contract.js'
import { broadcastRouteKey } from '../../../backend/broadcast/route-key.js'
import { OrderedStubs } from './ordered-stubs.js'
import type { TelefuncDurableObjectNamespace } from './namespace.js'
import { CLOUDFLARE_COLO_LOCATION_HINT_MAP } from './coloLocationHintMap.js'
import { ServerBroadcast } from '../../server-broadcast.js'
import { disposeBackend, installBackend } from '../../../backend/install.js'
import { CloudflareBackend } from './room/backend.js'
import { CloudflareRoomSessionManager } from './room/subscription.js'
import { ChannelMux } from '../../mux.js'
import type { BackendPayload, SubscriptionAttempt, SubscriptionState } from '../../../backend/subscription.js'

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

/** Cloudflare's SQLite storage API over node:sqlite, as far as the adapter uses it. */
function createSqlState(): DurableObjectState {
  const db = new DatabaseSync(':memory:')
  const cursor = (rows: unknown[]) => ({ toArray: () => rows, one: () => rows[0] })
  const sql = {
    exec(query: string, ...bindings: SQLInputValue[]) {
      if (bindings.length === 0 && !/^\s*SELECT/i.test(query)) {
        db.exec(query)
        return cursor([])
      }
      const statement = db.prepare(query)
      return cursor(/^\s*SELECT/i.test(query) ? statement.all(...bindings) : (statement.run(...bindings), []))
    },
  }
  const storage = {
    sql,
    transactionSync<T>(fn: () => T): T {
      db.exec('BEGIN')
      try {
        const result = fn()
        db.exec('COMMIT')
        return result
      } catch (error) {
        db.exec('ROLLBACK')
        throw error
      }
    },
  }
  return { storage } as unknown as DurableObjectState
}

function createAuthorityState() {
  return new CloudflareBroadcastAuthorityState(createSqlState())
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
  return Object.fromEntries(authority.livePresence(broadcastRouteKey(route), Date.now()))
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

/** Stubs that deliver like Cloudflare's: calls through one stub arrive in call order, after that stub's latency;
 *  calls through different stubs race. The n-th stub to carry a call gets `latencies[n]` (default 0). */
function createRacingBinding(
  latencies: number[],
  handlers: {
    onForward: (request: any) => Promise<void>
    onDeliver: (request: any) => Promise<void>
    onPresence: (request: any) => Promise<void>
  },
) {
  let used = 0
  return {
    idFromName(name: string) {
      return { name }
    },
    idFromString(id: string) {
      return { name: id }
    },
    get() {
      let latency: number | undefined
      let arrival = Promise.resolve()
      const send =
        (handler: (request: any) => Promise<void>) =>
        (request: any): Promise<void> => {
          const delay = (latency ??= latencies[used++] ?? 0)
          arrival = arrival.then(() => new Promise((resolve) => setTimeout(resolve, delay)))
          return arrival.then(() => handler(request))
        }
      return {
        telefuncBroadcastForward: send(handlers.onForward),
        telefuncBroadcastDeliver: send(handlers.onDeliver),
        telefuncBroadcastPresence: handlers.onPresence,
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
    // A hash of the key alone, so every isolate, and every version of a gradual deploy, picks the same coordinator.
    expect(getDeterministicKeyBucketIndex('room/alpha', 2)).toBe(0)
    expect(getDeterministicKeyBucketIndex('room/alpha', 3)).toBe(2)
  })

  it('assigns room keys only within the bucket-coordinator subset', () => {
    for (const bucket of ['weur', 'apac', 'oc'] as const) {
      const shards = getBucketCoordinatorShardIndices(6, bucket)
      const picked = new Set(
        Array.from({ length: 20 }, (_, n) => shards[getDeterministicKeyBucketIndex(`room/${n}`, shards.length)]),
      )
      expect([...picked].sort()).toEqual(shards)
    }
  })

  it('partitions shards by bucket when the scale is uniform', () => {
    expect(getShardIndicesForBucket(2, 'wnam')).toEqual([0, 1])
    expect(getShardIndicesForBucket(2, 'enam')).toEqual([0, 1])
    expect(getShardIndicesForBucket(2, 'weur')).toEqual([0, 1])
    expect(getShardIndicesForBucket(2, 'eeur')).toEqual([0, 1])
    expect(getShardIndicesForBucket(2, 'apac')).toEqual([0, 1])
    expect(getShardIndicesForBucket(2, 'oc')).toEqual([0, 1])
  })

  it('partitions bucket coordinators at ceil(sessionScale / 2) per bucket', () => {
    expect(getBucketCoordinatorShardIndices(1, 'wnam')).toEqual([0])
    expect(getBucketCoordinatorShardIndices(2, 'wnam')).toEqual([0])
    expect(getBucketCoordinatorShardIndices(3, 'wnam')).toEqual([0, 1])
    expect(getBucketCoordinatorShardIndices(4, 'wnam')).toEqual([0, 1])
    expect(getBucketCoordinatorShardIndices(4, 'enam')).toEqual([0, 1])
  })

  it('uses scale maps to control the session and bucket-coordinator shard subsets together', () => {
    expect(getShardIndicesForBucket({ weur: 2, enam: 1 }, 'enam')).toEqual([0])
    expect(getShardIndicesForBucket({ weur: 2, enam: 1 }, 'weur')).toEqual([0, 1])
    expect(getBucketCoordinatorShardIndices({ weur: 2, enam: 1 }, 'enam')).toEqual([0])
    expect(getBucketCoordinatorShardIndices({ weur: 2, enam: 1 }, 'weur')).toEqual([0])
    expect(getBucketCoordinatorShardIndices({ weur: 3, enam: 1 }, 'weur')).toEqual([0, 1])
  })

  it('uses only canonical bucket scale entries', () => {
    expect(getShardIndicesForBucket({ weur: 2, apac: 1 }, 'weur')).toEqual([0, 1])
    expect(getShardIndicesForBucket({ weur: 2, apac: 1 }, 'apac')).toEqual([0])
    expect(getBucketCoordinatorShardIndices({ weur: 2, apac: 1 }, 'apac')).toEqual([0])
  })

  it('routes one session token to one shard of its region, however often it is routed', () => {
    const request = createCloudflareRequest({ colo: 'LHR' })
    const shards = new Set(
      Array.from(
        { length: 20 },
        () => resolveSessionRoutingTarget('telefunc', { weur: 4 }, request, 'weur', 'token-a').shardOrdinal,
      ),
    )
    expect(shards.size).toBe(1)
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

  it('records presence at the key’s authority on subscribe and reads it during publish fanout', async () => {
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
    expect(liveMembers(authority, route)).toEqual({ weur: ['member-weur-0'] })
    const binary = await broadcast.publish({ key: 'room:test', kind: 'binary' }, new Uint8Array([1]))
    const text = await broadcast.publish(route, '"text"')
    expect([binary.receivers, text.receivers]).toEqual([0, 1])
    await subscription.unsubscribe()
  })

  it('a publish waits only for its own session’s subscription, and leaves from its own session', async () => {
    const recorded = Promise.withResolvers<void>()
    const publishBuckets: string[] = []
    const broadcast = createBroadcast(
      createBasicBinding({
        onPresence: () => recorded.promise,
        onPublish(_id, request) {
          publishBuckets.push(JSON.parse(request.payload))
          return Promise.resolve({ seq: publishBuckets.length, timestamp: Date.now() })
        },
      }),
    )
    installCloudflareBroadcast(broadcast)
    const weur = createMember(broadcast)
    const enam = broadcast.member('member-enam-0', new OrderedStubs())
    enam.locate('enam')
    const publishFrom = (member: CloudflareBroadcastMember) =>
      inSession(member, () => new ServerBroadcast<string>({ key: 'room:test' }).publish(member.bucket!))
    inSession(weur, () => new ServerBroadcast<string>({ key: 'room:test' }).subscribe(() => {}))
    const published = [publishFrom(weur), publishFrom(enam)]
    await vi.waitFor(() => expect(publishBuckets).toEqual(['enam']))
    recorded.resolve()
    await Promise.all(published)
    expect(publishBuckets).toEqual(['enam', 'weur'])
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
    for (const bucket of ['weur', 'apac', 'eeur'] as const)
      authorityState.setPresence({ key: 'room:test', kind: 'text', member: `telefunc-shard-${bucket}-0`, bucket })
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

  it("forwards presence from a region that left the scale through the fallback region's coordinator", async () => {
    const authorityState = createAuthorityState()
    const forwards: Array<{ coordinator: string; members: string[] }> = []
    const broadcast = new CloudflareBroadcast({
      baseInstanceName: 'telefunc',
      scale: { weur: 1 },
      locationFallback: 'weur',
      namespace: () =>
        createBasicBinding({
          onForward(id, request) {
            forwards.push({ coordinator: id.name, members: [...request.members].sort() })
            return Promise.resolve()
          },
        }),
    })
    for (const [member, bucket] of [
      ['telefunc-shard-weur-0', 'weur'],
      ['telefunc-shard-apac-0', 'apac'],
    ] as const)
      authorityState.setPresence({ key: 'room:redeployed', kind: 'text', member, bucket })
    const receipt = await broadcast.publishToSubscribers(authorityState, new OrderedStubs(), {
      key: 'room:redeployed',
      kind: 'text',
      payload: '"hello"',
    })
    expect(forwards).toEqual([
      { coordinator: 'telefunc:broadcast:weur:0', members: ['telefunc-shard-apac-0', 'telefunc-shard-weur-0'] },
    ])
    expect(receipt.receivers).toBe(2)
  })

  it('a member that fails to take a publish loses it: the publish resolves and the loss is logged', async () => {
    const authority = createAuthorityState()
    const calls: BroadcastCalls = new OrderedStubs()
    const coordinatorCalls: BroadcastCalls = new OrderedStubs()
    const broadcast: CloudflareBroadcast = createBroadcast(
      createBasicBinding({
        onPresence: presenceAt(authority),
        onPublish: (_id, request) => broadcast.publishToSubscribers(authority, calls, request),
        onForward: (_id, request) => broadcast.forwardToBucket(coordinatorCalls, request),
        onDeliver: () => Promise.reject(new Error('member reset')),
      }),
    )
    installCloudflareBroadcast(broadcast)
    const report = vi.spyOn(console, 'error').mockImplementation(() => {})
    await inSession(createMember(broadcast), async () => {
      const room = new ServerBroadcast<string>({ key: 'room:test' })
      room.subscribe(() => {})
      await expect(room.publish('lost')).resolves.toMatchObject({ receivers: 1 })
    })
    expect(report).toHaveBeenCalledWith(expect.stringContaining("delivery of 'room:test' lost to 1/1"))
  })

  it('a forward delivers wide ordering positions to every named DO', async () => {
    const deliveredTo: string[] = []
    const received: Array<{ text: BackendPayload; seq: number; timestamp: number }> = []
    const broadcast: CloudflareBroadcast = createBroadcast(
      createBasicBinding({
        onDeliver(id, request) {
          deliveredTo.push(id.name)
          return member.deliver(request)
        },
      }),
    )
    const member = createMember(broadcast)
    const subscription = member.openSubscription({ key: 'room:test', kind: 'text' }, (payload, info) => {
      received.push({ text: payload, ...info })
    })
    await untilReady(subscription)
    await broadcast.forwardToBucket(new OrderedStubs(), {
      key: 'room:test',
      kind: 'text',
      payload: '{"text":"hello"}',
      info: { seq: 0x1_0000_0000, timestamp: 0x1_0000_0001 },
      members: ['member-weur-0', 'member-weur-1'],
    })
    expect(deliveredTo.sort()).toEqual(['member-weur-0', 'member-weur-1'])
    expect(received).toEqual([
      { text: '{"text":"hello"}', seq: 0x1_0000_0000, timestamp: 0x1_0000_0001 },
      { text: '{"text":"hello"}', seq: 0x1_0000_0000, timestamp: 0x1_0000_0001 },
    ])
    await subscription.unsubscribe()
  })

  it('delivers a key’s publishes to each member in seq order while calls through different stubs race', async () => {
    const authority = createAuthorityState()
    const calls: BroadcastCalls = new OrderedStubs()
    const coordinatorCalls: BroadcastCalls = new OrderedStubs()
    // The first stub opened is the slowest, so a publish sent through a fresh stub overtakes the one before it.
    const broadcast: CloudflareBroadcast = createBroadcast(
      createRacingBinding([20], {
        onForward: (request) => broadcast.forwardToBucket(coordinatorCalls, request),
        onDeliver: async (request) => member.deliver(request),
        onPresence: async (request) => authority.setPresence(request),
      }),
    )
    const member = createMember(broadcast)
    const received: number[] = []
    const route = { key: 'room:order', kind: 'text' } as const
    const subscription = member.openSubscription(route, (_payload, info) => void received.push(info.seq))
    await untilReady(subscription)
    const publish = () => broadcast.publishToSubscribers(authority, calls, { ...route, payload: '"x"' })
    await Promise.all([publish(), publish(), publish()])
    expect(received).toEqual([1, 2, 3])
    await subscription.unsubscribe()
  })

  it("publishes a caller's buffer as it was at the call, even on a line held behind a failed call", async () => {
    const replies: Array<PromiseWithResolvers<{ seq: number; timestamp: number }>> = []
    const published: number[][] = []
    const broadcast = createBroadcast(
      createBasicBinding({
        onPublish: (_id, request) => {
          // An RPC serializes its arguments when it is made.
          published.push(Array.from(request.payload as Uint8Array))
          const reply = Promise.withResolvers<{ seq: number; timestamp: number }>()
          replies.push(reply)
          return reply.promise
        },
      }),
    )
    installCloudflareBroadcast(broadcast)
    const scratch = Buffer.from([1])
    await inSession(createMember(broadcast), async () => {
      const channel = new ServerBroadcast({ key: 'room:reused-buffer' })
      const failing = channel.publishBinary(scratch).catch(() => {})
      const inFlight = channel.publishBinary(scratch)
      replies[0]!.reject(new Error('transport error'))
      await failing
      // The next call waits for the failed stub's calls; the caller reuses its buffer meanwhile.
      scratch[0] = 3
      const later = channel.publishBinary(scratch)
      scratch[0] = 9
      replies[1]!.resolve({ seq: 2, timestamp: 1 })
      await inFlight
      await vi.waitFor(() => expect(replies).toHaveLength(3))
      replies[2]!.resolve({ seq: 3, timestamp: 1 })
      await later
    })
    expect(published).toEqual([[1], [1], [3]])
  })

  it('publishes from outside a session, as from a cron trigger, to the key’s authority', async () => {
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
    for (const bucket of ['weur', 'apac'] as const)
      authorityState.setPresence({ key: 'room:test', kind: 'text', member: `telefunc-shard-${bucket}-0`, bucket })
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
  })

  it('withdraws presence at the authority on unsubscribe', async () => {
    const authority = createAuthorityState()
    const broadcast = createBroadcast(createBasicBinding({ onPresence: presenceAt(authority) }))
    const member = createMember(broadcast)
    const route = { key: 'room:test', kind: 'text' } as const
    const subscription = member.openSubscription(route, () => {})
    await untilReady(subscription)
    expect(liveMembers(authority, route)).toEqual({ weur: ['member-weur-0'] })
    await subscription.unsubscribe()
    expect(liveMembers(authority, route)).toEqual({})
  })

  it('keeps presence generation-safe across setup and withdrawal churn', async () => {
    const setup = Promise.withResolvers<void>()
    const hooks: PresenceHooks = { beforeRecord: () => setup.promise }
    const authority = createAuthorityState()
    const broadcast = createBroadcast(createBasicBinding({ onPresence: presenceAt(authority, hooks) }))
    const member = createMember(broadcast)
    const route = { key: 'room:presence-churn', kind: 'text' } as const
    const first = member.openSubscription(route, () => {})
    await first.unsubscribe()
    const successor = member.openSubscription(route, () => {})
    setup.resolve()
    await untilReady(successor)
    await flushMicrotasks()
    expect(liveMembers(authority, route)).toEqual({ weur: ['member-weur-0'] })
    const releaseWithdrawal = Promise.withResolvers<void>()
    hooks.beforeWithdraw = () => releaseWithdrawal.promise
    const teardown = successor.unsubscribe()
    const replacement = member.openSubscription(route, () => {})
    await flushMicrotasks()
    expect(replacement.state()).toBe('establishing')
    releaseWithdrawal.resolve()
    await Promise.all([teardown, untilReady(replacement)])
    expect(liveMembers(authority, route)).toEqual({ weur: ['member-weur-0'] })
    await replacement.unsubscribe()
  })

  it('a subscription opened during a deferred presence teardown establishes fresh presence', async () => {
    const setup = Promise.withResolvers<void>()
    const withdrawing = Promise.withResolvers<void>()
    const withdrawal = Promise.withResolvers<void>()
    const hooks: PresenceHooks = { beforeRecord: () => setup.promise }
    const authority = createAuthorityState()
    const broadcast = createBroadcast(createBasicBinding({ onPresence: presenceAt(authority, hooks) }))
    const member = createMember(broadcast)
    const route = { key: 'room:deferred-teardown', kind: 'text' } as const
    await member.openSubscription(route, () => {}).unsubscribe()
    hooks.beforeWithdraw = () => {
      withdrawing.resolve()
      return withdrawal.promise
    }
    setup.resolve()
    await withdrawing.promise
    const replacement = member.openSubscription(route, () => {})
    withdrawal.resolve()
    await untilReady(replacement)
    await flushMicrotasks()
    expect(liveMembers(authority, route)).toEqual({ weur: ['member-weur-0'] })
    await replacement.unsubscribe()
  })

  it('surfaces presence refresh loss and recovery through subscription state, and reports the loss once', async () => {
    vi.useFakeTimers()
    const report = vi.spyOn(console, 'error').mockImplementation(() => {})
    let presenceCalls = 0
    const broadcast = createBroadcast(
      createBasicBinding({
        onPresence: () => {
          presenceCalls += 1
          return presenceCalls === 2 ? Promise.reject(new Error('presence refresh rejected')) : Promise.resolve()
        },
      }),
    )
    const member = createMember(broadcast)
    const subscription = member.openSubscription({ key: 'room:refresh', kind: 'text' }, () => {})
    await untilReady(subscription)
    const states: string[] = []
    const stopObserving = subscription.onStateChange((state) => states.push(state))
    try {
      await vi.advanceTimersByTimeAsync(30_000)
      expect(subscription.state()).toBe('lost')
      expect(states).toEqual(['lost'])
      await vi.advanceTimersByTimeAsync(30_000)
      expect(subscription.state()).toBe('ready')
      expect(states).toEqual(['lost', 'ready'])
      expect(report).toHaveBeenCalledOnce()
      expect(report.mock.calls[0]![0]).toMatchObject({ cause: { message: 'presence refresh rejected' } })
    } finally {
      stopObserving()
      await subscription.unsubscribe()
      vi.useRealTimers()
    }
  })

  it('keeps delivering while a failed refresh has the route lost, as the authority still forwards to it', async () => {
    vi.useFakeTimers()
    const authority = createAuthorityState()
    const calls: BroadcastCalls = new OrderedStubs()
    const coordinatorCalls: BroadcastCalls = new OrderedStubs()
    const record = presenceAt(authority)
    let presenceCalls = 0
    const broadcast = createBroadcast(
      createBasicBinding({
        onPresence: (id, request) =>
          ++presenceCalls === 2 ? Promise.reject(new Error('presence refresh rejected')) : record(id, request),
        onForward: (_, request) => broadcast.forwardToBucket(coordinatorCalls, request),
        onDeliver: (_, request) => member.deliver(request),
      }),
    )
    const member = createMember(broadcast)
    const route = { key: 'room:lost-delivery', kind: 'text' } as const
    const received: BackendPayload[] = []
    const subscription = member.openSubscription(route, (payload) => void received.push(payload))
    try {
      await untilReady(subscription)
      await vi.advanceTimersByTimeAsync(30_000)
      expect(subscription.state()).toBe('lost')
      await broadcast.publishToSubscribers(authority, calls, {
        ...route,
        payload: '"during"',
      })
      expect(received).toEqual(['"during"'])
    } finally {
      await subscription.unsubscribe()
      vi.useRealTimers()
    }
  })
})
